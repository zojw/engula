// Copyright 2022 The Engula Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
use std::{sync::mpsc, time::Duration};

use clap::Parser;
use engula_client::{AppError, ClientOptions, Collection, Database, EngulaClient, Partition};
use engula_server::runtime::{sync::WaitGroup, Shutdown, ShutdownNotifier};
use rand::{rngs::OsRng, RngCore};
use tokio::{runtime::Runtime, select, time::MissedTickBehavior};
use tracing::{debug, info};

use super::{config::*, report, report::ReportContext, worker::*};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

struct Context {
    wait_group: WaitGroup,
    shutdown: Shutdown,
    runtime: Runtime,
}

#[derive(Parser)]
#[clap(name = "engula", version, author, about)]
pub struct Command {
    #[clap(long)]
    conf: Option<String>,
}

impl Command {
    pub fn run(self) {
        let cfg = load_config(self).unwrap();

        info!("config {:#?}", cfg);

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .worker_threads(cfg.num_threads)
            .build()
            .unwrap();

        let co = runtime
            .block_on(async { open_collection(&cfg).await })
            .expect("open collection");
        let notifier = ShutdownNotifier::default();
        let ctx = Context {
            wait_group: WaitGroup::new(),
            shutdown: notifier.subscribe(),
            runtime,
        };

        let (send, recv) = mpsc::channel();
        let handle = ctx.runtime.spawn(async move {
            notifier.ctrl_c().await;
            info!("receive CTRL-C, exit");
            send.send(()).unwrap_or_default();
        });

        let base_seed = cfg.seed.unwrap_or_else(|| OsRng.next_u64());

        info!(
            "spawn {} workers with base seed {base_seed}",
            cfg.worker.num_worker
        );
        let report_waiter = spawn_reporter(&ctx, cfg.clone());

        let num_op = cfg.operation / cfg.worker.num_worker;
        for i in 0..cfg.worker.num_worker {
            let seed = base_seed + i as u64;
            spawn_worker(&ctx, cfg.clone(), i, seed, num_op, co.clone());
            if let Some(interval) = cfg.worker.start_intervals {
                if recv.recv_timeout(interval).is_ok() {
                    break;
                }
            }
        }

        info!("all workers are spawned, wait ...");

        let wait_group = ctx.wait_group;
        ctx.runtime.block_on(async move {
            wait_group.wait().await;
            handle.abort();
            report_waiter.wait().await;
        });
    }
}

fn spawn_worker(ctx: &Context, cfg: AppConfig, i: usize, seed: u64, num_op: usize, co: Collection) {
    debug!("spawn worker {i} with seed {seed}");

    let job = Job::new(co, seed, num_op, cfg);
    let shutdown = ctx.shutdown.clone();
    let wait_group = ctx.wait_group.clone();
    ctx.runtime.spawn(async move {
        select! {
            _ = shutdown => {
                debug!("worker {i} receives shutdown signal");
            },
            _ = worker_main(i, job) => {
                debug!("worker {i} finish all operations");
            }
        }
        drop(wait_group);
    });
}

async fn create_or_open_database(client: &EngulaClient, database: &str) -> Result<Database> {
    match client.create_database(database.to_owned()).await {
        Ok(db) => Ok(db),
        Err(AppError::AlreadyExists(_)) => Ok(client.open_database(database.to_owned()).await?),
        Err(e) => Err(e.into()),
    }
}

async fn create_or_open_collection(
    db: &Database,
    collection: &str,
    num_shards: u32,
) -> Result<Collection> {
    let partition = Partition::Hash { slots: num_shards };
    match db
        .create_collection(collection.to_owned(), Some(partition))
        .await
    {
        Ok(co) => Ok(co),
        Err(AppError::AlreadyExists(_)) => Ok(db.open_collection(collection.to_owned()).await?),
        Err(e) => Err(e.into()),
    }
}

async fn open_collection(cfg: &AppConfig) -> Result<Collection> {
    let opts = ClientOptions {
        connect_timeout: Some(Duration::from_millis(200)),
        timeout: Some(Duration::from_millis(500)),
    };
    let client = EngulaClient::new(opts, cfg.addrs.clone()).await?;
    let database = match client.open_database(cfg.database.clone()).await {
        Ok(db) => db,
        Err(AppError::NotFound(_)) if cfg.create_if_missing => {
            create_or_open_database(&client, &cfg.database).await?
        }
        Err(e) => {
            return Err(e.into());
        }
    };

    let co = match database.open_collection(cfg.database.clone()).await {
        Ok(co) => co,
        Err(AppError::NotFound(_)) if cfg.create_if_missing => {
            create_or_open_collection(&database, &cfg.collection, cfg.num_shards).await?
        }
        Err(e) => {
            return Err(e.into());
        }
    };
    Ok(co)
}

fn spawn_reporter(ctx: &Context, cfg: AppConfig) -> WaitGroup {
    let shutdown = ctx.shutdown.clone();
    let wait_group = WaitGroup::new();
    let cloned_wait_group = wait_group.clone();
    ctx.runtime.spawn(async move {
        let mut ctx = ReportContext::default();
        select! {
            _ = shutdown => {},
            _ = reporter_main(cfg, &mut ctx) => {},
        }
        report::display(&mut ctx);
        drop(wait_group);
    });
    cloned_wait_group
}

async fn reporter_main(cfg: AppConfig, ctx: &mut ReportContext) {
    let mut interval = tokio::time::interval(cfg.report_interval);
    interval.set_missed_tick_behavior(MissedTickBehavior::Delay);

    // The first tick completes immediately
    interval.tick().await;
    loop {
        interval.tick().await;
        report::display(ctx);
    }
}

fn load_config(cmd: Command) -> Result<AppConfig> {
    use ::config::{Config, Environment, File};

    let mut builder = Config::builder()
        .add_source(Config::try_from(&AppConfig::default()).unwrap())
        .set_default("addrs", vec!["127.0.0.1:21805"])?;
    if let Some(conf) = cmd.conf {
        builder = builder.add_source(File::with_name(&conf));
    }
    let cfg = builder
        .add_source(
            Environment::with_prefix("EB")
                .try_parsing(true)
                .separator("_")
                .list_separator(" "),
        )
        .build()?;

    Ok(cfg.try_deserialize()?)
}
