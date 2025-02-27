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

use std::time::Duration;

use engula_api::server::v1::{
    report_request::GroupUpdates, GroupDesc, ReplicaState, ReportRequest, ScheduleState,
};
use engula_client::RootClient;
use futures::{channel::mpsc, StreamExt};
use tracing::warn;

use crate::{node::metrics::take_report_metrics, record_latency, runtime::TaskPriority, Provider};

#[derive(Clone)]
pub struct StateChannel {
    sender: mpsc::UnboundedSender<GroupUpdates>,
}

pub(crate) fn setup(provider: &Provider) -> StateChannel {
    let (sender, receiver) = mpsc::unbounded();

    let client = provider.root_client.clone();
    crate::runtime::current().spawn(None, TaskPriority::IoHigh, async move {
        report_state_worker(receiver, client).await;
    });

    StateChannel::new(sender)
}

async fn report_state_worker(
    mut receiver: mpsc::UnboundedReceiver<GroupUpdates>,
    root_client: RootClient,
) {
    while let Some(updates) = wait_state_updates(&mut receiver).await {
        let req = ReportRequest { updates };
        record_latency!(take_report_metrics());
        report_state_updates(&root_client, req).await;
    }
}

/// Wait until at least a new request is received or the channel is closed. Returns `None` if the
/// channel is closed.
async fn wait_state_updates(
    receiver: &mut mpsc::UnboundedReceiver<GroupUpdates>,
) -> Option<Vec<GroupUpdates>> {
    use prost::Message;

    // TODO(walter) skip root group?
    if let Some(update) = receiver.next().await {
        let mut size = update.encoded_len();
        let mut updates = vec![update];
        while size < 32 * 1024 {
            match receiver.try_next() {
                Ok(Some(update)) => {
                    size += update.encoded_len();
                    updates.push(update);
                }
                _ => break,
            }
        }
        return Some(updates);
    }
    None
}

/// Issue report rpc request to root server.
///
/// This function is executed synchronously, and it will not affect normal reporting, because
/// a node has only a small number of replicas, and the replica state changes are not frequent.
/// Using a synchronous method can simplify the sequence problem introduced by asynchronous
/// reporting.
///
/// If one day you find that reporting has become a bottleneck, you can consider optimizing this
/// code.
async fn report_state_updates(root_client: &RootClient, request: ReportRequest) {
    let mut interval = 1;
    while let Err(e) = root_client.report(&request).await {
        warn!("report state updates: {e}");
        crate::runtime::time::sleep(Duration::from_millis(interval)).await;
        interval = std::cmp::min(interval * 2, 120);
    }
}

impl StateChannel {
    pub fn new(sender: mpsc::UnboundedSender<GroupUpdates>) -> Self {
        StateChannel { sender }
    }

    #[inline]
    pub fn broadcast_replica_state(&mut self, group_id: u64, replica_state: ReplicaState) {
        let update = GroupUpdates {
            group_id,
            replica_state: Some(replica_state),
            ..Default::default()
        };
        self.sender.start_send(update).unwrap_or_default();
    }

    #[inline]
    pub fn broadcast_group_descriptor(&mut self, group_id: u64, group_desc: GroupDesc) {
        let update = GroupUpdates {
            group_id,
            group_desc: Some(group_desc),
            ..Default::default()
        };
        self.sender.start_send(update).unwrap_or_default();
    }

    #[inline]
    pub fn broadcast_schedule_state(&self, group_id: u64, schedule_state: ScheduleState) {
        let update = GroupUpdates {
            group_id,
            schedule_state: Some(schedule_state),
            ..Default::default()
        };
        self.sender.clone().start_send(update).unwrap_or_default();
    }
}
