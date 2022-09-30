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

use engula_client::{AppError, ClientOptions, EngulaClient, Partition};

#[tokio::main]
async fn main() -> Result<(), AppError> {
    tracing_subscriber::fmt::init();

    let addrs = vec!["127.0.0.1:21805".to_owned()];
    let client = EngulaClient::new(ClientOptions::default(), addrs).await?;
    let db = client.create_database("test_db".to_string()).await?;
    let co = db
        .create_collection("test_co".to_string(), Some(Partition::Hash { slots: 30 }))
        .await?;

    for i in 1..300000 {
        let k = format!("book_name{i}").as_bytes().to_vec();
        let v = "rust_in_actions".as_bytes().to_vec();
        co.put(k.clone(), v).await?;
    }
    Ok(())
}
