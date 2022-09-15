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

use std::collections::HashSet;

use tracing::info;

use super::scheduler::EventWaker;

#[derive(Default)]
pub struct CommonEventSource {
    waker: Option<EventWaker>,
    active_tasks: HashSet<u64>,
    subscribe_tasks: HashSet<u64>,
}

/// An abstracted trait express an event source which is watched by some tasks.
pub trait EventSource: Send + Sync {
    /// Bind this event source to a scheduler waker.
    fn bind(&self, waker: EventWaker);

    /// Return the active tasks which fired by the new events.
    fn active_tasks(&self) -> HashSet<u64>;

    /// Watch this event source.
    fn watch(&self, task_id: u64);
}

impl CommonEventSource {
    pub fn new() -> Self {
        CommonEventSource::default()
    }

    #[inline]
    pub fn watch(&mut self, task_id: u64) {
        self.subscribe_tasks.insert(task_id);
    }

    #[inline]
    pub fn bind(&mut self, waker: EventWaker) {
        self.waker = Some(waker);
    }

    #[inline]
    pub fn fire(&mut self) {
        let move_tasks = std::mem::take(&mut self.subscribe_tasks);
        if !move_tasks.is_empty() {
            info!("fire move task: {:?}", &move_tasks);
        }
        self.active_tasks.extend(move_tasks.iter());
        if let Some(waker) = self.waker.take() {
            waker.wake();
            self.waker = Some(waker);
            info!("fire wake up...")
        } else {
            info!("fire wake up nothing....")
        }
    }

    #[inline]
    pub fn active_tasks(&mut self) -> HashSet<u64> {
        let active_migrate = std::mem::take(&mut self.active_tasks);
        info!("active migrate: {:?}", active_migrate);
        active_migrate
    }
}
