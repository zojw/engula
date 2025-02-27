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

use std::{
    cmp::Ordering,
    collections::{BinaryHeap, HashMap, HashSet},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
    time::{Duration, Instant},
};

use futures::Future;

use super::{
    event_source::EventSource,
    task::{Task, TaskState},
    tasks::{GroupLockTable, GENERATED_TASK_ID},
    ScheduleStateObserver,
};
use crate::{
    node::{replica::ReplicaConfig, Replica},
    Provider,
};

#[derive(Clone)]
pub struct EventWaker {
    state: Arc<Mutex<WakerState>>,
}

#[derive(Default, Clone)]
struct EventWaiter {
    state: Arc<Mutex<WakerState>>,
}

#[derive(Default)]
struct WakerState {
    waked: bool,
    inner_waker: Option<std::task::Waker>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct Deadline {
    deadline: Instant,
    timer_id: u64,
    task_id: u64,
}

#[derive(Default)]
struct TaskTimer {
    next_timer_id: u64,
    timer_heap: BinaryHeap<Deadline>,
    timer_indexes: HashMap</* task_id */ u64, /* timer_id */ u64>,
}

pub struct ScheduleContext<'a> {
    pub group_id: u64,
    pub replica_id: u64,
    pub current_term: u64,
    pub execution_time: Duration,
    pub replica: Arc<Replica>,
    pub(crate) provider: Arc<Provider>,
    pub cfg: &'a ReplicaConfig,
    pub group_lock_table: &'a mut GroupLockTable,
    next_task_id: &'a mut u64,
    pending_tasks: &'a mut Vec<Box<dyn Task>>,
}

pub struct Scheduler
where
    Self: Send,
{
    group_id: u64,
    replica_id: u64,
    cfg: ReplicaConfig,
    replica: Arc<Replica>,
    provider: Arc<Provider>,
    schedule_state_observer: Arc<dyn ScheduleStateObserver>,

    event_waiter: EventWaiter,
    event_sources: Vec<Arc<dyn EventSource>>,
    incoming_tasks: Vec<u64>,
    next_task_id: u64,
    jobs: HashMap<u64, Job>,
    timer: TaskTimer,
    group_lock_table: GroupLockTable,
}

struct Job {
    _start_at: Instant,
    advanced_at: Instant,

    task: Box<dyn Task>,
}

impl Scheduler {
    pub(crate) fn new(
        cfg: ReplicaConfig,
        replica: Arc<Replica>,
        provider: Arc<Provider>,
        event_sources: Vec<Arc<dyn EventSource>>,
        schedule_state_observer: Arc<dyn ScheduleStateObserver>,
    ) -> Self {
        let info = replica.replica_info();
        let group_id = info.group_id;
        let replica_id = info.replica_id;
        let event_waiter = EventWaiter::new();
        for source in &event_sources {
            source.bind(event_waiter.waker());
        }
        Scheduler {
            group_id,
            replica_id,
            replica,
            provider,
            schedule_state_observer,
            cfg,

            event_sources,
            event_waiter,
            incoming_tasks: vec![],
            next_task_id: GENERATED_TASK_ID,
            jobs: HashMap::default(),
            timer: TaskTimer::new(),
            group_lock_table: GroupLockTable::new(group_id),
        }
    }

    #[inline]
    async fn wait_new_events(&mut self) {
        if self.incoming_tasks.is_empty() {
            let waiter = self.event_waiter.clone();
            self.timer.timeout(waiter).await;
        }
    }

    pub async fn advance(&mut self, current_term: u64) {
        self.wait_new_events().await;

        let mut pending_tasks: Vec<Box<dyn Task>> = vec![];
        for task_id in self.collect_active_tasks() {
            self.advance_task(current_term, task_id, &mut pending_tasks)
                .await;
            crate::runtime::yield_now().await;
        }

        self.install_tasks(std::mem::take(&mut pending_tasks));
    }

    async fn advance_task(
        &mut self,
        current_term: u64,
        task_id: u64,
        pending_tasks: &mut Vec<Box<dyn Task>>,
    ) {
        if let Some(job) = self.jobs.get_mut(&task_id) {
            loop {
                let mut ctx = ScheduleContext {
                    cfg: &self.cfg,
                    group_id: self.group_id,
                    replica_id: self.replica_id,
                    replica: self.replica.clone(),
                    provider: self.provider.clone(),
                    current_term,
                    execution_time: Instant::now().duration_since(job.advanced_at),
                    next_task_id: &mut self.next_task_id,
                    pending_tasks,
                    group_lock_table: &mut self.group_lock_table,
                };
                match job.task.poll(&mut ctx).await {
                    TaskState::Pending(interval) => {
                        if let Some(interval) = interval {
                            self.timer.add(task_id, Instant::now() + interval);
                        }
                        break;
                    }
                    TaskState::Advanced => {
                        job.advanced_at = Instant::now();
                    }
                    TaskState::Terminated => {
                        self.jobs.remove(&task_id);
                        break;
                    }
                }
            }

            if let Some(schedule_state) = self.group_lock_table.take_updated_states() {
                self.schedule_state_observer
                    .on_schedule_state_updated(schedule_state);
            }
        }
    }

    pub fn install_tasks(&mut self, tasks: Vec<Box<dyn Task>>) {
        let now = Instant::now();
        for task in tasks {
            let task_id = task.id();
            self.incoming_tasks.push(task_id);
            self.jobs.insert(
                task_id,
                Job {
                    _start_at: now,
                    advanced_at: now,
                    task,
                },
            );
        }
    }

    fn collect_active_tasks(&mut self) -> HashSet<u64> {
        let mut active_tasks = self.timer.take_fired_tasks();
        for source in &mut self.event_sources {
            active_tasks.extend(source.active_tasks().iter());
        }
        active_tasks.extend(std::mem::take(&mut self.incoming_tasks).iter());
        active_tasks
    }
}

impl<'a> ScheduleContext<'a> {
    #[inline]
    pub fn delegate(&mut self, task: Box<dyn Task>) {
        self.pending_tasks.push(task);
    }

    #[inline]
    pub fn next_task_id(&mut self) -> u64 {
        let task_id = *self.next_task_id;
        *self.next_task_id += 1;
        task_id
    }
}

impl EventWaker {
    pub fn wake(&self) {
        let mut state = self.state.lock().expect("poisoned");
        state.waked = true;
        if let Some(waker) = state.inner_waker.take() {
            waker.wake();
        }
    }
}

impl EventWaiter {
    fn new() -> Self {
        EventWaiter::default()
    }

    fn waker(&self) -> EventWaker {
        EventWaker {
            state: self.state.clone(),
        }
    }
}

impl std::future::Future for EventWaiter {
    type Output = ();

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let mut state = this.state.lock().expect("poisoned");
        if state.waked {
            state.waked = false;
            state.inner_waker = None;
            Poll::Ready(())
        } else {
            state.inner_waker = Some(cx.waker().clone());
            Poll::Pending
        }
    }
}

impl Ord for Deadline {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .deadline
            .cmp(&self.deadline)
            .then_with(|| other.timer_id.cmp(&self.timer_id))
    }
}

impl PartialOrd for Deadline {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl TaskTimer {
    fn new() -> Self {
        TaskTimer::default()
    }

    fn add(&mut self, task_id: u64, deadline: Instant) {
        let timer_id = self.next_timer_id;
        self.next_timer_id += 1;
        self.timer_indexes.insert(task_id, timer_id);
        self.timer_heap.push(Deadline {
            deadline,
            timer_id,
            task_id,
        });
    }

    async fn timeout<T: Future<Output = ()>>(&self, f: T) {
        if let Some(event) = self.timer_heap.peek() {
            let _ = tokio::time::timeout_at(event.deadline.into(), f).await;
        } else {
            f.await;
        }
    }

    fn take_fired_tasks(&mut self) -> HashSet<u64> {
        let now = Instant::now();
        let mut result = HashSet::default();
        while let Some(Deadline {
            deadline,
            timer_id,
            task_id,
        }) = self.timer_heap.peek()
        {
            if *deadline > now {
                break;
            }
            if self
                .timer_indexes
                .get(task_id)
                .map(|t| *t == *timer_id)
                .unwrap_or_default()
            {
                self.timer_indexes.remove(task_id);
                result.insert(*task_id);
            }
            self.timer_heap.pop();
        }
        result
    }
}
