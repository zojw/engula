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

use std::{cmp::Ordering, collections::HashMap, sync::Arc, time::Duration};

use engula_api::server::v1::{GroupDesc, NodeDesc, ReplicaDesc, ReplicaRole};
use serde::{Deserialize, Serialize};

use self::{
    policy_leader_cnt::LeaderCountPolicy, policy_replica_cnt::ReplicaCountPolicy,
    policy_shard_cnt::ShardCountPolicy, source::NodeFilter,
};
use super::{metrics, RootShared};
use crate::{bootstrap::REPLICA_PER_GROUP, Result};

#[cfg(test)]
mod sim_test;

mod policy_leader_cnt;
mod policy_replica_cnt;
mod policy_shard_cnt;
mod source;

pub use source::{AllocSource, SysAllocSource};

#[derive(Clone, Debug)]
pub enum ReplicaRoleAction {
    Replica(ReplicaAction),
    Leader(LeaderAction),
}

#[derive(Clone, Debug)]
pub enum GroupAction {
    Noop,
    Add(usize),
    Remove(Vec<u64>),
}

#[derive(Clone, Debug)]
pub enum ReplicaAction {
    Migrate(ReallocateReplica),
}

#[derive(Clone, Debug)]
pub enum ShardAction {
    Migrate(ReallocateShard),
}

#[derive(Clone, Debug)]
pub enum LeaderAction {
    Noop,
    Shed(TransferLeader),
}

#[derive(Debug, Clone)]
pub struct TransferLeader {
    pub group: u64,
    pub src_node: u64,
    pub src_replica: u64,
    pub target_node: u64,
    pub target_replica: u64,
}

#[derive(Clone, Debug)]
pub struct ReallocateReplica {
    pub group: u64,
    pub source_node: u64,
    pub source_replica: u64,
    pub target_node: NodeDesc,
}

#[derive(Clone, Debug)]
pub struct ReallocateShard {
    pub shard: u64,
    pub source_group: u64,
    pub target_group: u64,
}

#[derive(PartialEq, Eq, Debug)]
enum BalanceStatus {
    Overfull,
    Balanced,
    Underfull,
}

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct RootConfig {
    pub replicas_per_group: usize,
    pub enable_group_balance: bool,
    pub enable_replica_balance: bool,
    pub enable_shard_balance: bool,
    pub enable_leader_balance: bool,
    pub liveness_threshold_sec: u64,
    pub heartbeat_timeout_sec: u64,
    pub schedule_interval_sec: u64,
    pub max_create_group_retry_before_rollback: u64,
}

impl Default for RootConfig {
    fn default() -> Self {
        Self {
            replicas_per_group: REPLICA_PER_GROUP,
            enable_group_balance: true,
            enable_replica_balance: true,
            enable_shard_balance: true,
            enable_leader_balance: true,
            liveness_threshold_sec: 30,
            heartbeat_timeout_sec: 4,
            schedule_interval_sec: 1,
            max_create_group_retry_before_rollback: 10,
        }
    }
}

impl RootConfig {
    pub fn heartbeat_interval(&self) -> Duration {
        Duration::from_secs(self.liveness_threshold_sec - self.heartbeat_timeout_sec)
    }
}

pub enum GroupReplicaAction {
    LeaveJoint,
    LeaveLeaner,
}

#[derive(Default, Clone)]
struct NodeCandidate {
    node: NodeDesc,
    disk_full: bool,
    balance_score: f64,
    replica_count: u64,
    converges_score: f64,
}

impl NodeCandidate {
    fn worse(&self, o: &NodeCandidate) -> bool {
        score_compare_candidate(self, o) < 0.0
    }
}

fn score_compare_candidate(c: &NodeCandidate, o: &NodeCandidate) -> f64 {
    // the greater value, the more suitable for `c` than `o` to be a memeber of current node.
    if o.disk_full && !c.disk_full {
        return 3.0;
    }
    if c.disk_full && !o.disk_full {
        return -3.0;
    }
    if c.converges_score != o.converges_score {
        if c.converges_score > o.converges_score {
            return 2.0 + ((c.converges_score - o.converges_score) / 10.0);
        }
        return -(2.0 + ((o.converges_score - c.converges_score) / 10.0));
    }
    if c.balance_score != o.balance_score {
        if c.balance_score > o.balance_score {
            return 1.0 + ((c.balance_score - o.balance_score) / 10.0);
        }
        return -(1.0 + ((o.balance_score - c.balance_score) / 10.0));
    }
    if c.replica_count == o.replica_count {
        return 0.0;
    }
    if c.replica_count < o.replica_count {
        return (c.replica_count - o.replica_count) as f64 / (c.replica_count as f64);
    }
    -((o.replica_count - c.replica_count) as f64 / o.replica_count as f64)
}

impl Ord for NodeCandidate {
    fn cmp(&self, other: &Self) -> Ordering {
        let score_cmp = score_compare_candidate(self, other);
        if score_cmp < 0.0 {
            return Ordering::Less;
        }
        if score_cmp > 0.0 {
            return Ordering::Greater;
        }
        Ordering::Equal
    }
}

impl PartialOrd for NodeCandidate {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for NodeCandidate {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for NodeCandidate {}

struct PotentialReplacement {
    existing: NodeDesc,
    candidates: Vec<NodeCandidate>,
}

struct BalanceOption {
    existing: NodeCandidate,
    candidates: Vec<NodeCandidate>,
}

#[derive(Clone)]
pub struct Allocator<T: AllocSource> {
    alloc_source: Arc<T>,
    config: RootConfig,
}

impl<T: AllocSource> Allocator<T> {
    pub fn new(alloc_source: Arc<T>, config: RootConfig) -> Self {
        Self {
            alloc_source,
            config,
        }
    }

    pub fn replicas_per_group(&self) -> usize {
        self.config.replicas_per_group
    }

    pub async fn compute_groups(&self) -> Result<Vec<(u64, GroupReplicaAction)>> {
        let mut gaction = Vec::new();
        let groups = self.alloc_source.groups();
        for (group_id, desc) in &groups {
            let in_joint = desc.replicas.iter().any(|r| {
                r.role == ReplicaRole::IncomingVoter as i32
                    || r.role == ReplicaRole::DemotingVoter as i32
            });
            if in_joint {
                gaction.push((group_id.to_owned(), GroupReplicaAction::LeaveJoint));
                continue;
            }
            let in_learner = desc
                .replicas
                .iter()
                .any(|r| r.role == ReplicaRole::Learner as i32);
            if in_learner {
                gaction.push((group_id.to_owned(), GroupReplicaAction::LeaveLeaner));
                continue;
            }
            let balance_opts = self.try_balance(desc, true).await?;
            if balance_opts.is_none() {
                continue;
            }
        }
        Ok(gaction)
    }

    async fn best_balance_target(
        &self,
        opts: &mut [BalanceOption],
    ) -> (Option<NodeCandidate>, Option<NodeCandidate>) {
        let mut best_idx = None;
        let mut best_target = None;
        let mut best_existing = None;
        for (i, opt) in opts.iter().enumerate() {
            if opt.candidates.is_empty() {
                continue;
            }
            let target = opt.candidates.get(0).unwrap(); // TODO: ...
            let existing = opt.existing.to_owned();
            let better_target = self
                .better_target(
                    target,
                    &existing,
                    best_target.to_owned(),
                    best_existing.to_owned(),
                )
                .await;
            if better_target.node.id == target.node.id {
                best_target = Some(target.to_owned());
                best_existing = Some(existing.to_owned());
                best_idx = Some(i);
            }
        }
        if best_idx.is_none() {
            return (None, None);
        }
        let target = best_target.as_ref().unwrap().to_owned();
        let best_opt = opts.get_mut(best_idx.unwrap()).unwrap();
        best_opt.candidates.retain(|c| c.node.id == target.node.id);
        (Some(target), Some(best_opt.existing.to_owned()))
    }

    async fn better_target(
        &self,
        new_target: &NodeCandidate,
        new_existing: &NodeCandidate,
        old_target: Option<NodeCandidate>,
        old_existing: Option<NodeCandidate>,
    ) -> NodeCandidate {
        if old_target.is_none() {
            return new_target.to_owned();
        }
        let cmp_score1 = score_compare_candidate(new_target, new_existing);
        let cmp_score2 =
            score_compare_candidate(old_target.as_ref().unwrap(), old_existing.as_ref().unwrap());
        if f64::abs(cmp_score1 - cmp_score2) >= 1e-10 {
            if cmp_score1 > cmp_score2 {
                return new_target.to_owned();
            }
            if cmp_score1 < cmp_score2 {
                return old_target.as_ref().unwrap().to_owned();
            }
        }
        if new_target.worse(old_target.as_ref().unwrap()) {
            return old_target.as_ref().unwrap().to_owned();
        }
        new_target.to_owned()
    }

    async fn try_balance(
        &self,
        group: &GroupDesc,
        within_voter: bool,
    ) -> Result<Option<(u64, u64)>> {
        let nodes = self.alloc_source.nodes(NodeFilter::Schedulable);

        let mut other_replicas = Vec::new();
        let mut excluded_replicas = Vec::new();
        let voters = group
            .replicas
            .iter()
            .filter(|r| {
                if !nodes.iter().any(|n| n.id == r.node_id) {
                    return false;
                }
                let replica_state = self.alloc_source.replica_state(&r.id);
                if replica_state.is_none() {
                    return false;
                }
                if replica_state.as_ref().unwrap().role != ReplicaRole::Voter as i32 {
                    return false;
                }
                true
            })
            .collect::<Vec<_>>();
        let non_voters = group
            .replicas
            .iter()
            .filter(|r| {
                if !nodes.iter().any(|n| n.id == r.node_id) {
                    return false;
                }
                let replica_state = self.alloc_source.replica_state(&r.id);
                if replica_state.is_none() {
                    return false;
                }
                if replica_state.as_ref().unwrap().role == ReplicaRole::Voter as i32 {
                    return false;
                }
                true
            })
            .collect::<Vec<_>>();

        let replica_to_balance = if within_voter {
            other_replicas.extend_from_slice(&non_voters);
            voters.to_owned()
        } else {
            other_replicas.extend_from_slice(&voters);
            excluded_replicas.extend_from_slice(&voters);
            non_voters.to_owned()
        };

        let all_nodes = self.alloc_source.nodes(NodeFilter::Schedulable);
        let mut exist_candiates = HashMap::new();
        let mut require_transfer_from = false;
        for n in &all_nodes {
            if !replica_to_balance.iter().any(|r| r.node_id == n.id) {
                continue;
            }
            let disk_full = self.check_node_full(n);
            if disk_full {
                require_transfer_from = true;
            }
            exist_candiates.insert(
                n.id,
                NodeCandidate {
                    node: n.to_owned(),
                    disk_full,
                    ..Default::default()
                },
            );
        }

        let mut potential_replacements = Vec::new();
        let mut require_transfer_to = false;
        for exist_candidate in exist_candiates.values() {
            let mut replace_candidates = Vec::new();
            for n in &all_nodes {
                if n.id == exist_candidate.node.id {
                    continue;
                }
                if excluded_replicas.iter().any(|r| r.node_id == n.id) {
                    continue;
                }
                let disk_full = self.check_node_full(n);
                let cand = NodeCandidate {
                    node: n.to_owned(),
                    disk_full,
                    ..Default::default()
                };
                if !cand.worse(exist_candidate) {
                    if exist_candidate.worse(&cand) {
                        require_transfer_to = true;
                    }
                    replace_candidates.push(cand);
                }
            }
            if !replace_candidates.is_empty() {
                replace_candidates.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));
                potential_replacements.push(PotentialReplacement {
                    existing: exist_candidate.node.to_owned(),
                    candidates: replace_candidates,
                });
            }
        }

        let mut need_balance = require_transfer_from || require_transfer_to;
        if !need_balance {
            for rep in &potential_replacements {
                if !self.should_balance_by_replica_count(rep) {
                    need_balance = true;
                    break;
                }
            }
        }

        if !need_balance {
            return Ok(None);
        }

        let mut balance_opts = Vec::with_capacity(potential_replacements.len());
        for potential_replacement in potential_replacements.iter_mut() {
            let mut ex_cand = exist_candiates
                .get(&potential_replacement.existing.id)
                .unwrap()
                .to_owned();
            (ex_cand.balance_score, ex_cand.converges_score) = self.balance_score_by_replica_count(
                &potential_replacement.existing,
                &potential_replacement.candidates,
                true,
            );
            ex_cand.replica_count = ex_cand.node.capacity.as_ref().unwrap().replica_count;

            let mut candidates = Vec::new();
            for candidate_node in &potential_replacement.candidates {
                if exist_candiates.contains_key(&candidate_node.node.id) {
                    continue;
                }
                let mut c = candidate_node.to_owned();
                (c.balance_score, c.converges_score) = self.balance_score_by_replica_count(
                    &c.node,
                    &potential_replacement.candidates,
                    false,
                );
                c.replica_count = candidate_node.node.capacity.as_ref().unwrap().replica_count;
                candidates.push(c);
            }

            if candidates.is_empty() {
                continue;
            }

            candidates.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));

            let candidates = candidates
                .iter()
                .take_while(|c| !c.worse(&ex_cand))
                .cloned()
                .collect::<Vec<_>>();
            if candidates.is_empty() {
                continue;
            }

            balance_opts.push(BalanceOption {
                existing: ex_cand,
                candidates,
            });
        }

        if balance_opts.is_empty() {
            return Ok(None);
        }

        loop {
            let (target, _existing) = self.best_balance_target(&mut balance_opts).await;
            if target.is_none() {
                break;
            }
            let mut exist_replica_candidates = replica_to_balance.to_owned();
            let fake_new_replica = ReplicaDesc {
                id: exist_replica_candidates.iter().map(|r| r.id).max().unwrap() + 1,
                node_id: target.as_ref().unwrap().node.id,
                ..Default::default()
            };
            exist_replica_candidates.push(&fake_new_replica);

            let replica_candidates = exist_replica_candidates.to_owned();
            // TODO: filter out non-updated replicas from replica_candidates.

            let remove_candidate = self
                .sim_remove_target(
                    fake_new_replica.node_id,
                    replica_candidates,
                    exist_replica_candidates,
                    other_replicas.to_owned(),
                    within_voter,
                )
                .await?;
            if remove_candidate.is_none() {
                return Ok(None);
            }

            if remove_candidate.as_ref().unwrap().node.id != target.as_ref().unwrap().node.id {
                return Ok(Some((
                    target.as_ref().unwrap().node.id,
                    remove_candidate.as_ref().unwrap().node.id,
                )));
            }
        }

        Ok(None)
    }

    async fn sim_remove_target(
        &self,
        _remove_node: u64,
        replica_cands: Vec<&ReplicaDesc>,
        exist_cands: Vec<&ReplicaDesc>,
        other_replicas: Vec<&ReplicaDesc>,
        within_voter: bool,
    ) -> Result<Option<NodeCandidate>> {
        let nodes = self.alloc_source.nodes(NodeFilter::Schedulable);
        let candidate_nodes = replica_cands
            .iter()
            .map(|r| nodes.iter().find(|n| n.id == r.node_id).unwrap().to_owned())
            .collect::<Vec<_>>();
        if within_voter {
            self.remove_target(candidate_nodes, exist_cands, other_replicas, within_voter)
                .await
        } else {
            // TODO: remove non-voter
            todo!()
        }
    }

    async fn remove_target(
        &self,
        candidate_nodes: Vec<NodeDesc>,
        exist_cands: Vec<&ReplicaDesc>,
        other_replicas: Vec<&ReplicaDesc>,
        _within_voter: bool,
    ) -> Result<Option<NodeCandidate>> {
        assert!(!candidate_nodes.is_empty());
        let mut candidates = Vec::new();
        for n in &candidate_nodes {
            candidates.push(NodeCandidate {
                node: n.to_owned(),
                disk_full: self.check_node_full(n),
                ..Default::default()
            })
        }
        candidates.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));

        let mut fcandidates = Vec::new();
        for c in &candidates {
            let mut c = c.to_owned();
            (c.balance_score, c.converges_score) =
                self.balance_score_by_replica_count(&c.node, &candidates, true);
            c.replica_count = c.node.capacity.as_ref().unwrap().replica_count;
            fcandidates.push(c);
        }
        fcandidates.sort_by_key(|w| std::cmp::Reverse(w.to_owned()));

        // TODO: ...

        let mut exist_replicas = exist_cands.to_owned();
        exist_replicas.extend_from_slice(&other_replicas);

        let bad_candidate = fcandidates.last().unwrap(); // TODO:....

        for replica in exist_replicas {
            if replica.node_id == bad_candidate.node.id {
                return Ok(Some(bad_candidate.to_owned()));
            }
        }

        Ok(None)
    }

    fn check_node_full(&self, _n: &NodeDesc) -> bool {
        false // TODO:...
    }

    fn balance_score_by_replica_count(
        &self,
        node: &NodeDesc,
        cands: &[NodeCandidate],
        from: bool,
    ) -> (f64, f64) {
        let current = node.capacity.as_ref().unwrap().replica_count as f64;
        let (mean, min, max) = self.replica_count_threshold(cands);
        let mut balance_score = 0.0;
        if current < min {
            balance_score = 1.0;
        }
        if current > max {
            balance_score = -1.0;
        }
        let new_val = if from { current - 1.0 } else { current + 1.0 };
        let converges_score = if f64::abs(new_val - mean) >= f64::abs(current - mean) {
            1.0
        } else {
            0.0
        };
        (balance_score, converges_score)
    }

    fn should_balance_by_replica_count(&self, rep: &PotentialReplacement) -> bool {
        if rep.candidates.is_empty() {
            return false;
        }
        let (mean, min, max) = self.replica_count_threshold(&rep.candidates);
        if let Some(c) = &rep.existing.capacity {
            let cnt = c.replica_count as f64;
            if cnt > max {
                // balance if over max a lot.
                return true;
            }
            if cnt > mean {
                // balance if over mean and others is underfull.
                for c in &rep.candidates {
                    if let Some(cc) = &c.node.capacity {
                        if (cc.replica_count as f64) < min {
                            return true;
                        }
                    }
                }
            }
        }

        false
    }

    fn replica_count_threshold(&self, cands: &[NodeCandidate]) -> (f64, f64, f64) {
        const THRESHOLD_FRACTION: f64 = 0.05;
        const MIN_RANGE_DELTA: f64 = 2.0;
        let mean = self.mean_candidate_replica_count(cands);
        let delta = f64::max(mean as f64 * THRESHOLD_FRACTION, MIN_RANGE_DELTA);
        (mean, mean - delta, mean + delta)
    }

    fn mean_candidate_replica_count(&self, cands: &[NodeCandidate]) -> f64 {
        let total = cands
            .iter()
            .filter(|c| c.node.capacity.is_some())
            .fold(0, |t, c| {
                t + c.node.capacity.as_ref().unwrap().replica_count
            }) as f64;
        let cnt = cands.iter().filter(|c| c.node.capacity.is_some()).count() as f64;
        total / cnt
    }

    /// Compute group change action.
    pub async fn compute_group_action(&self) -> Result<GroupAction> {
        if !self.config.enable_group_balance {
            return Ok(GroupAction::Noop);
        }

        self.alloc_source.refresh_all().await?;

        if self.alloc_source.nodes(NodeFilter::NotDecommissioned).len()
            < self.config.replicas_per_group
        {
            // group alloctor start work after node_count > replicas_per_group.
            return Ok(GroupAction::Noop);
        }

        Ok(match self.current_groups().cmp(&self.desired_groups()) {
            std::cmp::Ordering::Less => {
                // it happend when:
                // - new join node
                // - increase cpu quota for exist node(e.g. via cgroup)
                // - increate replica_num configuration
                GroupAction::Add(self.desired_groups() - self.current_groups())
            }
            std::cmp::Ordering::Greater => {
                // it happens when:
                //  - joined node exit
                //  - decrease cpu quota for exist node(e.g. via cgroup)
                //  - decrease replica_num configuration
                let want_remove = self.current_groups() - self.desired_groups();
                GroupAction::Remove(self.preferred_remove_groups(want_remove))
            }
            std::cmp::Ordering::Equal => GroupAction::Noop,
        })
    }

    /// Compute replica change action.
    pub async fn compute_replica_action(&self) -> Result<Vec<ReplicaAction>> {
        if !self.config.enable_replica_balance {
            return Ok(vec![]);
        }

        self.alloc_source.refresh_all().await?;

        // TODO: try qps rebalance.

        // try replica-count rebalance.
        let actions = ReplicaCountPolicy::with(self.alloc_source.to_owned()).compute_balance()?;
        if !actions.is_empty() {
            return Ok(actions);
        }

        Ok(Vec::new())
    }

    pub async fn compute_shard_action(&self) -> Result<Vec<ShardAction>> {
        if !self.config.enable_shard_balance {
            return Ok(vec![]);
        }

        self.alloc_source.refresh_all().await?;

        if self.alloc_source.nodes(NodeFilter::All).len() >= self.config.replicas_per_group {
            let actions = ShardCountPolicy::with(self.alloc_source.to_owned()).compute_balance()?;
            if !actions.is_empty() {
                metrics::RECONCILE_ALREADY_BALANCED_INFO
                    .group_shard_count
                    .set(0);
                return Ok(actions);
            }
        }
        metrics::RECONCILE_ALREADY_BALANCED_INFO
            .group_shard_count
            .set(1);
        Ok(Vec::new())
    }

    /// Allocate new replica in one group.
    pub async fn allocate_group_replica(
        &self,
        existing_replicas: Vec<u64>,
        wanted_count: usize,
    ) -> Result<Vec<NodeDesc>> {
        self.alloc_source.refresh_all().await?;

        ReplicaCountPolicy::with(self.alloc_source.to_owned())
            .allocate_group_replica(existing_replicas, wanted_count)
    }

    /// Find a group to place shard.
    pub async fn place_group_for_shard(&self, n: usize) -> Result<Vec<GroupDesc>> {
        self.alloc_source.refresh_all().await?;

        ShardCountPolicy::with(self.alloc_source.to_owned()).allocate_shard(n)
    }

    pub async fn compute_leader_action(&self) -> Result<Vec<LeaderAction>> {
        if !self.config.enable_leader_balance {
            return Ok(vec![]);
        }
        self.alloc_source.refresh_all().await?;
        match LeaderCountPolicy::with(self.alloc_source.to_owned()).compute_balance()? {
            LeaderAction::Noop => {}
            e @ LeaderAction::Shed { .. } => return Ok(vec![e]),
        }
        Ok(Vec::new())
    }
}

impl<T: AllocSource> Allocator<T> {
    fn preferred_remove_groups(&self, want_remove: usize) -> Vec<u64> {
        // TODO:
        // 1 remove groups from unreachable nodes that indicated by NodeLiveness(they also need
        // repair replicas).
        // 2 remove groups from unmatch cpu-quota nodes.
        // 3. remove groups with lowest migration cost.
        self.alloc_source
            .nodes(NodeFilter::NotDecommissioned)
            .iter()
            .take(want_remove)
            .map(|n| n.id)
            .collect()
    }

    fn desired_groups(&self) -> usize {
        let total_cpus = self
            .alloc_source
            .nodes(NodeFilter::NotDecommissioned)
            .iter()
            .map(|n| n.capacity.as_ref().unwrap().cpu_nums)
            .fold(0_f64, |acc, x| acc + x);
        (total_cpus / self.config.replicas_per_group as f64).ceil() as usize
    }

    fn current_groups(&self) -> usize {
        self.alloc_source.groups().len()
    }
}

// Allocate Group's replica between nodes.
impl<T: AllocSource> Allocator<T> {}

// Allocate Group leader replica.
impl<T: AllocSource> Allocator<T> {}
