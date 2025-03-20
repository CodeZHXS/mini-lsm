// Copyright (c) 2022-2025 Alex Chi Z
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

mod leveled;
mod simple_leveled;
mod tiered;

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
pub use leveled::{LeveledCompactionController, LeveledCompactionOptions, LeveledCompactionTask};
use serde::{Deserialize, Serialize};
pub use simple_leveled::{
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, SimpleLeveledCompactionTask,
};
pub use tiered::{TieredCompactionController, TieredCompactionOptions, TieredCompactionTask};

use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::lsm_storage::{LsmStorageInner, LsmStorageState};
use crate::table::{SsTable, SsTableBuilder, SsTableIterator};

#[derive(Debug, Serialize, Deserialize)]
pub enum CompactionTask {
    Leveled(LeveledCompactionTask),
    Tiered(TieredCompactionTask),
    Simple(SimpleLeveledCompactionTask),
    ForceFullCompaction {
        l0_sstables: Vec<usize>,
        l1_sstables: Vec<usize>,
    },
}

impl CompactionTask {
    fn compact_to_bottom_level(&self) -> bool {
        match self {
            CompactionTask::ForceFullCompaction { .. } => true,
            CompactionTask::Leveled(task) => task.is_lower_level_bottom_level,
            CompactionTask::Simple(task) => task.is_lower_level_bottom_level,
            CompactionTask::Tiered(task) => task.bottom_tier_included,
        }
    }
}

pub(crate) enum CompactionController {
    Leveled(LeveledCompactionController),
    Tiered(TieredCompactionController),
    Simple(SimpleLeveledCompactionController),
    NoCompaction,
}

impl CompactionController {
    pub fn generate_compaction_task(&self, snapshot: &LsmStorageState) -> Option<CompactionTask> {
        match self {
            CompactionController::Leveled(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Leveled),
            CompactionController::Simple(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Simple),
            CompactionController::Tiered(ctrl) => ctrl
                .generate_compaction_task(snapshot)
                .map(CompactionTask::Tiered),
            CompactionController::NoCompaction => unreachable!(),
        }
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &CompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        match (self, task) {
            (CompactionController::Leveled(ctrl), CompactionTask::Leveled(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output, in_recovery)
            }
            (CompactionController::Simple(ctrl), CompactionTask::Simple(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            (CompactionController::Tiered(ctrl), CompactionTask::Tiered(task)) => {
                ctrl.apply_compaction_result(snapshot, task, output)
            }
            _ => unreachable!(),
        }
    }
}

impl CompactionController {
    pub fn flush_to_l0(&self) -> bool {
        matches!(
            self,
            Self::Leveled(_) | Self::Simple(_) | Self::NoCompaction
        )
    }
}

#[derive(Debug, Clone)]
pub enum CompactionOptions {
    /// Leveled compaction with partial compaction + dynamic level support (= RocksDB's Leveled
    /// Compaction)
    Leveled(LeveledCompactionOptions),
    /// Tiered compaction (= RocksDB's universal compaction)
    Tiered(TieredCompactionOptions),
    /// Simple leveled compaction
    Simple(SimpleLeveledCompactionOptions),
    /// In no compaction mode (week 1), always flush to L0
    NoCompaction,
}

impl LsmStorageInner {
    fn compact(&self, task: &CompactionTask) -> Result<Vec<Arc<SsTable>>> {
        let sstables = self.get_sstables_snapshot();
        let mut ans = vec![];
        match task {
            CompactionTask::Leveled(leveled_compaction_task) => todo!(),
            CompactionTask::Tiered(tiered_compaction_task) => todo!(),
            CompactionTask::Simple(simple_leveled_compaction_task) => todo!(),
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let mut l0_sst_iters = Vec::with_capacity(l0_sstables.len());
                for id in l0_sstables {
                    let table = sstables[id].clone();
                    l0_sst_iters.push(Box::new(SsTableIterator::create_and_seek_to_first(table)?));
                }
                let l0_merge_iter = MergeIterator::create(l0_sst_iters);

                let l1_sst = l1_sstables.iter().map(|id| sstables[id].clone()).collect();
                let l1_concat_iter = SstConcatIterator::create_and_seek_to_first(l1_sst)?;

                let mut iter = TwoMergeIterator::create(l0_merge_iter, l1_concat_iter)?;
                let mut builder = None;

                while iter.is_valid() {
                    if iter.value().is_empty() {
                        iter.next()?;
                        continue;
                    }
                    if builder.is_none() {
                        builder = Some(SsTableBuilder::new(self.options.block_size));
                    }
                    let builder_inner = builder.as_mut().unwrap();
                    builder_inner.add(iter.key(), iter.value());
                    if builder_inner.estimated_size() >= self.options.target_sst_size {
                        let builder = builder.take().unwrap();
                        let id = self.next_sst_id();
                        let table = builder.build(
                            id,
                            Some(self.block_cache.clone()),
                            self.path_of_sst(id),
                        )?;
                        ans.push(Arc::new(table));
                    }
                    iter.next()?;
                }

                if builder.is_some() {
                    let builder = builder.take().unwrap();
                    let id = self.next_sst_id();
                    let table =
                        builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
                    ans.push(Arc::new(table));
                }
            }
        }
        Ok(ans)
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        let (l0_sstables, l1_sstables) = self.get_l0_and_l1_sst_snapshot();
        let task = &CompactionTask::ForceFullCompaction {
            l0_sstables: (l0_sstables.clone()),
            l1_sstables: (l1_sstables.clone()),
        };
        println!("force full compaction: {:?}", task);

        let sstables = self.compact(task)?;

        {
            let state_lock = self.state_lock.lock();
            let mut snapshot = self.state.read().as_ref().clone();
            for id in l0_sstables.iter().chain(l1_sstables.iter()) {
                snapshot.sstables.remove(id);
            }
            let mut l1 = Vec::with_capacity(sstables.len());
            for table in sstables {
                l1.push(table.sst_id());
                snapshot.sstables.insert(table.sst_id(), table);
            }

            println!("force full compaction done, new SSTs: {:?}", l1);
            snapshot.levels[0].1 = l1;

            let l0_truncate_len = snapshot.l0_sstables.len() - l0_sstables.len();
            snapshot.l0_sstables.truncate(l0_truncate_len);

            *self.state.write() = Arc::new(snapshot);
        }

        for id in l0_sstables.iter().chain(l1_sstables.iter()) {
            std::fs::remove_file(self.path_of_sst(*id))?;
        }

        Ok(())
    }

    fn trigger_compaction(&self) -> Result<()> {
        unimplemented!()
    }

    pub(crate) fn spawn_compaction_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        if let CompactionOptions::Leveled(_)
        | CompactionOptions::Simple(_)
        | CompactionOptions::Tiered(_) = self.options.compaction_options
        {
            let this = self.clone();
            let handle = std::thread::spawn(move || {
                let ticker = crossbeam_channel::tick(Duration::from_millis(50));
                loop {
                    crossbeam_channel::select! {
                        recv(ticker) -> _ => if let Err(e) = this.trigger_compaction() {
                            eprintln!("compaction failed: {}", e);
                        },
                        recv(rx) -> _ => return
                    }
                }
            });
            return Ok(Some(handle));
        }
        Ok(None)
    }

    fn trigger_flush(&self) -> Result<()> {
        let cnt = self.state.read().imm_memtables.len() + 1;
        if cnt > self.options.num_memtable_limit {
            self.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub(crate) fn spawn_flush_thread(
        self: &Arc<Self>,
        rx: crossbeam_channel::Receiver<()>,
    ) -> Result<Option<std::thread::JoinHandle<()>>> {
        let this = self.clone();
        let handle = std::thread::spawn(move || {
            let ticker = crossbeam_channel::tick(Duration::from_millis(50));
            loop {
                crossbeam_channel::select! {
                    recv(ticker) -> _ => if let Err(e) = this.trigger_flush() {
                        eprintln!("flush failed: {}", e);
                    },
                    recv(rx) -> _ => return
                }
            }
        });
        Ok(Some(handle))
    }

    fn get_sstables_snapshot(&self) -> HashMap<usize, Arc<SsTable>> {
        self.state.read().sstables.clone()
    }

    fn get_l0_and_l1_sst_snapshot(&self) -> (Vec<usize>, Vec<usize>) {
        let guard = self.state.read();
        (guard.l0_sstables.clone(), guard.levels[0].1.clone())
    }
}
