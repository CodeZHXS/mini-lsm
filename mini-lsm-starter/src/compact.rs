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
use std::vec;

use anyhow::{Ok, Result};
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
use crate::key::KeySlice;
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
        let sstables = &self.state.read().sstables;
        match task {
            CompactionTask::Leveled(task) => todo!(),
            CompactionTask::Tiered(TieredCompactionTask {
                tiers,
                bottom_tier_included,
            }) => {
                let iter = Self::merge_sst_concat_iter_from_tiers(sstables, tiers)?;
                self.compact_result_from_iter(iter, task.compact_to_bottom_level())
            }
            CompactionTask::Simple(SimpleLeveledCompactionTask {
                upper_level,
                upper_level_sst_ids,
                lower_level_sst_ids,
                ..
            }) => {
                let lower_iter = Self::sst_concat_iter_from_ids(sstables, lower_level_sst_ids)?;
                match upper_level {
                    Some(_) => {
                        let upper_iter =
                            Self::sst_concat_iter_from_ids(sstables, upper_level_sst_ids)?;
                        let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_result_from_iter(iter, task.compact_to_bottom_level())
                    }
                    None => {
                        let upper_iter =
                            Self::sst_merge_iter_from_ids(sstables, upper_level_sst_ids)?;
                        let iter = TwoMergeIterator::create(upper_iter, lower_iter)?;
                        self.compact_result_from_iter(iter, task.compact_to_bottom_level())
                    }
                }
            }
            CompactionTask::ForceFullCompaction {
                l0_sstables,
                l1_sstables,
            } => {
                let l0_merge_iter = Self::sst_merge_iter_from_ids(sstables, l0_sstables)?;
                let l1_concat_iter = Self::sst_concat_iter_from_ids(sstables, l1_sstables)?;
                let iter = TwoMergeIterator::create(l0_merge_iter, l1_concat_iter)?;
                self.compact_result_from_iter(iter, task.compact_to_bottom_level())
            }
        }
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

            snapshot.remove_sst_batch(&l0_sstables);
            snapshot.remove_sst_batch(&l1_sstables);

            let l1_sst_ids = sstables.iter().map(|t| t.sst_id()).collect();
            snapshot.add_sst_batch(sstables);

            println!("force full compaction done, new SSTs: {:?}", l1_sst_ids);
            snapshot.levels[0].1 = l1_sst_ids;

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
        let snapshot = self.state.read().clone();
        let task = self
            .compaction_controller
            .generate_compaction_task(&snapshot);

        let Some(task) = task else {
            return Ok(());
        };

        let new_sst = self.compact(&task)?;
        let new_sst_ids: Vec<usize> = new_sst.iter().map(|t| t.sst_id()).collect();

        let state_guard = self.state_lock.lock();
        let (mut snapshot, unused_sst_ids) = self.compaction_controller.apply_compaction_result(
            &self.state.read(),
            &task,
            &new_sst_ids,
            false,
        );
        snapshot.remove_sst_batch(&unused_sst_ids);
        snapshot.add_sst_batch(new_sst);
        *self.state.write() = Arc::new(snapshot);
        drop(state_guard);

        for id in unused_sst_ids {
            std::fs::remove_file(self.path_of_sst(id))?;
        }

        Ok(())
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

    fn get_l0_and_l1_sst_snapshot(&self) -> (Vec<usize>, Vec<usize>) {
        let guard = self.state.read();
        (guard.l0_sstables.clone(), guard.levels[0].1.clone())
    }

    fn compact_result_from_iter(
        &self,
        mut iter: impl for<'a> StorageIterator<KeyType<'a> = KeySlice<'a>>,
        is_bottom_level: bool,
    ) -> Result<Vec<Arc<SsTable>>> {
        let mut ans = vec![];
        let mut builder = None;

        while iter.is_valid() {
            if is_bottom_level && iter.value().is_empty() {
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
                let table =
                    builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
                ans.push(Arc::new(table));
            }
            iter.next()?;
        }

        if builder.is_some() {
            let builder = builder.take().unwrap();
            let id = self.next_sst_id();
            let table = builder.build(id, Some(self.block_cache.clone()), self.path_of_sst(id))?;
            ans.push(Arc::new(table));
        }
        Ok(ans)
    }

    fn sst_merge_iter_from_ids(
        sstables: &HashMap<usize, Arc<SsTable>>,
        ids: &Vec<usize>,
    ) -> Result<MergeIterator<SsTableIterator>> {
        let mut iters = Vec::with_capacity(ids.len());
        for id in ids {
            let table = sstables[id].clone();
            iters.push(Box::new(SsTableIterator::create_and_seek_to_first(table)?));
        }
        Ok(MergeIterator::create(iters))
    }

    fn sst_concat_iter_from_ids(
        sstables: &HashMap<usize, Arc<SsTable>>,
        ids: &Vec<usize>,
    ) -> Result<SstConcatIterator> {
        let sst = ids.iter().map(|id| sstables[id].clone()).collect();
        Ok(SstConcatIterator::create_and_seek_to_first(sst)?)
    }

    fn merge_sst_concat_iter_from_tiers(
        sstables: &HashMap<usize, Arc<SsTable>>,
        tiers: &Vec<(usize, Vec<usize>)>,
    ) -> Result<MergeIterator<SstConcatIterator>> {
        let mut concat_iters = Vec::with_capacity(tiers.len());
        for (_, ids) in tiers {
            concat_iters.push(Box::new(Self::sst_concat_iter_from_ids(sstables, ids)?));
        }
        Ok(MergeIterator::create(concat_iters))
    }
}

impl LsmStorageState {
    fn remove_sst_batch(&mut self, ids: &Vec<usize>) {
        for id in ids {
            let old: Option<Arc<SsTable>> = self.sstables.remove(id);
            assert!(old.is_some());
        }
    }

    fn add_sst_batch(&mut self, ids: Vec<Arc<SsTable>>) {
        for table in ids {
            let old = self.sstables.insert(table.sst_id(), table);
            assert!(old.is_none());
        }
    }
}
