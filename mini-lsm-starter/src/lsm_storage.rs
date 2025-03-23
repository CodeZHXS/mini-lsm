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

use std::collections::HashMap;
use std::fs::File;
use std::ops::Bound;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use std::vec;

use anyhow::Result;
use bytes::Bytes;
use farmhash::hash32;
use parking_lot::{Mutex, MutexGuard, RwLock};

use crate::block::Block;
use crate::compact::{
    CompactionController, CompactionOptions, LeveledCompactionController, LeveledCompactionOptions,
    SimpleLeveledCompactionController, SimpleLeveledCompactionOptions, TieredCompactionController,
};
use crate::iterators::concat_iterator::SstConcatIterator;
use crate::iterators::merge_iterator::MergeIterator;
use crate::iterators::two_merge_iterator::TwoMergeIterator;
use crate::iterators::StorageIterator;
use crate::key::KeySlice;
use crate::lsm_iterator::{FusedIterator, LsmIterator};
use crate::manifest::{Manifest, ManifestRecord};
use crate::mem_table::MemTable;
use crate::mvcc::LsmMvccInner;
use crate::table::{FileObject, SsTable, SsTableBuilder, SsTableIterator};

pub type BlockCache = moka::sync::Cache<(usize, usize), Arc<Block>>;

/// Represents the state of the storage engine.
#[derive(Clone)]
pub struct LsmStorageState {
    /// The current memtable.
    pub memtable: Arc<MemTable>,
    /// Immutable memtables, from latest to earliest.
    pub imm_memtables: Vec<Arc<MemTable>>,
    /// L0 SSTs, from latest to earliest.
    pub l0_sstables: Vec<usize>,
    /// SsTables sorted by key range; L1 - L_max for leveled compaction, or tiers for tiered
    /// compaction.
    pub levels: Vec<(usize, Vec<usize>)>,
    /// SST objects.
    pub sstables: HashMap<usize, Arc<SsTable>>,
}

pub enum WriteBatchRecord<T: AsRef<[u8]>> {
    Put(T, T),
    Del(T),
}

impl LsmStorageState {
    fn create(options: &LsmStorageOptions) -> Self {
        let levels = match &options.compaction_options {
            CompactionOptions::Leveled(LeveledCompactionOptions { max_levels, .. })
            | CompactionOptions::Simple(SimpleLeveledCompactionOptions { max_levels, .. }) => (1
                ..=*max_levels)
                .map(|level| (level, Vec::new()))
                .collect::<Vec<_>>(),
            CompactionOptions::Tiered(_) => Vec::new(),
            CompactionOptions::NoCompaction => vec![(1, Vec::new())],
        };
        Self {
            memtable: Arc::new(MemTable::create(0)),
            imm_memtables: Vec::new(),
            l0_sstables: Vec::new(),
            levels,
            sstables: Default::default(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct LsmStorageOptions {
    // Block size in bytes
    pub block_size: usize,
    // SST size in bytes, also the approximate memtable capacity limit
    pub target_sst_size: usize,
    // Maximum number of memtables in memory, flush to L0 when exceeding this limit
    pub num_memtable_limit: usize,
    pub compaction_options: CompactionOptions,
    pub enable_wal: bool,
    pub serializable: bool,
}

impl LsmStorageOptions {
    pub fn default_for_week1_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 50,
            serializable: false,
        }
    }

    pub fn default_for_week1_day6_test() -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 2 << 20,
            compaction_options: CompactionOptions::NoCompaction,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }

    pub fn default_for_week2_test(compaction_options: CompactionOptions) -> Self {
        Self {
            block_size: 4096,
            target_sst_size: 1 << 20, // 1MB
            compaction_options,
            enable_wal: false,
            num_memtable_limit: 2,
            serializable: false,
        }
    }
}

#[derive(Clone, Debug)]
pub enum CompactionFilter {
    Prefix(Bytes),
}

/// The storage interface of the LSM tree.
pub(crate) struct LsmStorageInner {
    pub(crate) state: Arc<RwLock<Arc<LsmStorageState>>>,
    pub(crate) state_lock: Mutex<()>,
    path: PathBuf,
    pub(crate) block_cache: Arc<BlockCache>,
    next_sst_id: AtomicUsize,
    pub(crate) options: Arc<LsmStorageOptions>,
    pub(crate) compaction_controller: CompactionController,
    pub(crate) manifest: Option<Manifest>,
    pub(crate) mvcc: Option<LsmMvccInner>,
    pub(crate) compaction_filters: Arc<Mutex<Vec<CompactionFilter>>>,
}

/// A thin wrapper for `LsmStorageInner` and the user interface for MiniLSM.
pub struct MiniLsm {
    pub(crate) inner: Arc<LsmStorageInner>,
    /// Notifies the L0 flush thread to stop working. (In week 1 day 6)
    flush_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the flush thread. (In week 1 day 6)
    flush_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
    /// Notifies the compaction thread to stop working. (In week 2)
    compaction_notifier: crossbeam_channel::Sender<()>,
    /// The handle for the compaction thread. (In week 2)
    compaction_thread: Mutex<Option<std::thread::JoinHandle<()>>>,
}

impl Drop for MiniLsm {
    fn drop(&mut self) {
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
    }
}

impl MiniLsm {
    pub fn close(&self) -> Result<()> {
        self.inner.sync_dir()?;
        self.compaction_notifier.send(()).ok();
        self.flush_notifier.send(()).ok();
        let mut compaction_thread = self.compaction_thread.lock();
        if let Some(compaction_thread) = compaction_thread.take() {
            compaction_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }
        let mut flush_thread = self.flush_thread.lock();
        if let Some(flush_thread) = flush_thread.take() {
            flush_thread
                .join()
                .map_err(|e| anyhow::anyhow!("{:?}", e))?;
        }

        if self.inner.options.enable_wal {
            return Ok(());
        }

        // create memtable and skip updating manifest
        if !self.inner.state.read().memtable.is_empty() {
            let state_lock = self.inner.state_lock.lock();
            self.inner.force_freeze_memtable(&state_lock)?;
        }

        while {
            let snapshot = self.inner.state.read();
            !snapshot.imm_memtables.is_empty()
        } {
            self.inner.force_flush_next_imm_memtable()?;
        }
        self.inner.sync_dir()?;

        Ok(())
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Arc<Self>> {
        let inner = Arc::new(LsmStorageInner::open(path, options)?);
        let (tx1, rx) = crossbeam_channel::unbounded();
        let compaction_thread = inner.spawn_compaction_thread(rx)?;
        let (tx2, rx) = crossbeam_channel::unbounded();
        let flush_thread = inner.spawn_flush_thread(rx)?;
        Ok(Arc::new(Self {
            inner,
            flush_notifier: tx2,
            flush_thread: Mutex::new(flush_thread),
            compaction_notifier: tx1,
            compaction_thread: Mutex::new(compaction_thread),
        }))
    }

    pub fn new_txn(&self) -> Result<()> {
        self.inner.new_txn()
    }

    pub fn write_batch<T: AsRef<[u8]>>(&self, batch: &[WriteBatchRecord<T>]) -> Result<()> {
        self.inner.write_batch(batch)
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        self.inner.add_compaction_filter(compaction_filter)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        self.inner.get(key)
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        self.inner.put(key, value)
    }

    pub fn delete(&self, key: &[u8]) -> Result<()> {
        self.inner.delete(key)
    }

    pub fn sync(&self) -> Result<()> {
        self.inner.sync()
    }

    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        self.inner.scan(lower, upper)
    }

    /// Only call this in test cases due to race conditions
    pub fn force_flush(&self) -> Result<()> {
        if !self.inner.state.read().memtable.is_empty() {
            self.inner
                .force_freeze_memtable(&self.inner.state_lock.lock())?;
        }
        if !self.inner.state.read().imm_memtables.is_empty() {
            self.inner.force_flush_next_imm_memtable()?;
        }
        Ok(())
    }

    pub fn force_full_compaction(&self) -> Result<()> {
        self.inner.force_full_compaction()
    }
}

impl LsmStorageInner {
    pub(crate) fn next_sst_id(&self) -> usize {
        self.next_sst_id
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    }

    /// Start the storage engine by either loading an existing directory or creating a new one if the directory does
    /// not exist.
    pub(crate) fn open(path: impl AsRef<Path>, options: LsmStorageOptions) -> Result<Self> {
        let path = path.as_ref();
        if !path.exists() {
            std::fs::create_dir(path)?;
        }

        let mut state = LsmStorageState::create(&options);

        let compaction_controller = match &options.compaction_options {
            CompactionOptions::Leveled(options) => {
                CompactionController::Leveled(LeveledCompactionController::new(options.clone()))
            }
            CompactionOptions::Tiered(options) => {
                CompactionController::Tiered(TieredCompactionController::new(options.clone()))
            }
            CompactionOptions::Simple(options) => CompactionController::Simple(
                SimpleLeveledCompactionController::new(options.clone()),
            ),
            CompactionOptions::NoCompaction => CompactionController::NoCompaction,
        };

        let block_cache = Arc::new(BlockCache::new(1 << 20)); // 4GB
        let mut next_sst_id = 0;

        let manifest_path = path.join("MANIFEST");
        let manifest = if !manifest_path.exists() {
            Manifest::create(&manifest_path)?
        } else {
            let (manifest, manifest_recordss) = Manifest::recover(manifest_path)?;
            for record in manifest_recordss {
                match record {
                    ManifestRecord::Flush(id) => {
                        if compaction_controller.flush_to_l0() {
                            state.l0_sstables.insert(0, id);
                        } else {
                            state.levels.insert(0, (id, vec![id]));
                        }
                    }
                    ManifestRecord::NewMemtable(id) => {
                        unimplemented!()
                    }
                    ManifestRecord::Compaction(task, output) => {
                        state = compaction_controller
                            .apply_compaction_result(&state, &task, &output, true)
                            .0;
                    }
                }
            }

            for id in state
                .l0_sstables
                .iter()
                .chain(state.levels.iter().flat_map(|(id, ids)| ids))
            {
                next_sst_id = next_sst_id.max(*id + 1);
                state.sstables.insert(
                    *id,
                    Arc::new(SsTable::open(
                        *id,
                        Some(block_cache.clone()),
                        FileObject::open(&Self::path_of_sst_static(path, *id))?,
                    )?),
                );
            }

            if let CompactionOptions::Leveled(_) = options.compaction_options {
                for (_, ids) in state.levels.iter_mut() {
                    ids.sort_by(|x, y| {
                        state.sstables[x]
                            .first_key()
                            .cmp(state.sstables[y].first_key())
                    });
                }
            }

            manifest
        };

        state.memtable = Arc::new(MemTable::create(next_sst_id));

        let storage = Self {
            state: Arc::new(RwLock::new(Arc::new(state))),
            state_lock: Mutex::new(()),
            path: path.to_path_buf(),
            block_cache,
            next_sst_id: AtomicUsize::new(next_sst_id + 1),
            compaction_controller,
            manifest: Some(manifest),
            options: options.into(),
            mvcc: None,
            compaction_filters: Arc::new(Mutex::new(Vec::new())),
        };

        storage.sync_dir()?;

        Ok(storage)
    }

    pub fn sync(&self) -> Result<()> {
        unimplemented!()
    }

    pub fn add_compaction_filter(&self, compaction_filter: CompactionFilter) {
        let mut compaction_filters = self.compaction_filters.lock();
        compaction_filters.push(compaction_filter);
    }

    /// Get a key from the storage. In day 7, this can be further optimized by using a bloom filter.
    pub fn get(&self, key: &[u8]) -> Result<Option<Bytes>> {
        let snapshot = self.state.read().clone();

        if let Some(value) = snapshot.memtable.get(key) {
            if value.is_empty() {
                return Ok(None);
            }
            return Ok(Some(value));
        }

        for t in snapshot.imm_memtables.iter() {
            if let Some(value) = t.get(key) {
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(value));
            }
        }

        for id in snapshot.l0_sstables.iter() {
            let table = snapshot.sstables[id].clone();

            if let Some(bloom) = &table.bloom {
                if !bloom.may_contain(hash32(key)) {
                    continue;
                }
            }

            let iter = SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?;
            if iter.is_valid() && iter.key().raw_ref() == key {
                let value = iter.value();
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(value)));
            }
        }

        for i in 1..=snapshot.levels.len() {
            let iter = self.get_level_sst_concat_iter(
                &snapshot,
                i,
                Bound::Included(key),
                Bound::Included(key),
            )?;
            if iter.is_valid() && iter.key().raw_ref() == key {
                let value = iter.value();
                if value.is_empty() {
                    return Ok(None);
                }
                return Ok(Some(Bytes::copy_from_slice(value)));
            }
        }

        Ok(None)
    }

    /// Write a batch of data into the storage. Implement in week 2 day 7.
    pub fn write_batch<T: AsRef<[u8]>>(&self, _batch: &[WriteBatchRecord<T>]) -> Result<()> {
        unimplemented!()
    }

    fn target_sst_size(&self) -> usize {
        self.options.target_sst_size
    }

    /// Put a key-value pair into the storage by writing into the current memtable.
    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let new_approximate_size = self.state.read().memtable.put_and_get_size(key, value);
        self.try_freeze_memtable(new_approximate_size)
    }

    /// Remove a key from the storage by writing an empty value.
    pub fn delete(&self, key: &[u8]) -> Result<()> {
        let new_approximate_size = self.state.read().memtable.put_and_get_size(key, &[]);
        self.try_freeze_memtable(new_approximate_size)
    }

    pub(crate) fn path_of_sst_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.sst", id))
    }

    pub(crate) fn path_of_sst(&self, id: usize) -> PathBuf {
        Self::path_of_sst_static(&self.path, id)
    }

    pub(crate) fn path_of_wal_static(path: impl AsRef<Path>, id: usize) -> PathBuf {
        path.as_ref().join(format!("{:05}.wal", id))
    }

    pub(crate) fn path_of_wal(&self, id: usize) -> PathBuf {
        Self::path_of_wal_static(&self.path, id)
    }

    pub(super) fn sync_dir(&self) -> Result<()> {
        File::open(&self.path)?.sync_all()?;
        Ok(())
    }

    pub fn try_freeze_memtable(&self, new_approximate_size: usize) -> Result<()> {
        if new_approximate_size >= self.target_sst_size() {
            let state_lock = self.state_lock.lock();
            if self.state.read().memtable.approximate_size() >= self.target_sst_size() {
                self.force_freeze_memtable(&state_lock)?;
            }
        }
        Ok(())
    }

    /// Force freeze the current memtable to an immutable memtable
    pub fn force_freeze_memtable(&self, _state_lock_observer: &MutexGuard<'_, ()>) -> Result<()> {
        let new_memtable = Arc::new(MemTable::create(self.next_sst_id()));

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            let old_memtable = std::mem::replace(&mut snapshot.memtable, new_memtable);
            snapshot.imm_memtables.insert(0, old_memtable.clone());
            *guard = Arc::new(snapshot);
        }

        Ok(())
    }

    /// Force flush the earliest-created immutable memtable to disk
    pub fn force_flush_next_imm_memtable(&self) -> Result<()> {
        let state_guard = self.state_lock.lock();

        let last_memtable = self.state.read().imm_memtables.last().unwrap().clone();
        let mut sst_builder = SsTableBuilder::new(self.options.block_size);
        last_memtable.flush(&mut sst_builder)?;

        let id = last_memtable.id();
        let sst = Arc::new(sst_builder.build(
            id,
            Some(self.block_cache.clone()),
            self.path_of_sst(id),
        )?);

        {
            let mut guard = self.state.write();
            let mut snapshot = guard.as_ref().clone();
            snapshot.imm_memtables.pop().unwrap();
            if self.compaction_controller.flush_to_l0() {
                snapshot.l0_sstables.insert(0, id);
            } else {
                snapshot.levels.insert(0, (id, vec![id]));
            }
            println!("flushed {}.sst with size={}", id, sst.table_size());
            snapshot.sstables.insert(id, sst);
            *guard = Arc::new(snapshot);
        }

        self.sync_dir()?;

        if let Some(manifest) = &self.manifest {
            manifest.add_record(&state_guard, ManifestRecord::Flush(id))?;
        }

        Ok(())
    }

    pub fn new_txn(&self) -> Result<()> {
        // no-op
        Ok(())
    }

    /// Create an iterator over a range of keys.
    pub fn scan(
        &self,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<FusedIterator<LsmIterator>> {
        let snapshot = self.state.read().clone();

        let mut mem_iters = Vec::with_capacity(snapshot.imm_memtables.len() + 1);
        mem_iters.push(Box::new(snapshot.memtable.scan(lower, upper)));
        for t in snapshot.imm_memtables.iter() {
            mem_iters.push(Box::new(t.scan(lower, upper)));
        }
        let mem_merge_iter = MergeIterator::create(mem_iters);

        let l0_sst_merge_iter = self.get_l0_sst_merge_iter(&snapshot, lower, upper)?;
        let mem_with_l0_iter = TwoMergeIterator::create(mem_merge_iter, l0_sst_merge_iter)?;

        let all_level_merge_iter = self.get_all_level_merge_iter(&snapshot, lower, upper)?;

        let inner_iter = TwoMergeIterator::create(mem_with_l0_iter, all_level_merge_iter)?;
        let lsm_iters = LsmIterator::new(inner_iter, upper)?;
        Ok(FusedIterator::new(lsm_iters))
    }

    pub fn collect_sst_from_ids(
        &self,
        snapshot: &LsmStorageState,
        ids: &[usize],
    ) -> Vec<Arc<SsTable>> {
        ids.iter().map(|id| snapshot.sstables[id].clone()).collect()
    }

    /// for unordered l0_sst, use brute force method to filter
    fn filter_l0_sst(
        &self,
        snapshot: &LsmStorageState,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Vec<Arc<SsTable>> {
        let mut ans: Vec<Arc<SsTable>> = vec![];
        for id in snapshot.l0_sstables.iter() {
            let table: Arc<SsTable> = snapshot.sstables[id].clone();
            if table.overlap(lower, upper) {
                ans.push(table);
            }
        }
        ans
    }

    /// for ordered sst, use binary search method to filter
    fn filter_level_sst(
        &self,
        snapshot: &LsmStorageState,
        ids: &[usize],
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Vec<Arc<SsTable>> {
        let start = match lower {
            Bound::Included(lower) => {
                ids.partition_point(|id| snapshot.sstables[id].clone().last_key().raw_ref() < lower)
            }
            Bound::Excluded(lower) => ids
                .partition_point(|id| snapshot.sstables[id].clone().last_key().raw_ref() <= lower),
            Bound::Unbounded => 0,
        };

        let end = match upper {
            Bound::Included(upper) => ids
                .partition_point(|id| snapshot.sstables[id].clone().first_key().raw_ref() <= upper),
            Bound::Excluded(upper) => ids
                .partition_point(|id| snapshot.sstables[id].clone().first_key().raw_ref() < upper),
            Bound::Unbounded => ids.len(),
        };

        self.collect_sst_from_ids(snapshot, &ids[start..end])
    }

    fn get_l0_sst_merge_iter(
        &self,
        snapshot: &LsmStorageState,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<MergeIterator<SsTableIterator>> {
        let l0_sst = self.filter_l0_sst(snapshot, lower, upper);
        let mut sst_iters = Vec::with_capacity(l0_sst.len());
        for table in l0_sst {
            let iter = match lower {
                Bound::Included(key) => {
                    SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?
                }
                Bound::Excluded(key) => {
                    let mut iter =
                        SsTableIterator::create_and_seek_to_key(table, KeySlice::from_slice(key))?;
                    if iter.is_valid() && iter.key().raw_ref() == key {
                        iter.next()?;
                    }
                    iter
                }
                Bound::Unbounded => SsTableIterator::create_and_seek_to_first(table)?,
            };
            sst_iters.push(Box::new(iter));
        }
        Ok(MergeIterator::create(sst_iters))
    }

    fn get_level_sst_concat_iter(
        &self,
        snapshot: &LsmStorageState,
        level: usize,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<SstConcatIterator> {
        assert!(level >= 1 && level <= snapshot.levels.len());

        let sst_ids = snapshot.levels[level - 1].1.as_ref();
        let l1_sst = self.filter_level_sst(snapshot, sst_ids, lower, upper);
        let iter = match lower {
            Bound::Included(key) => {
                SstConcatIterator::create_and_seek_to_key(l1_sst, KeySlice::from_slice(key))?
            }
            Bound::Excluded(key) => {
                let mut iter =
                    SstConcatIterator::create_and_seek_to_key(l1_sst, KeySlice::from_slice(key))?;
                if iter.is_valid() && iter.key().raw_ref() == key {
                    iter.next()?;
                }
                iter
            }
            Bound::Unbounded => SstConcatIterator::create_and_seek_to_first(l1_sst)?,
        };
        Ok(iter)
    }

    fn get_all_level_merge_iter(
        &self,
        snapshot: &LsmStorageState,
        lower: Bound<&[u8]>,
        upper: Bound<&[u8]>,
    ) -> Result<MergeIterator<SstConcatIterator>> {
        let mut level_iters = Vec::with_capacity(snapshot.levels.len());
        for i in 1..=snapshot.levels.len() {
            let iter = self.get_level_sst_concat_iter(snapshot, i, lower, upper)?;
            level_iters.push(Box::new(iter));
        }
        Ok(MergeIterator::create(level_iters))
    }
}
