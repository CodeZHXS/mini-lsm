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

use std::sync::Arc;

use anyhow::Result;

use super::StorageIterator;
use crate::{
    key::KeySlice,
    table::{SsTable, SsTableIterator},
};

/// Concat multiple iterators ordered in key order and their key ranges do not overlap. We do not want to create the
/// iterators when initializing this iterator to reduce the overhead of seeking.
pub struct SstConcatIterator {
    current: Option<SsTableIterator>,
    next_sst_idx: usize,
    sstables: Vec<Arc<SsTable>>,
}

impl SstConcatIterator {
    pub fn create_and_seek_to_first(sstables: Vec<Arc<SsTable>>) -> Result<Self> {
        let mut current = None;
        let mut next_sst_idx = 0;

        if !sstables.is_empty() {
            let iter = SsTableIterator::create_and_seek_to_first(sstables[0].clone())?;
            assert!(iter.is_valid());
            current = Some(iter);
            next_sst_idx = 1;
        }

        Ok(Self {
            current,
            next_sst_idx,
            sstables,
        })
    }

    pub fn create_and_seek_to_key(sstables: Vec<Arc<SsTable>>, key: KeySlice) -> Result<Self> {
        let mut l = 0;
        let mut r = sstables.len();
        let index = loop {
            if l == r {
                break l;
            }
            let mid = (l + r) / 2;
            let table = sstables[mid].as_ref();
            if table.first_key().as_key_slice() <= key {
                if key <= table.last_key().as_key_slice() {
                    break mid;
                } else {
                    l = mid + 1;
                }
            } else {
                r = mid;
            }
        };

        let mut current = None;
        let mut next_sst_idx = sstables.len();
        if index < sstables.len() {
            let iter = SsTableIterator::create_and_seek_to_key(sstables[index].clone(), key)?;
            assert!(iter.is_valid());
            current = Some(iter);
            next_sst_idx = index + 1;
        }

        Ok(Self {
            current,
            next_sst_idx,
            sstables,
        })
    }
}

impl StorageIterator for SstConcatIterator {
    type KeyType<'a> = KeySlice<'a>;

    fn key(&self) -> KeySlice {
        self.current.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.current.as_ref().unwrap().value()
    }

    fn is_valid(&self) -> bool {
        self.current.is_some()
    }

    fn next(&mut self) -> Result<()> {
        self.current.as_mut().unwrap().next()?;
        if !self.current.as_ref().unwrap().is_valid() {
            self.current = if self.next_sst_idx == self.sstables.len() {
                None
            } else {
                let iter = SsTableIterator::create_and_seek_to_first(
                    self.sstables[self.next_sst_idx].clone(),
                )?;
                assert!(iter.is_valid());
                self.next_sst_idx += 1;
                Some(iter)
            }
        }
        Ok(())
    }

    fn num_active_iterators(&self) -> usize {
        1
    }
}
