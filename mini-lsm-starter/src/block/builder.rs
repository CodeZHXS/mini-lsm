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

use bytes::BufMut;

use crate::{
    block::SIZEOF_U16,
    key::{KeySlice, KeyVec},
};

use super::{Block, SIZEOF_U64};

/// Builds a block.
pub struct BlockBuilder {
    /// Offsets of each key-value entries.
    offsets: Vec<u16>,
    /// All serialized key-value pairs in the block.
    data: Vec<u8>,
    /// The expected block size.
    block_size: usize,
    /// The first key in the block
    first_key: KeyVec,
}

impl BlockBuilder {
    /// Creates a new block builder.
    pub fn new(block_size: usize) -> Self {
        Self {
            offsets: vec![],
            data: vec![],
            block_size,
            first_key: KeyVec::new(),
        }
    }

    /// Adds a key-value pair to the block. Returns false when the block is full.
    #[must_use]
    pub fn add(&mut self, key: KeySlice, value: &[u8]) -> bool {
        let ts = key.ts();
        let key_ref = key.key_ref();
        let key_len = key_ref.len() as u16;
        let value_len = value.len() as u16;

        if self.is_empty() {
            self.add_kv(0, key_len, key_ref, ts, value_len, value);
            self.first_key = key.to_key_vec();
            return true;
        }

        let key_overlap_len = self.get_overlap_len(key);
        let key_rest_len = key_len - key_overlap_len;

        if self.current_block_size()
            + (key_rest_len + value_len) as usize
            + 4 * SIZEOF_U16 // offset(u16) + key_overlap_len(u16) + key_rest_len(u16) + value_len(u16) = 4 * SIZEOF_U16
            + SIZEOF_U64 // ts(u64) = SIZEOF_U64
            > self.block_size
        {
            return false;
        }

        self.add_kv(key_overlap_len, key_rest_len, key_ref, ts, value_len, value);
        true
    }

    /// Check if there is no key-value pair in the block.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Finalize the block.
    pub fn build(self) -> Block {
        Block {
            data: self.data,
            offsets: self.offsets,
        }
    }

    fn current_block_size(&self) -> usize {
        self.data.len() + self.offsets.len()
    }

    fn get_overlap_len(&self, key: KeySlice) -> u16 {
        let first_key = self.first_key.key_ref();
        let key = key.key_ref();
        let mut i = 0;
        while i < first_key.len() && i < key.len() && first_key[i] == key[i] {
            i += 1;
        }
        i as u16
    }

    fn add_kv(
        &mut self,
        key_overlap_len: u16,
        key_rest_len: u16,
        key: &[u8],
        ts: u64,
        value_len: u16,
        value: &[u8],
    ) {
        self.offsets.push(self.data.len() as u16);
        self.data.put_u16(key_overlap_len);
        self.data.put_u16(key_rest_len);
        self.data.put(&key[key_overlap_len as usize..]);
        self.data.put_u64(ts);
        self.data.put_u16(value_len);
        self.data.put(value);
    }
}
