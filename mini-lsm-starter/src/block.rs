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

pub(crate) const SIZEOF_U8: usize = std::mem::size_of::<u8>();
pub(crate) const SIZEOF_U16: usize = std::mem::size_of::<u16>();
pub(crate) const SIZEOF_U32: usize = std::mem::size_of::<u32>();

mod builder;
mod iterator;

pub use builder::BlockBuilder;
use bytes::{Buf, BufMut, Bytes};
pub use iterator::BlockIterator;

use crate::key::KeyVec;

/// A block is the smallest unit of read and caching in LSM tree. It is a collection of sorted key-value pairs.
pub struct Block {
    pub(crate) data: Vec<u8>,
    pub(crate) offsets: Vec<u16>,
}

impl Block {
    /// Encode the internal data to the data layout illustrated in the course
    /// Note: You may want to recheck if any of the expected field is missing from your output
    pub fn encode(&self) -> Bytes {
        let mut buf = self.data.clone();
        for x in self.offsets.iter() {
            buf.put_u16(*x);
        }
        buf.put_u16(self.offsets.len() as u16);
        buf.into()
    }

    /// Decode from the data layout, transform the input `data` to a single `Block`
    pub fn decode(data: &[u8]) -> Self {
        let n = data.len();
        let num_of_elements = (&data[n - SIZEOF_U16..]).get_u16() as usize;
        let data_end = n - (num_of_elements + 1) * SIZEOF_U16;
        let offsets_raw = &data[data_end..data.len() - SIZEOF_U16];
        let offsets = offsets_raw
            .chunks(SIZEOF_U16)
            .map(|mut x| x.get_u16())
            .collect();
        let data = data[..data_end].to_vec();
        Self { data, offsets }
    }

    pub fn get_key(&self, nth: usize) -> KeyVec {
        if nth >= self.offsets.len() {
            return KeyVec::new();
        }
        let (key_overlap_len, key_rest_len) = self.get_key_len(nth);
        let offset = self.offsets[nth] as usize;
        let mut buf = self.first_key_prefix(key_overlap_len).to_vec();
        buf.extend(self.get_data_by_range((
            offset + 2 * SIZEOF_U16,
            offset + 2 * SIZEOF_U16 + key_rest_len,
        )));
        KeyVec::from_vec(buf)
    }

    pub fn get_value_range(&self, nth: usize) -> (usize, usize) {
        if nth >= self.offsets.len() {
            return (0, 0);
        }
        let (_, key_rest_len) = self.get_key_len(nth);
        let offset = self.offsets[nth] as usize;
        let value_offset = offset + 2 * SIZEOF_U16 + key_rest_len;
        let value_len = self
            .get_data_by_range((value_offset, value_offset + SIZEOF_U16))
            .get_u16() as usize;
        (
            value_offset + SIZEOF_U16,
            value_offset + SIZEOF_U16 + value_len,
        )
    }

    pub fn get_element_nums(&self) -> usize {
        self.offsets.len()
    }

    fn get_key_len(&self, nth: usize) -> (usize, usize) {
        let offset = self.offsets[nth] as usize;
        let key_overlap_len = self
            .get_data_by_range((offset, offset + SIZEOF_U16))
            .get_u16() as usize;
        let key_rest_len = self
            .get_data_by_range((offset + SIZEOF_U16, offset + 2 * SIZEOF_U16))
            .get_u16() as usize;
        (key_overlap_len, key_rest_len)
    }

    fn get_data_by_range(&self, range: (usize, usize)) -> &[u8] {
        &self.data[range.0..range.1]
    }

    fn first_key_prefix(&self, key_overlap_len: usize) -> &[u8] {
        self.get_data_by_range((2 * SIZEOF_U16, 2 * SIZEOF_U16 + key_overlap_len))
    }
}
