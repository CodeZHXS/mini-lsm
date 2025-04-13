// REMOVE THIS LINE after fully implementing this functionality
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

use std::io::{BufWriter, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{bail, Context, Ok, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::block::{SIZEOF_U16, SIZEOF_U32, SIZEOF_U64};
use crate::key::{KeyBytes, KeySlice};

pub struct Wal {
    file: Arc<Mutex<BufWriter<File>>>,
}

impl Wal {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(
                File::options()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .context("failed to create WAL")?,
            ))),
        })
    }

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<KeyBytes, Bytes>) -> Result<Self> {
        let mut file = File::options()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to open WAL")?;
        let mut file_content = vec![];
        file.read_to_end(&mut file_content)?;
        let mut rbuf = &file_content[..];

        while rbuf.has_remaining() {
            let batch_size = rbuf.get_u32() as usize;
            if rbuf.remaining() < batch_size + SIZEOF_U32 {
                bail!("incomplete WAL");
            }

            let mut batch_buf = &rbuf[..batch_size];
            let checksum = crc32fast::hash(batch_buf);

            while batch_buf.has_remaining() {
                let key_len = batch_buf.get_u16() as usize;
                let key = batch_buf.copy_to_bytes(key_len);
                let ts = batch_buf.get_u64();
                let value_len = batch_buf.get_u16() as usize;
                let value = batch_buf.copy_to_bytes(value_len);

                skiplist.insert(KeyBytes::from_bytes_with_ts(key, ts), value);
            }
            rbuf.advance(batch_size);

            if rbuf.get_u32() != checksum {
                bail!("checksum mismatch");
            }
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: KeySlice, value: &[u8]) -> Result<()> {
        self.put_batch(&[(key, value)])
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, data: &[(KeySlice, &[u8])]) -> Result<()> {
        // key_len(2) + key(key_len) + ts(8) + value_len(2) + value(value_len)
        let batch_size = (2 * SIZEOF_U16 + SIZEOF_U64) * data.len()
            + data
                .iter()
                .map(|(key, value)| key.key_len() + value.len())
                .sum::<usize>();

        // batch_size(4) + checksum(4)
        let mut buf = Vec::with_capacity(batch_size + SIZEOF_U32 * 2);
        buf.put_u32(batch_size as u32);

        for (key, value) in data {
            let key_len = key.key_len();
            let value_len = value.len();

            buf.put_u16(key_len as u16);
            buf.put_slice(key.key_ref());
            buf.put_u64(key.ts());
            buf.put_u16(value_len as u16);
            buf.put_slice(value);
        }

        // checksum is not include batch_size
        let checksum = crc32fast::hash(&buf[SIZEOF_U32..]);
        buf.put_u32(checksum);
        self.file.lock().write_all(&buf)?;

        Ok(())
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
