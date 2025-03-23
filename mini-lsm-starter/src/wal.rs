#![allow(dead_code)]
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
#![allow(unused_variables)] // TODO(you): remove this lint after implementing this mod
#![allow(dead_code)] // TODO(you): remove this lint after implementing this mod

use std::io::{BufWriter, Read};
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{Context, Ok, Result};
use bytes::{Buf, BufMut, Bytes};
use crossbeam_skiplist::SkipMap;
use parking_lot::Mutex;

use crate::block::SIZEOF_U16;

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

    pub fn recover(path: impl AsRef<Path>, skiplist: &SkipMap<Bytes, Bytes>) -> Result<Self> {
        let mut file = File::options()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to open WAL")?;
        let mut buf = vec![];
        file.read_to_end(&mut buf)?;
        let mut rbuf = &buf[..];
        while rbuf.has_remaining() {
            let key_len = rbuf.get_u16() as usize;
            let key = rbuf.copy_to_bytes(key_len);
            let value_len = rbuf.get_u16() as usize;
            let value = rbuf.copy_to_bytes(value_len);
            skiplist.insert(key, value);
        }
        Ok(Self {
            file: Arc::new(Mutex::new(BufWriter::new(file))),
        })
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<()> {
        let key_len = key.len();
        let value_len = value.len();
        let mut buf = Vec::with_capacity(2 * SIZEOF_U16 + key_len + value_len);
        buf.put_u16(key_len as u16);
        buf.put_slice(key);
        buf.put_u16(value_len as u16);
        buf.put_slice(value);
        self.file.lock().write_all(&buf)?;
        Ok(())
    }

    /// Implement this in week 3, day 5.
    pub fn put_batch(&self, _data: &[(&[u8], &[u8])]) -> Result<()> {
        unimplemented!()
    }

    pub fn sync(&self) -> Result<()> {
        let mut file = self.file.lock();
        file.flush()?;
        file.get_mut().sync_all()?;
        Ok(())
    }
}
