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

use std::io::Read;
use std::path::Path;
use std::sync::Arc;
use std::{fs::File, io::Write};

use anyhow::{bail, Context, Result};
use bytes::{Buf, BufMut};
use parking_lot::{Mutex, MutexGuard};
use serde::{Deserialize, Serialize};

use crate::compact::CompactionTask;

pub struct Manifest {
    file: Arc<Mutex<File>>,
}

#[derive(Serialize, Deserialize)]
pub enum ManifestRecord {
    Flush(usize),
    NewMemtable(usize),
    Compaction(CompactionTask, Vec<usize>),
}

impl Manifest {
    pub fn create(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: Arc::new(Mutex::new(
                File::options()
                    .read(true)
                    .write(true)
                    .create_new(true)
                    .open(path)
                    .context("failed to create manifest")?,
            )),
        })
    }

    pub fn recover(path: impl AsRef<Path>) -> Result<(Self, Vec<ManifestRecord>)> {
        let mut file = File::options()
            .read(true)
            .append(true)
            .open(path)
            .context("failed to create manifest")?;
        let mut data = vec![];
        file.read_to_end(&mut data)?;
        let mut buf = data.as_slice();
        let mut records = vec![];

        while buf.has_remaining() {
            let record_len = buf.get_u64();
            let record_raw = &buf[..record_len as usize];
            let record = serde_json::from_slice(record_raw)?;

            buf.advance(record_len as usize);
            let checksum = buf.get_u32();
            if checksum != crc32fast::hash(record_raw) {
                bail!("checksum mismatched!");
            }
            records.push(record);
        }

        Ok((
            Self {
                file: Arc::new(Mutex::new(file)),
            },
            records,
        ))
    }

    pub fn add_record(
        &self,
        _state_lock_observer: &MutexGuard<()>,
        record: ManifestRecord,
    ) -> Result<()> {
        self.add_record_when_init(record)
    }

    pub fn add_record_when_init(&self, record: ManifestRecord) -> Result<()> {
        let mut data = serde_json::to_vec(&record)?;
        let checksum = crc32fast::hash(&data);
        let mut file = self.file.lock();
        file.write_all(&(data.len() as u64).to_be_bytes())?;
        data.put_u32(checksum);
        file.write_all(&data)?;
        file.sync_all()?;
        Ok(())
    }
}
