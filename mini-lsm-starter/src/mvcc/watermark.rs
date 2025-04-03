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

use std::collections::VecDeque;

pub struct Watermark {
    readers: VecDeque<(u64, usize)>,
    active_ts: usize,
}

impl Watermark {
    pub fn new() -> Self {
        Self {
            readers: VecDeque::new(),
            active_ts: 0,
        }
    }

    pub fn add_reader(&mut self, ts: u64) {
        if let Some((back_ts, back_cnt)) = self.readers.back_mut() {
            if *back_ts == ts {
                *back_cnt += 1;
            } else {
                self.readers.push_back((ts, 1));
            }
        } else {
            self.readers.push_back((ts, 1));
        }

        if self.readers.back().unwrap().1 == 1 {
            self.active_ts += 1;
        }
    }

    pub fn remove_reader(&mut self, ts: u64) {
        let index = ts - self.readers.front().unwrap().0;
        assert!(index < self.readers.len() as u64);
        self.readers[index as usize].1 -= 1;

        if self.readers[index as usize].1 == 0 {
            self.active_ts -= 1;
        }

        while !self.readers.is_empty() && self.readers.front().unwrap().1 == 0 {
            self.readers.pop_front();
        }
    }

    pub fn num_retained_snapshots(&self) -> usize {
        self.active_ts
    }

    pub fn watermark(&self) -> Option<u64> {
        self.readers.front().map(|(ts, _)| *ts)
    }
}
