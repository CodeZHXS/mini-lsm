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

use std::collections::HashSet;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::{key::Key, lsm_storage::LsmStorageState};

#[derive(Debug, Serialize, Deserialize)]
pub struct LeveledCompactionTask {
    // if upper_level is `None`, then it is L0 compaction
    pub upper_level: Option<usize>,
    pub upper_level_sst_ids: Vec<usize>,
    pub lower_level: usize,
    pub lower_level_sst_ids: Vec<usize>,
    pub is_lower_level_bottom_level: bool,
}

#[derive(Debug, Clone)]
pub struct LeveledCompactionOptions {
    pub level_size_multiplier: usize,
    pub level0_file_num_compaction_trigger: usize,
    pub max_levels: usize,
    pub base_level_size_mb: usize,
}

pub struct LeveledCompactionController {
    options: LeveledCompactionOptions,
}

impl LeveledCompactionController {
    pub fn new(options: LeveledCompactionOptions) -> Self {
        Self { options }
    }

    fn find_overlapping_range(
        &self,
        snapshot: &LsmStorageState,
        first_key: &Key<Bytes>,
        last_key: &Key<Bytes>,
        in_level: usize,
    ) -> (usize, usize) {
        let sstables = &snapshot.sstables;
        let level_ids = &snapshot.levels[in_level - 1].1;

        let start = level_ids.partition_point(|id| sstables[id].last_key() < first_key);
        let end = level_ids.partition_point(|id| sstables[id].first_key() <= last_key);

        (start, end)
    }

    fn find_overlapping_ssts(
        &self,
        snapshot: &LsmStorageState,
        first_key: &Key<Bytes>,
        last_key: &Key<Bytes>,
        in_level: usize,
    ) -> Vec<usize> {
        let (start, end) = self.find_overlapping_range(snapshot, first_key, last_key, in_level);
        snapshot.levels[in_level - 1].1[start..end].to_vec()
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<LeveledCompactionTask> {
        let sstables = &snapshot.sstables;
        let levels = &snapshot.levels;
        let n = levels.len();

        let mut current_size = Vec::with_capacity(self.options.max_levels);
        for i in 0..self.options.max_levels {
            current_size.push(
                snapshot.levels[i]
                    .1
                    .iter()
                    .map(|x| snapshot.sstables.get(x).unwrap().table_size())
                    .sum::<u64>() as usize,
            );
        }

        let base_level_size_bytes = self.options.base_level_size_mb * 1024 * 1024;
        let mut next_target_size = current_size[n - 1].max(base_level_size_bytes);
        let mut target_size = vec![0; n];
        let mut base_level = 1;

        for i in (0..n).rev() {
            target_size[i] = next_target_size;
            next_target_size /= self.options.level_size_multiplier;
            if target_size[i] <= base_level_size_bytes {
                base_level = i + 1;
                break;
            }
        }

        if snapshot.l0_sstables.len() >= self.options.level0_file_num_compaction_trigger {
            println!("flush L0 SST to base level {}", base_level);
            let upper_level_sst_ids = snapshot.l0_sstables.clone();
            let first_key = upper_level_sst_ids
                .iter()
                .map(|id| sstables[id].first_key())
                .min()
                .unwrap();
            let last_key = upper_level_sst_ids
                .iter()
                .map(|id| sstables[id].last_key())
                .max()
                .unwrap();
            let lower_level_sst_ids =
                self.find_overlapping_ssts(snapshot, first_key, last_key, base_level);
            return Some(LeveledCompactionTask {
                upper_level: None,
                upper_level_sst_ids,
                lower_level: base_level,
                lower_level_sst_ids,
                is_lower_level_bottom_level: base_level == n - 1,
            });
        }

        let mut priorities = Vec::with_capacity(n);

        for i in base_level..n {
            if current_size[i - 1] <= target_size[i - 1] {
                continue;
            }
            let current_priority = current_size[i - 1] as f64 / target_size[i - 1] as f64;
            priorities.push((current_priority, i));
        }

        priorities.sort_by(|a, b| a.partial_cmp(b).unwrap().reverse());

        let priority = priorities.first();
        if let Some((_, upper_level)) = priority {
            println!(
                "target level sizes: {:?}, real level sizes: {:?}, base_level: {}",
                target_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                current_size
                    .iter()
                    .map(|x| format!("{:.3}MB", *x as f64 / 1024.0 / 1024.0))
                    .collect::<Vec<_>>(),
                base_level,
            );

            let oldest_sst_id = levels[upper_level - 1]
                .1
                .iter()
                .map(|id| sstables[id].sst_id())
                .min()
                .unwrap();

            println!(
                "compaction triggered by priority: {upper_level} out of {:?}, select {oldest_sst_id} for compaction",
                priorities
            );
            let upper_level_sst_ids = vec![oldest_sst_id];
            let first_key = sstables[&oldest_sst_id].first_key();
            let last_key = sstables[&oldest_sst_id].last_key();
            let lower_level_sst_ids =
                self.find_overlapping_ssts(snapshot, first_key, last_key, upper_level + 1);
            return Some(LeveledCompactionTask {
                upper_level: Some(*upper_level),
                upper_level_sst_ids,
                lower_level: upper_level + 1,
                lower_level_sst_ids,
                is_lower_level_bottom_level: upper_level + 1 == n,
            });
        }

        None
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &LeveledCompactionTask,
        output: &[usize],
        in_recovery: bool,
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let unused_sst_ids = task
            .upper_level_sst_ids
            .iter()
            .chain(task.lower_level_sst_ids.iter())
            .cloned()
            .collect();

        if in_recovery {
            match task.upper_level {
                Some(upper_level) => {
                    let set = task
                        .upper_level_sst_ids
                        .iter()
                        .cloned()
                        .collect::<HashSet<usize>>();
                    snapshot.levels[upper_level - 1]
                        .1
                        .retain(|id| !set.contains(id));
                }
                None => {
                    let l0_truncate_len =
                        snapshot.l0_sstables.len() - task.upper_level_sst_ids.len();
                    snapshot.l0_sstables.truncate(l0_truncate_len);
                }
            }

            let set = task
                .lower_level_sst_ids
                .iter()
                .cloned()
                .collect::<HashSet<usize>>();
            snapshot.levels[task.lower_level - 1]
                .1
                .retain(|id| !set.contains(id));
            snapshot.levels[task.lower_level - 1]
                .1
                .extend(output.iter().cloned());
            return (snapshot, unused_sst_ids);
        }

        let (upper_first_key, upper_last_key) = match task.upper_level {
            Some(upper_level) => {
                assert!(task.upper_level_sst_ids.len() == 1);
                let upper_first_key = snapshot.sstables[&task.upper_level_sst_ids[0]].first_key();
                let upper_last_key = snapshot.sstables[&task.upper_level_sst_ids[0]].last_key();

                let (beg, end) = self.find_overlapping_range(
                    &snapshot,
                    upper_first_key,
                    upper_last_key,
                    upper_level,
                );
                snapshot.levels[upper_level - 1].1.drain(beg..end);
                (upper_first_key, upper_last_key)
            }
            None => {
                let l0_truncate_len = snapshot.l0_sstables.len() - task.upper_level_sst_ids.len();
                snapshot.l0_sstables.truncate(l0_truncate_len);
                let upper_first_key = task
                    .upper_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables[id].first_key())
                    .min()
                    .unwrap();
                let upper_last_key = task
                    .upper_level_sst_ids
                    .iter()
                    .map(|id| snapshot.sstables[id].last_key())
                    .max()
                    .unwrap();
                (upper_first_key, upper_last_key)
            }
        };

        let low_level = task.lower_level;
        let (first_key, last_key) = if task.lower_level_sst_ids.is_empty() {
            (upper_first_key, upper_last_key)
        } else {
            let lower_first_key =
                snapshot.sstables[task.lower_level_sst_ids.first().unwrap()].first_key();
            let lower_last_key =
                snapshot.sstables[task.lower_level_sst_ids.last().unwrap()].last_key();
            (
                upper_first_key.max(lower_first_key),
                upper_last_key.min(lower_last_key),
            )
        };

        let (beg, end) = self.find_overlapping_range(&snapshot, first_key, last_key, low_level);
        snapshot.levels[low_level - 1]
            .1
            .splice(beg..end, output.iter().cloned());

        (snapshot, unused_sst_ids)
    }
}
