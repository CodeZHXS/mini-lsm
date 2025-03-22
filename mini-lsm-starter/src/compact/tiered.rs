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

use serde::{Deserialize, Serialize};

use crate::lsm_storage::LsmStorageState;

#[derive(Debug, Serialize, Deserialize)]
pub struct TieredCompactionTask {
    pub tiers: Vec<(usize, Vec<usize>)>,
    pub bottom_tier_included: bool,
}

#[derive(Debug, Clone)]
pub struct TieredCompactionOptions {
    pub num_tiers: usize,
    pub max_size_amplification_percent: usize,
    pub size_ratio: usize,
    pub min_merge_width: usize,
    pub max_merge_width: Option<usize>,
}

pub struct TieredCompactionController {
    options: TieredCompactionOptions,
}

impl TieredCompactionController {
    pub fn new(options: TieredCompactionOptions) -> Self {
        assert!(options.min_merge_width <= options.num_tiers);
        Self { options }
    }

    pub fn generate_compaction_task(
        &self,
        snapshot: &LsmStorageState,
    ) -> Option<TieredCompactionTask> {
        let levels = &snapshot.levels;
        let level_size: Vec<f64> = levels.iter().map(|(_, ids)| ids.len() as f64).collect();
        let n = level_size.len();

        if n < self.options.num_tiers {
            return None;
        }

        let upper_size: f64 = level_size.iter().take(n - 1).sum();
        let lower_size = *level_size.last().unwrap();
        let space_amp_ratio = upper_size / lower_size * 100.0;
        if space_amp_ratio >= self.options.max_size_amplification_percent as f64 {
            println!(
                "compaction triggered by space amplification ratio: {}",
                space_amp_ratio
            );
            return Some(TieredCompactionTask {
                tiers: levels.clone(),
                bottom_tier_included: true,
            });
        }

        let size_ratio_trigger = (100.0 + self.options.size_ratio as f64) / 100.0;
        let mut upper_size: f64 = level_size.iter().take(self.options.min_merge_width).sum();
        for i in self.options.min_merge_width..n {
            let lower_size = level_size[i];
            let size_ratio = lower_size / upper_size;
            if size_ratio <= size_ratio_trigger {
                upper_size += lower_size;
            } else {
                println!(
                    "compaction triggered by size ratio: {} > {}",
                    size_ratio * 100.0,
                    size_ratio_trigger * 100.0
                );
                return Some(TieredCompactionTask {
                    tiers: levels[..i].to_vec(),
                    bottom_tier_included: false,
                });
            }
        }

        println!("compaction triggered by reducing sorted runs");
        let num_tiers_to_take = n.min(self.options.max_merge_width.unwrap_or(usize::MAX));
        Some(TieredCompactionTask {
            tiers: levels
                .iter()
                .take(num_tiers_to_take)
                .cloned()
                .collect::<Vec<_>>(),
            bottom_tier_included: n >= num_tiers_to_take,
        })
    }

    pub fn apply_compaction_result(
        &self,
        snapshot: &LsmStorageState,
        task: &TieredCompactionTask,
        output: &[usize],
    ) -> (LsmStorageState, Vec<usize>) {
        let mut snapshot = snapshot.clone();
        let levels = &mut snapshot.levels;
        match task.bottom_tier_included {
            true => {
                let usused_tier = levels.split_off(levels.len() - task.tiers.len());
                let unused_sst_ids = usused_tier
                    .into_iter()
                    .flat_map(|(_, ids)| ids.into_iter())
                    .collect();

                levels.push((output[0], output.to_vec()));
                (snapshot, unused_sst_ids)
            }
            false => {
                let unused_sst_ids = task
                    .tiers
                    .iter()
                    .flat_map(|(_, ids)| ids.iter().cloned())
                    .collect();
                let pos = levels
                    .iter()
                    .position(|(t, _)| *t == task.tiers[0].0)
                    .unwrap();
                let k = task.tiers.len();
                levels.drain(pos..pos + k - 1);
                levels[pos] = (output[0], output.to_vec());
                (snapshot, unused_sst_ids)
            }
        }
    }
}
