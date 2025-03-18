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

use anyhow::{Ok, Result};

use super::StorageIterator;

/// Merges two iterators of different types into one. If the two iterators have the same key, only
/// produce the key once and prefer the entry from A.
pub struct TwoMergeIterator<A: StorageIterator, B: StorageIterator> {
    a: A,
    b: B,
    // Add fields as need
    next_is_a: bool,
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > TwoMergeIterator<A, B>
{
    pub fn create(a: A, b: B) -> Result<Self> {
        let next_is_a = Self::choose_first(&a, &b);
        Ok(Self { a, b, next_is_a })
    }

    fn choose_first(a: &A, b: &B) -> bool {
        if !b.is_valid() {
            return true;
        }

        if !a.is_valid() {
            return false;
        }

        a.key() <= b.key()
    }
}

impl<
        A: 'static + StorageIterator,
        B: 'static + for<'a> StorageIterator<KeyType<'a> = A::KeyType<'a>>,
    > StorageIterator for TwoMergeIterator<A, B>
{
    type KeyType<'a> = A::KeyType<'a>;

    fn key(&self) -> Self::KeyType<'_> {
        match self.next_is_a {
            true => self.a.key(),
            false => self.b.key(),
        }
    }

    fn value(&self) -> &[u8] {
        match self.next_is_a {
            true => self.a.value(),
            false => self.b.value(),
        }
    }

    fn is_valid(&self) -> bool {
        match self.next_is_a {
            true => self.a.is_valid(),
            false => self.b.is_valid(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self.next_is_a {
            true => {
                if self.b.is_valid() && self.a.key() == self.b.key() {
                    self.b.next()?;
                }
                self.a.next()?;
            }
            false => {
                // no need for self.a.next(), because we always choose self.a first
                self.b.next()?;
            }
        };
        self.next_is_a = Self::choose_first(&self.a, &self.b);
        Ok(())
    }
}
