use std;
use std::cmp::*;
use std::collections::hash_map::*;
use std::collections::BinaryHeap;
use std::hash::Hash;

use fnv;

type Map<K, V> = fnv::FnvHashMap<K, V>;

// for dijkstra search
#[derive(Debug)]
pub struct HeapEntry<K> {
    pub cost: u32,
    pub hop: u16,
    pub key: K,
}
impl<K> PartialEq for HeapEntry<K> {
    fn eq(&self, other: &Self) -> bool {
        other.cost == self.cost
    }
}
impl<K> Eq for HeapEntry<K> {}
impl<K> PartialOrd for HeapEntry<K> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        other.cost.partial_cmp(&self.cost)
    }
}
impl<K> Ord for HeapEntry<K> {
    fn cmp(&self, other: &Self) -> Ordering {
        other.cost.cmp(&self.cost)
    }
}

pub struct Cost<K> {
    pub prev_key: K,
    pub cost: u32,
    pub visited: bool,
}

pub struct Search<K>
where
    K: Hash + Eq,
{
    heap: BinaryHeap<HeapEntry<K>>,
    min_costs: Map<K, Cost<K>>,

    // constraint
    /// do not find path which cost is greater than or *equal* to cost_limit.
    pub cost_limit: u32,
    /// limit maximum # of heap items.
    pub heap_limit: usize,
    pub hop_limit: u16,

    // metrics
    added_count: usize,
}

impl<K> Search<K>
where
    K: Hash + Eq + Copy,
{
    pub fn new() -> Self {
        Self::with_capacity(512)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        let mut min_costs = Map::default();
        min_costs.reserve(capacity);
        Self {
            heap: Default::default(),
            min_costs,

            cost_limit: std::u32::MAX,
            heap_limit: std::usize::MAX,
            hop_limit: std::u16::MAX,

            added_count: 0,
        }
    }

    #[allow(unused)]
    pub fn clear(&mut self) {
        self.heap.clear();
        self.min_costs.clear();
        self.added_count = 0;
    }

    pub fn add_src(&mut self, src: K) {
        // initial condition
        self.heap.push(HeapEntry {
            key: src,
            hop: 0,
            cost: 0,
        });
        self.min_costs.insert(
            src,
            Cost {
                prev_key: src,
                cost: 0,
                visited: false,
            },
        );
    }

    #[inline]
    pub fn next(&mut self) -> Option<HeapEntry<K>> {
        while let Some(entry) = self.heap.pop() {
            if self.cost_limit <= entry.cost {
                //TODO: maybe peek?
                return None;
            }

            // check if key is already visited
            if let Some(m) = self.min_costs.get_mut(&entry.key) {
                if m.visited {
                    continue;
                }
                m.visited = true;
            }
            return Some(entry);
        }
        None
    }

    #[inline]
    pub fn update(&mut self, entry: &HeapEntry<K>, next_key: K, next_cost: u32) -> bool {
        if self.heap_limit <= self.added_count {
            return false;
        }
        if self.hop_limit <= entry.hop {
            return false;
        }
        if self.cost_limit <= next_cost {
            return false;
        }

        let key = entry.key;
        let cost_item = Cost {
            prev_key: key,
            cost: next_cost,
            visited: false,
        };
        match self.min_costs.entry(next_key) {
            Entry::Occupied(mut o) => {
                let m = o.get_mut();
                if m.visited || m.cost <= next_cost {
                    return false;
                }
                *m = cost_item;
            }
            Entry::Vacant(o) => {
                o.insert(cost_item);
            }
        }

        self.added_count += 1;
        self.heap.push(HeapEntry {
            key: next_key,
            hop: entry.hop + 1,
            cost: next_cost,
        });
        true
    }

    #[allow(unused)]
    pub fn decode(&mut self, key: K) -> Vec<K> {
        let mut path = vec![key];
        let mut cur_key = key;
        while let Some(prev_cost) = self.min_costs.get(&cur_key) {
            let prev_key = prev_cost.prev_key;
            if prev_key == cur_key {
                // starting node
                break;
            }
            path.push(prev_key);
            cur_key = prev_key;
        }
        path.reverse();
        path
    }

    pub fn visited_len(&self) -> usize {
        self.min_costs.len()
    }

    pub fn get_cost(&self, k: &K) -> Option<&Cost<K>> {
        self.min_costs.get(k)
    }
}
