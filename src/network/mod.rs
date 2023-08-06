use super::*;
use rayon::prelude::*;

pub mod road;
pub mod shp;
pub mod walk;

#[derive(Clone, Copy, PartialOrd, Ord, PartialEq, Eq, Debug, Serialize, Deserialize)]
pub struct IdxNodeKey(u32);
impl std::hash::Hash for IdxNodeKey {
    #[inline]
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        state.write_u32(self.0)
    }
}
impl IdxNodeKey {
    #[inline]
    pub fn new(val: usize) -> Self {
        IdxNodeKey(val as u32)
    }
    #[inline]
    pub fn index(&self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum IdxLinkDir {
    Forward = 1,
    Backward = 2,
}
impl IdxLinkDir {
    fn from_u32(val: u32) -> Self {
        match val {
            1 => IdxLinkDir::Forward,
            2 => IdxLinkDir::Backward,
            _ => panic!("invalid val for IdxLinkDir: {}", val),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdxLink {
    pub enode_idx: IdxNodeKey,
    // Lower 30 bits store cost for link, upper 2 bits store direction.
    cost: u32,
}
impl IdxLink {
    pub fn new(enode_idx: IdxNodeKey, cost: u32, dir: IdxLinkDir) -> Self {
        let dir = (dir as u32) << 30;
        let cost = dir | (cost & ((1 << 30) - 1));
        Self { enode_idx, cost }
    }
    #[inline]
    pub fn dir(&self) -> IdxLinkDir {
        IdxLinkDir::from_u32(self.cost >> 30)
    }
    #[inline]
    pub fn cost(&self) -> u32 {
        self.cost & ((1 << 30) - 1)
    }
    #[inline]
    pub fn set_cost(&mut self, cost: u32) {
        let dir = self.cost & (0b11 << 30);
        let cost = cost & ((1 << 30) - 1);
        self.cost = dir | cost;
    }
}
impl PartialOrd for IdxLink {
    #[inline]
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.dir().partial_cmp(&other.dir())
    }
}
impl Ord for IdxLink {
    #[inline]
    fn cmp(&self, other: &Self) -> Ordering {
        self.dir().cmp(&other.dir())
    }
}

pub struct Graph {
    pub node_len: usize,
    pub idx_links: Vec<Vec<IdxLink>>,
}

pub fn map_rev(list: &[Vec<IdxLink>]) -> Vec<Vec<IdxLink>> {
    let len = list.len();
    let mut rev: Vec<Vec<IdxLink>> = Vec::new();
    rev.resize(len, Vec::new());
    for (from_idx, links) in list.iter().enumerate() {
        let snode_idx = IdxNodeKey::new(from_idx);
        for link in links.iter() {
            let link_rev = IdxLink::new(snode_idx, link.cost(), IdxLinkDir::Backward);
            rev[link.enode_idx.index()].push(link_rev);
        }
    }
    rev
}

impl Graph {
    fn from_links(mut idx_links: Vec<Vec<IdxLink>>) -> Self {
        let len = idx_links.len();
        let mut idx_links_rev = map_rev(idx_links.as_slice());

        for (i, v) in idx_links.iter_mut().enumerate() {
            v.append(&mut idx_links_rev[i]);
        }

        if false {
            let _count: usize = idx_links
                .par_iter()
                .enumerate()
                .map(|(i, links)| {
                    let idx = IdxNodeKey::new(i);
                    for link in links {
                        let pair_links = &idx_links[link.enode_idx.index()];
                        pair_links
                            .iter()
                            .find(|pair| pair.enode_idx == idx && pair.dir() != link.dir())
                            .unwrap();
                    }
                    0
                })
                .sum();
        }

        Self {
            node_len: len,
            idx_links,
        }
    }

    fn search_step(
        &self,
        search: &mut dijkstra::Search<IdxNodeKey>,
        dir: IdxLinkDir,
    ) -> Option<dijkstra::HeapEntry<IdxNodeKey>> {
        if let Some(entry) = search.next() {
            let key = entry.key;

            for idx_link in self.idx_links[key.index()].iter() {
                if idx_link.dir() != dir {
                    continue;
                }
                let next_key = idx_link.enode_idx;
                let next_cost = entry.cost + idx_link.cost();
                search.update(&entry, next_key, next_cost);
            }
            return Some(entry);
        }
        None
    }

    pub fn search(&self, src: IdxNodeKey, dst: IdxNodeKey) -> Option<(Vec<IdxNodeKey>, u32)> {
        let mut search = dijkstra::Search::new();
        search.add_src(src);

        while let Some(entry) = self.search_step(&mut search, IdxLinkDir::Forward) {
            if entry.key == dst {
                let decoded = search.decode(dst);
                return Some((decoded, entry.cost));
            }
        }
        None
    }

    pub fn search_bidir(&self, src: IdxNodeKey, dst: IdxNodeKey) -> Option<(Vec<IdxNodeKey>, u32)> {
        let mut search_f = dijkstra::Search::new();
        search_f.add_src(src);

        let mut search_b = dijkstra::Search::new();
        search_b.add_src(dst);

        let mut costs = Vec::new();
        costs.resize(self.node_len, 0);

        let entry = 'outer: loop {
            let mut updated = false;
            if let Some(entry) = self.search_step(&mut search_f, IdxLinkDir::Forward) {
                updated = true;

                let idx = entry.key.0 as usize;
                if costs[idx] != 0 {
                    break 'outer Some(entry);
                }
                costs[idx] = entry.cost;
            }

            while search_b.visited_len() < search_f.visited_len() {
                if let Some(entry) = self.search_step(&mut search_b, IdxLinkDir::Backward) {
                    updated = true;

                    let idx = entry.key.0 as usize;
                    if costs[idx] != 0 {
                        break 'outer Some(entry);
                    }
                    costs[idx] = entry.cost;
                } else {
                    break;
                }
            }
            if !updated {
                break None;
            }
        };

        entry.map(move |entry| {
            let mut path_f = search_f.decode(entry.key);
            let mut path_b = search_b.decode(entry.key);
            path_b.reverse();
            path_f.pop();
            path_f.append(&mut path_b);

            let idx = entry.key.0 as usize;
            (path_f, entry.cost + costs[idx])
        })
    }
}
