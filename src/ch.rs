use anyhow::Result;
use ordslice::Ext;
use rayon::prelude::*;

use super::*;

use fnv::*;

#[derive(Default)]
struct Stat {
    val: i64,
    count: usize,
}
impl Stat {
    fn push(&mut self, val: i64) {
        self.val += val;
        self.count += 1;
    }
    fn avg(&self) -> f64 {
        if self.count == 0 {
            0f64
        } else {
            self.val as f64 / self.count as f64
        }
    }
    fn clear(&mut self) {
        self.val = 0;
        self.count = 0;
    }
}

#[derive(PartialEq, Eq, Debug)]
struct CHEntry {
    score: i32,
    key: IdxNodeKey,
}
impl PartialOrd for CHEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        self.score.partial_cmp(&other.score)
    }
}
impl Ord for CHEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score.cmp(&other.score)
    }
}

#[derive(Serialize, Deserialize)]
pub struct CHContraction {
    snode_idx: IdxNodeKey,
    enode_idx: IdxNodeKey,
    mnode_idx: IdxNodeKey,
    length: u32,
}

/// data structure for contraction hierarchies
pub struct CH<'a> {
    graph: &'a Graph,

    /// order of each node, starting from 1. 0 if given node is not ordered yet
    order: Vec<u32>,

    /// contractions
    contractions: Vec<Vec<IdxLink>>,

    all_contractions: Vec<CHContraction>,
}

pub fn filter_order(links: Vec<Vec<IdxLink>>, order: &[u32]) -> Vec<Vec<IdxLink>> {
    links
        .into_par_iter()
        .enumerate()
        .map(|(i, v)| {
            v.into_iter()
                .filter(|l| order[l.enode_idx.index()] > order[i])
                .collect::<Vec<_>>()
        })
        .collect::<Vec<_>>()
}

impl<'a> CH<'a> {
    pub fn new(graph: &'a Graph) -> Self {
        let node_len = graph.node_len;

        let mut order = Vec::with_capacity(node_len);
        order.resize(node_len, std::u32::MAX);

        Self {
            graph,
            order,
            contractions: graph.idx_links.clone(),
            all_contractions: Vec::new(),
        }
    }

    pub fn from_file<P>(graph: &'a Graph, path: P) -> Result<Self>
    where
        P: AsRef<Path>,
    {
        let node_len = graph.node_len;

        let path = path.as_ref();
        let mut all_contractions: Vec<CHContraction> = decode_csv(path.join("contractions.csv"))?;
        let order: Vec<u32> = decode_csv_noheader(path.join("order.csv"))?;

        all_contractions.sort_unstable_by_key(|c| c.snode_idx);

        let mut contractions = Vec::with_capacity(node_len);
        contractions.resize(node_len, Vec::new());

        let link_partitions =
            partition::partition_range_by_key(all_contractions.as_slice(), |c| c.snode_idx);
        for (k, r) in link_partitions.into_iter() {
            let mut v = all_contractions[r]
                .iter()
                .map(|c| IdxLink::new(c.enode_idx, c.length, IdxLinkDir::Forward))
                .collect::<Vec<_>>();

            let idx = k.index();
            v.extend_from_slice(&graph.idx_links[idx]);
            contractions[idx] = v;
        }

        let contractions_rev = map_rev(contractions.as_slice());

        let mut contractions = filter_order(contractions, order.as_slice());
        let mut contractions_rev = filter_order(contractions_rev, order.as_slice());

        for (i, v) in contractions.iter_mut().enumerate() {
            v.append(&mut contractions_rev[i]);
        }

        Ok(Self {
            graph,
            order,
            contractions,
            all_contractions,
        })
    }

    pub fn write<P>(&self, path: P) -> Result<()>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();
        encode_csv(
            path.join("contractions.csv"),
            self.all_contractions.as_slice(),
        )?;
        encode_csv(path.join("order.csv"), self.order.as_slice())?;
        Ok(())
    }
}

impl<'a> CH<'a> {
    fn add_contraction_dir(
        &mut self,
        snode_idx: IdxNodeKey,
        enode_idx: IdxNodeKey,
        length: u32,
        dir: IdxLinkDir,
    ) {
        let v = &mut self.contractions[snode_idx.index()];
        let mut found = false;
        if let Some(prev_link) = v
            .iter_mut()
            .find(|l| l.enode_idx == enode_idx && l.dir() == dir)
        {
            if prev_link.cost() > length {
                prev_link.set_cost(length);
            }
            found = true;
        }
        if !found {
            let link = IdxLink::new(enode_idx, length, dir);
            let idx = v.upper_bound(&link);
            v.insert(idx, link);
        }
    }

    fn add_contraction(&mut self, c: CHContraction) {
        self.add_contraction_dir(c.snode_idx, c.enode_idx, c.length, IdxLinkDir::Forward);
        self.add_contraction_dir(c.enode_idx, c.snode_idx, c.length, IdxLinkDir::Backward);

        self.all_contractions.push(c);
    }

    fn gc_contraction(&mut self, key: IdxNodeKey, contractions: &[IdxLink]) {
        for link in contractions.iter() {
            let other_contractions = &mut self.contractions[link.enode_idx.index()];
            let idx = other_contractions
                .iter()
                .position(|c| c.enode_idx == key && c.dir() != link.dir());

            match idx {
                Some(idx) => other_contractions.remove(idx),
                None => {
                    panic!(
                        "invalid contraction: key={:?}, link={:?}, from={:?}, to={:?}",
                        key, link, contractions, other_contractions
                    );
                }
            };
        }
    }

    fn nearby_nodes(&self, key: IdxNodeKey, dir: IdxLinkDir) -> Vec<IdxLink> {
        let idx = key.index();
        let raw_links = &self.contractions[idx];
        let mut links: Vec<IdxLink> = Vec::with_capacity(raw_links.len());
        for link in raw_links.iter() {
            if link.dir() != dir {
                continue;
            }
            links.push(*link);
        }
        links
    }

    /// simple 1-N dijkstra impl.
    #[allow(unused)]
    fn dijkstra_contract_dist_backward1(
        &self,
        src: IdxNodeKey,
        from_link: IdxLink,
        neighbors: &[IdxLink],
        hop_limit: u16,
    ) -> Vec<u32> {
        let dsts_map = neighbors
            .iter()
            .enumerate()
            .map(|(i, link)| (link.enode_idx, i))
            .collect::<FnvHashMap<_, _>>();
        if dsts_map.len() != neighbors.len() {
            panic!("invalid neighbors: {:?}", neighbors);
        }

        let max_cost = neighbors
            .iter()
            .map(|link| link.cost())
            .max()
            .expect("invalid neighbor count");

        let mut search = dijkstra::Search::with_capacity(1024);
        search.heap_limit = 1024;
        search.hop_limit = hop_limit;
        search.add_src(from_link.enode_idx);
        search.cost_limit = from_link.cost() + max_cost;

        // build distances
        let dst_len = dsts_map.len();
        let mut costs = Vec::with_capacity(dst_len);
        let mut found = 0;
        costs.resize(dst_len, std::u32::MAX);

        while let Some(entry) = search.next() {
            let key = entry.key;
            if key == src {
                continue;
            }

            if let Some(idx) = dsts_map.get(&key) {
                costs[*idx] = entry.cost;
                found += 1;
                if found == dst_len {
                    break;
                }
            }

            // same with nearby_nodes, but inline to remove allocation
            for link in &self.contractions[key.index()] {
                if link.dir() != IdxLinkDir::Forward {
                    break;
                }
                // assert!(self.order[link.enode_idx.index()] > order);

                let next_key = link.enode_idx;
                let next_cost = entry.cost + link.cost();
                search.update(&entry, next_key, next_cost);
            }
        }

        costs
    }

    /// asymmetric bidirection dijkstra impl. described in paper.
    #[allow(unused)]
    fn dijkstra_contract_dist_backward2(
        &self,
        src: IdxNodeKey,
        from_link: IdxLink,
        neighbors: &[IdxLink],
        hop_limit: u16,
    ) -> Vec<u32> {
        let mut bucket = Vec::new();
        let mut backwards: FnvHashSet<_> = Default::default();

        for (i, neighbor) in neighbors.iter().enumerate() {
            for next_neighbor in self.contractions[neighbor.enode_idx.index()].iter() {
                if next_neighbor.dir() == IdxLinkDir::Forward {
                    continue;
                }
                bucket.push((next_neighbor.enode_idx, i, next_neighbor.cost()));
                backwards.insert(next_neighbor.enode_idx);
            }
        }

        let max_cost = neighbors
            .iter()
            .map(|link| link.cost())
            .max()
            .expect("invalid neighbor count");
        let backward_min_cost = bucket
            .iter()
            .map(|&(_, _, cost)| cost)
            .min()
            .expect("invalid backwards");

        // build distances
        let dst_len = neighbors.len();
        let mut costs = Vec::with_capacity(dst_len);
        costs.resize(dst_len, std::u32::MAX);

        if hop_limit == 1 {
            let key = from_link.enode_idx;
            if backwards.contains(&key) {
                for &(n2, idx, cost) in bucket.iter() {
                    if key != n2 {
                        continue;
                    }
                    costs[idx] = std::cmp::min(costs[idx], cost);
                }
            }
        } else {
            let mut search = dijkstra::Search::with_capacity(128);
            search.heap_limit = 1024;
            search.hop_limit = hop_limit - 1;
            search.add_src(from_link.enode_idx);
            search.cost_limit = from_link.cost() + max_cost - backward_min_cost;

            while let Some(entry) = search.next() {
                let key = entry.key;
                if key == src {
                    continue;
                }

                if backwards.contains(&key) {
                    for &(n2, idx, cost) in bucket.iter() {
                        if key != n2 {
                            continue;
                        }
                        costs[idx] = std::cmp::min(costs[idx], entry.cost + cost);
                    }
                }

                // same with nearby_nodes, but inline to remove allocation
                for link in &self.contractions[key.index()] {
                    if link.dir() != IdxLinkDir::Forward {
                        break;
                    }
                    search.update(&entry, link.enode_idx, entry.cost + link.cost());
                }
            }
        }

        costs
    }

    fn dijkstra_contract_from(
        &self,
        src: IdxNodeKey,
        from_link: IdxLink,
        neighbors: &[IdxLink],
        hop_limit: u16,
    ) -> Vec<CHContraction> {
        if neighbors.is_empty() {
            return Vec::new();
        }

        // let costs = self.dijkstra_contract_dist_backward1(src, from_link, neighbors, hop_limit);
        let costs = self.dijkstra_contract_dist_backward2(src, from_link, neighbors, hop_limit);

        // add contractions
        let mut contractions = Vec::new();
        for (i, link) in neighbors.iter().enumerate() {
            let to_key = link.enode_idx;
            if from_link.enode_idx == to_key {
                continue;
            }

            let to_cost = link.cost();
            let cost = costs[i];

            // cost == 0 if dijkstra search failed to find route within limit, which means that
            // a route via 'src' is shortest one so we should add contraction. Same when cost >
            // dist1 + dist2
            let via_cost = from_link.cost() + to_cost;
            if cost > via_cost {
                let c = CHContraction {
                    snode_idx: from_link.enode_idx,
                    enode_idx: to_key,
                    mnode_idx: src,
                    length: via_cost,
                };
                contractions.push(c);
            }
        }
        contractions
    }

    fn dijkstra_contract(
        &self,
        src: IdxNodeKey,
        neighbors: &[IdxLink],
        hop_limit: u16,
    ) -> Vec<CHContraction> {
        let mut contractions = Vec::with_capacity(neighbors.len() * 2);

        let mut forward_links: Vec<IdxLink> = Vec::with_capacity(neighbors.len());
        for (i, link) in neighbors.iter().enumerate() {
            if link.dir() != IdxLinkDir::Backward {
                continue;
            }

            forward_links.clear();
            for (j, forward_link) in neighbors.iter().enumerate() {
                if i != j && forward_link.dir() == IdxLinkDir::Forward {
                    forward_links.push(forward_link.clone());
                }
            }

            let mut v = self.dijkstra_contract_from(src, *link, &forward_links, hop_limit);
            contractions.append(&mut v);
        }
        contractions
    }

    fn heap_entry(&self, key: IdxNodeKey, deleted_count: &[u16], hop_limit: u16) -> CHEntry {
        let idx = key.index();
        let neighbors = self.contractions[idx].as_slice();
        let contractions = self.dijkstra_contract(key, neighbors, hop_limit);

        let edge_difference = (neighbors.len() as i32) - (contractions.len() as i32);
        let deleted_count = deleted_count[idx];
        let score = edge_difference - deleted_count as i32;
        CHEntry { score, key }
    }

    fn build_heap(&self, deleted_count: &[u16], hop_limit: u16) -> BinaryHeap<CHEntry> {
        let sw = Timer::new();
        let len = self.graph.node_len;

        let entries = (0..len)
            .into_par_iter()
            .filter(|idx| !self.contractions[*idx].is_empty())
            .map(|idx| {
                let key = IdxNodeKey::new(idx);
                self.heap_entry(key, deleted_count, hop_limit)
            })
            .collect::<Vec<_>>();

        let heap = BinaryHeap::from(entries);

        eprintln!("building heap took: {}", sw.took(),);
        heap
    }

    // `rebuild_contractions` removes all redundent edges on graph. An edge is redundent if there
    // is no shortest path which passes given edge.
    fn rebuild_contractions(&mut self, hop_limit: u16) {
        let sw = Timer::new();
        let len = self.graph.node_len;

        let entries = (0..len)
            .into_par_iter()
            .filter(|idx| !self.contractions[*idx].is_empty())
            .flat_map(|idx| {
                let key = IdxNodeKey::new(idx);
                let neighbors = self.contractions[idx].as_slice();
                let contractions = self.dijkstra_contract(key, neighbors, hop_limit);
                let mut links = Vec::new();
                for n in neighbors.iter() {
                    for c in contractions.iter() {
                        if n.enode_idx == c.enode_idx && n.dir() == IdxLinkDir::Forward {
                            links.push((key, n.enode_idx, n.cost()));
                            break;
                        }
                        if n.enode_idx == c.snode_idx && n.dir() == IdxLinkDir::Backward {
                            links.push((n.enode_idx, key, n.cost()));
                            break;
                        }
                    }
                }
                links
            })
            .collect::<Vec<_>>();

        self.contractions = Vec::with_capacity(len);
        self.contractions.resize(len, Vec::new());

        for (from, to, cost) in entries.into_iter() {
            self.add_contraction_dir(from, to, cost, IdxLinkDir::Forward);
            self.add_contraction_dir(to, from, cost, IdxLinkDir::Backward);
        }
        eprintln!("rebuilding contractions took: {}", sw.took(),);
    }

    pub fn build(&mut self) {
        let node_len = self.graph.node_len;
        let link_len: usize = self.contractions.par_iter().map(|v| v.len()).sum();

        eprintln!("start contraction: nodes={}, links={}", node_len, link_len);

        let mut deleted_count = Vec::with_capacity(node_len);
        deleted_count.resize(node_len, 0);

        let mut hop_limit = 1;
        let hop_steppings = [(8., 5), (5., 3), (3.3, 2)];
        let mut heap = self.build_heap(&deleted_count, hop_limit);

        // parameters
        const STEP: usize = 10000;
        const SCORE_TOLERANCE: i32 = 2;

        let mut order = 0;
        let mut try_count = 0;

        const HEAP_MIN_INTERVAL: usize = STEP * 2;
        let mut rebuild_try_count = std::cmp::max(HEAP_MIN_INTERVAL, heap.len() / 10);

        let sw = Timer::new();
        let mut stat_score = Stat::default();
        let mut stat_diff = Stat::default();
        let mut stat_neighbors = Stat::default();

        while let Some(entry) = heap.pop() {
            if self.order[entry.key.index()] < order {
                continue;
            }

            try_count += 1;
            rebuild_try_count -= 1;
            if rebuild_try_count == 0 {
                let remain_count = node_len - order as usize;
                let num_vertices: usize = self.contractions.par_iter().map(|c| c.len()).sum();
                let avg_degree = (num_vertices as f32) / (remain_count) as f32 / 2f32;

                let prev_hop_limit = hop_limit;
                for &(deg, hop) in hop_steppings.iter() {
                    if avg_degree > deg {
                        hop_limit = std::cmp::max(hop_limit, hop);
                    }
                }
                if hop_limit != prev_hop_limit {
                    self.rebuild_contractions(hop_limit);
                    deleted_count.clear();
                    deleted_count.resize(node_len, 0);
                }

                eprintln!(
                    "rebuilding heap, elapsed: {}, avg_degrees={:.3}, hop_limit={}",
                    sw.took(),
                    avg_degree,
                    hop_limit,
                );
                heap = self.build_heap(&deleted_count, hop_limit);
                rebuild_try_count = std::cmp::max(HEAP_MIN_INTERVAL, heap.len() / 10);
            }

            // lazy update: re-calculate score
            let key = entry.key;
            let idx = key.index();
            let mut neighbors = Vec::new();
            std::mem::swap(&mut neighbors, &mut self.contractions[idx]);
            let contractions = self.dijkstra_contract(key, neighbors.as_slice(), hop_limit);
            let edge_difference = (neighbors.len() as i32) - (contractions.len() as i32);
            let score = edge_difference - deleted_count[idx] as i32;

            // score is updated
            if score < entry.score - SCORE_TOLERANCE {
                std::mem::swap(&mut neighbors, &mut self.contractions[idx]);
                heap.push(CHEntry { score, key });
                continue;
            }

            stat_score.push(score as i64);
            stat_diff.push(edge_difference as i64);
            stat_neighbors.push(neighbors.len() as i64);

            order += 1;
            if (order as usize) % STEP == 0 {
                eprintln!(
                    "{}/{}/{}, contractions={}, score={:.2}, diff={:.2}, neighbors={:.2}",
                    try_count / STEP,
                    (order as usize) / STEP,
                    node_len / STEP,
                    self.all_contractions.len(),
                    stat_score.avg(),
                    stat_diff.avg(),
                    stat_neighbors.avg(),
                );

                stat_score.clear();
                stat_diff.clear();
                stat_neighbors.clear();
            }

            for neighbor in neighbors.iter() {
                deleted_count[neighbor.enode_idx.index()] += 1;
            }

            // add contractions
            for contraction in contractions.into_iter() {
                self.add_contraction(contraction);
            }

            // update order
            self.order[idx] = order;
            self.gc_contraction(key, neighbors.as_slice());
        }

        self.all_contractions.sort_unstable_by_key(|c| c.snode_idx);
    }
}

// search
impl<'a> CH<'a> {
    fn search_step(
        &self,
        search: &mut dijkstra::Search<IdxNodeKey>,
        dir: IdxLinkDir,
    ) -> Option<dijkstra::HeapEntry<IdxNodeKey>> {
        if let Some(entry) = search.next() {
            let key = entry.key;
            let nodes = self.nearby_nodes(key, dir);
            for link in nodes.iter() {
                let next_key = link.enode_idx;
                let next_cost = entry.cost + link.cost();
                search.update(&entry, next_key, next_cost);
            }
            return Some(entry);
        }
        return None;
    }

    pub fn search(&self, src: IdxNodeKey, dst: IdxNodeKey) -> Option<(Vec<IdxNodeKey>, u32)> {
        let mut search_f = dijkstra::Search::new();
        search_f.add_src(src);

        let mut search_b = dijkstra::Search::new();
        search_b.add_src(dst);

        let mut min_cost = std::u32::MAX;
        let mut min_key = IdxNodeKey::new(0);

        loop {
            // update threshold
            search_f.cost_limit = min_cost;
            search_b.cost_limit = min_cost;
            let mut updated = false;

            macro_rules! step {
                ($f:ident, $b:ident, $dir:expr) => {
                    if let Some(entry) = self.search_step(&mut $f, $dir) {
                        updated = true;
                        if let Some(cost) = $b.get_cost(&entry.key) {
                            if !cost.visited {
                                continue;
                            }
                            let cost = entry.cost + cost.cost;
                            if cost < min_cost {
                                min_cost = cost;
                                min_key = entry.key;
                            }
                        }
                    }
                };
            }

            step!(search_f, search_b, IdxLinkDir::Forward);
            step!(search_b, search_f, IdxLinkDir::Backward);

            if !updated {
                break;
            }
        }
        if min_cost == std::u32::MAX {
            eprintln!("f={}, b={}", search_f.visited_len(), search_b.visited_len());
            return None;
        }

        let all_contractions = self.all_contractions.as_slice();
        let mut path = {
            let mut path_f = search_f.decode(min_key);
            let mut path_b = search_b.decode(min_key);
            path_b.reverse();
            path_f.pop();
            path_f.append(&mut path_b);
            path_f
        };

        let mut decoded = Vec::new();
        // use path as a stack
        while !path.is_empty() {
            if path.len() == 1 {
                decoded.push(path.pop().unwrap());
                break;
            }

            let key1 = path.pop().unwrap();
            let key2 = *path.last().unwrap();

            // find contracted edge
            let r = all_contractions.equal_range_by_key(&key1, |c| c.snode_idx);

            let mut found = None;
            let mut minlen = std::u32::MAX;
            for c in &all_contractions[r] {
                if c.enode_idx == key2 && c.length < minlen {
                    found = Some(c);
                    minlen = c.length;
                }
            }

            if let Some(c) = found {
                path.push(c.mnode_idx);
                path.push(key1);
            } else {
                decoded.push(key1);
            }
        }
        decoded.reverse();

        eprintln!("f={}, b={}", search_f.visited_len(), search_b.visited_len(),);

        Some((decoded, min_cost))
    }
}

const _CHECK_CHENTRY: [u8; 8] = [0; std::mem::size_of::<CHEntry>()];
const _CHECK_CHCONTRACTION: [u8; 16] = [0; std::mem::size_of::<CHContraction>()];
