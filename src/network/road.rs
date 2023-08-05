use rayon::prelude::*;

use super::*;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct NodeKey {
    mesh_id: u32,
    node_id: u32,
}

impl NodeKey {
    pub fn new(mesh_id: u32, node_id: u32) -> Self {
        Self { mesh_id, node_id }
    }
}
impl std::hash::Hash for NodeKey {
    fn hash<H>(&self, state: &mut H)
    where
        H: std::hash::Hasher,
    {
        let v = unsafe { std::mem::transmute::<NodeKey, u64>(*self) };
        state.write_u64(v)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LinkKey {
    mesh_id: u32,
    link_id: u32,
    node_id: u32,
}
impl LinkKey {
    pub fn new(mesh_id: u32, link_id: u32, node_id: u32) -> Self {
        Self {
            mesh_id,
            link_id,
            node_id,
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct Link {
    mid: u64,
    #[serde(rename = "mesh")]
    mesh_id: u32,
    link_id: u32,
    snode_id: u32,
    enode_id: u32,
    #[serde(rename = "link_l")]
    length: u32,

    max_speed: u8,

    // 1 (allow), 2 (deny)
    pass_code: u8,
    // 1(bidirectional), 2(deny), 3(forward), 4(backward)
    // 6(forward?), 7(backward?)
    #[serde(rename = "k_control")]
    control: u8,

    #[serde(skip)]
    reversed: bool,
}

impl Link {
    fn normalize(&mut self) {
        let next_control = match self.control {
            4 => 3,
            7 => 6,
            control => control,
        };
        if next_control != self.control {
            self.control = next_control;
            self.reversed = !self.reversed;
            std::mem::swap(&mut self.snode_id, &mut self.enode_id);
        }
    }

    fn reverse(&self) -> Option<Self> {
        // do not generate reversed link for one-way road
        if self.control != 1 {
            return None;
        }
        Some(Self {
            mid: self.mid,
            mesh_id: self.mesh_id,
            link_id: self.link_id,
            snode_id: self.enode_id,
            enode_id: self.snode_id,
            length: self.length,

            max_speed: self.max_speed,

            pass_code: self.pass_code,
            control: self.control,

            reversed: !self.reversed,
        })
    }

    fn deny(&self) -> bool {
        self.pass_code == 2 || self.control == 2
    }

    fn passable(&self) -> bool {
        self.pass_code == 1 && (self.control == 1 || self.control == 3 || self.control == 6)
    }

    fn node_key(&self) -> NodeKey {
        NodeKey::new(self.mesh_id, self.snode_id)
    }

    fn link_key(&self) -> LinkKey {
        LinkKey {
            mesh_id: self.mesh_id,
            node_id: self.snode_id,
            link_id: self.link_id,
        }
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Node {
    #[allow(unused)]
    mid: u64,
    #[serde(rename = "mesh")]
    mesh_id: u32,
    node_id: u32,
    #[serde(rename = "edge_mesh")]
    edge_mesh_id: u32,
    #[serde(rename = "edge_node")]
    edge_node_id: u32,
}
impl Node {
    fn node_key(&self) -> NodeKey {
        NodeKey::new(self.mesh_id, self.node_id)
    }
}

pub struct Network {
    pub links: Vec<Link>,
    pub nodes: Vec<Node>,
    pub edge_nodes: Vec<Node>,

    // indices
    pub node_map: HashMap<NodeKey, usize>,
    pub link_map: HashMap<LinkKey, usize>,
    pub links_map: HashMap<NodeKey, Range<usize>>,
}

type Neighbor<'a> = &'a Link;

impl Network {
    pub fn from_path<P>(path: P) -> Result<Network>
    where
        P: AsRef<Path>,
    {
        let path = path.as_ref();

        let nodes = {
            let mut nodes: Vec<Node> = decode_csv(path.join("node.csv"))?;
            nodes.as_mut_slice().sort_unstable_by_key(Node::node_key);
            nodes
        };

        let node_map = nodes
            .iter()
            .enumerate()
            .map(|(i, node)| (node.node_key(), i))
            .collect::<HashMap<_, _>>();

        let links: Vec<Link> = decode_csv(path.join("link.csv"))?;
        let raw_links_len = links.len();
        let mut links = links
            .into_par_iter()
            .filter(|l| {
                if l.deny() {
                    return false;
                }
                if !node_map.contains_key(&NodeKey::new(l.mesh_id, l.snode_id)) {
                    return false;
                }
                if !node_map.contains_key(&NodeKey::new(l.mesh_id, l.enode_id)) {
                    return false;
                }
                true
            })
            .collect::<Vec<_>>();

        eprintln!("links: {} -> {}", raw_links_len, links.len());

        //TODO: refactor
        let mut links_rev = Vec::with_capacity(links.len());
        for link in links.iter_mut() {
            link.normalize();
            if let Some(link_rev) = link.reverse() {
                links_rev.push(link_rev);
            }
        }
        links.append(&mut links_rev);
        links.as_mut_slice().sort_unstable_by_key(Link::node_key);

        let link_map = links
            .iter()
            .enumerate()
            .map(|(i, link)| (link.link_key(), i))
            .collect::<HashMap<_, _>>();

        let link_partitions = partition::partition_range_by_key(links.as_slice(), Link::node_key);
        let links_map = link_partitions.into_iter().collect::<HashMap<_, _>>();

        let edge_nodes = {
            let mut edge_nodes = nodes
                .iter()
                .filter(|node| node.edge_mesh_id != 0)
                .map(|node| node.clone())
                .collect::<Vec<_>>();
            edge_nodes
                .as_mut_slice()
                .sort_unstable_by_key(Node::node_key);
            edge_nodes
        };

        eprintln!(
            "links: {}, nodes: {}, edge_nodes:{}",
            links.len(),
            nodes.len(),
            edge_nodes.len(),
        );

        //TODO: validate uniqueness
        Ok(Self {
            links,
            nodes,
            edge_nodes,

            node_map,
            link_map,
            links_map,
        })
    }

    pub fn link_key_to_idx(&self, key: LinkKey) -> IdxNodeKey {
        match self.link_map.get(&key) {
            Some(idx) => IdxNodeKey::new(*idx),
            None => panic!("unknown link_key: {:?}", key),
        }
    }

    /// find all connected nodes
    fn next_links(&self, src: NodeKey, link_id: u32) -> Vec<Neighbor> {
        let mut nodes = self.mesh_next_links(src, link_id);

        // find nodes on other meshes
        let mesh_node = self
            .edge_nodes
            .as_slice()
            .binary_search_by_key(&src, Node::node_key);
        if let Ok(idx) = mesh_node {
            let node = &self.edge_nodes[idx];
            //TODO: move to Node
            let other_src = NodeKey::new(node.edge_mesh_id, node.edge_node_id);
            //TODO: link_id?
            let mut other_nodes = self.mesh_next_links(other_src, std::u32::MAX);
            nodes.append(&mut other_nodes);
        }
        nodes
    }

    /// find all connected links within same mesh, with `link_id` link starting from `src` as
    /// `snode_id`
    fn mesh_next_links(&self, src: NodeKey, _link_id: u32) -> Vec<Neighbor> {
        let links = self.links.as_slice();
        let range = self.links_map.get(&src).cloned().unwrap_or(0..0);

        let links = &links[range];
        let mut out = Vec::with_capacity(links.len());
        for link in links {
            assert!(link.mesh_id == src.mesh_id);
            assert!(link.snode_id == src.node_id);
            //TODO: use link_id
            if !link.passable() {
                continue;
            }

            out.push(link);
        }
        out
    }
}

impl<'a> From<&'a Network> for Graph {
    fn from(network: &'a Network) -> Self {
        let links = network.links.as_slice();

        let idx_links = links
            .par_iter()
            .map(|link| {
                // we should find following links, so use enode_id instead of node_key()
                let key = NodeKey {
                    mesh_id: link.mesh_id,
                    node_id: link.enode_id,
                };

                let next_links = network.next_links(key, link.link_id);
                next_links
                    .into_iter()
                    .map(|nearby_link| {
                        let nearby_link_key = nearby_link.link_key();
                        let idx = network.link_key_to_idx(nearby_link_key);

                        let len = nearby_link.length;
                        let max_speed = match nearby_link.max_speed {
                            0 => 40,
                            speed => speed,
                        };
                        // seconds = len / (max_speed / 3.6)
                        let cost = (len as f32 * 3.6 / max_speed as f32) as u32;
                        IdxLink::new(idx, cost, IdxLinkDir::Forward)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Self::from_links(idx_links)
    }
}
