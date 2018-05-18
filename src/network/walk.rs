use rayon::prelude::*;

use super::*;

// alternative to mid
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

#[derive(Deserialize, Debug)]
pub struct Link {
    mid: u64,
    #[serde(rename = "mesh")] mesh_id: u32,
    link_id: u32,
    snode_id: u32,
    enode_id: u32,
    #[serde(rename = "link_l")] length: u32,
}

impl Link {
    fn reverse(&self) -> Self {
        Self {
            mid: self.mid,
            mesh_id: self.mesh_id,
            link_id: self.link_id,
            snode_id: self.enode_id,
            enode_id: self.snode_id,
            length: self.length,
        }
    }

    fn node_key(&self) -> NodeKey {
        NodeKey::new(self.mesh_id, self.snode_id)
    }
}

#[derive(Deserialize, Debug, Clone)]
pub struct Node {
    mid: u64,
    #[serde(rename = "mesh")] mesh_id: u32,
    node_id: u32,
    #[serde(rename = "edge_mesh")] edge_mesh_id: u32,
    #[serde(rename = "edge_node")] edge_node_id: u32,
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
    pub links_map: HashMap<NodeKey, Range<usize>>,
}

type Neighbor<'a> = (NodeKey, &'a Link);

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
                if !node_map.contains_key(&NodeKey::new(l.mesh_id, l.snode_id)) {
                    return false;
                }
                if !node_map.contains_key(&NodeKey::new(l.mesh_id, l.enode_id)) {
                    return false;
                }
                true
            })
            .collect::<Vec<_>>();

        eprintln!("links: {} -> {}", links.len(), raw_links_len);

        //TODO: refactor
        let mut links_rev = Vec::with_capacity(links.len());
        for link in links.iter() {
            links_rev.push(link.reverse());
        }
        links.append(&mut links_rev);
        links.as_mut_slice().sort_unstable_by_key(Link::node_key);

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
            links_map,
        })
    }

    pub fn node_key_to_idx(&self, key: NodeKey) -> IdxNodeKey {
        match self.node_map.get(&key) {
            Some(idx) => IdxNodeKey::new(*idx),
            None => panic!("unknown node_key: {:?}", key),
        }
    }

    /// find all connected nodes
    fn nearby_nodes(&self, src: NodeKey) -> Vec<Neighbor> {
        let mut nodes = self.nearby_mesh_nodes(src);

        // find nodes on other meshes
        let mesh_node = self.edge_nodes
            .as_slice()
            .binary_search_by_key(&src, Node::node_key);
        if let Ok(idx) = mesh_node {
            let node = &self.edge_nodes[idx];
            //TODO: move to Node
            let other_src = NodeKey::new(node.edge_mesh_id, node.edge_node_id);
            let mut other_nodes = self.nearby_mesh_nodes(other_src);
            nodes.append(&mut other_nodes);
        }
        nodes
    }

    /// find all connected nodes within same mesh
    fn nearby_mesh_nodes(&self, src: NodeKey) -> Vec<Neighbor> {
        let links = self.links.as_slice();
        let range = self.links_map.get(&src).cloned().unwrap_or(0..0);

        let links = &links[range];
        let mut out = Vec::with_capacity(links.len());
        for link in links {
            assert!(link.mesh_id == src.mesh_id);
            assert!(link.snode_id == src.node_id);

            let node_key = NodeKey::new(link.mesh_id, link.enode_id);
            out.push((node_key, link));
        }
        out
    }
}

impl<'a> From<&'a Network> for Graph {
    fn from(network: &'a Network) -> Self {
        let nodes = network.nodes.as_slice();

        let idx_links = nodes
            .par_iter()
            .map(|node| {
                let key = node.node_key();
                network
                    .nearby_nodes(key)
                    .into_iter()
                    .map(|tup| {
                        let nearby_key = tup.0;
                        let idx = network.node_key_to_idx(nearby_key);
                        IdxLink::new(idx, tup.1.length, IdxLinkDir::Forward)
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        Self::from_links(idx_links)
    }
}
