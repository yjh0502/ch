use clap::{Arg, Command};

use ch::*;

fn main() {
    let args = Command::new("ch-search")
        .author("Jihyun Yu <j.yu@naverlabs.com>")
        .arg(Arg::new("network").long("network").required(true))
        .arg(Arg::new("ch").long("ch").required(true))
        .arg(Arg::new("smesh").long("smesh").required(true))
        .arg(Arg::new("snode").long("snode").required(true))
        .arg(Arg::new("slink").long("slink").required(true))
        .arg(Arg::new("emesh").long("emesh").required(true))
        .arg(Arg::new("elink").long("elink").required(true))
        .arg(Arg::new("enode").long("enode").required(true))
        .get_matches();

    let network_path = args.get_one::<String>("network").unwrap();
    let ch_path = args.get_one::<String>("ch").unwrap();

    let network = road::Network::from_path(network_path).expect("failed to load network");
    let graph = Graph::from(&network);
    let ch = CH::from_file(&graph, ch_path).expect("failed to load ch");

    let smesh = args
        .get_one::<String>("smesh")
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let slink = args
        .get_one::<String>("slink")
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let snode = args
        .get_one::<String>("snode")
        .unwrap()
        .parse::<u32>()
        .unwrap();

    let emesh = args
        .get_one::<String>("emesh")
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let elink = args
        .get_one::<String>("elink")
        .unwrap()
        .parse::<u32>()
        .unwrap();
    let enode = args
        .get_one::<String>("enode")
        .unwrap()
        .parse::<u32>()
        .unwrap();

    let src = road::LinkKey::new(smesh, slink, snode);
    let dst = road::LinkKey::new(emesh, elink, enode);

    let src = network.link_key_to_idx(src);
    let dst = network.link_key_to_idx(dst);

    let sw = took::Timer::new();
    let (_seq, dist) = ch.search(src, dst).expect("failed to find");
    eprintln!("took {}, {} links, cost={}", sw.took(), _seq.len(), dist);
}
