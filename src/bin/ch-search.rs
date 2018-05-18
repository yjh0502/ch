extern crate ch;
extern crate clap;
extern crate stopwatch;

use stopwatch::Stopwatch;
use clap::{App, Arg};

use ch::*;

fn main() {
    let args = App::new("ch-search")
        .author("Jihyun Yu <j.yu@naverlabs.com>")
        .arg(
            Arg::with_name("network")
                .long("network")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("ch")
                .long("ch")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("smesh")
                .long("smesh")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("snode")
                .long("snode")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("slink")
                .long("slink")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("emesh")
                .long("emesh")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("elink")
                .long("elink")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("enode")
                .long("enode")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let network_path = args.value_of("network").unwrap();
    let ch_path = args.value_of("ch").unwrap();

    let network = road::Network::from_path(network_path).expect("failed to load network");
    let graph = Graph::from(&network);
    let ch = CH::from_file(&graph, ch_path).expect("failed to load ch");

    let smesh = args.value_of("smesh").unwrap().parse::<u32>().unwrap();
    let slink = args.value_of("slink").unwrap().parse::<u32>().unwrap();
    let snode = args.value_of("snode").unwrap().parse::<u32>().unwrap();

    let emesh = args.value_of("emesh").unwrap().parse::<u32>().unwrap();
    let elink = args.value_of("elink").unwrap().parse::<u32>().unwrap();
    let enode = args.value_of("enode").unwrap().parse::<u32>().unwrap();

    let src = road::LinkKey::new(smesh, slink, snode);
    let dst = road::LinkKey::new(emesh, elink, enode);

    let src = network.link_key_to_idx(src);
    let dst = network.link_key_to_idx(dst);

    let sw = Stopwatch::start_new();
    let (_seq, dist) = ch.search(src, dst).expect("failed to find");
    eprintln!(
        "took {} ms, {} links, cost={}",
        sw.elapsed_ms(),
        _seq.len(),
        dist
    );
}
