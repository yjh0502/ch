extern crate ch;
extern crate clap;
extern crate stopwatch;

use clap::{Command, Arg};
use stopwatch::Stopwatch;

use ch::*;

fn main() {
    let args = Command::new("ch-build")
        .author("Jihyun Yu <j.yu@naverlabs.com>")
        .arg(
            Arg::new("network")
                .long("network")
                .required(true)
        )
        .arg(
            Arg::new("out")
                .long("out")
                .required(true)
        )
        .get_matches();

    let network_path = args.get_one::<String>("network").unwrap();
    let out_path = args.get_one::<String>("out").unwrap();

    // let network = walk::Network::from_path(network_path).unwrap();
    let network = road::Network::from_path(network_path).unwrap();
    let graph = Graph::from(&network);

    let mut ch = CH::new(&graph);
    let sw = Stopwatch::start_new();
    ch.build();
    eprintln!("ch build took: {} ms", sw.elapsed_ms());

    ch.write(out_path).expect("failed to write");
}
