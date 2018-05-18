extern crate ch;
extern crate clap;
extern crate stopwatch;

use stopwatch::Stopwatch;
use clap::{App, Arg};

use ch::*;

fn main() {
    let args = App::new("ch-build")
        .author("Jihyun Yu <j.yu@naverlabs.com>")
        .arg(
            Arg::with_name("network")
                .long("network")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("out")
                .long("out")
                .required(true)
                .takes_value(true),
        )
        .get_matches();

    let network_path = args.value_of("network").unwrap();
    let out_path = args.value_of("out").unwrap();

    // let network = walk::Network::from_path(network_path).unwrap();
    let network = road::Network::from_path(network_path).unwrap();
    let graph = Graph::from(&network);

    let mut ch = CH::new(&graph);
    let sw = Stopwatch::start_new();
    ch.build();
    eprintln!("ch build took: {} ms", sw.elapsed_ms());

    ch.write(out_path).expect("failed to write");
}
