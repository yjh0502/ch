use anyhow::*;
use clap::{Arg, Command};

use ch::*;

fn main() -> Result<()> {
    let args = Command::new("ch-build")
        .author("Jihyun Yu <j.yu@naverlabs.com>")
        .arg(Arg::new("network").long("network").required(true))
        .arg(Arg::new("ty").long("ty").required(true))
        .arg(Arg::new("out").long("out").required(true))
        .get_matches();

    let network_path = args.get_one::<String>("network").unwrap();
    let out_path = args.get_one::<String>("out").unwrap();
    let ty = args.get_one::<String>("ty").unwrap();

    let sw = took::Timer::new();
    let g = match ty.as_str() {
        "walk" => {
            let network = walk::Network::from_path(network_path)?;
            Graph::from(&network)
        }
        "road" => {
            let network = road::Network::from_path(network_path)?;
            Graph::from(&network)
        }
        "shp" => {
            let network = shp::Network::from_path(network_path)?;
            Graph::from(&network)
        }
        _ => {
            bail!("unknown type: {}", ty);
        }
    };
    eprintln!("graph took: {}", sw.took());

    let mut ch = CH::new(&g);
    let sw = took::Timer::new();
    ch.build();
    eprintln!("ch build took: {}", sw.took());

    ch.write(out_path).expect("failed to write");

    Ok(())
}
