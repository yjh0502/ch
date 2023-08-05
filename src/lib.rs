#[macro_use]
extern crate error_chain;

use std::cmp::*;
use std::collections::hash_map::*;
use std::collections::BinaryHeap;
use std::ops::Range;
use std::path::Path;
use serde_derive::*;

use took::Timer;

mod ch;
mod dijkstra;
mod network;
pub mod partition;

pub use crate::ch::*;
pub use network::*;

pub mod errors {
    error_chain! {
        foreign_links {
            De(::serde::de::value::Error);
            Csv(::csv::Error);
        }
    }
}
use errors::*;

fn decode_csv_noheader<T, P>(p: P) -> Result<Vec<T>>
where
    T: for<'de> serde::Deserialize<'de> + Send + 'static,
    P: AsRef<Path>,
{
    let mut v = Vec::new();
    let mut rdr = csv::ReaderBuilder::new()
        .has_headers(false)
        .from_path(p)
        .map_err(Error::from)?;
    for result in rdr.deserialize() {
        let item: T = result.map_err(Error::from)?;
        v.push(item);
    }
    Ok(v)
}

fn decode_csv<T, P>(p: P) -> Result<Vec<T>>
where
    T: for<'de> serde::Deserialize<'de> + Send + 'static,
    P: AsRef<Path>,
{
    let mut v = Vec::new();
    let mut rdr = csv::ReaderBuilder::new()
        .from_path(p)
        .map_err(Error::from)?;
    for result in rdr.deserialize() {
        let item: T = result.map_err(Error::from)?;
        v.push(item);
    }
    Ok(v)
}

pub fn encode_csv<T, P>(p: P, data: &[T]) -> Result<()>
where
    T: serde::Serialize + Send + 'static,
    P: AsRef<Path>,
{
    let mut writer = csv::WriterBuilder::new()
        .from_path(p)
        .map_err(Error::from)?;

    for v in data.iter() {
        writer.serialize(v)?;
    }
    Ok(())
}

pub fn run() {
    // 양재
    let _key_yangjae = walk::NodeKey::new(6732, 411);
    // 일산
    let _key_ilsan = walk::NodeKey::new(5784, 6080);

    // 강남역
    let _key_gangnam = walk::NodeKey::new(6733, 75834);
    // 뱅뱅사거리
    let _key_bangbang = walk::NodeKey::new(6732, 23091);
    // 양재역
    let _key_yangjae_station = walk::NodeKey::new(6732, 19789);

    let sw = Timer::new();
    let network = walk::Network::from_path("wlink").unwrap();
    eprintln!("network loading took: {} ms", sw.took());

    let sw = Timer::new();
    let g = Graph::from(&network);
    eprintln!("graph took: {} ms", sw.took());

    let test_queries = [
        [_key_bangbang, _key_ilsan],
        [_key_yangjae, _key_yangjae_station],
    ];

    {
        for query in test_queries.iter() {
            let sw = Timer::new();
            let src = network.node_key_to_idx(query[0]);
            let dst = network.node_key_to_idx(query[1]);
            let (_seq, distance) = g
                .search(src, dst)
                .expect("failed to find path with dijkstra");
            eprintln!(
                "dijkstra took: {} ms, distance={}, links={}",
                sw.took(),
                distance,
                _seq.len(),
            );

            let sw = Timer::new();
            let (_seq, distance) = g
                .search_bidir(src, dst)
                .expect("failed to find path with bi-dijkstra");
            eprintln!(
                "dijkstra-bidir took: {} ms, distance={}, links={}",
                sw.took(),
                distance,
                _seq.len(),
            );
        }
    }

    {
        let sw = Timer::new();
        let ch = CH::from_file(&g, "./wlink_ch2").expect("failed to load");
        if false {
            let mut ch = CH::new(&g);
            ch.build();
        }
        eprintln!("loading ch took: {} ms", sw.took());

        for query in test_queries.iter() {
            let sw = Timer::new();
            let src = network.node_key_to_idx(query[0]);
            let dst = network.node_key_to_idx(query[1]);
            let (_seq, distance) = ch.search(src, dst).expect("failed to find path with ch");
            eprintln!(
                "ch took: {} ms, distance={}, links={}",
                sw.took(),
                distance,
                _seq.len(),
            );
        }
    }
}

pub fn run_car() {
    // 양재
    let _key_yangjae = road::LinkKey::new(6732, 5109, 2025);
    // 양재전화국
    let _key_yangjae2 = road::LinkKey::new(6732, 5317, 2142);
    // 신사
    let _key_sinsa = road::LinkKey::new(6733, 4651, 1871);
    // 일산
    let _key_ilsan = road::LinkKey::new(5784, 40505, 11354);
    // 부산
    let _key_busan = road::LinkKey::new(8413, 6942, 2383);

    let sw = Timer::new();
    let network = road::Network::from_path("link").unwrap();
    eprintln!("network loading took: {} ms", sw.took());

    let sw = Timer::new();
    let g = Graph::from(&network);
    eprintln!("graph took: {} ms", sw.took());

    let test_queries = [
        //
        [_key_yangjae, _key_yangjae2],
        [_key_yangjae, _key_sinsa],
        [_key_yangjae, _key_ilsan],
        [_key_yangjae, _key_busan],
    ];

    {
        for query in test_queries.iter() {
            let sw = Timer::new();
            let src = network.link_key_to_idx(query[0]);
            let dst = network.link_key_to_idx(query[1]);
            let (_seq, cost) = g
                .search(src, dst)
                .expect("failed to find path with dijkstra");
            eprintln!(
                "dijkstra took: {} ms, cost={}, links={}",
                sw.took(),
                cost,
                _seq.len(),
            );

            let sw = Timer::new();
            let (_seq, cost) = g
                .search_bidir(src, dst)
                .expect("failed to find path with bi-dijkstra");
            eprintln!(
                "dijkstra-bidir took: {} ms, cost={}, links={}",
                sw.took(),
                cost,
                _seq.len(),
            );
        }
    }

    {
        let sw = Timer::new();
        let ch = CH::from_file(&g, "./link_ch2").expect("failed to load");
        if false {
            let mut ch = CH::new(&g);
            ch.build();
        }
        eprintln!("loading ch took: {} ms", sw.took());

        // dry run
        for _i in 0..5 {
            for query in test_queries.iter() {
                let src = network.link_key_to_idx(query[0]);
                let dst = network.link_key_to_idx(query[1]);
                ch.search(src, dst).expect("failed to find path with ch");
            }
        }

        for query in test_queries.iter() {
            let sw = Timer::new();
            let src = network.link_key_to_idx(query[0]);
            let dst = network.link_key_to_idx(query[1]);
            let (_seq, cost) = ch.search(src, dst).expect("failed to find path with ch");
            eprintln!(
                "ch took: {} ms, cost={}, links={}",
                sw.took(),
                cost,
                _seq.len(),
            );
        }
    }
}

const _CHECK_IDXNODEKEY: [u8; 4] = [0; std::mem::size_of::<IdxNodeKey>()];
const _CHECK_IDXLINK: [u8; 8] = [0; std::mem::size_of::<IdxLink>()];
const _CHECK_HEAPENTRY: [u8; 12] = [0; std::mem::size_of::<dijkstra::HeapEntry<IdxNodeKey>>()];
