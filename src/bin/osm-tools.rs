extern crate clap;
extern crate failure;
extern crate fnv;
extern crate memmap;
extern crate osmpbf;
extern crate rayon;
extern crate stopwatch;

use std::fs::File;

use clap::{Command, Arg};
use failure::Error;
use memmap::MmapOptions;
use osmpbf::*;
use rayon::prelude::*;

type Result<T> = std::result::Result<T, Error>;

// const FILENAME: &'static str = "data/planet-latest.osm.pbf";
const FILENAME: &'static str = "data/seoul.osm.pbf";

use std::f64;
#[derive(Clone, Copy)]
struct BBox {
    top: f64,
    bottom: f64,
    left: f64,
    right: f64,
}
impl BBox {
    fn new() -> Self {
        Self {
            top: f64::MIN,
            bottom: f64::MAX,
            left: f64::MAX,
            right: f64::MIN,
        }
    }

    fn push(&mut self, lat: f64, lon: f64) {
        self.top = f64::max(lat, self.top);
        self.bottom = f64::min(lat, self.bottom);
        self.left = f64::min(lon, self.left);
        self.right = f64::max(lon, self.right);
    }

    #[allow(unused)]
    fn is_empty(&self) -> bool {
        self.top <= self.bottom || self.right <= self.left
    }
}
impl std::fmt::Debug for BBox {
    fn fmt(&self, fmt: &mut std::fmt::Formatter) -> std::fmt::Result {
        if self.is_empty() {
            write!(fmt, "[bbox empty]")
        } else {
            write!(
                fmt,
                "[bbox top={} left={} bottom={} right={}]",
                self.top, self.left, self.bottom, self.right
            )
        }
    }
}

#[derive(Debug)]
struct Stats {
    bbox: BBox,
    node_count: usize,
    way_count: usize,
    rel_count: usize,
}

fn data_to_stats(block: &PrimitiveBlock) -> Stats {
    let mut stats = Stats {
        bbox: BBox::new(),
        node_count: 0,
        way_count: 0,
        rel_count: 0,
    };

    for group in block.groups() {
        for node in group.nodes() {
            stats.node_count += 1;
            stats.bbox.push(node.lat(), node.lon());
        }

        for node in group.dense_nodes() {
            stats.node_count += 1;
            stats.bbox.push(node.lat(), node.lon());
        }

        stats.way_count += group.ways().count();
        stats.rel_count += group.relations().count();
    }

    stats
}

#[allow(unused)]
fn seq_mmap(filename: &str) -> Result<()> {
    let file = File::open(filename)?;
    let mmap = unsafe { Mmap::from_file(&file) }.unwrap();

    let mut blobs = Vec::new();
    for blob in mmap.blob_iter() {
        blobs.push(blob.unwrap());
    }

    blobs.par_iter().enumerate().for_each(|(i, blob)| {
        let sw = stopwatch::Stopwatch::start_new();

        if let BlobDecode::OsmData(data) = blob.decode().unwrap() {
            let stats = data_to_stats(&data);
            eprintln!("{}/{:?}", i, stats);
        }
    });

    println!("Number of blobs: {}", blobs.len());
    Ok(())
}

#[allow(unused)]
fn par_mmap() -> Result<()> {
    let file = File::open(FILENAME)?;
    let mmap = unsafe { MmapOptions::new().map(&file)? };
    let slice: &[u8] = &mmap;

    let reader = ElementReader::new(slice);

    // Count the ways
    let ways = reader
        .par_map_reduce(
            |element| match element {
                Element::Way(_) => 1,
                _ => 0,
            },
            || 0_u64,     // Zero is the identity value for addition
            |a, b| a + b, // Sum the partial results
        )
        .unwrap();

    println!("Number of ways: {}", ways);
    Ok(())
}

fn main() {
    let args = Command::new("ch-build")
        .author("Jihyun Yu <j.yu@naverlabs.com>")
        .arg(
            Arg::new("filename")
                .short('f')
                .required(true)
        )
        .get_matches();

    let filename = args.get_one::<String>("filename").unwrap();

    seq_mmap(&filename).expect("failed to read");
}
