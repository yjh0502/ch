# CH: Contraction Hierarchies implementation

[![Build Status](https://travis-ci.org/yjh0502/ch.svg?branch=master)](https://travis-ci.org/yjh0502/ch)

## Getting Started

```sh
# install rustup
curl https://sh.rustup.rs -sSf | sh
cargo build --release

# dump data from DB
(cd link && sh dump.sh)

# run contraction
mkdir link_ch
RUST_BACKTRACE=1 ./target/release/ch-build --network link --out link_ch/

# run query: test query find route from seoul to busan
./target/release/ch-search --network link --ch link_ch \
    --smesh 6732 --slink 5109 --snode 2025 \
    --emesh 8413 --elink 6942 --enode 2383
```

## osm

```sh
# source: https://data.humdata.org/dataset/hotosm_kor_roads?
mkdir data
(cd data && wget https://export.hotosm.org/downloads/3a156bc4-7f04-418f-818e-7b97728fd7db/hotosm_kor_roads_lines_shp.zip && unzip hotosm_kor_roads_lines_shp.zip)

# build contractions
cargo run --release --bin ch-build -- --ty shp --network data/hotosm_kor_roads_lines.shp --out link_ch3

# test
cargo watch -x check -x test -x 'run --release --bin ch-run'

```
