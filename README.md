## CH: Contraction Hierarchies implementation


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
