# gossipy

Distributed systems challenges from [fly.io/dist-sys](https://fly.io/dist-sys) implemented in Rust.

The challenges are built on top of a platform called [Maelstrom](https://github.com/jepsen-io/maelstrom). This platform lets you build out a “node” in your distributed system and Maelstrom will handle the routing of messages between the those nodes. This lets Maelstrom inject failures and perform verification checks based on the consistency guarantees required by each challenge.

## Usage

### 1) Echo

```shell
 cargo build && maelstrom/maelstrom test -w echo --bin ./target/debug/echo -- node-count 1 --time-limit 10 --log-stderr
```

### 2) Unique ID Generation

```shell
cargo build && maelstrom/maelstrom test -w unique-ids --bin ./target/debug/unique-ids --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition --log-stderr
```

### 3a) Single-Node Broadcast

```shell
cargo build && maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 1 --time-limit 20 --rate 10 --log-stderr
```

### 3b) Multi-Node Broadcast

```shell
cargo build && maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --log-stderr
```

### 3c) Fault Tolerant Broadcast

```shell
cargo build && maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10 --log-stderr --nemesis partition
```

### 3d) Efficient Broadcast, Part I

Challenge is to achieve (in parenthesis are achieved numbers):
* Messages-per-operation is below 30 (<26)
* Median latency is below 400ms (348ms)
* Maximum latency is below 600ms (583ms)

**Topology** used: total (all nodes are connected, each node is a neighbor of every other node)

```shell
cargo build && maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast 500 --node-count 25 --time-limit 20 --rate 100 --latency 100 --log-stderr --topology total
```
_Is it stll fault tolerant?_ Yes!
```shell
cargo build && maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast 500 --node-count 25 --time-limit 20 --rate 100 --latency 100 --log-stderr --topology total --nemesis partition
```

### 3e) Efficient Broadcast, Part II

Challenge is to achieve (in parenthesis are achieved numbers):
* Messages-per-operation is below 20 (<9)
* Median latency is below 1 second (952ms)
* Maximum latency is below 2 seconds (1566ms)

**Topology** used: grid (5x5 for 25 nodes in this case)

```shell
cargo build && maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --log-stderr --topology grid
```

_Is it stll fault tolerant?_ Yes!
```shell
cargo build && maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --log-stderr --topology grid --nemesis partition
```
