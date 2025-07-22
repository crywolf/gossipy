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
_Is it still fault tolerant?_ Yes!
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

_Is it still fault tolerant?_ Yes!
```shell
cargo build && maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100 --log-stderr --topology grid --nemesis partition
```

### 4) Stateless Grow-Only Counter

Nodes are using a [sequentially-consistent](https://jepsen.io/consistency/models/sequential) key/value store service provided by Maelstrom.

Other [Consistency Models](https://jepsen.io/consistency/models)

A **Conflict-free Replicated Data Type (CRDT)** is a data structure used in distributed
computing that allows multiple copies of data to be updated independently and concurrently,
ensuring that all copies eventually converge to a consistent state.
This makes CRDTs useful for applications like collaborative editing and distributed databases,
where changes can happen simultaneously without complex coordination.

Conflict-free Replicated Data Types (CRDTs) are used in systems with optimistic replication, where they take care of conflict resolution. CRDTs ensure that, no matter what data modifications are made on different replicas, the data can always be merged into a consistent state. This merge is performed automatically by the CRDT, without requiring any special conflict resolution code or user intervention.

https://crdt.tech/

https://en.wikipedia.org/wiki/Conflict-free_replicated_data_type


```shell
cargo build && maelstrom/maelstrom test -w g-counter --bin ./target/debug/g-counter --node-count 3 --rate 100 --time-limit 20 --log-stderr --nemesis partition
```

### 5a) Single-Node Kafka-Style Log

```shell
cargo build && maelstrom/maelstrom test -w kafka --bin ./target/debug/kafka-single-node --node-count 1 --concurrency 2n --time-limit 20 --rate 1000 --log-stderr
```

### 5b + 5c) Multi-Node Kafka-Style Log

Nodes are using  [linearizable](https://jepsen.io/consistency/models/linearizable) and [sequentially-consistent](https://jepsen.io/consistency/models/sequential) key/value store services provided by Maelstrom.

```shell
cargo build && maelstrom/maelstrom test -w kafka --bin ./target/debug/kafka-multi-node --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --log-stderr
```

### 6a) Single-Node, Totally-Available Transactions

```shell
cargo build && maelstrom/maelstrom test -w txn-rw-register --bin ./target/debug/txn --log-stderr --node-count 1 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total
```

### 6b) Multi-Node, Totally-Available, Read Uncommitted Transactions

Transaction writes are replicated across all nodes while ensuring a [Read Uncommitted](https://jepsen.io/consistency/models/read-uncommitted) consistency model and total availability.

```shell
cargo build && maelstrom/maelstrom test -w txn-rw-register --bin ./target/debug/txn --log-stderr --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition
```

### 6c) Multi-Node, Totally-Available, Read Committed Transactions

Consistency model is strengthened to [Read Committed](https://jepsen.io/consistency/models/read-committed) while also preserving total availability.

```shell
cargo build && maelstrom/maelstrom test -w txn-rw-register --bin ./target/debug/txn --log-stderr --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition
```

```shell
cargo build && maelstrom/maelstrom test -w txn-rw-register --bin ./target/debug/txn --log-stderr --node-count 2 --concurrency 10n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total --nemesis partition --max-txn-length 20 --max-writes-per-key 10000000 --key-count 2
```
