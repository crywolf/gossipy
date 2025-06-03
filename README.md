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
cargo build && maelstrom/maelstrom test -w broadcast --bin ./target/debug/broadcast --node-count 5 --time-limit 20 --rate 10  --nemesis partition --log-stderr
```
