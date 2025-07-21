use std::{collections::HashMap, sync::mpsc::Sender};

use anyhow::Context;
use gossipy::{Handler, Message, Node};
use serde::{de, ser::SerializeSeq, Deserialize, Deserializer, Serialize, Serializer};

#[derive(Debug, Clone, Copy)]
enum Operation {
    Read { key: usize, val: Option<usize> },
    Write { key: usize, val: usize },
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Txn { txn: Vec<Operation> },
    TxnOk { txn: Vec<Operation> },
    Replicate { changes: HashMap<String, usize> },
    ReplicateOk,
}

/// Multi-Node, Totally-Available Transactions System
struct TxnHandler {
    store: Store,
    changes: HashMap<String, usize>,
    command_tx: Sender<Command>,
}

impl Handler<Payload, Command> for TxnHandler {
    fn handle(&mut self, mut msg: Message<Payload>, mut node: Node<Command>) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let reply = match msg.body.payload {
            Payload::Txn { ref mut txn } => {
                let mut resp = vec![];

                for op in txn.iter_mut() {
                    match op {
                        Operation::Read { key, ref mut val } => {
                            let v = self.store.read(key);
                            *val = v.copied();
                        }
                        Operation::Write { key, val } => {
                            let changed = self.store.write(*key, *val);
                            if changed {
                                self.changes.insert(key.to_string(), *val);
                            }
                        }
                    }

                    resp.push(*op);
                }

                if !self.changes.is_empty() {
                    self.command_tx
                        .send(Command::Replicate)
                        .context("send Replicate command")?;
                }

                Payload::TxnOk { txn: resp }
            }

            Payload::TxnOk { .. } => return Ok(()),

            Payload::Replicate { ref changes } => {
                for (k, v) in changes {
                    self.store.write(k.parse()?, *v);
                }
                Payload::ReplicateOk
            }

            Payload::ReplicateOk => return Ok(()),
        };

        node.reply(msg, reply)
    }

    fn handle_command(&mut self, cmd: Command, mut node: Node<Command>) -> anyhow::Result<()> {
        match cmd {
            Command::Replicate => {
                if self.changes.is_empty() {
                    return Ok(());
                }
                // send changes to other nodes in the cluster
                for node_id in node.node_ids() {
                    if node_id == node.id() {
                        continue;
                    }
                    node.send_to(
                        &node_id,
                        Payload::Replicate {
                            changes: self.changes.clone(),
                        },
                    )?;
                }
                self.changes.clear();
            }
        }
        Ok(())
    }
}

impl TxnHandler {
    fn new(command_tx: Sender<Command>) -> Self {
        Self {
            store: Default::default(),
            changes: Default::default(),
            command_tx,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::new()?;

    let (tx, rx) = std::sync::mpsc::channel::<Command>();

    node.register_command_receiver(rx);

    let txn_handler = TxnHandler::new(tx);

    node.run(txn_handler)?;

    Ok(())
}

#[derive(Debug, Clone)]
enum Command {
    Replicate,
}

#[derive(Default)]
/// Key-value store
struct Store(HashMap<usize, usize>);

impl Store {
    fn read(&self, key: &usize) -> Option<&usize> {
        self.0.get(key)
    }

    fn write(&mut self, key: usize, val: usize) -> bool {
        if let Some(old) = self.0.insert(key, val) {
            old != val // value was changed
        } else {
            false // value is still the same, no change
        }
    }
}

impl Serialize for Operation {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        match self {
            Operation::Read { key, val } => {
                seq.serialize_element("r")?;
                seq.serialize_element(key)?;
                seq.serialize_element(val)?;
            }
            Operation::Write { key, val } => {
                seq.serialize_element("w")?;
                seq.serialize_element(key)?;
                seq.serialize_element(val)?;
            }
        };
        seq.end()
    }
}

impl<'de> Deserialize<'de> for Operation {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct InnerOp(char, usize, Option<usize>);

        let op = InnerOp::deserialize(deserializer)?;
        match op.0 {
            'r' => Ok(Operation::Read {
                key: op.1,
                val: op.2,
            }),
            'w' => Ok(Operation::Write {
                key: op.1,
                val: op.2.expect("must be a value"),
            }),
            x => Err(de::Error::custom(format!(
                "unexpected operation type '{}'",
                x
            ))),
        }
    }
}
