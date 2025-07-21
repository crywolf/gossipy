use std::collections::HashMap;

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
}

/// Single-Node, Totally-Available Transactions System
struct TxnHandler {
    /// Key-value store
    store: HashMap<usize, usize>,
}

impl Handler<Payload> for TxnHandler {
    fn handle(&mut self, mut msg: Message<Payload>, mut node: Node) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let reply = match msg.body.payload {
            Payload::Txn { ref mut txn } => {
                let mut resp = vec![];

                for op in txn.iter_mut() {
                    match op {
                        Operation::Read { key, ref mut val } => {
                            let v = self.store.get(key);
                            *val = v.copied();
                        }
                        Operation::Write { key, val } => {
                            self.store.insert(*key, *val);
                        }
                    }

                    resp.push(*op);
                }

                Payload::TxnOk { txn: resp }
            }

            Payload::TxnOk { .. } => return Ok(()), // we ignore these messages
        };

        node.reply(msg, reply)
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

fn main() -> anyhow::Result<()> {
    let mut node = Node::new()?;

    let txn_handler = TxnHandler {
        store: HashMap::new(),
    };

    node.run(txn_handler)?;

    Ok(())
}
