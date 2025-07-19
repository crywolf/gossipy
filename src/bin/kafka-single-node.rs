use std::collections::HashMap;

use gossipy::{Handler, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Send {
        key: String,
        msg: u64,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, u64)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
}

/// Single-Node Kafka-Style Log
#[derive(Default)]
struct KafkaLog {
    //    log_id => ([messages], committed)
    logs: HashMap<String, (Vec<u64>, usize)>,
}

impl Handler<Payload> for KafkaLog {
    fn handle(&mut self, msg: Message<Payload>, mut node: Node) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let reply = match msg.body.payload {
            Payload::Send { ref key, msg } => {
                self.logs
                    .entry(key.to_string())
                    .and_modify(|(v, _c)| v.push(msg))
                    .or_insert((vec![msg], 0));
                let offset = self.logs.get(key).expect("log is present").0.len() - 1;
                Payload::SendOk { offset }
            }
            Payload::Poll { ref offsets } => {
                let mut msgs = HashMap::new();

                for (key, &offset) in offsets.iter() {
                    let log = self.logs.get(key);
                    if log.is_none() {
                        continue;
                    }

                    let log = log.expect("log exists");

                    let mut messages = vec![];
                    for i in offset..=(offset + 2) {
                        if let Some(msg) = log.0.get(i) {
                            messages.push((i, *msg));
                        }
                    }

                    msgs.insert(key.to_string(), messages);
                }

                Payload::PollOk { msgs }
            }
            Payload::CommitOffsets { ref offsets } => {
                for (key, offset) in offsets.iter() {
                    self.logs
                        .entry(key.to_string())
                        .and_modify(|(_v, committed)| *committed = *offset);
                }
                Payload::CommitOffsetsOk
            }

            Payload::ListCommittedOffsets { ref keys } => {
                let mut offsets = HashMap::new();
                for key in keys.iter() {
                    if let Some((_, committed)) = self.logs.get(key) {
                        offsets.insert(key.to_string(), *committed);
                    }
                }
                Payload::ListCommittedOffsetsOk { offsets }
            }

            Payload::SendOk { .. }
            | Payload::PollOk { .. }
            | Payload::CommitOffsetsOk { .. }
            | Payload::ListCommittedOffsetsOk { .. } => return Ok(()),
        };

        node.reply(msg, reply)
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::new()?;

    let handler = KafkaLog::default();

    node.run(handler)?;

    Ok(())
}
