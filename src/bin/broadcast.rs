use std::collections::{HashMap, HashSet};

use anyhow::bail;
use gossipy::{Handler, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Broadcast {
        message: isize,
    },
    BroadcastOk,
    Read,
    ReadOk {
        messages: HashSet<isize>,
    },
    Topology {
        topology: HashMap<String, Vec<String>>,
    },
    TopologyOk,
}

/// Single-Node Broadcast system
struct BroadcastHandler {
    messages: HashSet<isize>,
}

impl Handler<Payload> for BroadcastHandler {
    fn handle(&mut self, msg: Message<Payload>, node: &mut Node) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let reply = match msg.body.payload {
            Payload::Broadcast { message } => {
                self.messages.insert(message);
                Payload::BroadcastOk
            }
            Payload::Read {} => Payload::ReadOk {
                messages: self.messages.clone(),
            },
            Payload::Topology { .. } => Payload::TopologyOk,
            _ => bail!("Unexpected message received: {:?}", msg),
        };

        node.reply(msg, reply)
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::new()?;

    let broadcast_handler = BroadcastHandler {
        messages: HashSet::new(),
    };

    node.run(broadcast_handler)
}
