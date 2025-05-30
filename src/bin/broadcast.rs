use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context};
use gossipy::{Handler, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
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
    topology: HashMap<String, Vec<String>>,
    neighbours: Vec<String>,
    others_know: HashMap<String, HashSet<isize>>,
    chan: std::sync::mpsc::Sender<Vec<String>>,
}

impl Handler<Payload> for BroadcastHandler {
    fn handle(&mut self, msg: Message<Payload>, mut node: Node) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let reply = match msg.body.payload {
            Payload::Broadcast { message } => {
                self.messages.insert(message);

                // Send the message to node's neighbours
                for dst in self.neighbours.iter() {
                    // do not send back to the source node
                    if &msg.src == dst {
                        continue;
                    }

                    // do not send what neighbour already knows
                    if let Some(neighbours_know) = self.others_know.get(dst) {
                        if neighbours_know.contains(&message) {
                            continue;
                        }
                    }
                    node.send_to(dst, msg.body.payload.clone())?;
                }

                Payload::BroadcastOk
            }
            Payload::Read {} => Payload::ReadOk {
                messages: self.messages.clone(),
            },
            Payload::ReadOk { messages } => {
                self.others_know
                    .get_mut(&msg.src)
                    .expect("message from unknown node in the cluster")
                    .extend(messages);
                return Ok(());
            }
            Payload::Topology { ref topology } => {
                self.topology = topology.clone();
                self.neighbours = self
                    .topology
                    .get(&node.id())
                    .expect("our node must be included in topology map")
                    .clone();
                self.chan.send(self.neighbours.clone())?;

                Payload::TopologyOk
            }
            Payload::BroadcastOk => return Ok(()), // we do not care about these messages
            _ => bail!("Unexpected message received: {:?}", msg),
        };

        node.reply(msg, reply)
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::new()?;

    let (tx, rx) = std::sync::mpsc::channel();

    let broadcast_handler = BroadcastHandler {
        messages: HashSet::new(),
        topology: HashMap::new(),
        neighbours: Vec::new(),
        others_know: node
            // preallocate with all the nodes in the cluster
            .node_ids()
            .iter()
            .map(|id| (id.clone(), HashSet::new()))
            .collect(),
        chan: tx,
    };

    let mut node_clone = node.clone();
    std::thread::spawn(move || {
        // periodically check what neighbours already know
        let neighbours = rx.recv().unwrap();
        loop {
            std::thread::sleep(std::time::Duration::from_millis(300));

            for neighbour in neighbours.iter() {
                if let Err(e) = node_clone
                    .send_to(neighbour, Payload::Read)
                    .context("sending 'Read' to {neighbour}")
                {
                    eprintln!("ERROR: {e:?}");
                    break;
                }
            }
        }
    });

    node.run(broadcast_handler)?;
    Ok(())
}
