use std::{
    collections::{HashMap, HashSet},
    thread::JoinHandle,
};

use anyhow::Context;
use gossipy::{Handler, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
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
    Gossip {
        have: HashSet<isize>,
    },
    GossipOk {
        have: HashSet<isize>,
    },
}

#[derive(Debug, Clone)]
enum Command {
    SendGossip,
}

/// Single-Node Broadcast system
struct BroadcastHandler {
    messages: HashSet<isize>,
    topology: HashMap<String, Vec<String>>,
    neighbours: Vec<String>,
    others_know: HashMap<String, HashSet<isize>>,
}

impl Handler<Payload, Command> for BroadcastHandler {
    fn handle(&mut self, msg: Message<Payload>, mut node: Node<Command>) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let reply = match msg.body.payload {
            Payload::Broadcast { message } => {
                self.messages.insert(message);

                Payload::BroadcastOk
            }
            Payload::Gossip { ref have } => {
                // add to what we know
                self.messages.extend(have.iter());
                // update what neighbour already knows
                self.others_know
                    .get_mut(&msg.src)
                    .expect("message from unknown node in the cluster")
                    .extend(have.iter());
                // ackowledge what we just have received
                Payload::GossipOk { have: have.clone() }
            }
            Payload::Read {} => Payload::ReadOk {
                messages: self.messages.clone(),
            },
            Payload::ReadOk { messages } => {
                self.others_know.insert(msg.src, messages);
                return Ok(());
            }
            Payload::Topology { ref topology } => {
                self.topology = topology.clone();
                self.neighbours = self
                    .topology
                    .get(&node.id())
                    .expect("our node must be included in topology map")
                    .clone();

                Payload::TopologyOk
            }
            Payload::BroadcastOk | Payload::TopologyOk => return Ok(()), // we ignore these messages
            Payload::GossipOk { ref have } => {
                // add to what the neighbour acknowledged to know
                self.others_know
                    .get_mut(&msg.src)
                    .expect("message from unknown node in the cluster")
                    .extend(have.iter());
                return Ok(());
            }
        };

        node.reply(msg, reply)
    }

    fn handle_command(&mut self, cmd: Command, mut node: Node<Command>) -> anyhow::Result<()> {
        for neighbour in self.neighbours.iter() {
            match cmd {
                Command::SendGossip => {
                    let mut we_know = HashSet::new();
                    if let Some(neighbours_know) = self.others_know.get(neighbour) {
                        // do not send what neighbour already knows
                        we_know = self
                            .messages
                            .iter()
                            .filter(|m| !neighbours_know.contains(m))
                            .copied()
                            .collect();
                    }

                    if we_know.is_empty() {
                        // neighbour already knows about all our messages => do not send
                        continue;
                    }

                    node.send_to(neighbour, Payload::Gossip { have: we_know })?;
                }
            }
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut args = std::env::args();
    let gossip_interval = args.nth(1).unwrap_or("200".to_string()).parse()?;
    eprintln!("Using gossip interval {} ms", gossip_interval);

    let mut node = Node::new()?;

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
    };

    let (tx, rx) = std::sync::mpsc::channel::<Command>();

    node.register_command_receiver(rx);

    let jh: JoinHandle<Result<_, anyhow::Error>> = std::thread::spawn(move || {
        // periodically gossip new messages to the other nodes in the cluster
        loop {
            std::thread::sleep(std::time::Duration::from_millis(gossip_interval));
            tx.send(Command::SendGossip)
                .context("sending Gossip command")?;
        }
    });

    node.run(broadcast_handler)?;

    jh.join()
        .expect("could not join gossip command thread")
        .context("gossip command thread errored")?;

    Ok(())
}
