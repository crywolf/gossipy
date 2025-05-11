use gossipy::{GossipyNode, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

/// Replies to Echo messages
struct EchoNode {}

impl Node<Payload> for EchoNode {
    fn handle(&mut self, msg: Message<Payload>, node: &mut GossipyNode) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let reply = match msg.body.payload {
            Payload::Echo { ref echo } => Payload::EchoOk {
                echo: echo.to_owned(),
            },
            Payload::EchoOk { .. } => return Ok(()), // we do not care about these messages
        };

        node.reply(msg, reply)
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = GossipyNode::new()?;

    let echo_node = EchoNode {};

    node.run(echo_node)
}
