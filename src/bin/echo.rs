use anyhow::Context;
use gossipy::{Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

struct EchoNode {
    node: Node,
}

impl EchoNode {
    fn run() -> anyhow::Result<()> {
        let mut node = EchoNode { node: Node::new()? };

        let messages = node.node.messages::<Message<Payload>>();

        for msg in messages {
            let msg = msg.context("deserializing Maelstrom message from STDIN failed")?;
            node.handle(msg)?;
        }

        Ok(())
    }

    fn handle(&mut self, incoming_msg: Message<Payload>) -> anyhow::Result<()> {
        let resp_payload = match incoming_msg.body.payload {
            Payload::Echo { ref echo } => Payload::EchoOk {
                echo: echo.to_owned(),
            },
            Payload::EchoOk { .. } => return Ok(()), // we do not care about these messages
        };

        self.node.reply(incoming_msg, resp_payload)
    }
}

fn main() -> anyhow::Result<()> {
    EchoNode::run()
}
