use gossipy::{Handler, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Echo { echo: String },
    EchoOk { echo: String },
}

/// Replies to Echo messages
struct EchoHandler {}

impl Handler<Payload> for EchoHandler {
    fn handle(&mut self, msg: Message<Payload>, mut node: Node) -> anyhow::Result<()>
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
    let mut node = Node::new()?;

    let echo_handler = EchoHandler {};

    node.run(echo_handler, None)
}
