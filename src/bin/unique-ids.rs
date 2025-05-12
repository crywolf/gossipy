use gossipy::{Handler, Message, Node};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
pub enum Payload {
    Generate,
    GenerateOk { id: String },
}

/// Generates globally unique IDs
struct GenerateIdHandler {
    id: usize,
}

impl Handler<Payload> for GenerateIdHandler {
    fn handle(&mut self, msg: Message<Payload>, node: &mut Node) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let unique_id = format!("{}-{}", node.id, self.id);

        let reply = match msg.body.payload {
            Payload::Generate => Payload::GenerateOk { id: unique_id },
            Payload::GenerateOk { .. } => return Ok(()), // we do not care about these messages
        };

        self.id += 1;

        node.reply(msg, reply)
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::new()?;

    let handler = GenerateIdHandler { id: 1 };

    node.run(handler)
}
