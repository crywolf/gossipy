use std::io::{BufRead, StdoutLock, Write};

use anyhow::{bail, Context};
use gossipy::{Body, Message, Payload};

struct EchoNode {
    id: String,
    node_ids: Vec<String>,
    msg_id: usize,
}

impl EchoNode {
    fn handle(&mut self, input: Message, stdout: &mut StdoutLock) -> anyhow::Result<()> {
        let resp_payload = match input.body.payload {
            Payload::Init { node_id, node_ids } => {
                self.id = node_id;
                self.node_ids = node_ids;
                Payload::InitOk {}
            }
            Payload::Echo { echo } => Payload::EchoOk { echo },
            Payload::EchoOk { .. } => return Ok(()), // we do not care about these messages
            _ => {
                bail!("Unexpected message received: {:?}", input);
            }
        };

        let body = Body {
            id: Some(self.msg_id),
            in_reply_to: input.body.id,
            payload: resp_payload,
        };

        let response = Message {
            src: self.id.clone(),
            dst: input.src,
            body,
        };

        serde_json::to_writer(&mut *stdout, &response).context("writing message to STDOUT")?;
        stdout.write_all(b"\n").context("write new line")?;

        self.msg_id += 1;

        Ok(())
    }

    fn run() -> anyhow::Result<()> {
        let stdin = std::io::stdin().lock();
        let mut stdout = std::io::stdout().lock();

        let mut node = EchoNode {
            id: String::new(),
            node_ids: vec![],
            msg_id: 1,
        };

        for line in stdin.lines() {
            let line = line.context("failed to read from STDIN")?;
            let input = serde_json::from_str::<Message>(&line)
                .context("deserializing Maelstrom message from STDIN")?;

            node.handle(input, &mut stdout)?;
        }

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    EchoNode::run()
}
