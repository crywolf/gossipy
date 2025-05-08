use std::io::{BufRead, Write};

use anyhow::Context;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Message {
    src: String,
    #[serde(rename = "dest")]
    dst: String,
    body: Body,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Body {
    #[serde(rename = "msg_id")]
    id: Option<usize>,
    in_reply_to: Option<usize>,
    #[serde(flatten)]
    payload: Payload,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum Payload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {},
    Echo {
        echo: String,
    },
    EchoOk {
        echo: String,
    },
}

fn main() -> anyhow::Result<()> {
    let stdin = std::io::stdin().lock();
    let mut stdout = std::io::stdout().lock();

    let mut my_node_id = String::new();
    let mut _all_node_ids = Vec::new();
    let mut msg_id = 1usize;

    for line in stdin.lines() {
        let line = line.context("failed to read from STDIN")?;
        let msg = serde_json::from_str::<Message>(&line)
            .context("deserializing Maelstrom message from STDIN")?;

        let resp_payload = match msg.body.payload {
            Payload::Init { node_id, node_ids } => {
                my_node_id = node_id;
                _all_node_ids = node_ids;
                Payload::InitOk {}
            }
            Payload::Echo { echo } => Payload::EchoOk { echo },
            _ => {
                eprintln!("Unexpected message received: {:?}", msg);
                continue;
            }
        };

        let body = Body {
            id: Some(msg_id),
            in_reply_to: msg.body.id,
            payload: resp_payload,
        };

        let response = Message {
            src: my_node_id.clone(),
            dst: msg.src,
            body,
        };

        serde_json::to_writer(&mut stdout, &response).context("writing message to STDOUT")?;
        stdout.write_all(b"\n").context("write new line")?;
        msg_id += 1;
    }

    Ok(())
}
