use std::io::Write;

use anyhow::{anyhow, bail, Context};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message<P> {
    pub src: String,
    #[serde(rename = "dest")]
    pub dst: String,
    pub body: Body<P>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Body<P> {
    #[serde(rename = "msg_id")]
    pub id: Option<usize>,
    pub in_reply_to: Option<usize>,
    #[serde(flatten)]
    pub payload: P,
}

/// Message handler
///
/// Every node must implement this trait to handle incoming messages
pub trait Handler<Payload> {
    fn handle(&mut self, msg: Message<Payload>, node: &mut Node) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
#[serde(rename_all = "snake_case")]
enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {},
}

/// Common node functionality
pub struct Node {
    pub id: String,
    node_ids: Vec<String>,
    msg_id: usize,
    stdin: std::io::Stdin,
    stdout: std::io::Stdout,
}

impl Node {
    /// Creates new [`Node`] instance initialized by Maelstrom `init` message
    pub fn new() -> anyhow::Result<Self> {
        let mut node = Self {
            id: String::new(),
            node_ids: Vec::new(),
            msg_id: 1,
            stdin: std::io::stdin(),
            stdout: std::io::stdout(),
        };

        let stdin = node.stdin.lock();

        let msg: Message<InitPayload> = serde_json::Deserializer::from_reader(stdin)
            .into_iter()
            .next()
            .ok_or(anyhow!("failed to read Init message from STDIN"))?
            .context("deserializing Init message from STDIN")?;

        let reply_payload = match msg.body.payload {
            InitPayload::Init { node_id, node_ids } => {
                node.id = node_id;
                node.node_ids = node_ids;
                InitPayload::InitOk {}
            }
            InitPayload::InitOk {} => bail!("Unexpected message received: {:?}", msg),
        };

        let body = Body {
            id: Some(node.new_msg_id()),
            in_reply_to: msg.body.id,
            payload: reply_payload,
        };

        let reply = Message {
            src: node.id.clone(),
            dst: msg.src,
            body,
        };

        node.send(reply)?;

        Ok(node)
    }

    /// Starts main loop that processes incoming messages
    pub fn run<H, Payload>(&mut self, mut handler: H) -> anyhow::Result<()>
    where
        H: Handler<Payload>,
        Payload: Serialize + DeserializeOwned,
    {
        let messages = self.messages::<Message<Payload>>();

        for msg in messages {
            let msg = msg.context("deserializing Maelstrom message from STDIN failed")?;
            handler.handle(msg, self)?;
        }

        Ok(())
    }

    /// Replies to the incoming message with a reply with specified new payload
    pub fn reply<P>(&mut self, incoming_msg: Message<P>, new_payload: P) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        let body = Body {
            id: Some(self.new_msg_id()),
            in_reply_to: incoming_msg.body.id,
            payload: new_payload,
        };

        let reply = Message {
            src: self.id.clone(),
            dst: incoming_msg.src,
            body,
        };

        self.send(reply)
    }

    /// Sends provided message
    pub fn send<P>(&mut self, msg: Message<P>) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        serde_json::to_writer(&mut self.stdout.lock(), &msg)
            .context("writing message to STDOUT")?;
        self.stdout.write_all(b"\n").context("write new line")?;

        Ok(())
    }

    /// Returns iterator over deserialized incoming messages
    fn messages<T>(&mut self) -> impl Iterator<Item = Result<T, serde_json::Error>>
    where
        T: for<'a> Deserialize<'a>,
    {
        serde_json::Deserializer::from_reader(self.stdin.lock()).into_iter::<T>()
    }

    /// Produces new message ID
    fn new_msg_id(&mut self) -> usize {
        let msg_id = self.msg_id;
        self.msg_id += 1;
        msg_id
    }
}
