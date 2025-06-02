use std::{
    io::Write,
    sync::{Arc, Mutex},
};

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

#[derive(Clone, Debug)]
pub enum Event<Payload, Command> {
    Message(Message<Payload>),
    Command(Command),
}

/// Message handler
///
/// Every node must implement this trait to handle incoming messages
pub trait Handler<Payload, Command = ()> {
    /// Handles message
    fn handle(&mut self, msg: Message<Payload>, node: Node) -> anyhow::Result<()>;
    /// Handles command. Only node that issues commands needs to implement this method.
    fn handle_command(&mut self, _cmd: Command, _node: Node) -> anyhow::Result<()> {
        unimplemented!("Node handler using commands must implement this method");
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum InitPayload {
    Init {
        node_id: String,
        node_ids: Vec<String>,
    },
    InitOk {},
}

/// Common node functionality
#[derive(Clone)]
pub struct Node {
    inner: Arc<Mutex<Inner>>,
}

pub struct Inner {
    /// Node ID
    pub id: String,
    /// List of all nodes in the cluster, including the recipient
    pub node_ids: Vec<String>,
    /// Message ID counter
    msg_id: usize,
    stdin: std::io::Stdin,
    stdout: std::io::Stdout,
}

impl Node {
    /// Creates new [`Node`] instance initialized by Maelstrom `init` message
    pub fn new() -> anyhow::Result<Self> {
        let mut node = Self {
            inner: Arc::new(Mutex::new(Inner {
                id: String::new(),
                node_ids: Vec::new(),
                msg_id: 1,
                stdin: std::io::stdin(),
                stdout: std::io::stdout(),
            })),
        };

        let stdin = node.inner.lock().expect("lock").stdin.lock();

        let msg: Message<InitPayload> = serde_json::Deserializer::from_reader(stdin)
            .into_iter()
            .next()
            .ok_or(anyhow!("failed to read Init message from STDIN"))?
            .context("deserializing Init message from STDIN")?;

        let reply_payload = match msg.body.payload {
            InitPayload::Init { node_id, node_ids } => {
                let mut node = node.inner.lock().expect("lock");
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
            src: node.inner.lock().expect("lock").id.clone(),
            dst: msg.src,
            body,
        };

        node.send(reply)?;

        Ok(node)
    }

    /// Starts main loop that processes incoming messages
    pub fn run<H, Payload, Command>(
        &mut self,
        mut handler: H,
        command_rx: Option<std::sync::mpsc::Receiver<Command>>,
    ) -> anyhow::Result<()>
    where
        H: Handler<Payload, Command> + Send + 'static,
        Payload: Serialize + DeserializeOwned + Send + 'static + Sync,
        Command: Send + 'static + Sync,
    {
        let (event_tx, event_rx) = std::sync::mpsc::channel::<Event<Payload, Command>>();

        let event_tx_clone = event_tx.clone();
        if let Some(rx) = command_rx {
            std::thread::spawn(move || loop {
                let cmd = rx.recv().unwrap();
                if let Err(e) = event_tx_clone
                    .send(Event::Command(cmd))
                    .context("sending command to the channel")
                {
                    {
                        eprintln!("ERROR: {e:?}");
                    }
                }
            });
        }

        let node = self.clone();
        std::thread::spawn(move || loop {
            let event = event_rx.recv().unwrap();
            match event {
                Event::Message(msg) => {
                    if let Err(e) = handler
                        .handle(msg, node.clone())
                        .context("sending message event to the channel")
                    {
                        {
                            eprintln!("ERROR: {e:?}");
                        }
                    }
                }
                Event::Command(cmd) => {
                    if let Err(e) = handler
                        .handle_command(cmd, node.clone())
                        .context("sending command event to the channel")
                    {
                        {
                            eprintln!("ERROR: {e:?}");
                        }
                    }
                }
            }
        });

        let incomming_messages = self.messages::<Message<Payload>>();

        for msg in incomming_messages {
            let msg = msg.context("deserializing Maelstrom message from STDIN failed")?;
            event_tx.send(Event::Message(msg))?;
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
            src: self.inner.lock().expect("lock").id.clone(),
            dst: incoming_msg.src,
            body,
        };

        self.send(reply)
    }

    /// Sends new message with `payload` to `dst`
    pub fn send_to<P>(&mut self, dst: &str, payload: P) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        let body = Body {
            id: Some(self.new_msg_id()),
            in_reply_to: None,
            payload,
        };

        let reply = Message {
            src: self.inner.lock().expect("lock").id.clone(),
            dst: dst.to_owned(),
            body,
        };

        self.send(reply)
    }

    /// Sends provided message
    pub fn send<P>(&mut self, msg: Message<P>) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        let mut stdout = self.inner.lock().expect("lock").stdout.lock();

        serde_json::to_writer(&mut stdout, &msg).context("writing message to STDOUT")?;
        stdout.write_all(b"\n").context("write new line")?;

        Ok(())
    }

    /// Returns node's ID
    pub fn id(&self) -> String {
        self.inner.lock().expect("lock").id.clone()
    }

    /// Returns the list of all nodes in the cluster
    pub fn node_ids(&self) -> Vec<String> {
        self.inner.lock().expect("lock").node_ids.clone()
    }

    /// Returns iterator over deserialized incoming messages
    fn messages<T>(&mut self) -> impl Iterator<Item = Result<T, serde_json::Error>>
    where
        T: for<'a> Deserialize<'a>,
    {
        serde_json::Deserializer::from_reader(self.inner.lock().expect("lock").stdin.lock())
            .into_iter::<T>()
    }

    /// Produces new message ID
    fn new_msg_id(&mut self) -> usize {
        let mut node = self.inner.lock().expect("lock");
        let msg_id = node.msg_id;
        node.msg_id += 1;
        msg_id
    }
}
