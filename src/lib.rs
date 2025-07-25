use std::{
    io::Write,
    sync::{Arc, Mutex},
    thread::JoinHandle,
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

impl<P> Message<P> {
    pub fn new_empty(payload: P) -> Self {
        Self {
            src: Default::default(),
            dst: Default::default(),
            body: Body {
                id: None,
                in_reply_to: None,
                payload,
            },
        }
    }
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
    fn handle(&mut self, msg: Message<Payload>, node: Node<Command>) -> anyhow::Result<()>;
    /// Handles command. Only node that issues commands needs to implement this method.
    fn handle_command(&mut self, _cmd: Command, _node: Node<Command>) -> anyhow::Result<()> {
        unimplemented!("Node handler using commands must implement this method!!!");
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
pub struct Node<Command = ()> {
    inner: Arc<Mutex<Inner>>,
    command_rx: Option<Arc<Mutex<std::sync::mpsc::Receiver<Command>>>>,
}

pub struct Inner {
    /// Node ID
    pub id: String,
    /// List of all nodes in the cluster, including the recipient
    pub node_ids: Vec<String>,
    /// Message ID counter
    msg_id: usize,
}

impl<Command> Node<Command>
where
    Command: Clone,
{
    /// Creates new [`Node`] instance initialized by Maelstrom `init` message
    pub fn new() -> anyhow::Result<Self> {
        let mut node = Self {
            inner: Arc::new(Mutex::new(Inner {
                id: String::new(),
                node_ids: Vec::new(),
                msg_id: 1,
            })),
            command_rx: None,
        };

        let msg: Message<InitPayload> = serde_json::Deserializer::from_reader(std::io::stdin())
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
            src: node.id(),
            dst: msg.src,
            body,
        };

        node.send(reply)?;

        Ok(node)
    }

    /// Registers Receiver part of the channel to receive commands
    pub fn register_command_receiver(&mut self, command_rx: std::sync::mpsc::Receiver<Command>)
    where
        Command: Send + 'static + Sync,
    {
        self.command_rx = Some(Arc::new(Mutex::new(command_rx)));
    }

    /// Starts main loop that processes incoming messages
    pub fn run<H, Payload>(&mut self, mut handler: H) -> anyhow::Result<()>
    where
        H: Handler<Payload, Command> + Send + 'static,
        Payload: Serialize + DeserializeOwned + Send + 'static + Sync,
        Command: Send + 'static + Sync,
    {
        let (event_tx, event_rx) = std::sync::mpsc::channel::<Event<Payload, Command>>();
        let event_tx_clone = event_tx.clone();

        let mut cmd_jh: Option<JoinHandle<Result<_, anyhow::Error>>> = None;
        if let Some(command_rx) = self.command_rx.clone() {
            // if the node has command receiver registered,
            // use it to create events of type Command and send it to the event channel
            let jh = std::thread::spawn(move || loop {
                let cmd = command_rx
                    .lock()
                    .expect("lock")
                    .recv()
                    .context("receive from command channel")?;

                event_tx_clone
                    .send(Event::Command(cmd))
                    .context("sending received command to the event channel")?;
            });
            cmd_jh = Some(jh);
        }

        // listen for events from the event channel and handle either message or command
        let node = self.clone();
        let event_jh: JoinHandle<Result<_, anyhow::Error>> = std::thread::spawn(move || loop {
            let event = event_rx.recv().context("receive from event channel")?;
            match event {
                Event::Message(msg) => {
                    handler
                        .handle(msg, node.clone())
                        .context("handling message from the event channel")?;
                }
                Event::Command(cmd) => {
                    handler
                        .handle_command(cmd, node.clone())
                        .context("handling command from the event channel")?;
                }
            }
        });

        // send incoming messages to event channel as a Message events
        let incoming_messages = self.messages::<Message<Payload>>();

        let mut error: Option<anyhow::Error> = None;
        for msg in incoming_messages {
            let msg = msg.context("deserializing Maelstrom message from STDIN failed")?;
            if let Err(e) = event_tx
                .send(Event::Message(msg))
                .context("sending message from stdin to the event channel")
            {
                error = Some(e);
                break;
            };
        }

        if let Some(cmd_jh) = cmd_jh {
            cmd_jh
                .join()
                .expect("could not join command thread")
                .context("command thread errored")?;
        }
        event_jh
            .join()
            .expect("could not join event thread")
            .context("event thread errored")?;

        if let Some(err) = error {
            // log input loop error only after threads finished
            eprintln!("Error: {err:?}");
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
            src: self.id(),
            dst: incoming_msg.src,
            body,
        };

        self.send(reply)
    }

    /// Sends new message with `payload` to `dst` and returns message id
    pub fn send_to<P>(&mut self, dst: &str, payload: P) -> anyhow::Result<usize>
    where
        P: Serialize,
    {
        let msg_id = self.new_msg_id();

        let body = Body {
            id: Some(msg_id),
            in_reply_to: None,
            payload,
        };

        let reply = Message {
            src: self.id(),
            dst: dst.to_owned(),
            body,
        };

        self.send(reply)?;

        Ok(msg_id)
    }

    /// Sends provided message
    fn send<P>(&mut self, msg: Message<P>) -> anyhow::Result<()>
    where
        P: Serialize,
    {
        let mut stdout = std::io::stdout();

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
        serde_json::Deserializer::from_reader(std::io::stdin()).into_iter::<T>()
    }

    /// Produces new message ID
    fn new_msg_id(&mut self) -> usize {
        let mut node = self.inner.lock().expect("lock");
        let msg_id = node.msg_id;
        node.msg_id += 1;
        msg_id
    }
}

pub mod kv_store {
    pub const SEQ_KV_SERVICE_ID: &str = "seq-kv";
    pub const LIN_KV_SERVICE_ID: &str = "lin-kv";

    pub const ERROR_KEY_DOES_NOT_EXIST: u16 = 20;
    pub const ERROR_PRECONDITION_FAILED: u16 = 22;
}
