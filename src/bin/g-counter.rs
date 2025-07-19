use std::{
    collections::HashMap,
    thread::JoinHandle,
    time::{SystemTime, UNIX_EPOCH},
};

use anyhow::{bail, Context};
use gossipy::kv_store::{ERROR_PRECONDITION_FAILED, SEQ_KV_SERVICE_ID};
use gossipy::{Handler, Message, Node};
use serde::{Deserialize, Serialize};

const COUNTER_KEY: &str = "g-counter";

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Add { delta: usize },
    AddOk,
    Read,
    ReadOk { value: usize },
    Error { code: u16, text: String },
    WriteOk,
    CasOk,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum KvStorePayload {
    Write {
        key: &'static str,
        value: usize,
    },
    Read {
        key: &'static str,
    },
    /// Compare And Swap
    Cas {
        key: &'static str,
        from: usize,
        to: usize,
        create_if_not_exists: bool,
    },
}

#[derive(Debug, Clone)]
enum Command {
    /// Initialize KV store
    InitStore,
}

/// Stateless Grow-Only Counter
#[derive(Default)]
struct GCounter {
    /// Messages sent to KV store
    //    kv_msg_id => (orig_msg_src, orig_msg_id)
    kv_msg_ids: HashMap<usize, (String, usize)>,
    /// Delta values added by clients
    //    kv_msg_id => delta
    deltas: HashMap<usize, usize>,
}

impl Handler<Payload, Command> for GCounter {
    fn handle(&mut self, msg: Message<Payload>, mut node: Node<Command>) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        let reply = match msg.body.payload {
            Payload::Add { delta } => {
                // Read from KV store and later handle ReadOk message returned by KV store
                let read = KvStorePayload::Read { key: COUNTER_KEY };
                let kv_msg_id = node
                    .send_to(SEQ_KV_SERVICE_ID, read)
                    .context("read from kv store")?;

                self.deltas.insert(kv_msg_id, delta);
                Payload::AddOk
            }
            Payload::Read => {
                // Write timestamp to force the KV store read newest value,
                // ie. to prevent Stale Read which is permitted in sequentially consistent system
                // (https://jepsen.io/consistency/phenomena/stale-read)
                let now = SystemTime::now();
                let timestamp = now.duration_since(UNIX_EPOCH).unwrap().as_nanos();
                let write = KvStorePayload::Write {
                    key: "timestamp",
                    value: timestamp as usize,
                };
                node.send_to(SEQ_KV_SERVICE_ID, write)
                    .context("write timestamp to kv store")?;

                // Read the value of the counter
                let payload = KvStorePayload::Read { key: COUNTER_KEY };
                let kv_msg_id = node
                    .send_to(SEQ_KV_SERVICE_ID, payload)
                    .context("read from kv store")?;

                // Store the original message to be able to respond back
                // when we receive the response message from the KV store
                let orig_msg_src = msg.src;
                let orig_msg_id = msg.body.id.expect("message id must be set");
                self.kv_msg_ids
                    .insert(kv_msg_id, (orig_msg_src, orig_msg_id));

                return Ok(());
            }
            Payload::ReadOk { value } => {
                let kv_read_msg_id = msg.body.in_reply_to.expect("in_reply_to must be set");

                if let Some((orig_src, orig_msg_id)) = self.kv_msg_ids.remove(&kv_read_msg_id) {
                    // this is reply from kv_store => send reply back to the client
                    // that asked for the value of the counter
                    let mut fake_orig_msg = msg;
                    fake_orig_msg.src = orig_src;
                    fake_orig_msg.body.id = Some(orig_msg_id);

                    node.reply(fake_orig_msg, Payload::ReadOk { value })?;
                } else {
                    // this is reply to our request to kv_store when client wanted to add delta
                    // and we read current value of the counter
                    if let Some(delta) = self.deltas.remove(&kv_read_msg_id) {
                        let cas = KvStorePayload::Cas {
                            key: COUNTER_KEY,
                            from: value,
                            to: value + delta,
                            create_if_not_exists: false,
                        };

                        let kv_cas_msg_id = node
                            .send_to(SEQ_KV_SERVICE_ID, cas)
                            .context("add to kv store")?;

                        self.deltas.insert(kv_cas_msg_id, delta);
                    } else {
                        bail!("msg_id {} is missing in deltas", kv_read_msg_id)
                    }
                }
                return Ok(());
            }
            Payload::CasOk => {
                // housekeeping
                let cas_msg_id = msg.body.in_reply_to.expect("in_reply_to must be set");
                self.deltas.remove(&cas_msg_id);
                return Ok(());
            }
            Payload::Error { code, ref text } => {
                if code == ERROR_PRECONDITION_FAILED {
                    // CAS operation failed (outdated 'from' value caused by stale read) => retry again
                    eprintln!("INFO: CAS operation failed: '{}', retrying", text);

                    let kv_msg_id = msg.body.in_reply_to.expect("in_reply_to must be set");
                    if let Some(delta) = self.deltas.remove(&kv_msg_id) {
                        let read = KvStorePayload::Read { key: COUNTER_KEY };
                        let kv_msg_id = node
                            .send_to(SEQ_KV_SERVICE_ID, read)
                            .context("read from kv store")?;

                        self.deltas.insert(kv_msg_id, delta);
                    }
                } else {
                    // other error encountered
                    eprintln!("Error: {:?}", msg.body);
                }
                return Ok(());
            }
            Payload::AddOk | Payload::WriteOk => return Ok(()), // we do not care about these messages
        };

        node.reply(msg, reply)
    }

    fn handle_command(&mut self, cmd: Command, mut node: Node<Command>) -> anyhow::Result<()> {
        match cmd {
            Command::InitStore => {
                let payload = KvStorePayload::Cas {
                    key: COUNTER_KEY,
                    from: 0,
                    to: 0,
                    create_if_not_exists: true,
                };
                eprintln!("INFO: Initializing {}", COUNTER_KEY);

                node.send_to(SEQ_KV_SERVICE_ID, payload)?;

                Ok(())
            }
        }
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::new()?;

    let handler = GCounter::default();

    let (tx, rx) = std::sync::mpsc::channel::<Command>();

    node.register_command_receiver(rx);

    let jh: JoinHandle<Result<_, anyhow::Error>> = std::thread::spawn(move || {
        // initialize KV store
        tx.send(Command::InitStore)
            .context("sending InitStore command")
    });

    node.run(handler)?;

    jh.join()
        .expect("could not join InitStore command thread")
        .context("InitStore command thread errored")?;

    Ok(())
}
