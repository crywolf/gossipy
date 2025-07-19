use std::collections::{HashMap, HashSet};

use anyhow::{bail, Context};
use gossipy::kv_store::{
    ERROR_KEY_DOES_NOT_EXIST, ERROR_PRECONDITION_FAILED, LIN_KV_SERVICE_ID, SEQ_KV_SERVICE_ID,
};
use gossipy::{Handler, Message, Node};
use serde::{Deserialize, Serialize};

const NUM_POLLED_MESSAGES: usize = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
enum Payload {
    Send {
        key: String,
        msg: u64,
    },
    SendOk {
        offset: usize,
    },
    Poll {
        offsets: HashMap<String, usize>,
    },
    PollOk {
        msgs: HashMap<String, Vec<(usize, u64)>>,
    },
    CommitOffsets {
        offsets: HashMap<String, usize>,
    },
    CommitOffsetsOk,
    ListCommittedOffsets {
        keys: Vec<String>,
    },
    ListCommittedOffsetsOk {
        offsets: HashMap<String, usize>,
    },
    ReadOk {
        value: usize,
    },
    CasOk,
    WriteOk,
    Error {
        code: u16,
        text: String,
    },
}

/// Payload used to communicate with KV store
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "type")]
pub enum KvStorePayload {
    Write {
        key: String,
        value: usize,
    },
    Read {
        key: String,
    },
    /// Compare And Swap
    Cas {
        key: String,
        from: usize,
        to: usize,
        create_if_not_exists: bool,
    },
}

struct SendEntry {
    orig_msg_id: usize,
    orig_msg_src: String,
    key: String,
    offset: usize,
    msg: u64,
}

struct PollEntry {
    orig_msg_id: usize,
    orig_msg_src: String,
    requested_key_offsets: HashMap<String, usize>,
    key: String,
    offset: usize,
}

/// Container for all polled messages for specific Poll request
#[derive(Default)]
struct PolledMessages {
    /// Logged messages to be returned back as a reply to Poll msg
    messages: HashMap<String, Vec<OffsetMsg>>, // key => vec[]
    requested_keys: Vec<String>,
    completed_keys: HashSet<String>,
}

impl PolledMessages {
    fn all_completed(&self) -> bool {
        self.requested_keys.len() == self.completed_keys.len()
    }
}

struct OffsetMsg {
    offset: usize,
    msg: u64,
}

struct CommitOffsetEntry {
    orig_msg_id: usize,
    orig_msg_src: String,
    key: String,
    requested_keys_count: usize,
}

/// Container for all collected committed offsets for specific ListCommittedOffsets request
#[derive(Default)]
struct CommittedOffsets {
    /// key => offset
    offsets: HashMap<String, usize>,
    requested_keys: Vec<String>,
}

impl CommittedOffsets {
    fn all_completed(&self) -> bool {
        self.requested_keys.len() == self.offsets.len()
    }
}

/// Kafka-Style Log
#[derive(Default)]
struct KafkaLog {
    node: Option<Node>,

    /// msg_id => SendEntry
    send_msg_keys: HashMap<usize, SendEntry>,
    /// msg_id => offset key
    offset_reads: HashMap<usize, String>,
    /// msg_id => (offset key, offset)
    offset_updates: HashMap<usize, (String, usize)>,

    /// msg_id => PollEntry
    poll_msg_keys: HashMap<usize, PollEntry>,
    /// "src-msg_id" => PolledMessages
    polled_messages: HashMap<String, PolledMessages>,

    /// msg_id => CommitOffsetEntry
    commit_msg_keys: HashMap<usize, CommitOffsetEntry>,
    /// msg_id => CommitOffsetEntry
    committed_offset_reads: HashMap<usize, CommitOffsetEntry>,
    /// "src-msg_id" => count
    committed_offsets_written: HashMap<String, usize>,
    /// "src-msg_id" => {key => offset}
    list_committed_offsets: HashMap<String, CommittedOffsets>,
}

impl Handler<Payload> for KafkaLog {
    fn handle(&mut self, message: Message<Payload>, node: Node) -> anyhow::Result<()>
    where
        Payload: Serialize,
    {
        if self.node.is_none() {
            self.node = Some(node);
        }

        match message.body.payload {
            Payload::Send { ref key, msg } => {
                // 1) read latest offset for the key
                // 2) increment it and CAS it
                // 3) write one new log entry 'entry-key-offset' => msg
                // 4) reply send_ok

                // Send 1) read latest offset for the key
                // Send Read msg to KV store and later handle ReadOk message returned by KV store
                let kv_offset_key = self.offset_key(key);
                let read_offset = KvStorePayload::Read {
                    key: kv_offset_key.clone(),
                };

                let read_msg_id = self
                    .send_to_lin_kv(read_offset)
                    .with_context(|| format!("read latest offset for the key {}", key))?;

                self.offset_reads.insert(read_msg_id, kv_offset_key);

                // Store the original message to be able to respond back
                // when we process the response message from the KV store
                let orig_msg_src = message.src;
                let orig_msg_id = message.body.id.expect("message id must be set");

                self.send_msg_keys.insert(
                    read_msg_id,
                    SendEntry {
                        orig_msg_id,
                        orig_msg_src,
                        key: key.to_string(),
                        offset: 0, // unknown offset
                        msg,
                    },
                );

                Ok(())
            }

            Payload::Poll { ref offsets } => {
                if offsets.is_empty() {
                    // empty request => immediately return empty response
                    self.node
                        .as_mut()
                        .expect("node was set in handle fn")
                        .reply(
                            message,
                            Payload::PollOk {
                                msgs: HashMap::new(),
                            },
                        )?;

                    return Ok(());
                }

                // Poll 1) read entry-key-offset (3x times) or until ERROR_KEY_DOES_NOT_EXIST

                // Store the original message to be able to respond back
                // when we process the response message from the KV store
                let orig_msg_id = message.body.id.expect("message id must be set");
                let orig_msg_src = message.src;

                // initialize empty container for polled messages
                let mut messages = HashMap::new();
                for key in offsets.keys() {
                    messages.insert(key.clone(), vec![]);
                }

                let k = format!("{}-{}", orig_msg_src, orig_msg_id);
                self.polled_messages.insert(
                    k,
                    PolledMessages {
                        messages,
                        requested_keys: offsets.keys().cloned().collect(),
                        completed_keys: HashSet::new(),
                    },
                );

                // send Read requests
                for (key, offset) in offsets.iter() {
                    let mut offset = *offset;
                    if offset == 0 {
                        offset = 1;
                    }
                    // TODO instead of NUM_POLLED_MESSAGES const ask for the current (ie. max) offset for the key an use it
                    for i in offset..(offset + NUM_POLLED_MESSAGES) {
                        let logged_msg_key = self.logged_msg_key(key, i);
                        let read_msg_id = self
                            .send_to_lin_kv(KvStorePayload::Read {
                                key: logged_msg_key,
                            })
                            .context("read message from the log")?;

                        self.poll_msg_keys.insert(
                            read_msg_id,
                            PollEntry {
                                orig_msg_id,
                                orig_msg_src: orig_msg_src.clone(),
                                requested_key_offsets: offsets.clone(),
                                key: key.to_string(),
                                offset: i,
                            },
                        );
                    }
                }

                Ok(())
            }

            Payload::CommitOffsets { ref offsets } => {
                let orig_msg_src = message.src;
                let orig_msg_id = message.body.id.expect("message id must be set");

                self.committed_offsets_written
                    .insert(format!("{}-{}", orig_msg_src, orig_msg_id), 0);

                for (key, &offset) in offsets.iter() {
                    let write_msg_id = self
                        .send_to_lin_kv(KvStorePayload::Write {
                            key: self.committed_offset_key(key),
                            value: offset,
                        })
                        .context("commit new offset")?;

                    self.commit_msg_keys.insert(
                        write_msg_id,
                        CommitOffsetEntry {
                            orig_msg_id,
                            orig_msg_src: orig_msg_src.clone(),
                            key: key.clone(),
                            requested_keys_count: offsets.len(),
                        },
                    );
                }

                Ok(())
            }

            Payload::ListCommittedOffsets { ref keys } => {
                let orig_msg_src = message.src;
                let orig_msg_id = message.body.id.expect("message id must be set");

                for key in keys.iter() {
                    let committed_offset_key = self.committed_offset_key(key);
                    let read_msg_id = self
                        .send_to_lin_kv(KvStorePayload::Read {
                            key: committed_offset_key,
                        })
                        .context("read committed offset")?;

                    self.committed_offset_reads.insert(
                        read_msg_id,
                        CommitOffsetEntry {
                            orig_msg_id,
                            orig_msg_src: orig_msg_src.clone(),
                            key: key.clone(),
                            requested_keys_count: keys.len(),
                        },
                    );
                }

                let k = format!("{}-{}", orig_msg_src, orig_msg_id);

                self.list_committed_offsets.insert(
                    k,
                    CommittedOffsets {
                        requested_keys: keys.to_vec(),
                        offsets: HashMap::new(),
                    },
                );

                Ok(())
            }

            Payload::ReadOk { value } => {
                let msg_id = message.body.in_reply_to.expect("in_reply_to must be set");

                if let Some(kv_offset_key) = self.offset_reads.remove(&msg_id) {
                    // Send 2) we've read latest offset, increment it and update it
                    let incremented_offset = value + 1;
                    let new_offset = KvStorePayload::Cas {
                        key: kv_offset_key.clone(),
                        from: value,
                        to: incremented_offset,
                        create_if_not_exists: false,
                    };

                    let cas_msg_id = self
                        .send_to_lin_kv(new_offset)
                        .context("increment latest offset")?;

                    self.offset_updates
                        .insert(cas_msg_id, (kv_offset_key, incremented_offset));

                    if let Some(entry) = self.send_msg_keys.remove(&msg_id) {
                        self.send_msg_keys.insert(cas_msg_id, entry);
                    } else {
                        eprintln!("MISSING item in msg_keys!!!! (ReadOk)");
                    }
                    return Ok(());
                }

                if let Some(entry) = self.poll_msg_keys.remove(&msg_id) {
                    // Poll 2) store returned message (offset and value), return it later when error 'key does not exist' would be encountered

                    let orig_msg_id = entry.orig_msg_id;

                    let k = format!("{}-{}", entry.orig_msg_src, orig_msg_id);
                    let polled_messages = self
                        .polled_messages
                        .get_mut(&k)
                        .expect("entry in polled_messages was inserted in Poll msg branch");

                    polled_messages
                        .messages
                        .entry(entry.key.clone())
                        .and_modify(|v| {
                            v.push(OffsetMsg {
                                offset: entry.offset,
                                msg: value as u64,
                            })
                        });

                    if polled_messages
                        .messages
                        .get(&entry.key)
                        .expect("key must be present")
                        .len()
                        >= NUM_POLLED_MESSAGES
                    {
                        // we have all requested messages for this key
                        polled_messages.completed_keys.insert(entry.key.clone());
                    }

                    if !polled_messages.all_completed() {
                        // still not finished => continue
                        return Ok(());
                    }

                    // All requested messages were polled, return them
                    let mut returned_msgs = HashMap::new();
                    for (key, _) in entry.requested_key_offsets {
                        let msgs_for_key = polled_messages.messages.get(&key).unwrap();

                        let mut returned_messages_for_key = vec![];
                        for offset_msg in msgs_for_key {
                            returned_messages_for_key.push((offset_msg.offset, offset_msg.msg));
                        }

                        returned_msgs.insert(key.to_string(), returned_messages_for_key);
                    }

                    eprintln!(
                            "Poll Read (key {}) all completed, replying with PollOk msg, k:{}. messages len: {}",
                            entry.key, k, polled_messages.messages.len()
                        );

                    self.polled_messages.remove(&k); // clean up

                    self.reply(
                        entry.orig_msg_src,
                        orig_msg_id,
                        Payload::PollOk {
                            msgs: returned_msgs,
                        },
                    )?;

                    return Ok(());
                }

                if let Some(entry) = self.committed_offset_reads.remove(&msg_id) {
                    // ListCommittedOffsets
                    let k = format!("{}-{}", entry.orig_msg_src, entry.orig_msg_id);

                    let committed_offsets = self
                        .list_committed_offsets
                        .get_mut(&k)
                        .expect("item is present");

                    committed_offsets.offsets.insert(entry.key.clone(), value);

                    if !committed_offsets.all_completed() {
                        return Ok(());
                    }

                    // all completed => send the response back
                    let mut offsets = HashMap::new();
                    for (key, committed) in committed_offsets.offsets.iter() {
                        offsets.insert(key.to_string(), *committed);
                    }

                    self.list_committed_offsets.remove(&k); // clean up

                    self.reply(
                        entry.orig_msg_src,
                        entry.orig_msg_id,
                        Payload::ListCommittedOffsetsOk { offsets },
                    )?;

                    return Ok(());
                }

                bail!("Unhandled ReadOk message");
            }

            Payload::CasOk => {
                let msg_id = message.body.in_reply_to.expect("in_reply_to must be set");
                if let Some((_, incremented_offset)) = self.offset_updates.remove(&msg_id) {
                    // offset was incremented
                    if let Some(mut entry) = self.send_msg_keys.remove(&msg_id) {
                        entry.offset = incremented_offset;

                        // Send 3) write new message to the log
                        let write_msg_id = self
                            .send_to_lin_kv(KvStorePayload::Write {
                                key: self.logged_msg_key(&entry.key, incremented_offset),
                                value: entry.msg as usize,
                            })
                            .context("insert new entry to the log")?;

                        self.send_msg_keys.insert(write_msg_id, entry);
                        return Ok(());
                    } else {
                        bail!("MISSING item in msg_keys!!!! (CasOk)");
                    }
                }

                bail!("Unhandled CasOk message");
            }

            Payload::WriteOk => {
                let msg_id = message.body.in_reply_to.expect("in_reply_to must be set");
                if let Some(entry) = self.send_msg_keys.remove(&msg_id) {
                    // message was logged (written into KV store)

                    self.reply(
                        entry.orig_msg_src,
                        entry.orig_msg_id,
                        Payload::SendOk {
                            offset: entry.offset,
                        },
                    )?;

                    return Ok(());
                }

                if let Some(entry) = self.commit_msg_keys.remove(&msg_id) {
                    // CommitOffsets
                    // One CommitOffset was written
                    let k = format!("{}-{}", entry.orig_msg_src, entry.orig_msg_id);

                    let commits_written_count = self
                        .committed_offsets_written
                        .get_mut(&k)
                        .expect("item must be present");

                    *commits_written_count += 1;

                    if *commits_written_count < entry.requested_keys_count {
                        // continue until all offsets were written
                        return Ok(());
                    }
                    // All committed offsets were written => reply with ok message
                    eprintln!(
                        "--> INFO Reply CommitOffsetsOk: {} >= {} | msg-src {}",
                        commits_written_count, entry.requested_keys_count, k,
                    );

                    self.reply(
                        entry.orig_msg_src,
                        entry.orig_msg_id,
                        Payload::CommitOffsetsOk,
                    )?;

                    self.committed_offsets_written.remove(&k); // clean up
                    return Ok(());
                }

                bail!("Unhandled WriteOk message");
            }

            Payload::Error { code, ref text } => {
                let msg_id = message.body.in_reply_to.expect("in_reply_to must be set");
                if code == ERROR_KEY_DOES_NOT_EXIST {
                    if let Some(kv_offset_key) = self.offset_reads.remove(&msg_id) {
                        // Offset key does not exist, create it with value 0
                        let create_key = KvStorePayload::Cas {
                            key: kv_offset_key.clone(),
                            from: 0,
                            to: 1,
                            create_if_not_exists: true,
                        };
                        let cas_msg_id = self.send_to_lin_kv(create_key).with_context(|| {
                            format!("creating key {} for storing offsets", kv_offset_key)
                        })?;

                        self.offset_updates.insert(cas_msg_id, (kv_offset_key, 1));

                        if let Some(entry) = self.send_msg_keys.remove(&msg_id) {
                            self.send_msg_keys.insert(cas_msg_id, entry);
                        } else {
                            bail!("MISSING item in msg_keys!!!! (Error msg)");
                        }

                        return Ok(());
                    }

                    if let Some(entry) = self.poll_msg_keys.remove(&msg_id) {
                        // We asked for message with specific offset, but the key containing requested offset does not exist,
                        // which means that we reached last message for this key

                        let k = format!("{}-{}", entry.orig_msg_src, entry.orig_msg_id);
                        let polled_messages = self.polled_messages.get_mut(&k);
                        if polled_messages.is_none() {
                            // we removed messages because we already got them all and sent them back in reply
                            // => ignore this Error message
                            return Ok(());
                        }
                        let polled_messages =
                            polled_messages.expect("entry in polled_messages is set here for sure");

                        // there are no more messages for this key
                        polled_messages.completed_keys.insert(entry.key.clone());

                        if !polled_messages.all_completed() {
                            return Ok(());
                        }

                        // Reply back with all the messages, we got them already
                        let mut returned_msgs = HashMap::new();
                        for (key, _) in entry.requested_key_offsets {
                            let msgs_for_key = polled_messages.messages.get(&key).unwrap();

                            let mut returned_messages_for_key = vec![];
                            for offset_msg in msgs_for_key {
                                returned_messages_for_key.push((offset_msg.offset, offset_msg.msg));
                            }

                            returned_msgs.insert(key.to_string(), returned_messages_for_key);
                        }

                        self.polled_messages.remove(&k); // clean up
                        eprintln!(
                            "Poll Error (key {}) all completed, replying with PollOk msg",
                            entry.key
                        );

                        self.reply(
                            entry.orig_msg_src,
                            entry.orig_msg_id,
                            Payload::PollOk {
                                msgs: returned_msgs,
                            },
                        )?;

                        return Ok(());
                    }

                    if let Some(entry) = self.committed_offset_reads.remove(&msg_id) {
                        // empty committed offsets
                        self.reply(
                            entry.orig_msg_src,
                            entry.orig_msg_id,
                            Payload::ListCommittedOffsetsOk {
                                offsets: HashMap::new(),
                            },
                        )?;

                        return Ok(());
                    }

                    bail!("Uncaught ERROR_KEY_DOES_NOT_EXIST: {:?}", message.body);
                } else if code == ERROR_PRECONDITION_FAILED {
                    eprintln!("INFO: CAS operation failed: '{}', retrying", text);

                    if let Some((offset_key, value)) = self.offset_updates.remove(&msg_id) {
                        let incremented_offset = value + 1;
                        let new_offset = KvStorePayload::Cas {
                            key: offset_key.clone(),
                            from: value,
                            to: incremented_offset,
                            create_if_not_exists: false,
                        };

                        let cas_msg_id = self
                            .send_to_lin_kv(new_offset)
                            .context("retrying to increment offset")?;

                        self.offset_updates
                            .insert(cas_msg_id, (offset_key, incremented_offset));
                        if let Some(entry) = self.send_msg_keys.remove(&msg_id) {
                            self.send_msg_keys.insert(cas_msg_id, entry);
                        } else {
                            bail!("MISSING item in msg_keys!!!! (Error msg)");
                        }

                        return Ok(());
                    }

                    bail!("Uncaught ERROR_PRECONDITION_FAILED: {:?}", message.body);
                } else {
                    // other error encountered
                    bail!("Uncaught Error message: {:?}", message.body);
                }
            }

            Payload::SendOk { .. }
            | Payload::PollOk { .. }
            | Payload::CommitOffsetsOk { .. }
            | Payload::ListCommittedOffsetsOk { .. } => Ok(()), // do nothing
        }
    }
}

impl KafkaLog {
    fn logged_msg_key(&self, key: &str, offset: usize) -> String {
        format!("entry-{key}-{offset}")
    }

    fn offset_key(&self, key: &str) -> String {
        format!("offset-{key}")
    }

    fn committed_offset_key(&self, key: &str) -> String {
        format!("committed-offset-{key}")
    }

    /// Sends a message to the linearizable key-value store
    fn send_to_lin_kv(&mut self, payload: KvStorePayload) -> anyhow::Result<usize> {
        let msg_id = self.send_to_kv_store(LIN_KV_SERVICE_ID, payload)?;
        Ok(msg_id)
    }

    /// Sends a message to the sequentially consistent key-value store
    fn send_to_seq_kv(&mut self, payload: KvStorePayload) -> anyhow::Result<usize> {
        let msg_id = self.send_to_kv_store(SEQ_KV_SERVICE_ID, payload)?;
        Ok(msg_id)
    }

    fn send_to_kv_store(&mut self, dst: &str, payload: KvStorePayload) -> anyhow::Result<usize> {
        if self.node.is_none() {
            panic!("cannot send to KV store, node was not initialized")
        }
        let msg_id = self
            .node
            .as_mut()
            .expect("node was set in handle fn")
            .send_to(dst, payload)
            .with_context(|| format!("send to kv store: {dst}"))?;

        Ok(msg_id)
    }

    /// Reply to incoming message with result of requested operation
    fn reply(
        &mut self,
        orig_msg_src: String,
        orig_msg_id: usize,
        payload: Payload,
    ) -> anyhow::Result<()> {
        let fake_payload = Payload::WriteOk;
        let mut fake_message: Message<Payload> = Message::new_empty(fake_payload);
        fake_message.src = orig_msg_src;
        fake_message.body.id = Some(orig_msg_id);

        self.node
            .as_mut()
            .expect("node was set in handle fn")
            .reply(fake_message, payload)?;

        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let mut node = Node::new()?;

    let handler = KafkaLog::default();

    node.run(handler)?;

    Ok(())
}
