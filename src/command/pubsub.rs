#![allow(dead_code)]

//! Pub/Sub support: PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PSUBSCRIBE, PUNSUBSCRIBE, PUBSUB

use std::collections::{HashMap, HashSet};
use std::sync::RwLock;

use crate::protocol::Value;

/// Global pub/sub state
pub struct PubSub {
    /// Map from channel name -> set of subscriber connection IDs
    channels: RwLock<HashMap<String, HashSet<u64>>>,
    /// Map from pattern -> set of subscriber connection IDs
    patterns: RwLock<HashMap<String, HashSet<u64>>>,
    /// Per-connection subscribed channels
    conn_channels: RwLock<HashMap<u64, HashSet<String>>>,
    /// Per-connection subscribed patterns
    conn_patterns: RwLock<HashMap<u64, HashSet<String>>>,
}

impl PubSub {
    pub fn new() -> Self {
        PubSub {
            channels: RwLock::new(HashMap::new()),
            patterns: RwLock::new(HashMap::new()),
            conn_channels: RwLock::new(HashMap::new()),
            conn_patterns: RwLock::new(HashMap::new()),
        }
    }

    /// Subscribe a connection to a channel
    pub fn subscribe(&self, conn_id: u64, channel: &str) {
        {
            let mut channels = self.channels.write().unwrap();
            channels
                .entry(channel.to_string())
                .or_insert_with(HashSet::new)
                .insert(conn_id);
        }
        {
            let mut conn_channels = self.conn_channels.write().unwrap();
            conn_channels
                .entry(conn_id)
                .or_insert_with(HashSet::new)
                .insert(channel.to_string());
        }
    }

    /// Unsubscribe a connection from a channel (or all if channel is None)
    pub fn unsubscribe(&self, conn_id: u64, channel: Option<&str>) {
        match channel {
            Some(ch) => {
                {
                    let mut channels = self.channels.write().unwrap();
                    if let Some(subs) = channels.get_mut(ch) {
                        subs.remove(&conn_id);
                        if subs.is_empty() {
                            channels.remove(ch);
                        }
                    }
                }
                {
                    let mut conn_channels = self.conn_channels.write().unwrap();
                    if let Some(chs) = conn_channels.get_mut(&conn_id) {
                        chs.remove(ch);
                        if chs.is_empty() {
                            conn_channels.remove(&conn_id);
                        }
                    }
                }
            }
            None => {
                // Unsubscribe from all channels
                let channels: HashSet<String> = {
                    let conn_channels = self.conn_channels.read().unwrap();
                    conn_channels.get(&conn_id).cloned().unwrap_or_default()
                };
                for ch in channels {
                    let mut cs = self.channels.write().unwrap();
                    if let Some(subs) = cs.get_mut(&ch) {
                        subs.remove(&conn_id);
                        if subs.is_empty() {
                            cs.remove(&ch);
                        }
                    }
                }
                self.conn_channels.write().unwrap().remove(&conn_id);
            }
        }
    }

    /// Pattern-subscribe a connection
    pub fn psubscribe(&self, conn_id: u64, pattern: &str) {
        {
            let mut patterns = self.patterns.write().unwrap();
            patterns
                .entry(pattern.to_string())
                .or_insert_with(HashSet::new)
                .insert(conn_id);
        }
        {
            let mut conn_patterns = self.conn_patterns.write().unwrap();
            conn_patterns
                .entry(conn_id)
                .or_insert_with(HashSet::new)
                .insert(pattern.to_string());
        }
    }

    /// Pattern-unsubscribe
    pub fn punsubscribe(&self, conn_id: u64, pattern: Option<&str>) {
        match pattern {
            Some(pat) => {
                {
                    let mut patterns = self.patterns.write().unwrap();
                    if let Some(subs) = patterns.get_mut(pat) {
                        subs.remove(&conn_id);
                        if subs.is_empty() {
                            patterns.remove(pat);
                        }
                    }
                }
                {
                    let mut conn_patterns = self.conn_patterns.write().unwrap();
                    if let Some(pats) = conn_patterns.get_mut(&conn_id) {
                        pats.remove(pat);
                        if pats.is_empty() {
                            conn_patterns.remove(&conn_id);
                        }
                    }
                }
            }
            None => {
                let pats: HashSet<String> = {
                    let conn_patterns = self.conn_patterns.read().unwrap();
                    conn_patterns.get(&conn_id).cloned().unwrap_or_default()
                };
                for pat in pats {
                    let mut patterns = self.patterns.write().unwrap();
                    if let Some(subs) = patterns.get_mut(&pat) {
                        subs.remove(&conn_id);
                        if subs.is_empty() {
                            patterns.remove(&pat);
                        }
                    }
                }
                self.conn_patterns.write().unwrap().remove(&conn_id);
            }
        }
    }

    /// Publish a message to a channel. Returns the number of subscribers that received it.
    pub fn publish(&self, channel: &str, _message: &str) -> i64 {
        let mut count = 0i64;

        // Direct channel subscribers
        {
            let channels = self.channels.read().unwrap();
            if let Some(subs) = channels.get(channel) {
                count += subs.len() as i64;
            }
        }

        // Pattern-matched subscribers
        let matched_patterns: Vec<String> = {
            use crate::storage::memory::glob_match as gm;
            let patterns = self.patterns.read().unwrap();
            patterns
                .iter()
                .filter(|(pat, _)| gm(pat, channel))
                .map(|(pat, _)| pat.clone())
                .collect()
        };
        {
            let patterns = self.patterns.read().unwrap();
            for pat in &matched_patterns {
                if let Some(subs) = patterns.get(pat) {
                    count += subs.len() as i64;
                }
            }
        }

        count
    }

    /// Get count of subscribers for PUBSUB NUMSUB
    pub fn numsub(&self, channels_list: &[String]) -> Vec<(String, i64)> {
        let chans = self.channels.read().unwrap();
        channels_list
            .iter()
            .map(|ch| {
                let n = chans.get(ch).map(|s| s.len()).unwrap_or(0) as i64;
                (ch.clone(), n)
            })
            .collect()
    }

    /// Get number of patterns for PUBSUB NUMPAT
    pub fn numpat(&self) -> i64 {
        self.patterns.read().unwrap().len() as i64
    }

    /// Get list of channels for PUBSUB CHANNELS
    pub fn channels(&self, pattern: Option<&str>) -> Vec<Value> {
        use crate::storage::memory::glob_match as gm;
        let chans = self.channels.read().unwrap();
        let mut result: Vec<String> = chans.keys().cloned().collect();
        if let Some(pat) = pattern {
            result.retain(|ch| gm(pat, ch));
        }
        result
            .into_iter()
            .map(|s| Value::BulkString(Some(s)))
            .collect()
    }

    /// Remove a connection when it disconnects
    pub fn remove_connection(&self, conn_id: u64) {
        self.unsubscribe(conn_id, None);
        self.punsubscribe(conn_id, None);
    }

    /// Check if connection has any subscriptions
    pub fn has_subscriptions(&self, conn_id: u64) -> bool {
        let chans = self.conn_channels.read().unwrap();
        let pats = self.conn_patterns.read().unwrap();
        chans.contains_key(&conn_id) || pats.contains_key(&conn_id)
    }

    /// Get subscriptions for a connection
    pub fn get_subscription_count(&self, conn_id: u64) -> (usize, usize) {
        let chans = self.conn_channels.read().unwrap();
        let pats = self.conn_patterns.read().unwrap();
        (
            chans.get(&conn_id).map(|s| s.len()).unwrap_or(0),
            pats.get(&conn_id).map(|s| s.len()).unwrap_or(0),
        )
    }
}

impl Default for PubSub {
    fn default() -> Self {
        Self::new()
    }
}

lazy_static::lazy_static! {
    pub static ref PUBSUB: PubSub = PubSub::new();
}

// ─── Command handlers ──────────────────────────────────────────────────────────

use crate::command::{CommandError, CommandResult};

/// PUBLISH channel message — returns number of subscribers that received the message
pub fn publish(args: &[Value]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArgs("PUBLISH".into()));
    }
    let channel = args[0].as_str().unwrap_or("");
    let message = args[1].as_str().unwrap_or("");
    let n = PUBSUB.publish(channel, message);
    Ok(Value::Integer(n))
}

/// PUBSUB subcommands: CHANNELS, NUMSUB, NUMPAT
pub fn pubsub(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("PUBSUB".into()));
    }
    let sub = args[0].as_str().unwrap_or("").to_uppercase();
    match sub.as_str() {
        "CHANNELS" => {
            let pattern = args.get(1).and_then(|v| v.as_str());
            Ok(Value::Array(PUBSUB.channels(pattern)))
        }
        "NUMSUB" => {
            let channels: Vec<String> = args[1..]
                .iter()
                .filter_map(|v| v.as_str().map(String::from))
                .collect();
            let result = PUBSUB.numsub(&channels);
            Ok(Value::Array(
                result
                    .into_iter()
                    .flat_map(|(ch, n)| vec![Value::BulkString(Some(ch)), Value::Integer(n)])
                    .collect(),
            ))
        }
        "NUMPAT" => Ok(Value::Integer(PUBSUB.numpat())),
        _ => Err(CommandError::UnknownCommand(format!("PUBSUB {}", sub))),
    }
}
