//! Redis value types stored in memory.

use std::collections::{BTreeMap, HashMap, VecDeque};

/// A stored value with an optional TTL (absolute Unix timestamp in ms).
#[derive(Debug, Clone)]
pub struct StoredValue {
    pub data: RedisData,
    pub expire_at: Option<i64>,
}

impl StoredValue {
    pub fn new(data: RedisData) -> Self {
        StoredValue { data, expire_at: None }
    }

    pub fn with_ttl(data: RedisData, ttl_secs: i64) -> Self {
        StoredValue { data, expire_at: Some(current_time_ms() + ttl_secs * 1000) }
    }

    pub fn is_expired(&self) -> bool {
        self.expire_at.map(|ts| current_time_ms() > ts).unwrap_or(false)
    }

    pub fn type_name(&self) -> &'static str {
        self.data.type_name()
    }
}

fn current_time_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
}

/// All Redis data types.
#[derive(Debug, Clone)]
pub enum RedisData {
    String(String),
    List(VecDeque<String>),
    Hash(HashMap<String, String>),
    Set(Vec<String>),
    SortedSet(BTreeMap<String, f64>),
    Stream(HashMap<String, StreamEntry>),
}

impl RedisData {
    pub fn type_name(&self) -> &'static str {
        match self {
            RedisData::String(_) => "string",
            RedisData::List(_) => "list",
            RedisData::Hash(_) => "hash",
            RedisData::Set(_) => "set",
            RedisData::SortedSet(_) => "zset",
            RedisData::Stream(_) => "stream",
        }
    }

    pub fn as_string(&self) -> Option<&String> {
        match self { RedisData::String(s) => Some(s), _ => None }
    }

    pub fn as_hash(&self) -> Option<&HashMap<String, String>> {
        match self { RedisData::Hash(h) => Some(h), _ => None }
    }

    pub fn as_hash_mut(&mut self) -> Option<&mut HashMap<String, String>> {
        match self { RedisData::Hash(h) => Some(h), _ => None }
    }

    pub fn as_list(&self) -> Option<&VecDeque<String>> {
        match self { RedisData::List(l) => Some(l), _ => None }
    }

    pub fn as_list_mut(&mut self) -> Option<&mut VecDeque<String>> {
        match self { RedisData::List(l) => Some(l), _ => None }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamEntry {
    pub fields: HashMap<String, String>,
    pub id: String,
}
