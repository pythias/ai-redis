//! Command router — maps command names to handler functions.

use std::collections::HashMap;
use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;

/// A command handler takes a slice of Values (the command arguments) and returns a result.
type Handler = fn(&[Value]) -> CommandResult;

/// The command router maps uppercase command names to handler functions.
pub struct Router {
    // Key: UPPERCASE command name, e.g. "GET", "SET"
    commands: HashMap<String, Handler>,
}

impl Router {
    pub fn new() -> Self {
        let mut r = Router { commands: HashMap::new() };
        r.register_defaults();
        r
    }

    /// Register a command handler.
    pub fn register(&mut self, name: &str, handler: Handler) {
        self.commands.insert(name.to_uppercase(), handler);
    }

    /// Dispatch a command by name. Returns error if unknown command.
    pub fn dispatch(&self, name: &str, args: &[Value]) -> CommandResult {
        let handler = self.commands
            .get(&name.to_uppercase())
            .ok_or_else(|| CommandError::UnknownCommand(name.to_string()))?;
        handler(args)
    }

    fn register_defaults(&mut self) {
        // Generic key commands
        self.register("DEL", super::generic::del);
        self.register("EXISTS", super::generic::exists);
        self.register("TYPE", super::generic::r#type);
        self.register("EXPIRE", super::generic::expire);
        self.register("EXPIREAT", super::generic::expireat);
        self.register("TTL", super::generic::ttl);
        self.register("PTTL", super::generic::pttl);
        self.register("PERSIST", super::generic::persist);
        self.register("PING", super::generic::ping);
        self.register("ECHO", super::generic::echo);
        self.register("SELECT", super::generic::select);
        self.register("FLUSHDB", super::generic::flushdb);
        self.register("DBSIZE", super::generic::dbsize);
        self.register("KEYS", super::generic::keys);
        self.register("SCAN", super::generic::scan);

        // String commands
        self.register("SET", super::string::set);
        self.register("GET", super::string::get);
        self.register("MGET", super::string::mget);
        self.register("MSET", super::string::mset);
        self.register("SETNX", super::string::setnx);
        self.register("INCR", super::string::incr);
        self.register("INCRBY", super::string::incrby);
        self.register("DECR", super::string::decr);
        self.register("DECRBY", super::string::decrby);
        self.register("INCRBYFLOAT", super::string::incrbyfloat);
        self.register("APPEND", super::string::append);
        self.register("STRLEN", super::string::strlen);
        self.register("GETRANGE", super::string::getrange);
        self.register("SETRANGE", super::string::setrange);
        self.register("SETEX", super::string::setex);
        self.register("GETSET", super::string::getset);

        // Connection commands
        self.register("CLIENT", super::connection::client);

        // Hash commands
        self.register("HSET", super::hash::hset);
        self.register("HGET", super::hash::hget);
        self.register("HMGET", super::hash::hmget);
        self.register("HMSET", super::hash::hmset);
        self.register("HDEL", super::hash::hdel);
        self.register("HLEN", super::hash::hlen);
        self.register("HEXISTS", super::hash::hexists);
        self.register("HGETALL", super::hash::hgetall);
        self.register("HKEYS", super::hash::hkeys);
        self.register("HVALS", super::hash::hvals);
        self.register("HSETNX", super::hash::hsetnx);
        self.register("HINCRBY", super::hash::hincrby);
        self.register("HINCRBYFLOAT", super::hash::hincrbyfloat);
        self.register("HSTRLEN", super::hash::hstrlen);

        // List commands
        self.register("LPUSH", super::list::lpush);
        self.register("RPUSH", super::list::rpush);
        self.register("LPOP", super::list::lpop);
        self.register("RPOP", super::list::rpop);
        self.register("LRANGE", super::list::lrange);
        self.register("LLEN", super::list::llen);
        self.register("LINDEX", super::list::lindex);
        self.register("LSET", super::list::lset);
        self.register("LTRIM", super::list::ltrim);
        self.register("LPUSHX", super::list::lpushx);
        self.register("RPUSHX", super::list::rpushx);
        self.register("LREM", super::list::lrem);

        // TODO: Set, Sorted Set, Bitmap, etc.
    }
}

impl Default for Router {
    fn default() -> Self {
        Self::new()
    }
}
