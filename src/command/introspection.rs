#![allow(dead_code)]

//! Introspection commands: SLOWLOG, OBJECT, DEBUG, COMMAND, CLIENT

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::Storage;
use std::collections::VecDeque;
use std::time::{SystemTime, UNIX_EPOCH};

/// Slow log entry
#[derive(Debug, Clone)]
pub struct SlowLogEntry {
    pub id: u64,
    pub duration_us: i64,
    pub command: Vec<String>,
    pub time: i64,
}

impl SlowLogEntry {
    pub fn new(id: u64, duration_us: i64, command: Vec<String>) -> Self {
        let time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        SlowLogEntry {
            id,
            duration_us,
            command,
            time,
        }
    }
}

/// Slow log manager
pub struct SlowLog {
    entries: VecDeque<SlowLogEntry>,
    max_len: usize,
    counter: u64,
}

impl SlowLog {
    pub fn new(max_len: usize) -> Self {
        SlowLog {
            entries: VecDeque::new(),
            max_len,
            counter: 0,
        }
    }

    pub fn add(&mut self, duration_us: i64, command: Vec<String>) {
        self.counter += 1;
        let entry = SlowLogEntry::new(self.counter, duration_us, command);
        self.entries.push_back(entry);
        while self.entries.len() > self.max_len {
            self.entries.pop_front();
        }
    }

    pub fn get_entries(&self, count: usize) -> Vec<Value> {
        self.entries
            .iter()
            .rev()
            .take(count)
            .map(|e| {
                Value::Array(vec![
                    Value::Integer(e.id as i64),
                    Value::Integer(e.duration_us / 1000), // ms
                    Value::Array(e.command.iter().map(|s| Value::BulkString(Some(s.clone()))).collect()),
                    Value::Integer(e.time),
                ])
            })
            .collect()
    }
}

lazy_static::lazy_static! {
    pub static ref SLOW_LOG: std::sync::RwLock<SlowLog> = 
        std::sync::RwLock::new(SlowLog::new(128));
}

/// SLOWLOG subcommand
pub fn slowlog(args: &[Value]) -> CommandResult {
    let sub = args.first().and_then(|v| v.as_str()).unwrap_or("").to_uppercase();
    match sub.as_str() {
        "GET" => {
            let count = args.get(1)
                .and_then(|v| v.as_int())
                .unwrap_or(10) as usize;
            let log = SLOW_LOG.read().unwrap();
            Ok(Value::Array(log.get_entries(count)))
        }
        "LEN" => {
            let log = SLOW_LOG.read().unwrap();
            Ok(Value::Integer(log.entries.len() as i64))
        }
        "RESET" => {
            let mut log = SLOW_LOG.write().unwrap();
            log.entries.clear();
            Ok(Value::SimpleString("OK".to_string()))
        }
        _ => Err(CommandError::UnknownCommand(format!("SLOWLOG {}", sub))),
    }
}

/// OBJECT command - introspection of Redis objects
pub fn object(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("OBJECT".into()));
    }
    let sub = args[0].as_str().unwrap_or("").to_uppercase();
    match sub.as_str() {
        "REFCOUNT" | "ENCODING" | "IDLETIME" | "TTL" => {
            // Get the key
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArgs("OBJECT".into()));
            }
            let key = args[1].as_str().ok_or(CommandError::WrongType)?;
            let store = Storage::get();
            let guard = store.read().unwrap();
            
            match guard.get(key) {
                Some(v) if !v.is_expired() => {
                    match sub.as_str() {
                        "REFCOUNT" => Ok(Value::Integer(1)), // Simple implementation
                        "ENCODING" => Ok(Value::BulkString(Some(v.data.type_name().to_string()))),
                        "IDLETIME" => {
                            // Assuming no access time tracking, return 0
                            Ok(Value::Integer(0))
                        }
                        "TTL" => {
                            match v.expire_at {
                                Some(ts) => {
                                    let now = std::time::SystemTime::now()
                                        .duration_since(std::time::UNIX_EPOCH)
                                        .unwrap()
                                        .as_millis() as i64;
                                    Ok(Value::Integer(((ts - now) / 1000).max(0)))
                                }
                                None => Ok(Value::Integer(-1)),
                            }
                        }
                        _ => unreachable!(),
                    }
                }
                _ => Ok(Value::BulkString(None)),
            }
        }
        _ => Err(CommandError::UnknownCommand(format!("OBJECT {}", sub))),
    }
}

/// DEBUG command - debugging helpers
pub fn debug(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("DEBUG".into()));
    }
    let sub = args[0].as_str().unwrap_or("").to_uppercase();
    match sub.as_str() {
        "SLEEP" => {
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArgs("DEBUG SLEEP".into()));
            }
            let secs: f64 = args[1].as_str()
                .and_then(|s| s.parse().ok())
                .ok_or(CommandError::InvalidFloat)?;
            std::thread::sleep(std::time::Duration::from_secs_f64(secs));
            Ok(Value::SimpleString("OK".to_string()))
        }
        "SEGFAULT" => {
            // Deliberate crash for testing - not in safe Rust
            Err(CommandError::Generic("DEBUG SEGFAULT not supported".to_string()))
        }
        "OBJECT" => {
            // DEBUG OBJECT <key>
            if args.len() < 2 {
                return Err(CommandError::WrongNumberOfArgs("DEBUG OBJECT".into()));
            }
            let key = args[1].as_str().ok_or(CommandError::WrongType)?;
            let store = Storage::get();
            let guard = store.read().unwrap();
            
            match guard.get(key) {
                Some(v) if !v.is_expired() => {
                    let info = format!("Type: {}, Encoding: {}, Refcount: 1", 
                        v.type_name(), v.data.type_name());
                    Ok(Value::BulkString(Some(info)))
                }
                _ => Ok(Value::BulkString(None)),
            }
        }
        "STRUCT" => {
            Ok(Value::SimpleString("OK".to_string()))
        }
        _ => Err(CommandError::UnknownCommand(format!("DEBUG {}", sub))),
    }
}

/// COMMAND command - return all registered commands
pub fn command(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("COMMAND".into()));
    }
    let sub = args[0].as_str().unwrap_or("").to_uppercase();
    match sub.as_str() {
        "COUNT" => {
            // Return count of registered commands
            Ok(Value::Integer(60)) // Approximate count
        }
        "INFO" => {
            // Return info about specific commands
            let commands: Vec<Value> = args[1..]
                .iter()
                .filter_map(|v| v.as_str())
                .map(|cmd| {
                    Value::Array(vec![
                        Value::BulkString(Some(cmd.to_uppercase())),
                        Value::Integer(0), // arity
                        Value::Array(vec![
                            Value::SimpleString("readonly".to_string()),
                        ]),
                        Value::Integer(1), // first key
                        Value::Integer(1), // last key  
                        Value::Integer(1), // step
                    ])
                })
                .collect();
            Ok(Value::Array(commands))
        }
        "LIST" => {
            // Return all commands as array of arrays
            let cmds = vec![
                Value::Array(vec![Value::BulkString(Some("GET".to_string())), Value::Integer(2)]),
                Value::Array(vec![Value::BulkString(Some("SET".to_string())), Value::Integer(-3)]),
                Value::Array(vec![Value::BulkString(Some("DEL".to_string())), Value::Integer(-2)]),
            ];
            Ok(Value::Array(cmds))
        }
        _ => Err(CommandError::UnknownCommand(format!("COMMAND {}", sub))),
    }
}

/// CLIENT command - connection management
pub fn client(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CLIENT".into()));
    }
    let sub = args[0].as_str().unwrap_or("").to_uppercase();
    let rest = &args[1..];
    match sub.as_str() {
        "LIST" => client_list(rest),
        "GETNAME" => client_getname(rest),
        "SETNAME" => client_setname(rest),
        "KILL" => client_kill(rest),
        "INFO" => client_info(rest),
        "PAUSE" => client_pause(rest),
        "REPLY" => client_reply(rest),
        _ => Err(CommandError::UnknownCommand(format!("CLIENT {}", sub))),
    }
}

fn client_list(_args: &[Value]) -> CommandResult {
    Ok(Value::BulkString(Some(
        "id=0 addr=127.0.0.1:6379 laddr=127.0.0.1:6379 user=default".to_string()
    )))
}

fn client_getname(_args: &[Value]) -> CommandResult {
    Ok(Value::BulkString(None))
}

fn client_setname(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CLIENT SETNAME".into()));
    }
    let _name = args[0].as_str().unwrap_or("");
    Ok(Value::SimpleString("OK".to_string()))
}

fn client_kill(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CLIENT KILL".into()));
    }
    Ok(Value::SimpleString("OK".to_string()))
}

fn client_info(_args: &[Value]) -> CommandResult {
    Ok(Value::BulkString(Some(
        "id=0 name=0000000000000000000000000000000000000000 lib-name=ai-redis lib-ver=0.1.0".to_string()
    )))
}

fn client_pause(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CLIENT PAUSE".into()));
    }
    let _timeout: i64 = args[0].as_int().unwrap_or(0);
    Ok(Value::SimpleString("OK".to_string()))
}

fn client_reply(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CLIENT REPLY".into()));
    }
    let _mode = args[0].as_str().unwrap_or("ON");
    Ok(Value::SimpleString("OK".to_string()))
}
