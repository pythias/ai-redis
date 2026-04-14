#![allow(dead_code)]

//! CONFIG commands: GET, SET, REWRITE, RESETSTAT

use std::collections::HashMap;
use std::sync::RwLock;

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;

/// Runtime configuration parameters.
static CONFIG: RwLock<Option<HashMap<String, String>>> = RwLock::new(None);

/// Statistics counters that can be reset by CONFIG RESETSTAT.
static STATS: RwLock<Stats> = RwLock::new(Stats {
    total_commands_processed: 0,
    keyspace_hits: 0,
    keyspace_misses: 0,
});

#[derive(Default, Clone)]
pub struct Stats {
    pub total_commands_processed: u64,
    pub keyspace_hits: u64,
    pub keyspace_misses: u64,
}

/// Initialize config with defaults.
pub fn init_config() {
    let mut defaults = HashMap::new();
    defaults.insert("port".to_string(), "6379".to_string());
    defaults.insert("bind".to_string(), "127.0.0.1".to_string());
    defaults.insert("maxmemory".to_string(), "0".to_string());
    defaults.insert("maxmemory-policy".to_string(), "noeviction".to_string());
    defaults.insert("databases".to_string(), "16".to_string());
    defaults.insert("save".to_string(), "".to_string());
    defaults.insert("appendonly".to_string(), "no".to_string());
    defaults.insert("appendfsync".to_string(), "everysec".to_string());
    *CONFIG.write().unwrap() = Some(defaults);
}

/// Get the current config value or return empty string if not set.
pub fn get_config(key: &str) -> Option<String> {
    CONFIG.read().unwrap().as_ref()?.get(key).cloned()
}

/// Set a config value. Returns the old value if it existed.
pub fn set_config(key: &str, value: &str) -> Option<String> {
    CONFIG.write().unwrap().as_mut()?.insert(key.to_string(), value.to_string())
}

/// Get the full config map.
pub fn get_config_map() -> HashMap<String, String> {
    CONFIG.read().unwrap().as_ref().cloned().unwrap_or_default()
}

/// Increment a stat counter.
pub fn inc_stat(field: &str, delta: u64) {
    if let Ok(mut stats) = STATS.write() {
        match field {
            "total_commands_processed" => stats.total_commands_processed += delta,
            "keyspace_hits" => stats.keyspace_hits += delta,
            "keyspace_misses" => stats.keyspace_misses += delta,
            _ => {}
        }
    }
}

/// Get current stats as a map.
pub fn get_stats() -> Stats {
    STATS.read().unwrap().clone()
}

/// Reset statistics counters.
pub fn reset_stats() {
    if let Ok(mut stats) = STATS.write() {
        *stats = Stats::default();
    }
}

// ─── Commands ─────────────────────────────────────────────────────────────────

/// CONFIG subcommand dispatcher (GET, SET, REWRITE, RESETSTAT).
pub fn config_dispatch(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CONFIG".into()));
    }
    let sub = args[0].as_str().unwrap_or("").to_uppercase();
    let sub_args = &args[1..];
    match sub.as_str() {
        "GET" => config_get(sub_args),
        "SET" => config_set(sub_args),
        "REWRITE" => config_rewrite(sub_args),
        "RESETSTAT" => config_resetstat(sub_args),
        _ => Err(CommandError::Generic(format!("ERR CONFIG subcommand '{}' not supported", sub))),
    }
}

/// CONFIG GET <pattern>
/// Returns configuration parameters matching the given pattern.
pub fn config_get(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CONFIG GET".into()));
    }
    let pattern = args[0].as_str().unwrap_or("*");
    let config = get_config_map();

    // Simple glob matching
    let matches: Vec<Value> = config
        .iter()
        .filter(|(k, _)| glob_match(pattern, k))
        .flat_map(|(k, v)| {
            [Value::BulkString(Some(k.clone())), Value::BulkString(Some(v.clone()))]
        })
        .collect();

    Ok(Value::Array(matches))
}

/// CONFIG SET <key> <value>
/// Sets a runtime configuration parameter.
pub fn config_set(args: &[Value]) -> CommandResult {
    if args.len() != 2 {
        return Err(CommandError::WrongNumberOfArgs("CONFIG SET".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let value = args[1].as_str().ok_or(CommandError::WrongType)?;

    // Validate some known config keys
    match key {
        "port" | "maxmemory" | "databases" => {
            if value.parse::<u64>().is_err() {
                return Err(CommandError::Generic(format!("ERR invalid value for '{}'", key)));
            }
        }
        _ => {}
    }

    set_config(key, value);
    Ok(Value::SimpleString("OK".to_string()))
}

/// CONFIG REWRITE
/// Rewrites the config file (redis.conf) with current settings.
/// For simplicity, we just return OK — actual file rewrite is a TODO.
pub fn config_rewrite(_args: &[Value]) -> CommandResult {
    // In a real implementation, we would write back to redis.conf
    // For now, just return OK
    Ok(Value::SimpleString("OK".to_string()))
}

/// CONFIG RESETSTAT
/// Resets statistics counters.
pub fn config_resetstat(_args: &[Value]) -> CommandResult {
    reset_stats();
    Ok(Value::SimpleString("OK".to_string()))
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

/// Simple glob-style pattern matching.
/// Supports: * matches any sequence, ? matches single char.
fn glob_match(pattern: &str, name: &str) -> bool {
    if pattern == "*" {
        return true;
    }
    let pat = pattern.as_bytes();
    let name = name.as_bytes();
    let mut i = 0;
    let mut j = 0;
    while i < pat.len() && j < name.len() {
        match pat[i] {
            b'*' => {
                // Try matching zero or more chars
                if i + 1 < pat.len() {
                    let next = pat[i + 1];
                    // Find position of next non-* char in pattern
                    while j < name.len() {
                        if name[j] == next || (next == b'?' && name[j] != b'.') {
                            break;
                        }
                        j += 1;
                    }
                    i += 1;
                } else {
                    return true;
                }
            }
            b'?' => {
                i += 1;
                j += 1;
            }
            c => {
                if c == name[j] {
                    i += 1;
                    j += 1;
                } else {
                    return false;
                }
            }
        }
    }
    // Consume trailing * in pattern
    while i < pat.len() && pat[i] == b'*' {
        i += 1;
    }
    i == pat.len() && j == name.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("port", "port"));
        assert!(glob_match("max*", "maxmemory"));
        assert!(glob_match("max???:", "maxabc:"));
        assert!(glob_match("?abc", "xabc"));
    }
}
