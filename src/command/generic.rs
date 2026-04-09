//! Generic key commands: DEL, EXISTS, TYPE, PING, ECHO, SCAN, etc.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::memory::current_time_ms;

pub fn del(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("DEL".into())); }
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    let mut count = 0;
    for arg in args {
        if let Some(key) = arg.as_str() {
            if guard.remove(key).is_some() { count += 1; }
        }
    }
    Ok(Value::Integer(count))
}

pub fn exists(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("EXISTS".into())); }
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    let mut count = 0;
    for arg in args {
        if let Some(key) = arg.as_str() {
            if guard.get(key).map(|v| !v.is_expired()).unwrap_or(false) { count += 1; }
        }
    }
    Ok(Value::Integer(count))
}

pub fn r#type(args: &[Value]) -> CommandResult {
    check_args(args, 1, "TYPE")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    let t = match guard.get(key) {
        Some(v) if v.is_expired() => "none",
        Some(v) => v.type_name(),
        None => "none",
    };
    Ok(Value::SimpleString(t.to_string()))
}

pub fn expire(args: &[Value]) -> CommandResult {
    check_args(args, 2, "EXPIRE")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let secs: i64 = args[1].as_int()
        .or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    if let Some(v) = guard.get_mut(key) {
        if v.is_expired() { return Ok(Value::Integer(0)); }
        let now = current_time_ms();
        v.expire_at = Some(now + secs * 1000);
        Ok(Value::Integer(1))
    } else {
        Ok(Value::Integer(0))
    }
}

pub fn expireat(args: &[Value]) -> CommandResult {
    check_args(args, 2, "EXPIREAT")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let ts: i64 = args[1].as_int()
        .or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    if let Some(v) = guard.get_mut(key) {
        if v.is_expired() { return Ok(Value::Integer(0)); }
        v.expire_at = Some(ts * 1000);
        Ok(Value::Integer(1))
    } else {
        Ok(Value::Integer(0))
    }
}

pub fn ttl(args: &[Value]) -> CommandResult {
    check_args(args, 1, "TTL")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    let result = match guard.get(key) {
        Some(v) if v.is_expired() => -2i64,
        Some(v) => match v.expire_at {
            Some(ts) => ((ts - current_time_ms()) / 1000).max(0),
            None => -1i64,
        },
        None => -2i64,
    };
    Ok(Value::Integer(result))
}

pub fn pttl(args: &[Value]) -> CommandResult {
    check_args(args, 1, "PTTL")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    let result = match guard.get(key) {
        Some(v) if v.is_expired() => -2i64,
        Some(v) => match v.expire_at {
            Some(ts) => (ts - current_time_ms()).max(0),
            None => -1i64,
        },
        None => -2i64,
    };
    Ok(Value::Integer(result))
}

pub fn persist(args: &[Value]) -> CommandResult {
    check_args(args, 1, "PERSIST")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    if let Some(v) = guard.get_mut(key) {
        if v.is_expired() { return Ok(Value::Integer(0)); }
        v.expire_at = None;
        Ok(Value::Integer(1))
    } else {
        Ok(Value::Integer(0))
    }
}

pub fn ping(args: &[Value]) -> CommandResult {
    match args.first() {
        Some(v) => Ok(Value::SimpleString(v.as_str().unwrap_or("").to_string())),
        None => Ok(Value::SimpleString("PONG".to_string())),
    }
}

pub fn echo(args: &[Value]) -> CommandResult {
    check_args(args, 1, "ECHO")?;
    match &args[0] {
        Value::BulkString(Some(s)) => Ok(Value::BulkString(Some(s.clone()))),
        Value::SimpleString(s) => Ok(Value::SimpleString(s.clone())),
        _ => Ok(Value::BulkString(Some("".to_string()))),
    }
}

pub fn select(args: &[Value]) -> CommandResult {
    check_args(args, 1, "SELECT")?;
    let idx: i64 = args[0].as_int()
        .or_else(|| args[0].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    if idx < 0 || idx > 15 {
        return Err(CommandError::Generic("ERR DB index out of range".into()));
    }
    Ok(Value::SimpleString("OK".to_string()))
}

pub fn flushdb(_args: &[Value]) -> CommandResult {
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    guard.retain(|_, v| !v.is_expired());
    Ok(Value::SimpleString("OK".to_string()))
}

pub fn dbsize(_args: &[Value]) -> CommandResult {
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    Ok(Value::Integer(guard.iter().filter(|(_, v)| !v.is_expired()).count() as i64))
}

pub fn keys(args: &[Value]) -> CommandResult {
    let pattern = args.first().and_then(|v| v.as_str()).unwrap_or("*");
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    use crate::storage::memory::glob_match;
    let matches: Vec<Value> = guard.iter()
        .filter(|(_, v)| !v.is_expired())
        .map(|(k, _)| k.clone())
        .filter(|k| glob_match(pattern, k))
        .map(|k| Value::BulkString(Some(k)))
        .collect();
    Ok(Value::Array(matches))
}

pub fn scan(args: &[Value]) -> CommandResult {
    let cursor: usize = args.first()
        .and_then(|v| v.as_str())
        .and_then(|s: &str| s.parse().ok())
        .unwrap_or(0);
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    let all_keys: Vec<String> = guard.iter()
        .filter(|(_, v)| !v.is_expired())
        .map(|(k, _)| k.clone())
        .collect();
    drop(guard);
    let count = 10;
    let end = (cursor + count).min(all_keys.len());
    let keys: Vec<Value> = all_keys[cursor..end]
        .iter().cloned().map(|k| Value::BulkString(Some(k))).collect();
    let next = if end >= all_keys.len() { 0 } else { end };
    Ok(Value::Array(vec![
        Value::BulkString(Some(next.to_string())),
        Value::Array(keys),
    ]))
}

fn check_args(args: &[Value], expected: usize, cmd: &str) -> CommandResult {
    if args.len() != expected { Err(CommandError::WrongNumberOfArgs(cmd.into())) } else { Ok(Value::Null) }
}
