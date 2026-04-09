//! String-type Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
use crate::storage::Storage;

pub fn set(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("SET".into())); }

    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let value = args[1].as_str().ok_or(CommandError::WrongType)?;

    let mut nx = false;
    let mut xx = false;
    let mut ttl_secs: Option<i64> = None;
    let mut get_mode = false;
    let mut i = 2;

    while i < args.len() {
        let opt = args[i].as_str().unwrap_or("").to_uppercase();
        match opt.as_str() {
            "NX" => { nx = true; i += 1; }
            "XX" => { xx = true; i += 1; }
            "EX" => {
                if i + 1 >= args.len() { return Err(CommandError::WrongNumberOfArgs("SET EX".into())); }
                ttl_secs = Some(args[i+1].as_str().and_then(|s: &str| s.parse::<i64>().ok()).ok_or(CommandError::InvalidInt)?);
                i += 2;
            }
            "PX" => {
                if i + 1 >= args.len() { return Err(CommandError::WrongNumberOfArgs("SET PX".into())); }
                ttl_secs = Some(args[i+1].as_str().and_then(|s: &str| s.parse::<i64>().ok()).ok_or(CommandError::InvalidInt)? / 1000);
                i += 2;
            }
            "KEEPTTL" => { i += 1; }
            "GET" => { get_mode = true; i += 1; }
            _ => { i += 1; }
        }
    }

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    if nx {
        if let Some(v) = guard.get(key) {
            if !v.is_expired() { return Ok(Value::BulkString(None)); }
        }
    }
    if xx {
        let exists = guard.get(key).map(|v| !v.is_expired()).unwrap_or(false);
        if !exists { return Ok(Value::BulkString(None)); }
    }

    let stored = if let Some(s) = ttl_secs {
        StoredValue::with_ttl(RedisData::String(value.to_string()), s)
    } else {
        StoredValue::new(RedisData::String(value.to_string()))
    };

    if get_mode {
        let old = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().cloned() });
        guard.insert(key.to_string(), stored);
        return Ok(Value::BulkString(old));
    }

    guard.insert(key.to_string(), stored);
    Ok(Value::SimpleString("OK".to_string()))
}

pub fn get(args: &[Value]) -> CommandResult {
    check_args(args, 1, "GET")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let store = Storage::get();
    let guard = store.read().unwrap();
    Ok(Value::BulkString(guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().cloned() })))
}

pub fn mget(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("MGET".into())); }
    let store = Storage::get();
    let guard = store.read().unwrap();
    let results: Vec<Value> = args.iter().map(|arg| {
        let key: &str = arg.as_str().unwrap_or("");
        match guard.get(key) {
            Some(v) if !v.is_expired() => Value::BulkString(v.data.as_string().cloned()),
            _ => Value::BulkString(None),
        }
    }).collect();
    Ok(Value::Array(results))
}

pub fn mset(args: &[Value]) -> CommandResult {
    if args.len() % 2 != 0 { return Err(CommandError::WrongNumberOfArgs("MSET".into())); }
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    for pair in args.chunks(2) {
        let key: &str = pair[0].as_str().unwrap_or("");
        let val: &str = pair[1].as_str().unwrap_or("");
        guard.insert(key.to_string(), StoredValue::new(RedisData::String(val.to_string())));
    }
    Ok(Value::SimpleString("OK".to_string()))
}

pub fn setnx(args: &[Value]) -> CommandResult {
    check_args(args, 2, "SETNX")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let value = args[1].as_str().ok_or(CommandError::WrongType)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    let exists = guard.get(key).map(|v| !v.is_expired()).unwrap_or(false);
    if !exists {
        guard.insert(key.to_string(), StoredValue::new(RedisData::String(value.to_string())));
        Ok(Value::Integer(1))
    } else {
        Ok(Value::Integer(0))
    }
}

pub fn incr(args: &[Value]) -> CommandResult {
    check_args(args, 1, "INCR")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    let current: i64 = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().and_then(|s| s.parse::<i64>().ok()) }).unwrap_or(0);
    let new_val = current + 1;
    let exp = guard.get(key).and_then(|v| v.expire_at);
    guard.insert(key.to_string(), StoredValue { data: RedisData::String(new_val.to_string()), expire_at: exp });
    Ok(Value::Integer(new_val))
}

pub fn incrby(args: &[Value]) -> CommandResult {
    check_args(args, 2, "INCRBY")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let delta: i64 = args[1].as_int()
        .or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    let current: i64 = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().and_then(|s| s.parse::<i64>().ok()) }).unwrap_or(0);
    let new_val = current + delta;
    let exp = guard.get(key).and_then(|v| v.expire_at);
    let sv = StoredValue { data: RedisData::String(new_val.to_string()), expire_at: exp };
    guard.insert(key.to_string(), sv);
    Ok(Value::Integer(new_val))
}

pub fn decr(args: &[Value]) -> CommandResult {
    check_args(args, 1, "DECR")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    let current: i64 = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().and_then(|s| s.parse::<i64>().ok()) }).unwrap_or(0);
    let new_val = current - 1;
    let exp = guard.get(key).and_then(|v| v.expire_at);
    guard.insert(key.to_string(), StoredValue { data: RedisData::String(new_val.to_string()), expire_at: exp });
    Ok(Value::Integer(new_val))
}

pub fn decrby(args: &[Value]) -> CommandResult {
    check_args(args, 2, "DECRBY")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let delta: i64 = args[1].as_int()
        .or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    let current: i64 = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().and_then(|s| s.parse::<i64>().ok()) }).unwrap_or(0);
    let new_val = current - delta;
    let exp = guard.get(key).and_then(|v| v.expire_at);
    guard.insert(key.to_string(), StoredValue { data: RedisData::String(new_val.to_string()), expire_at: exp });
    Ok(Value::Integer(new_val))
}

pub fn incrbyfloat(args: &[Value]) -> CommandResult {
    check_args(args, 2, "INCRBYFLOAT")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let delta: f64 = args[1].as_str()
        .and_then(|s: &str| s.parse().ok())
        .ok_or(CommandError::InvalidFloat)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    let current: f64 = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().and_then(|s| s.parse::<f64>().ok()) }).unwrap_or(0.0);
    let new_val = current + delta;
    let exp = guard.get(key).and_then(|v| v.expire_at);
    guard.insert(key.to_string(), StoredValue { data: RedisData::String(new_val.to_string()), expire_at: exp });
    Ok(Value::BulkString(Some(new_val.to_string())))
}

pub fn append(args: &[Value]) -> CommandResult {
    check_args(args, 2, "APPEND")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let val: &str = args[1].as_str().ok_or(CommandError::WrongType)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    let current: String = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().cloned() }).unwrap_or_default();
    let new_val = format!("{}{}", current, val);
    let exp = guard.get(key).and_then(|v| v.expire_at);
    guard.insert(key.to_string(), StoredValue { data: RedisData::String(new_val.clone()), expire_at: exp });
    Ok(Value::Integer(new_val.len() as i64))
}

pub fn strlen(args: &[Value]) -> CommandResult {
    check_args(args, 1, "STRLEN")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let store = Storage::get();
    let guard = store.read().unwrap();
    let len = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().map(|s: &String| s.len() as i64) }).unwrap_or(0);
    Ok(Value::Integer(len))
}

pub fn getrange(args: &[Value]) -> CommandResult {
    check_args(args, 3, "GETRANGE")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let start: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let end: i64 = args[2].as_int().or_else(|| args[2].as_str().and_then(|s: &str| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let store = Storage::get();
    let guard = store.read().unwrap();
    let s: String = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().cloned() }).unwrap_or_default();
    drop(guard);
    let len = s.len() as i64;
    let start = norm_idx(start, len);
    let end = norm_idx(end, len);
    if start > end || start >= len as usize { return Ok(Value::BulkString(Some("".to_string()))); }
    let end = (end + 1).min(len as usize);
    Ok(Value::BulkString(Some(s[start..end].to_string())))
}

pub fn setrange(args: &[Value]) -> CommandResult {
    check_args(args, 3, "SETRANGE")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let offset: usize = args[1].as_int().or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok())).ok_or(CommandError::InvalidInt)? as usize;
    let val: &str = args[2].as_str().ok_or(CommandError::WrongType)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    let current: String = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().cloned() }).unwrap_or_default();
    let exp = guard.get(key).and_then(|v| v.expire_at);
    let mut s = current.into_bytes();
    if offset > s.len() { s.resize(offset, 0u8); }
    s.splice(offset..offset.min(s.len()), val.bytes());
    let new_val = String::from_utf8_lossy(&s).to_string();
    guard.insert(key.to_string(), StoredValue { data: RedisData::String(new_val.clone()), expire_at: exp });
    Ok(Value::Integer(new_val.len() as i64))
}

pub fn setex(args: &[Value]) -> CommandResult {
    check_args(args, 3, "SETEX")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let secs: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let val: &str = args[2].as_str().ok_or(CommandError::WrongType)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    guard.insert(key.to_string(), StoredValue::with_ttl(RedisData::String(val.to_string()), secs));
    Ok(Value::SimpleString("OK".to_string()))
}

pub fn getset(args: &[Value]) -> CommandResult {
    check_args(args, 2, "GETSET")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let val: &str = args[1].as_str().ok_or(CommandError::WrongType)?;
    let store = Storage::get();
    let mut guard = store.write().unwrap();
    let old = guard.get(key).and_then(|v| if v.is_expired() { None } else { v.data.as_string().cloned() });
    guard.insert(key.to_string(), StoredValue::new(RedisData::String(val.to_string())));
    Ok(Value::BulkString(old))
}

fn check_args(args: &[Value], expected: usize, cmd: &str) -> CommandResult {
    if args.len() != expected { Err(CommandError::WrongNumberOfArgs(cmd.into())) } else { Ok(Value::Null) }
}

fn norm_idx(idx: i64, len: i64) -> usize {
    if idx < 0 { (len + idx).max(0) as usize } else { idx.min(len) as usize }
}
