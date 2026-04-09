//! Hash-type Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
use crate::storage::Storage;

pub fn hset(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("HSET".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let field = args[1].as_str().ok_or(CommandError::WrongType)?;
    let value = args[2].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let is_new = match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Hash(h) => {
                    let was_new = !h.contains_key(field);
                    h.insert(field.to_string(), value.to_string());
                    was_new
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => {
            let mut h = std::collections::HashMap::new();
            h.insert(field.to_string(), value.to_string());
            guard.insert(key.to_string(), StoredValue::new(RedisData::Hash(h)));
            true
        }
    };

    Ok(Value::Integer(if is_new { 1 } else { 0 }))
}

pub fn hget(args: &[Value]) -> CommandResult {
    if args.len() != 2 { return Err(CommandError::WrongNumberOfArgs("HGET".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let field = args[1].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => Ok(Value::BulkString(h.get(field).cloned())),
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::BulkString(None)),
    }
}

pub fn hmget(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("HMGET".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let fields: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let results: Vec<Value> = fields.iter()
                        .map(|f| Value::BulkString(h.get(*f).cloned()))
                        .collect();
                    Ok(Value::Array(results))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(fields.iter().map(|_| Value::BulkString(None)).collect())),
    }
}

pub fn hmset(args: &[Value]) -> CommandResult {
    if args.len() < 3 || (args.len() - 1) % 2 != 0 {
        return Err(CommandError::WrongNumberOfArgs("HMSET".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let mut h = std::collections::HashMap::new();
    for pair in args[1..].chunks(2) {
        if pair.len() == 2 {
            if let (Some(k), Some(v)) = (pair[0].as_str(), pair[1].as_str()) {
                h.insert(k.to_string(), v.to_string());
            }
        }
    }

    guard.insert(key.to_string(), StoredValue::new(RedisData::Hash(h)));
    Ok(Value::SimpleString("OK".to_string()))
}

pub fn hdel(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("HDEL".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let fields: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Hash(h) => {
                    let deleted = fields.iter().filter(|f| h.remove(**f).is_some()).count();
                    return Ok(Value::Integer(deleted as i64));
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => {}
    }
    Ok(Value::Integer(0))
}

pub fn hlen(args: &[Value]) -> CommandResult {
    if args.len() != 1 { return Err(CommandError::WrongNumberOfArgs("HLEN".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => Ok(Value::Integer(h.len() as i64)),
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn hexists(args: &[Value]) -> CommandResult {
    if args.len() != 2 { return Err(CommandError::WrongNumberOfArgs("HEXISTS".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let field = args[1].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => Ok(Value::Integer(if h.contains_key(field) { 1 } else { 0 })),
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn hgetall(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("HGETALL".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let mut result = Vec::new();
                    for (k, val) in h.iter() {
                        result.push(Value::BulkString(Some(k.clone())));
                        result.push(Value::BulkString(Some(val.clone())));
                    }
                    Ok(Value::Array(result))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn hkeys(args: &[Value]) -> CommandResult {
    if args.len() != 1 { return Err(CommandError::WrongNumberOfArgs("HKEYS".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let keys: Vec<Value> = h.keys().map(|k| Value::BulkString(Some(k.clone()))).collect();
                    Ok(Value::Array(keys))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn hvals(args: &[Value]) -> CommandResult {
    if args.len() != 1 { return Err(CommandError::WrongNumberOfArgs("HVALS".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let vals: Vec<Value> = h.values().map(|v| Value::BulkString(Some(v.clone()))).collect();
                    Ok(Value::Array(vals))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn hsetnx(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("HSETNX".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let field = args[1].as_str().ok_or(CommandError::WrongType)?;
    let value = args[2].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data.clone() {
                RedisData::Hash(h) => {
                    if h.contains_key(field) {
                        return Ok(Value::Integer(0));
                    }
                    h.insert(field.to_string(), value.to_string());
                    Ok(Value::Integer(1))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => {
            let mut h = std::collections::HashMap::new();
            h.insert(field.to_string(), value.to_string());
            guard.insert(key.to_string(), StoredValue::new(RedisData::Hash(h)));
            Ok(Value::Integer(1))
        }
    }
}

pub fn hincrby(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("HINCRBY".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let field = args[1].as_str().ok_or(CommandError::WrongType)?;
    let delta: i64 = args[2].as_int()
        .or_else(|| args[2].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let (current, needs_insert) = match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let current_val: i64 = h.get(field).and_then(|s| s.parse().ok()).unwrap_or(0);
                    (current_val, false)
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => (0, true),
    };

    let new_val = current + delta;

    if needs_insert {
        let mut h = std::collections::HashMap::new();
        h.insert(field.to_string(), new_val.to_string());
        guard.insert(key.to_string(), StoredValue::new(RedisData::Hash(h)));
    } else if let Some(v) = guard.get_mut(key) {
        if let RedisData::Hash(h) = &mut v.data {
            h.insert(field.to_string(), new_val.to_string());
        }
    }

    Ok(Value::Integer(new_val))
}

pub fn hincrbyfloat(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("HINCRBYFLOAT".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let field = args[1].as_str().ok_or(CommandError::WrongType)?;
    let delta: f64 = args[2].as_str()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::InvalidFloat)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let current: f64 = match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => h.get(field).and_then(|s| s.parse().ok()).unwrap_or(0.0),
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => 0.0,
    };

    let new_val = current + delta;

    match guard.get_mut(key) {
        Some(v) => {
            if let RedisData::Hash(h) = &mut v.data {
                h.insert(field.to_string(), new_val.to_string());
            }
        }
        None => {
            let mut h = std::collections::HashMap::new();
            h.insert(field.to_string(), new_val.to_string());
            guard.insert(key.to_string(), StoredValue::new(RedisData::Hash(h)));
        }
        _ => {}
    }

    Ok(Value::BulkString(Some(new_val.to_string())))
}

pub fn hstrlen(args: &[Value]) -> CommandResult {
    if args.len() != 2 { return Err(CommandError::WrongNumberOfArgs("HSTRLEN".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let field = args[1].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let len = h.get(field).map(|s| s.len() as i64).unwrap_or(0);
                    Ok(Value::Integer(len))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}
