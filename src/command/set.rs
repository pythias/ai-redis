//! Set-type Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
use crate::storage::Storage;

pub fn sadd(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("SADD".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let members: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let added = match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Set(set) => {
                    let before = set.len();
                    for m in &members {
                        if !set.contains(&m.to_string()) {
                            set.push(m.to_string());
                        }
                    }
                    set.len() - before
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => {
            let mut set = Vec::new();
            for m in &members {
                set.push(m.to_string());
            }
            guard.insert(key.to_string(), StoredValue::new(RedisData::Set(set)));
            members.len()
        }
    };

    Ok(Value::Integer(added as i64))
}

pub fn srem(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("SREM".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let members: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let removed = match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Set(set) => {
                    let before = set.len();
                    for m in &members {
                        if let Some(pos) = set.iter().position(|s| s == m) {
                            set.remove(pos);
                        }
                    }
                    before - set.len()
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => 0,
    };

    Ok(Value::Integer(removed as i64))
}

pub fn spop(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("SPOP".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let count = if args.len() > 1 {
        args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).unwrap_or(1) as usize
    } else {
        1
    };

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Set(set) => {
                    if set.is_empty() {
                        return Ok(Value::BulkString(None));
                    }
                    let to_remove: Vec<String> = set.iter().take(count).cloned().collect();
                    for m in &to_remove {
                        if let Some(pos) = set.iter().position(|s| s == m) {
                            set.remove(pos);
                        }
                    }
                    if count == 1 {
                        Ok(Value::BulkString(Some(to_remove[0].clone())))
                    } else {
                        Ok(Value::Array(to_remove.iter().map(|m| Value::BulkString(Some(m.clone()))).collect()))
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::BulkString(None)),
    }
}

pub fn smembers(args: &[Value]) -> CommandResult {
    if args.len() != 1 { return Err(CommandError::WrongNumberOfArgs("SMEMBERS".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Set(set) => {
                    let members: Vec<Value> = set.iter().map(|m| Value::BulkString(Some(m.clone()))).collect();
                    Ok(Value::Array(members))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn sismember(args: &[Value]) -> CommandResult {
    if args.len() != 2 { return Err(CommandError::WrongNumberOfArgs("SISMEMBER".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let member = args[1].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Set(set) => {
                    Ok(Value::Integer(if set.contains(&member.to_string()) { 1 } else { 0 }))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn scard(args: &[Value]) -> CommandResult {
    if args.len() != 1 { return Err(CommandError::WrongNumberOfArgs("SCARD".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Set(set) => Ok(Value::Integer(set.len() as i64)),
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn smove(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("SMOVE".into())); }
    let src = args[0].as_str().ok_or(CommandError::WrongType)?;
    let dst = args[1].as_str().ok_or(CommandError::WrongType)?;
    let member = args[2].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let removed = match guard.get_mut(src) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Set(set) => {
                    if let Some(pos) = set.iter().position(|s| s == member) {
                        set.remove(pos);
                        // Add to destination
                        match guard.get_mut(dst) {
                            Some(v) if !v.is_expired() => {
                                match &mut v.data {
                                    RedisData::Set(dst_set) => {
                                        if !dst_set.contains(&member.to_string()) {
                                            dst_set.push(member.to_string());
                                        }
                                    }
                                    _ => return Err(CommandError::WrongType),
                                }
                            }
                            _ => {
                                let mut dst_set = Vec::new();
                                dst_set.push(member.to_string());
                                guard.insert(dst.to_string(), StoredValue::new(RedisData::Set(dst_set)));
                            }
                        }
                        true
                    } else {
                        false
                    }
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => false,
    };

    Ok(Value::Integer(if removed { 1 } else { 0 }))
}

pub fn sinter(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("SINTER".into())); }
    let keys: Vec<&str> = args.iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let guard = store.read().unwrap();

    let mut result_set: Option<Vec<String>> = None;

    for key in &keys {
        match guard.get(*key) {
            Some(v) if !v.is_expired() => {
                match &v.data {
                    RedisData::Set(set) => {
                        if let Some(ref mut rs) = result_set {
                            *rs = rs.iter().filter(|m| set.contains(m)).cloned().collect();
                        } else {
                            result_set = Some(set.clone());
                        }
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
            _ => {
                return Ok(Value::Array(vec![]));
            }
        }
    }

    let result = result_set.unwrap_or_default();
    let members: Vec<Value> = result.iter().map(|m| Value::BulkString(Some(m.clone()))).collect();
    Ok(Value::Array(members))
}

pub fn sunion(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("SUNION".into())); }
    let keys: Vec<&str> = args.iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let guard = store.read().unwrap();

    let mut result_set = Vec::new();

    for key in &keys {
        match guard.get(*key) {
            Some(v) if !v.is_expired() => {
                match &v.data {
                    RedisData::Set(set) => {
                        for m in set {
                            if !result_set.contains(m) {
                                result_set.push(m.clone());
                            }
                        }
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
            _ => {}
        }
    }

    let members: Vec<Value> = result_set.iter().map(|m| Value::BulkString(Some(m.clone()))).collect();
    Ok(Value::Array(members))
}

pub fn sdiff(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("SDIFF".into())); }
    let keys: Vec<&str> = args.iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let guard = store.read().unwrap();

    let first_key = keys[0];
    let first_set = match guard.get(first_key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Set(set) => set.clone(),
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => return Ok(Value::Array(vec![])),
    };

    let mut result_set = first_set;

    for key in &keys[1..] {
        match guard.get(*key) {
            Some(v) if !v.is_expired() => {
                match &v.data {
                    RedisData::Set(set) => {
                        result_set.retain(|m| !set.contains(m));
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
            _ => {}
        }
    }

    let members: Vec<Value> = result_set.iter().map(|m| Value::BulkString(Some(m.clone()))).collect();
    Ok(Value::Array(members))
}
