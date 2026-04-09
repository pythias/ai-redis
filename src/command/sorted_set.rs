//! Sorted Set (ZSET) Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
use crate::storage::Storage;
use std::collections::BTreeMap;

pub fn zadd(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("ZADD".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    
    let mut i = 1;
    let mut members = Vec::new();
    while i < args.len() {
        if args.len() - i < 2 {
            return Err(CommandError::WrongNumberOfArgs("ZADD".into()));
        }
        let score: f64 = args[i].as_str()
            .and_then(|s| s.parse().ok())
            .ok_or(CommandError::InvalidFloat)?;
        let member = args[i + 1].as_str().ok_or(CommandError::WrongType)?;
        members.push((score, member.to_string()));
        i += 2;
    }

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let added = match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::SortedSet(zset) => {
                    let before = zset.len();
                    for (score, member) in &members {
                        zset.insert(member.clone(), *score);
                    }
                    zset.len() - before
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => {
            let mut zset = BTreeMap::new();
            for (score, member) in &members {
                zset.insert(member.clone(), *score);
            }
            guard.insert(key.to_string(), StoredValue::new(RedisData::SortedSet(zset)));
            members.len()
        }
    };

    Ok(Value::Integer(added as i64))
}

pub fn zrange(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("ZRANGE".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let start: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let stop: i64 = args[2].as_int().or_else(|| args[2].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let withscores = args.len() > 3 && args[3].as_str().map(|s| s.to_uppercase()) == Some("WITHSCORES".to_string());

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::SortedSet(zset) => {
                    if zset.is_empty() {
                        return Ok(Value::Array(vec![]));
                    }
                    let len = zset.len() as i64;
                    let start = normalize_idx(start, len);
                    let stop = normalize_idx(stop, len);
                    
                    let items: Vec<(String, f64)> = zset.iter().map(|(m, s)| (m.clone(), *s)).collect();
                    let start = start.min(items.len());
                    let stop = stop.min(items.len() - 1);
                    
                    if start > stop {
                        return Ok(Value::Array(vec![]));
                    }
                    
                    let mut result = Vec::new();
                    for i in start..=stop {
                        let (member, score) = &items[i];
                        result.push(Value::BulkString(Some(member.clone())));
                        if withscores {
                            result.push(Value::BulkString(Some(score.to_string())));
                        }
                    }
                    Ok(Value::Array(result))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn zrevrange(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("ZREVRANGE".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let start: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let stop: i64 = args[2].as_int().or_else(|| args[2].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let withscores = args.len() > 3 && args[3].as_str().map(|s| s.to_uppercase()) == Some("WITHSCORES".to_string());

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::SortedSet(zset) => {
                    if zset.is_empty() {
                        return Ok(Value::Array(vec![]));
                    }
                    let items: Vec<(String, f64)> = zset.iter().map(|(m, s)| (m.clone(), *s)).collect();
                    let items: Vec<(String, f64)> = items.into_iter().rev().collect();
                    
                    let len = items.len() as i64;
                    let start = normalize_idx(start, len);
                    let stop = normalize_idx(stop, len);
                    
                    let start = start.min(items.len());
                    let stop = stop.min(items.len() - 1);
                    
                    if start > stop {
                        return Ok(Value::Array(vec![]));
                    }
                    
                    let mut result = Vec::new();
                    for i in start..=stop {
                        let (member, score) = &items[i];
                        result.push(Value::BulkString(Some(member.clone())));
                        if withscores {
                            result.push(Value::BulkString(Some(score.to_string())));
                        }
                    }
                    Ok(Value::Array(result))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn zrangebyscore(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("ZRANGEBYSCORE".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let min: f64 = args[1].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let max: f64 = args[2].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let withscores = args.len() > 3 && args[3].as_str().map(|s| s.to_uppercase()) == Some("WITHSCORES".to_string());

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::SortedSet(zset) => {
                    let mut result = Vec::new();
                    for (member, score) in zset {
                        if *score >= min && *score <= max {
                            result.push(Value::BulkString(Some(member.clone())));
                            if withscores {
                                result.push(Value::BulkString(Some(score.to_string())));
                            }
                        }
                    }
                    Ok(Value::Array(result))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn zrevrangebyscore(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("ZREVRANGEBYSCORE".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let max: f64 = args[1].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let min: f64 = args[2].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let withscores = args.len() > 3 && args[3].as_str().map(|s| s.to_uppercase()) == Some("WITHSCORES".to_string());

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::SortedSet(zset) => {
                    let items: Vec<(String, f64)> = zset.iter().map(|(m, s)| (m.clone(), *s)).collect();
                    let items: Vec<(String, f64)> = items.into_iter().rev().collect();
                    
                    let mut result = Vec::new();
                    for (member, score) in items {
                        if score >= min && score <= max {
                            result.push(Value::BulkString(Some(member.clone())));
                            if withscores {
                                result.push(Value::BulkString(Some(score.to_string())));
                            }
                        }
                    }
                    Ok(Value::Array(result))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn zincrby(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("ZINCRBY".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let delta: f64 = args[1].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let member = args[2].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::SortedSet(zset) => {
                    let current = *zset.get(member).unwrap_or(&0.0);
                    let new_score = current + delta;
                    zset.insert(member.to_string(), new_score);
                    Ok(Value::BulkString(Some(new_score.to_string())))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => {
            let mut zset = BTreeMap::new();
            zset.insert(member.to_string(), delta);
            guard.insert(key.to_string(), StoredValue::new(RedisData::SortedSet(zset)));
            Ok(Value::BulkString(Some(delta.to_string())))
        }
    }
}

pub fn zscore(args: &[Value]) -> CommandResult {
    if args.len() != 2 { return Err(CommandError::WrongNumberOfArgs("ZSCORE".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let member = args[1].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::SortedSet(zset) => {
                    Ok(Value::BulkString(zset.get(member).map(|s| s.to_string())))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::BulkString(None)),
    }
}

pub fn zcard(args: &[Value]) -> CommandResult {
    if args.len() != 1 { return Err(CommandError::WrongNumberOfArgs("ZCARD".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::SortedSet(zset) => Ok(Value::Integer(zset.len() as i64)),
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn zcount(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("ZCOUNT".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let min: f64 = args[1].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let max: f64 = args[2].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::SortedSet(zset) => {
                    let count = zset.values().filter(|s| **s >= min && **s <= max).count();
                    Ok(Value::Integer(count as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn zrank(args: &[Value]) -> CommandResult {
    if args.len() != 2 { return Err(CommandError::WrongNumberOfArgs("ZRANK".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let member = args[1].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::SortedSet(zset) => {
                    let items: Vec<&String> = zset.keys().collect();
                    match items.iter().position(|m| *m == member) {
                        Some(pos) => Ok(Value::Integer(pos as i64)),
                        None => Ok(Value::BulkString(None)),
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::BulkString(None)),
    }
}

pub fn zrevrank(args: &[Value]) -> CommandResult {
    if args.len() != 2 { return Err(CommandError::WrongNumberOfArgs("ZREVRANK".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let member = args[1].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::SortedSet(zset) => {
                    let items: Vec<&String> = zset.keys().rev().collect();
                    match items.iter().position(|m| *m == member) {
                        Some(pos) => Ok(Value::Integer(pos as i64)),
                        None => Ok(Value::BulkString(None)),
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::BulkString(None)),
    }
}

pub fn zrem(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("ZREM".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let members: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::SortedSet(zset) => {
                    let before = zset.len();
                    for m in &members {
                        zset.remove(*m);
                    }
                    Ok(Value::Integer((before - zset.len()) as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn zremrangebyrank(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("ZREMRANGEBYRANK".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let start: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let stop: i64 = args[2].as_int().or_else(|| args[2].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::SortedSet(zset) => {
                    if zset.is_empty() {
                        return Ok(Value::Integer(0));
                    }
                    let items: Vec<(String, f64)> = zset.iter().map(|(m, s)| (m.clone(), *s)).collect();
                    let len = items.len() as i64;
                    let start = normalize_idx(start, len);
                    let stop = normalize_idx(stop, len);
                    
                    let start = start.min(items.len());
                    let stop = (stop + 1).min(items.len());
                    
                    let to_remove: Vec<String> = items[start..stop].iter().map(|(m, _)| m.clone()).collect();
                    for m in to_remove {
                        zset.remove(&m);
                    }
                    Ok(Value::Integer((stop - start) as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn zremrangebyscore(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("ZREMRANGEBYSCORE".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let min: f64 = args[1].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let max: f64 = args[2].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::SortedSet(zset) => {
                    let before = zset.len();
                    zset.retain(|_, score| !(*score >= min && *score <= max));
                    Ok(Value::Integer((before - zset.len()) as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

fn normalize_idx(idx: i64, len: i64) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx.min(len) as usize
    }
}
