//! List-type Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
use crate::storage::Storage;
use std::collections::VecDeque;

pub fn lpush(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("LPUSH".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let values: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let list = match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::List(list) => list,
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => {
            guard.insert(key.to_string(), StoredValue::new(RedisData::List(VecDeque::new())));
            guard.get_mut(key).unwrap().data.as_list_mut().unwrap()
        }
    };

    for val in values.iter().rev() {
        list.push_front(val.to_string());
    }

    Ok(Value::Integer(list.len() as i64))
}

pub fn rpush(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("RPUSH".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let values: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let list = match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::List(list) => list,
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => {
            guard.insert(key.to_string(), StoredValue::new(RedisData::List(VecDeque::new())));
            guard.get_mut(key).unwrap().data.as_list_mut().unwrap()
        }
    };

    for val in values {
        list.push_back(val.to_string());
    }

    Ok(Value::Integer(list.len() as i64))
}

pub fn lpop(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("LPOP".into())); }
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
                RedisData::List(list) => {
                    if list.is_empty() {
                        return Ok(Value::BulkString(None));
                    }
                    if count == 1 {
                        Ok(Value::BulkString(list.pop_front()))
                    } else {
                        let mut result = Vec::new();
                        for _ in 0..count.min(list.len()) {
                            if let Some(item) = list.pop_front() {
                                result.push(Value::BulkString(Some(item)));
                            }
                        }
                        Ok(Value::Array(result))
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::BulkString(None)),
    }
}

pub fn rpop(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("RPOP".into())); }
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
                RedisData::List(list) => {
                    if list.is_empty() {
                        return Ok(Value::BulkString(None));
                    }
                    if count == 1 {
                        Ok(Value::BulkString(list.pop_back()))
                    } else {
                        let mut result = Vec::new();
                        for _ in 0..count.min(list.len()) {
                            if let Some(item) = list.pop_back() {
                                result.push(Value::BulkString(Some(item)));
                            }
                        }
                        Ok(Value::Array(result))
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::BulkString(None)),
    }
}

pub fn lrange(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("LRANGE".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let start: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let stop: i64 = args[2].as_int().or_else(|| args[2].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::List(list) => {
                    let len = list.len() as i64;
                    let start = normalize_index(start, len);
                    let stop = normalize_index(stop, len);
                    
                    if start >= len as usize {
                        return Ok(Value::Array(vec![]));
                    }
                    
                    let stop = stop.min(len as usize - 1);
                    let items: Vec<Value> = list.iter()
                        .skip(start)
                        .take(stop - start + 1)
                        .map(|s| Value::BulkString(Some(s.clone())))
                        .collect();
                    Ok(Value::Array(items))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn llen(args: &[Value]) -> CommandResult {
    if args.len() != 1 { return Err(CommandError::WrongNumberOfArgs("LLEN".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::List(list) => Ok(Value::Integer(list.len() as i64)),
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn lindex(args: &[Value]) -> CommandResult {
    if args.len() != 2 { return Err(CommandError::WrongNumberOfArgs("LINDEX".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let index: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::List(list) => {
                    let len = list.len() as i64;
                    if index < 0 && -index > len {
                        return Ok(Value::BulkString(None));
                    }
                    let idx = if index < 0 {
                        (len + index) as usize
                    } else {
                        index as usize
                    };
                    Ok(Value::BulkString(list.get(idx).cloned()))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::BulkString(None)),
    }
}

pub fn lset(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("LSET".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let index: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let value = args[2].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::List(list) => {
                    let len = list.len() as i64;
                    let idx = if index < 0 {
                        (len + index) as usize
                    } else {
                        index as usize
                    };
                    if idx >= list.len() {
                        return Err(CommandError::IndexOutOfRange);
                    }
                    let mut temp = list.iter().cloned().collect::<Vec<_>>();
                    temp[idx] = value.to_string();
                    *list = temp.into_iter().collect();
                    Ok(Value::SimpleString("OK".to_string()))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Err(CommandError::Generic("no such key".to_string())),
    }
}

pub fn ltrim(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("LTRIM".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let start: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let stop: i64 = args[2].as_int().or_else(|| args[2].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::List(list) => {
                    let len = list.len() as i64;
                    if len == 0 {
                        return Ok(Value::SimpleString("OK".to_string()));
                    }
                    let start = normalize_index(start, len);
                    let stop = normalize_index(stop, len);
                    
                    if start >= len as usize {
                        list.clear();
                        return Ok(Value::SimpleString("OK".to_string()));
                    }
                    
                    let stop = stop.min(len as usize - 1);
                    let stop = stop + 1;
                    
                    let items: Vec<String> = list.iter().cloned().collect();
                    *list = items.into_iter().skip(start).take(stop - start).collect();
                    
                    Ok(Value::SimpleString("OK".to_string()))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::SimpleString("OK".to_string())),
    }
}

pub fn lpushx(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("LPUSHX".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let value = args[1].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::List(list) => {
                    list.push_front(value.to_string());
                    Ok(Value::Integer(list.len() as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn rpushx(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("RPUSHX".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let value = args[1].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::List(list) => {
                    list.push_back(value.to_string());
                    Ok(Value::Integer(list.len() as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn lrem(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("LREM".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let count: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
    let value = args[2].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::List(list) => {
                    let removed = if count == 0 {
                        let before = list.len();
                        list.retain(|v| v != value);
                        before - list.len()
                    } else if count > 0 {
                        let mut removed = 0;
                        let mut i = 0;
                        let items: Vec<String> = list.iter().cloned().collect();
                        let mut new_list: VecDeque<String> = items.into_iter().collect();
                        let temp: Vec<String> = new_list.iter().cloned().collect();
                        let temp_list: VecDeque<String> = temp.into_iter().collect();
                        let mut remaining = temp_list;
                        list.clear();
                        for item in remaining {
                            if item == value && i < count as usize {
                                removed += 1;
                                i += 1;
                            } else {
                                list.push_back(item);
                            }
                        }
                        removed
                    } else {
                        let items: Vec<String> = list.iter().rev().cloned().collect();
                        let mut removed = 0;
                        let mut i = 0;
                        let mut new_list: VecDeque<String> = items.into_iter().collect();
                        let temp: Vec<String> = new_list.iter().cloned().collect();
                        list.clear();
                        for item in temp {
                            if item == value && i < (-count) as usize {
                                removed += 1;
                                i += 1;
                            } else {
                                list.push_front(item);
                            }
                        }
                        removed
                    };
                    Ok(Value::Integer(removed as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

fn normalize_index(idx: i64, len: i64) -> usize {
    if idx < 0 {
        (len + idx).max(0) as usize
    } else {
        idx.min(len) as usize
    }
}
