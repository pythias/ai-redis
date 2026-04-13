//! List-type Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
use crate::storage::Storage;
use std::collections::VecDeque;
use std::time::{Duration, Instant};

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
                        let new_list: VecDeque<String> = items.into_iter().collect();
                        let temp: Vec<String> = new_list.iter().cloned().collect();
                        let temp_list: VecDeque<String> = temp.into_iter().collect();
                        let remaining = temp_list;
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
                        let new_list: VecDeque<String> = items.into_iter().collect();
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

// ─────────────────────────────────────────────────────────────────────────────
// LMPOP / BLMPOP
// ─────────────────────────────────────────────────────────────────────────────

/// LMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count]
pub fn lmpop(args: &[Value]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArgs("LMPOP".into()));
    }

    let numkeys: i64 = args[0]
        .as_int()
        .or_else(|| args[0].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    if numkeys < 1 {
        return Err(CommandError::Generic("numkeys must be positive".into()));
    }
    if args.len() < (numkeys as usize) + 2 {
        return Err(CommandError::WrongNumberOfArgs("LMPOP".into()));
    }

    let keys: Vec<&str> = args[1..(numkeys as usize + 1)]
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    let direction = args[(numkeys as usize) + 1]
        .as_str()
        .unwrap_or("")
        .to_uppercase();
    let count = if args.len() > (numkeys as usize) + 3 {
        let op = args[(numkeys as usize) + 2]
            .as_str()
            .unwrap_or("")
            .to_uppercase();
        if op == "COUNT" {
            args[(numkeys as usize) + 3]
                .as_int()
                .or_else(|| args[(numkeys as usize) + 3].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(1) as usize
        } else {
            1
        }
    } else {
        1
    };

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let result = match direction.as_str() {
        "LEFT" => {
            for key in &keys {
                if let Some(v) = guard.get_mut(*key) {
                    if !v.is_expired() {
                        if let RedisData::List(list) = &mut v.data {
                            if !list.is_empty() {
                                let mut items = Vec::new();
                                for _ in 0..count.min(list.len()) {
                                    if let Some(item) = list.pop_front() {
                                        items.push(Value::BulkString(Some(item)));
                                    }
                                }
                                if !items.is_empty() {
                                    let key_str = (*key).to_string();
                                    drop(guard);
                                    crate::network::blocking::notify_waiters(&key_str);
                                    return Ok(Value::Array(vec![
                                        Value::BulkString(Some(key_str)),
                                        Value::Array(items),
                                    ]));
                                }
                            }
                        }
                    }
                }
            }
            Ok(Value::Null)
        }
        "RIGHT" => {
            for key in &keys {
                if let Some(v) = guard.get_mut(*key) {
                    if !v.is_expired() {
                        if let RedisData::List(list) = &mut v.data {
                            if !list.is_empty() {
                                let mut items = Vec::new();
                                for _ in 0..count.min(list.len()) {
                                    if let Some(item) = list.pop_back() {
                                        items.push(Value::BulkString(Some(item)));
                                    }
                                }
                                if !items.is_empty() {
                                    let key_str = (*key).to_string();
                                    drop(guard);
                                    crate::network::blocking::notify_waiters(&key_str);
                                    return Ok(Value::Array(vec![
                                        Value::BulkString(Some(key_str)),
                                        Value::Array(items),
                                    ]));
                                }
                            }
                        }
                    }
                }
            }
            Ok(Value::Null)
        }
        _ => Err(CommandError::SyntaxError),
    };

    drop(guard);
    result
}

/// BLMPOP numkeys key [key ...] <LEFT | RIGHT> [COUNT count] timeout
pub fn blmpop(args: &[Value]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArgs("BLMPOP".into()));
    }

    let numkeys: i64 = args[0]
        .as_int()
        .or_else(|| args[0].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    if numkeys < 1 {
        return Err(CommandError::Generic("numkeys must be positive".into()));
    }
    if args.len() < (numkeys as usize) + 3 {
        return Err(CommandError::WrongNumberOfArgs("BLMPOP".into()));
    }

    let keys: Vec<&str> = args[1..(numkeys as usize + 1)]
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    let direction = args[(numkeys as usize) + 1]
        .as_str()
        .unwrap_or("")
        .to_uppercase();
    let count = if args.len() > (numkeys as usize) + 4 {
        let op = args[(numkeys as usize) + 2]
            .as_str()
            .unwrap_or("")
            .to_uppercase();
        if op == "COUNT" {
            args[(numkeys as usize) + 3]
                .as_int()
                .or_else(|| args[(numkeys as usize) + 3].as_str().and_then(|s| s.parse().ok()))
                .unwrap_or(1) as usize
        } else {
            1
        }
    } else {
        1
    };

    let timeout_idx = if args.len() > (numkeys as usize) + 4
        && args[(numkeys as usize) + 2]
            .as_str()
            .unwrap_or("")
            .to_uppercase()
            == "COUNT"
    {
        (numkeys as usize) + 4
    } else {
        (numkeys as usize) + 2
    };
    let timeout_sec: i64 = args[timeout_idx]
        .as_int()
        .or_else(|| args[timeout_idx].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let timeout = if timeout_sec < 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(timeout_sec as u64)
    };
    let start = Instant::now();
    let deadline = start + timeout;

    loop {
        {
            let store = Storage::get();
            let mut guard = store.write().unwrap();

            match direction.as_str() {
                "LEFT" => {
                    for key in &keys {
                        if let Some(v) = guard.get_mut(*key) {
                            if !v.is_expired() {
                                if let RedisData::List(list) = &mut v.data {
                                    if !list.is_empty() {
                                        let mut items = Vec::new();
                                        for _ in 0..count.min(list.len()) {
                                            if let Some(item) = list.pop_front() {
                                                items.push(Value::BulkString(Some(item)));
                                            }
                                        }
                                        if !items.is_empty() {
                                            let key_str = (*key).to_string();
                                            drop(guard);
                                            crate::network::blocking::notify_waiters(&key_str);
                                            return Ok(Value::Array(vec![
                                                Value::BulkString(Some(key_str)),
                                                Value::Array(items),
                                            ]));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                "RIGHT" => {
                    for key in &keys {
                        if let Some(v) = guard.get_mut(*key) {
                            if !v.is_expired() {
                                if let RedisData::List(list) = &mut v.data {
                                    if !list.is_empty() {
                                        let mut items = Vec::new();
                                        for _ in 0..count.min(list.len()) {
                                            if let Some(item) = list.pop_back() {
                                                items.push(Value::BulkString(Some(item)));
                                            }
                                        }
                                        if !items.is_empty() {
                                            let key_str = (*key).to_string();
                                            drop(guard);
                                            crate::network::blocking::notify_waiters(&key_str);
                                            return Ok(Value::Array(vec![
                                                Value::BulkString(Some(key_str)),
                                                Value::Array(items),
                                            ]));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                _ => return Err(CommandError::SyntaxError),
            }
        }

        if deadline <= Instant::now() {
            return Ok(Value::Null);
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LMOVE / BLMOVE
// ─────────────────────────────────────────────────────────────────────────────

/// LMOVE srckey dstkey <LEFT|RIGHT> <LEFT|RIGHT>
pub fn lmove(args: &[Value]) -> CommandResult {
    if args.len() != 4 {
        return Err(CommandError::WrongNumberOfArgs("LMOVE".into()));
    }
    let src = args[0].as_str().ok_or(CommandError::WrongType)?;
    let dst = args[1].as_str().ok_or(CommandError::WrongType)?;
    let src_dir = args[2].as_str().unwrap_or("").to_uppercase();
    let dst_dir = args[3].as_str().unwrap_or("").to_uppercase();
    let from_left = src_dir == "LEFT";
    let to_left = dst_dir == "LEFT";

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let src_list = match guard.get_mut(src) {
        Some(v) if !v.is_expired() => match &mut v.data {
            RedisData::List(list) => list,
            _ => return Err(CommandError::WrongType),
        },
        _ => return Err(CommandError::Generic("no such key".into())),
    };

    let val = if from_left {
        src_list.pop_front()
    } else {
        src_list.pop_back()
    };
    let val = val.ok_or_else(|| CommandError::Generic("no such key".into()))?;

    drop(guard);

    let store2 = Storage::get();
    let mut guard2 = store2.write().unwrap();
    let dst_list = match guard2.get_mut(dst) {
        Some(v) if !v.is_expired() => match &mut v.data {
            RedisData::List(list) => list,
            _ => return Err(CommandError::WrongType),
        },
        _ => {
            guard2.insert(dst.to_string(), StoredValue::new(RedisData::List(VecDeque::new())));
            guard2.get_mut(dst).unwrap().data.as_list_mut().unwrap()
        }
    };

    if to_left {
        dst_list.push_front(val.clone());
    } else {
        dst_list.push_back(val.clone());
    }

    drop(guard2);
    crate::network::blocking::notify_waiters(src);
    crate::network::blocking::notify_waiters(dst);

    Ok(Value::BulkString(Some(val)))
}

/// BLMOVE srckey dstkey <LEFT|RIGHT> <LEFT|RIGHT> timeout
pub fn blmove(args: &[Value]) -> CommandResult {
    if args.len() != 5 {
        return Err(CommandError::WrongNumberOfArgs("BLMOVE".into()));
    }
    let src = args[0].as_str().ok_or(CommandError::WrongType)?;
    let dst = args[1].as_str().ok_or(CommandError::WrongType)?;
    let src_dir = args[2].as_str().unwrap_or("").to_uppercase();
    let dst_dir = args[3].as_str().unwrap_or("").to_uppercase();
    let timeout_sec: i64 = args[4]
        .as_int()
        .or_else(|| args[4].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let timeout = if timeout_sec < 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(timeout_sec as u64)
    };
    let from_left = src_dir == "LEFT";
    let to_left = dst_dir == "LEFT";
    let start = Instant::now();
    let deadline = start + timeout;

    loop {
        let store = crate::storage::Storage::get();
        let mut guard = store.write().unwrap();

        let src_list = match guard.get_mut(src) {
            Some(v) if !v.is_expired() => match &mut v.data {
                RedisData::List(list) => list,
                _ => return Err(CommandError::WrongType),
            },
            _ => {
                if deadline <= Instant::now() {
                    return Ok(Value::BulkString(None));
                }
                drop(guard);
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
        };

        if src_list.is_empty() {
            if deadline <= Instant::now() {
                return Ok(Value::BulkString(None));
            }
            drop(guard);
            std::thread::sleep(Duration::from_millis(50));
            continue;
        }

        let val = if from_left {
            src_list.pop_front()
        } else {
            src_list.pop_back()
        };
        let val = val.unwrap();

        let dst_list = match guard.get_mut(dst) {
            Some(v) if !v.is_expired() => match &mut v.data {
                RedisData::List(list) => list,
                _ => return Err(CommandError::WrongType),
            },
            _ => {
                guard.insert(
                    dst.to_string(),
                    StoredValue::new(RedisData::List(VecDeque::new())),
                );
                guard.get_mut(dst).unwrap().data.as_list_mut().unwrap()
            }
        };

        if to_left {
            dst_list.push_front(val.clone());
        } else {
            dst_list.push_back(val.clone());
        }

        drop(guard);
        crate::network::blocking::notify_waiters(src);
        crate::network::blocking::notify_waiters(dst);

        return Ok(Value::BulkString(Some(val)));
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// BLPOP / BRPOP / BRPOPLPUSH
// ─────────────────────────────────────────────────────────────────────────────

fn blocking_pop(args: &[Value], from_left: bool) -> CommandResult {
    // BLPOP key [key ...] timeout  (from_left=true)
    // BRPOP key [key ...] timeout  (from_left=false)
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArgs(if from_left { "BLPOP" } else { "BRPOP" }.into()));
    }
    let keys: Vec<&str> = args[..args.len() - 1]
        .iter()
        .filter_map(|v| v.as_str())
        .collect();
    let timeout_sec: i64 = args[args.len() - 1]
        .as_int()
        .or_else(|| args[args.len() - 1].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let timeout = if timeout_sec < 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(timeout_sec as u64)
    };
    let start = Instant::now();
    let deadline = start + timeout;

    loop {
        {
            let store = Storage::get();
            let mut guard = store.write().unwrap();

            for key in &keys {
                if let Some(v) = guard.get_mut(*key) {
                    if !v.is_expired() {
                        if let RedisData::List(list) = &mut v.data {
                            if !list.is_empty() {
                                let val = if from_left {
                                    list.pop_front()
                                } else {
                                    list.pop_back()
                                };
                                if let Some(val) = val {
                                    let key_str = (*key).to_string();
                                    drop(guard);
                                    crate::network::blocking::notify_waiters(&key_str);
                                    return Ok(Value::Array(vec![
                                        Value::BulkString(Some(key_str)),
                                        Value::BulkString(Some(val)),
                                    ]));
                                }
                            }
                        }
                    }
                }
            }
        }

        if deadline <= Instant::now() {
            return Ok(Value::BulkString(None));
        }
        std::thread::sleep(Duration::from_millis(50));
    }
}

pub fn blpop(args: &[Value]) -> CommandResult {
    blocking_pop(args, true)
}

pub fn brpop(args: &[Value]) -> CommandResult {
    blocking_pop(args, false)
}

pub fn brpoplpush(args: &[Value]) -> CommandResult {
    // BRPOPLPUSH source destination timeout
    if args.len() != 3 {
        return Err(CommandError::WrongNumberOfArgs("BRPOPLPUSH".into()));
    }
    let src = args[0].as_str().ok_or(CommandError::WrongType)?;
    let dst = args[1].as_str().ok_or(CommandError::WrongType)?;
    let timeout_sec: i64 = args[2]
        .as_int()
        .or_else(|| args[2].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let timeout = if timeout_sec < 0 {
        Duration::ZERO
    } else {
        Duration::from_secs(timeout_sec as u64)
    };
    let start = Instant::now();
    let deadline = start + timeout;

    loop {
        let store = crate::storage::Storage::get();
        let mut guard = store.write().unwrap();

        let val = {
            let src_list = match guard.get_mut(src) {
                Some(v) if !v.is_expired() => match &mut v.data {
                    RedisData::List(list) => list,
                    _ => return Err(CommandError::WrongType),
                },
                _ => {
                    if deadline <= Instant::now() {
                        return Ok(Value::BulkString(None));
                    }
                    drop(guard);
                    std::thread::sleep(Duration::from_millis(50));
                    continue;
                }
            };

            if src_list.is_empty() {
                if deadline <= Instant::now() {
                    return Ok(Value::BulkString(None));
                }
                drop(guard);
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }

            if src == dst {
                // Pop from right, push to left of same list (rotate)
                let val = src_list.pop_back().unwrap();
                src_list.push_front(val.clone());
                crate::network::blocking::notify_waiters(src);
                return Ok(Value::BulkString(Some(val)));
            }

            src_list.pop_back()
        };

        let val = match val {
            Some(v) => v,
            None => {
                if deadline <= Instant::now() {
                    return Ok(Value::BulkString(None));
                }
                drop(guard);
                std::thread::sleep(Duration::from_millis(50));
                continue;
            }
        };

        let dst_list = match guard.get_mut(dst) {
            Some(v) if !v.is_expired() => match &mut v.data {
                RedisData::List(list) => list,
                _ => return Err(CommandError::WrongType),
            },
            _ => {
                guard.insert(
                    dst.to_string(),
                    StoredValue::new(RedisData::List(VecDeque::new())),
                );
                guard.get_mut(dst).unwrap().data.as_list_mut().unwrap()
            }
        };

        dst_list.push_front(val.clone());

        drop(guard);
        crate::network::blocking::notify_waiters(src);
        crate::network::blocking::notify_waiters(dst);

        return Ok(Value::BulkString(Some(val)));
    }
}
