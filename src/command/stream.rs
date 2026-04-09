//! Stream Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue, StreamEntry};
use crate::storage::Storage;
use std::collections::HashMap;

pub fn xadd(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("XADD".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let id = args[1].as_str().ok_or(CommandError::WrongType)?;
    
    // Parse fields: field1 value1 field2 value2 ...
    let fields = parse_pairs(&args[2..])?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let stream_entry = StreamEntry {
        fields,
        id: id.to_string(),
    };

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Stream(s) => {
                    s.insert(id.to_string(), stream_entry);
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => {
            let mut s = HashMap::new();
            s.insert(id.to_string(), stream_entry);
            guard.insert(key.to_string(), StoredValue::new(RedisData::Stream(s)));
        }
    }

    Ok(Value::BulkString(Some(id.to_string())))
}

pub fn xlen(args: &[Value]) -> CommandResult {
    if args.len() != 1 { return Err(CommandError::WrongNumberOfArgs("XLEN".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Stream(s) => Ok(Value::Integer(s.len() as i64)),
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn xrange(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("XRANGE".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let start = args[1].as_str().ok_or(CommandError::WrongType)?;
    let end = args[2].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Stream(s) => {
                    let mut entries: Vec<Value> = Vec::new();
                    let mut ids: Vec<&String> = s.keys().collect();
                    ids.sort();
                    
                    for id in ids {
                        if id.as_str() >= start && id.as_str() <= end {
                            if let Some(entry) = s.get(id) {
                                let mut items = Vec::new();
                                items.push(Value::BulkString(Some(id.clone())));
                                
                                let mut fields = Vec::new();
                                for (k, val) in &entry.fields {
                                    fields.push(Value::Array(vec![
                                        Value::BulkString(Some(k.clone())),
                                        Value::BulkString(Some(val.clone())),
                                    ]));
                                }
                                items.push(Value::Array(fields));
                                entries.push(Value::Array(items));
                            }
                        }
                    }
                    Ok(Value::Array(entries))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn xrevrange(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("XREVRANGE".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let end = args[1].as_str().ok_or(CommandError::WrongType)?;
    let start = args[2].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Stream(s) => {
                    let mut entries: Vec<Value> = Vec::new();
                    let mut ids: Vec<&String> = s.keys().collect();
                    ids.sort();
                    ids.reverse();
                    
                    for id in ids {
                        if id.as_str() >= start && id.as_str() <= end {
                            if let Some(entry) = s.get(id) {
                                let mut items = Vec::new();
                                items.push(Value::BulkString(Some(id.clone())));
                                
                                let mut fields = Vec::new();
                                for (k, val) in &entry.fields {
                                    fields.push(Value::Array(vec![
                                        Value::BulkString(Some(k.clone())),
                                        Value::BulkString(Some(val.clone())),
                                    ]));
                                }
                                items.push(Value::Array(fields));
                                entries.push(Value::Array(items));
                            }
                        }
                    }
                    Ok(Value::Array(entries))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn xread(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("XREAD".into())); }
    
    // Parse streams: key1 id1 key2 id2 ...
    let streams = parse_stream_args(&args[1..])?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    let mut results: Vec<Value> = Vec::new();
    for (key, id) in streams {
        match guard.get(key) {
            Some(v) if !v.is_expired() => {
                match &v.data {
                    RedisData::Stream(s) => {
                        let mut entries: Vec<Value> = Vec::new();
                        let mut ids: Vec<&String> = s.keys().collect();
                        ids.sort();
                        
                        for eid in ids {
                            if eid.as_str() > id {
                                if let Some(entry) = s.get(eid) {
                                    let mut fields = Vec::new();
                                    for (k, val) in &entry.fields {
                                        fields.push(Value::Array(vec![
                                            Value::BulkString(Some(k.clone())),
                                            Value::BulkString(Some(val.clone())),
                                        ]));
                                    }
                                    entries.push(Value::Array(vec![
                                        Value::BulkString(Some(eid.clone())),
                                        Value::Array(fields),
                                    ]));
                                }
                            }
                        }
                        if !entries.is_empty() {
                            results.push(Value::Array(vec![
                                Value::BulkString(Some(key.to_string())),
                                Value::Array(entries),
                            ]));
                        }
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
            _ => {}
        }
    }

    Ok(Value::Array(results))
}

pub fn xgroup_create(_args: &[Value]) -> CommandResult {
    // Simplified: just acknowledge the command
    Ok(Value::SimpleString("OK".to_string()))
}

pub fn xinfo_stream(args: &[Value]) -> CommandResult {
    if args.len() < 1 { return Err(CommandError::WrongNumberOfArgs("XINFO STREAM".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Stream(s) => {
                    let mut ids: Vec<&String> = s.keys().collect();
                    ids.sort();
                    
                    let first = ids.first().map(|id| id.clone());
                    let last = ids.last().map(|id| id.clone());
                    
                    let info = vec![
                        Value::Array(vec![
                            Value::BulkString(Some("length".to_string())),
                            Value::Integer(s.len() as i64),
                        ]),
                        Value::Array(vec![
                            Value::BulkString(Some("first-entry".to_string())),
                            Value::BulkString(first.cloned()),
                        ]),
                        Value::Array(vec![
                            Value::BulkString(Some("last-entry".to_string())),
                            Value::BulkString(last.cloned()),
                        ]),
                    ];
                    Ok(Value::Array(info))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

pub fn xdel(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("XDEL".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let ids: Vec<String> = args[1..].iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Stream(s) => {
                    let before = s.len();
                    for id in &ids {
                        s.remove(id);
                    }
                    return Ok(Value::Integer((before - s.len()) as i64));
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => {}
    }
    Ok(Value::Integer(0))
}

fn parse_pairs(args: &[Value]) -> Result<HashMap<String, String>, CommandError> {
    let mut pairs = HashMap::new();
    let mut i = 0;
    while i + 1 < args.len() {
        let field = args[i].as_str().ok_or(CommandError::WrongType)?;
        let value = args[i + 1].as_str().ok_or(CommandError::WrongType)?;
        pairs.insert(field.to_string(), value.to_string());
        i += 2;
    }
    Ok(pairs)
}

fn parse_stream_args(args: &[Value]) -> Result<Vec<(&str, &str)>, CommandError> {
    let mut streams = Vec::new();
    let mut i = 0;
    while i + 1 < args.len() {
        let key = args[i].as_str().ok_or(CommandError::WrongType)?;
        let id = args[i + 1].as_str().ok_or(CommandError::WrongType)?;
        streams.push((key, id));
        i += 2;
    }
    Ok(streams)
}
