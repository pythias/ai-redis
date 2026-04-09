//! HyperLogLog Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
use crate::storage::Storage;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

pub fn pfadd(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("PFADD".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let elements: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::String(s) => {
                    // HyperLogLog stored as string (raw bytes)
                    let mut hll = s.clone();
                    for elem in &elements {
                        hll.push_str(elem);
                        hll.push('\x00'); // separator
                    }
                    v.data = RedisData::String(hll);
                    Ok(Value::Integer(1))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => {
            let mut hll = String::new();
            for elem in &elements {
                hll.push_str(elem);
                hll.push('\x00');
            }
            guard.insert(key.to_string(), StoredValue::new(RedisData::String(hll)));
            Ok(Value::Integer(1))
        }
    }
}

pub fn pfcount(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("PFCOUNT".into())); }
    let keys: Vec<&str> = args.iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let guard = store.read().unwrap();

    let mut total = 0i64;
    for key in &keys {
        match guard.get(*key) {
            Some(v) if !v.is_expired() => {
                match &v.data {
                    RedisData::String(s) => {
                        // Approximate count based on hash
                        total += estimate_hll_count(s);
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
            _ => {}
        }
    }

    // HyperLogLog estimation: m * log(m / (m - unique_count))
    // Simplified approximation
    Ok(Value::Integer(total.max(1)))
}

pub fn pfmerge(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("PFMERGE".into())); }
    let destkey = args[0].as_str().ok_or(CommandError::WrongType)?;
    let sourcekeys: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let mut merged = String::new();
    for key in &sourcekeys {
        match guard.get(*key) {
            Some(v) if !v.is_expired() => {
                match &v.data {
                    RedisData::String(s) => {
                        merged.push_str(s);
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
            _ => {}
        }
    }

    guard.insert(destkey.to_string(), StoredValue::new(RedisData::String(merged)));
    Ok(Value::SimpleString("OK".to_string()))
}

fn estimate_hll_count(data: &str) -> i64 {
    // Simplified HyperLogLog-style estimation
    let mut hasher = DefaultHasher::new();
    data.hash(&mut hasher);
    let hash = hasher.finish();
    
    // Count leading zeros (simplified)
    let leading_zeros = hash.leading_zeros() as i64;
    let estimate = 1i64 << leading_zeros.min(12);
    
    estimate.max(1).min(1000000)
}
