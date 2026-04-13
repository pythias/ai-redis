//! Bitmap Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
use crate::storage::Storage;

pub fn setbit(args: &[Value]) -> CommandResult {
    if args.len() != 3 { return Err(CommandError::WrongNumberOfArgs("SETBIT".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let offset: usize = args[1].as_int()
        .or_else(|| args[1].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)? as usize;
    let bit: i64 = args[2].as_int()
        .or_else(|| args[2].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;

    if bit != 0 && bit != 1 {
        return Err(CommandError::Generic("bit is out of range".to_string()));
    }

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    let byte_offset = offset / 8;
    let bit_offset = offset % 8;

    let old_val = match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::String(s) => {
                    let old_byte = s.as_bytes().get(byte_offset).copied().unwrap_or(0);
                    let old_bit = (old_byte >> (7 - bit_offset)) & 1;

                    let mut new_bytes = s.as_bytes().to_vec();
                    if byte_offset >= new_bytes.len() {
                        new_bytes.resize(byte_offset + 1, 0);
                    }
                    if bit == 1 {
                        new_bytes[byte_offset] |= 1 << (7 - bit_offset);
                    } else {
                        new_bytes[byte_offset] &= !(1 << (7 - bit_offset));
                    }
                    *s = String::from_utf8_lossy(&new_bytes).to_string();
                    old_bit
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => {
            let mut new_bytes = vec![0u8; byte_offset + 1];
            if bit == 1 {
                new_bytes[byte_offset] |= 1 << (7 - bit_offset);
            }
            guard.insert(key.to_string(), StoredValue::new(RedisData::String(String::from_utf8_lossy(&new_bytes).to_string())));
            0
        }
    };

    Ok(Value::Integer(old_val as i64))
}

pub fn getbit(args: &[Value]) -> CommandResult {
    if args.len() != 2 { return Err(CommandError::WrongNumberOfArgs("GETBIT".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let offset: usize = args[1].as_int()
        .or_else(|| args[1].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)? as usize;

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::String(s) => {
                    let bytes = s.as_bytes();
                    let byte_offset = offset / 8;
                    let bit_offset = offset % 8;
                    
                    if byte_offset >= bytes.len() {
                        return Ok(Value::Integer(0));
                    }
                    let bit = (bytes[byte_offset] >> (7 - bit_offset)) & 1;
                    Ok(Value::Integer(bit as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn bitcount(args: &[Value]) -> CommandResult {
    if args.is_empty() { return Err(CommandError::WrongNumberOfArgs("BITCOUNT".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    
    let (start, end) = if args.len() >= 3 {
        let start: i64 = args[1].as_int().or_else(|| args[1].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
        let end: i64 = args[2].as_int().or_else(|| args[2].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
        (Some(start), Some(end))
    } else {
        (None, None)
    };

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::String(s) => {
                    let bytes = s.as_bytes();
                    let byte_count = bytes.len();
                    
                    let (start_byte, end_byte) = match (start, end) {
                        (Some(s), Some(e)) => {
                            let s = if s < 0 { (byte_count as i64 + s).max(0) as usize } else { s as usize };
                            let e = if e < 0 { (byte_count as i64 + e).max(0) as usize } else { e as usize };
                            (s.min(byte_count), e.min(byte_count - 1))
                        }
                        _ => (0, byte_count.saturating_sub(1)),
                    };
                    
                    let mut count = 0;
                    for i in start_byte..=end_byte {
                        if i < bytes.len() {
                            count += bytes[i].count_ones() as i64;
                        }
                    }
                    Ok(Value::Integer(count))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

pub fn bitop(args: &[Value]) -> CommandResult {
    if args.len() < 3 { return Err(CommandError::WrongNumberOfArgs("BITOP".into())); }
    let op = args[0].as_str().ok_or(CommandError::WrongType)?.to_uppercase();
    let destkey = args[1].as_str().ok_or(CommandError::WrongType)?;
    let keys: Vec<&str> = args[2..].iter().filter_map(|v| v.as_str()).collect();

    let op_type = match op.as_str() {
        "AND" => BitOp::And,
        "OR" => BitOp::Or,
        "XOR" => BitOp::Xor,
        "NOT" => BitOp::Not,
        _ => return Err(CommandError::Generic("BITOP operation must be AND, OR, XOR or NOT".to_string())),
    };

    if op_type == BitOp::Not && keys.len() != 1 {
        return Err(CommandError::Generic("BITOP NOT must have exactly one source key".to_string()));
    }

    let store = Storage::get();
    let guard = store.read().unwrap();

    let mut result_bytes: Vec<u8> = Vec::new();

    for (i, key) in keys.iter().enumerate() {
        match guard.get(*key) {
            Some(v) if !v.is_expired() => {
                match &v.data {
                    RedisData::String(s) => {
                        let bytes = s.as_bytes();
                        if i == 0 {
                            result_bytes = bytes.to_vec();
                        } else {
                            match op_type {
                                BitOp::And => {
                                    for (j, &b) in bytes.iter().enumerate() {
                                        if j < result_bytes.len() {
                                            result_bytes[j] &= b;
                                        }
                                    }
                                }
                                BitOp::Or => {
                                    for (j, &b) in bytes.iter().enumerate() {
                                        if j < result_bytes.len() {
                                            result_bytes[j] |= b;
                                        } else {
                                            result_bytes.push(b);
                                        }
                                    }
                                }
                                BitOp::Xor => {
                                    for (j, &b) in bytes.iter().enumerate() {
                                        if j < result_bytes.len() {
                                            result_bytes[j] ^= b;
                                        } else {
                                            result_bytes.push(b);
                                        }
                                    }
                                }
                                BitOp::Not => {} // handled below
                            }
                        }
                    }
                    _ => return Err(CommandError::WrongType),
                }
            }
            _ => {
                if op_type == BitOp::And {
                    result_bytes.clear();
                }
            }
        }
    }

    if op_type == BitOp::Not {
        // NOT flips all bits in the source
        if let Some(&key) = keys.first() {
            if let Some(v) = guard.get(key) {
                if let RedisData::String(s) = &v.data {
                    result_bytes = s.as_bytes().iter().map(|b| !b).collect();
                }
            }
        }
    }

    drop(guard);

    let mut guard = store.write().unwrap();
    guard.insert(destkey.to_string(), StoredValue::new(RedisData::String(String::from_utf8_lossy(&result_bytes).to_string())));

    Ok(Value::Integer(result_bytes.len() as i64))
}

#[derive(PartialEq)]
enum BitOp {
    And,
    Or,
    Xor,
    Not,
}

pub fn bitpos(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("BITPOS".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let bit: i64 = args[1].as_int()
        .or_else(|| args[1].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;

    if bit != 0 && bit != 1 {
        return Err(CommandError::Generic("bit out of range".to_string()));
    }

    let (start, end) = if args.len() >= 4 {
        let start: i64 = args[2].as_int().or_else(|| args[2].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
        let end: i64 = args[3].as_int().or_else(|| args[3].as_str().and_then(|s| s.parse().ok())).ok_or(CommandError::InvalidInt)?;
        (Some(start as usize), Some(end as usize))
    } else {
        (None, None)
    };

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::String(s) => {
                    let bytes = s.as_bytes();
                    let len = bytes.len();
                    
                    let (start_byte, end_byte) = match (start, end) {
                        (Some(s), Some(e)) => (s, e.min(len.saturating_sub(1))),
                        _ => (0, len.saturating_sub(1)),
                    };
                    
                    if bit == 0 {
                        // Find first 0 bit
                        for byte_idx in start_byte..=end_byte {
                            let byte = bytes.get(byte_idx).copied().unwrap_or(0);
                            for bit_idx in 0..8 {
                                if (byte >> (7 - bit_idx)) & 1 == 0 {
                                    return Ok(Value::Integer((byte_idx * 8 + bit_idx) as i64));
                                }
                            }
                        }
                        Ok(Value::Integer(-1))
                    } else {
                        // Find first 1 bit
                        for byte_idx in start_byte..=end_byte {
                            let byte = bytes.get(byte_idx).copied().unwrap_or(0);
                            for bit_idx in 0..8 {
                                if (byte >> (7 - bit_idx)) & 1 == 1 {
                                    return Ok(Value::Integer((byte_idx * 8 + bit_idx) as i64));
                                }
                            }
                        }
                        // Check if we need to expand
                        if end_byte >= len {
                            return Ok(Value::Integer(-1));
                        }
                        // Bits beyond the string are 0, so return len * 8
                        Ok(Value::Integer((len * 8) as i64))
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => {
            if bit == 0 {
                Ok(Value::Integer(0))
            } else {
                Ok(Value::Integer(-1))
            }
        }
    }
}
