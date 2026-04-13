//! Generic key commands: DEL, EXISTS, TYPE, PING, ECHO, SCAN, etc.

use base64::Engine as _;
use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
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

pub fn pexpire(args: &[Value]) -> CommandResult {
    check_args(args, 2, "PEXPIRE")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let ms: i64 = args[1]
        .as_int()
        .or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    if let Some(v) = guard.get_mut(key) {
        if v.is_expired() {
            return Ok(Value::Integer(0));
        }
        let now = current_time_ms();
        v.expire_at = Some(now + ms);
        Ok(Value::Integer(1))
    } else {
        Ok(Value::Integer(0))
    }
}

pub fn pexpireat(args: &[Value]) -> CommandResult {
    check_args(args, 2, "PEXPIREAT")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let ts_ms: i64 = args[1]
        .as_int()
        .or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    if let Some(v) = guard.get_mut(key) {
        if v.is_expired() {
            return Ok(Value::Integer(0));
        }
        v.expire_at = Some(ts_ms);
        Ok(Value::Integer(1))
    } else {
        Ok(Value::Integer(0))
    }
}

pub fn rename(args: &[Value]) -> CommandResult {
    check_args(args, 2, "RENAME")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let new_key = args[1].as_str().ok_or(CommandError::WrongType)?;
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            let value = v.data.clone();
            let expire_at = v.expire_at;
            guard.remove(key);
            guard.insert(new_key.to_string(), StoredValue::with_expiry(value, expire_at));
            Ok(Value::SimpleString("OK".to_string()))
        }
        _ => Err(CommandError::Generic("no such key".to_string())),
    }
}

pub fn renamenx(args: &[Value]) -> CommandResult {
    check_args(args, 2, "RENAMENX")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let new_key = args[1].as_str().ok_or(CommandError::WrongType)?;
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    if guard.contains_key(new_key) {
        let exists = guard
            .get(new_key)
            .map(|v| !v.is_expired())
            .unwrap_or(false);
        if exists {
            return Ok(Value::Integer(0));
        }
    }
    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            let value = v.data.clone();
            let expire_at = v.expire_at;
            guard.remove(key);
            guard.insert(new_key.to_string(), StoredValue::with_expiry(value, expire_at));
            Ok(Value::Integer(1))
        }
        _ => Err(CommandError::Generic("no such key".to_string())),
    }
}

pub fn copy(args: &[Value]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArgs("COPY".into()));
    }
    let source = args[0].as_str().ok_or(CommandError::WrongType)?;
    let dest = args[1].as_str().ok_or(CommandError::WrongType)?;
    let mut replace = false;
    let mut i = 2;
    while i < args.len() {
        if args[i].as_str() == Some("REPLACE") {
            replace = true;
        }
        i += 1;
    }
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    let src_val = match guard.get(source) {
        Some(v) if !v.is_expired() => v.data.clone(),
        _ => return Ok(Value::Integer(0)),
    };
    let src_expire = guard.get(source).and_then(|v| v.expire_at);
    let dest_exists = guard
        .get(dest)
        .map(|v| !v.is_expired())
        .unwrap_or(false);
    if dest_exists && !replace {
        return Ok(Value::Integer(0));
    }
    if dest_exists {
        guard.remove(dest);
    }
    guard.insert(dest.to_string(), StoredValue::with_expiry(src_val, src_expire));
    Ok(Value::Integer(1))
}

pub fn msetnx(args: &[Value]) -> CommandResult {
    // MSETNX: Set multiple keys if none exist. Returns 1 if all keys were set, 0 if no key was set.
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("MSETNX".into()));
    }
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();

    // Parse key-value pairs
    let pairs: Vec<(&str, &str)> = args
        .chunks(2)
        .filter_map(|chunk| {
            if chunk.len() == 2 {
                Some((chunk[0].as_str()?, chunk[1].as_str()?))
            } else {
                None
            }
        })
        .collect();

    // Check if any key already exists
    for (key, _) in &pairs {
        let exists = guard
            .get(*key)
            .map(|v| !v.is_expired())
            .unwrap_or(false);
        if exists {
            return Ok(Value::Integer(0));
        }
    }

    // All keys are non-existent, so set them all
    for (key, value) in pairs {
        guard.insert(key.to_string(), StoredValue::new(RedisData::String(value.to_string())));
    }
    Ok(Value::Integer(1))
}

pub fn touch(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("TOUCH".into()));
    }
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    let mut count = 0;
    for arg in args {
        if let Some(key) = arg.as_str() {
            if guard.get(key).map(|v| !v.is_expired()).unwrap_or(false) {
                count += 1;
            }
        }
    }
    Ok(Value::Integer(count))
}

pub fn r#move(args: &[Value]) -> CommandResult {
    check_args(args, 2, "MOVE")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let dest_db = args[1]
        .as_int()
        .or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)? as usize;
    if dest_db >= 16 {
        return Err(CommandError::Generic("ERR DB index out of range".into()));
    }

    if crate::storage::Storage::move_key(key, dest_db).is_some() {
        Ok(Value::Integer(1))
    } else {
        Ok(Value::Integer(0))
    }
}

pub fn swapdb(args: &[Value]) -> CommandResult {
    check_args(args, 2, "SWAPDB")?;
    let db1 = args[0]
        .as_int()
        .or_else(|| args[0].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)? as usize;
    let db2 = args[1]
        .as_int()
        .or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)? as usize;
    if db1 >= 16 || db2 >= 16 {
        return Err(CommandError::Generic("ERR DB index out of range".into()));
    }
    crate::storage::Storage::swapdb(db1, db2);
    Ok(Value::SimpleString("OK".to_string()))
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
    let idx: i64 = args[0]
        .as_int()
        .or_else(|| args[0].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    if !(0..=15).contains(&idx) {
        return Err(CommandError::Generic("ERR DB index out of range".into()));
    }
    crate::storage::Storage::select(idx as usize);
    Ok(Value::SimpleString("OK".to_string()))
}

pub fn flushdb(_args: &[Value]) -> CommandResult {
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    guard.clear(); // FLUSHDB removes ALL keys from the current DB
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

    // Parse optional MATCH pattern and COUNT
    let mut pattern: Option<&str> = None;
    let mut count: usize = 10;
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            Some("MATCH") if i + 1 < args.len() => {
                pattern = args[i + 1].as_str();
                i += 2;
            }
            Some("COUNT") if i + 1 < args.len() => {
                count = args[i + 1].as_str()
                    .and_then(|s: &str| s.parse().ok())
                    .unwrap_or(10);
                i += 2;
            }
            _ => i += 1,
        }
    }

    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    use crate::storage::memory::glob_match;
    let all_keys: Vec<String> = guard.iter()
        .filter(|(_, v)| !v.is_expired())
        .map(|(k, _)| k.clone())
        .collect();
    drop(guard);

    // Filter by pattern if provided
    let filtered_keys: Vec<&String> = match pattern {
        Some(p) => all_keys.iter().filter(|k| glob_match(p, k)).collect(),
        None => all_keys.iter().collect(),
    };

    let end = (cursor + count).min(filtered_keys.len());
    let keys: Vec<Value> = filtered_keys[cursor..end]
        .iter().map(|k| Value::BulkString(Some((*k).clone()))).collect();
    let next = if end >= filtered_keys.len() { 0 } else { end };
    Ok(Value::Array(vec![
        Value::BulkString(Some(next.to_string())),
        Value::Array(keys),
    ]))
}

fn check_args(args: &[Value], expected: usize, cmd: &str) -> CommandResult {
    if args.len() != expected { Err(CommandError::WrongNumberOfArgs(cmd.into())) } else { Ok(Value::Null) }
}

// ---------------------------------------------------------------------------
// DUMP key
//
// Serializes the value at <key> using serde_json, then base64-encodes the
// resulting bytes and returns them as a BulkString.  Returns a Null bulk
// string when the key does not exist.
// ---------------------------------------------------------------------------
pub fn dump(args: &[Value]) -> CommandResult {
    check_args(args, 1, "DUMP")?;
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            let json_bytes = serde_json::to_vec(&v.data)
                .map_err(|e| CommandError::Generic(format!("DUMP serialize error: {}", e)))?;
            let encoded = base64::engine::general_purpose::STANDARD.encode(&json_bytes);
            Ok(Value::BulkString(Some(encoded)))
        }
        _ => Ok(Value::BulkString(None)),
    }
}

// ---------------------------------------------------------------------------
// RESTORE key ttl serialized-value [REPLACE]
//
// Deserializes a value produced by DUMP and stores it at <key>.
// <ttl> is in milliseconds; 0 means no expiry.
// The optional REPLACE flag allows overwriting an existing key.
// ---------------------------------------------------------------------------
pub fn restore(args: &[Value]) -> CommandResult {
    if args.len() < 3 {
        return Err(CommandError::WrongNumberOfArgs("RESTORE".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let ttl_ms: i64 = args[1].as_int()
        .or_else(|| args[1].as_str().and_then(|s: &str| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let serialized = args[2].as_str()
        .ok_or_else(|| CommandError::Generic("RESTORE requires a bulk-string payload".into()))?;

    // Parse optional REPLACE flag
    let mut replace = false;
    for arg in args.iter().skip(3) {
        if arg.as_str().map(|s| s.eq_ignore_ascii_case("REPLACE")).unwrap_or(false) {
            replace = true;
        }
    }

    // Decode base64 -> JSON bytes -> RedisData
    let json_bytes = base64::engine::general_purpose::STANDARD
        .decode(serialized)
        .map_err(|e| CommandError::Generic(format!("RESTORE base64 decode error: {}", e)))?;
    let data: RedisData = serde_json::from_slice(&json_bytes)
        .map_err(|e| CommandError::Generic(format!("RESTORE deserialize error: {}", e)))?;

    let expire_at: Option<i64> = if ttl_ms > 0 {
        Some(current_time_ms() + ttl_ms)
    } else {
        None
    };

    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();

    // Check for existing key
    if guard.get(key).map(|v| !v.is_expired()).unwrap_or(false) && !replace {
        return Err(CommandError::Generic(
            "BUSYKEY Target key name already exists.".into(),
        ));
    }

    guard.insert(key.to_string(), StoredValue::with_expiry(data, expire_at));
    Ok(Value::SimpleString("OK".to_string()))
}

// ---------------------------------------------------------------------------
// SORT key [ASC|DESC] [ALPHA] [LIMIT offset count]
//
// Works on list, set, and string types.
// - For lists and sets: collect all elements and sort them.
// - For strings: treat the value as a single element (return it as-is).
// - Default sort is numeric.  Use ALPHA for lexicographic order.
// ---------------------------------------------------------------------------
pub fn sort(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("SORT".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;

    // Parse options
    let mut alpha = false;
    let mut descending = false;
    let mut limit_offset: usize = 0;
    let mut limit_count: Option<usize> = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str().unwrap_or("").to_uppercase().as_str() {
            "ALPHA" => { alpha = true; i += 1; }
            "ASC"   => { descending = false; i += 1; }
            "DESC"  => { descending = true; i += 1; }
            "LIMIT" if i + 2 < args.len() => {
                limit_offset = args[i + 1].as_int()
                    .or_else(|| args[i + 1].as_str().and_then(|s: &str| s.parse().ok()))
                    .unwrap_or(0) as usize;
                limit_count = Some(
                    args[i + 2].as_int()
                        .or_else(|| args[i + 2].as_str().and_then(|s: &str| s.parse().ok()))
                        .unwrap_or(-1) as usize,
                );
                i += 3;
            }
            _ => { i += 1; }
        }
    }

    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();

    let mut elements: Vec<String> = match guard.get(key) {
        None => return Ok(Value::Array(vec![])),
        Some(v) if v.is_expired() => return Ok(Value::Array(vec![])),
        Some(v) => match &v.data {
            RedisData::List(list) => list.iter().cloned().collect(),
            RedisData::Set(set) => set.to_vec(),
            RedisData::String(s) => vec![s.clone()],
            _ => return Err(CommandError::WrongType),
        },
    };

    // Sort
    if alpha {
        elements.sort_by(|a, b| {
            if descending { b.cmp(a) } else { a.cmp(b) }
        });
    } else {
        // Numeric sort — elements that cannot be parsed as f64 produce an error
        let mut numeric: Vec<(f64, String)> = elements
            .iter()
            .map(|s| {
                s.parse::<f64>()
                    .map(|n| (n, s.clone()))
                    .map_err(|_| CommandError::Generic(
                        format!("ERR One or more scores can't be converted into double: '{}'", s)
                    ))
            })
            .collect::<Result<Vec<_>, _>>()?;

        numeric.sort_by(|(a, _), (b, _)| {
            let ord = a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal);
            if descending { ord.reverse() } else { ord }
        });
        elements = numeric.into_iter().map(|(_, s)| s).collect();
    }

    // Apply LIMIT
    if limit_offset < elements.len() {
        elements = elements.into_iter().skip(limit_offset).collect();
    } else {
        elements = vec![];
    }
    if let Some(cnt) = limit_count {
        elements.truncate(cnt);
    }

    let result: Vec<Value> = elements.into_iter()
        .map(|s| Value::BulkString(Some(s)))
        .collect();
    Ok(Value::Array(result))
}
