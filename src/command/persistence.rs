#![allow(dead_code)]

//! Persistence commands: SAVE, BGSAVE, LASTSAVE, BGREWRITEAOF, SHUTDOWN

use std::io::Write as IoWrite;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{fs, thread};

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::memory::DATABASES;

/// Tracks background save state.
static BG_SAVE_IN_PROGRESS: AtomicBool = AtomicBool::new(false);
static BG_SAVE_START_TIME: AtomicI64 = AtomicI64::new(0);
static BG_SAVE_COMPLETE_TIME: AtomicI64 = AtomicI64::new(0);

/// Default RDB directory and filename
fn default_rdb_path() -> PathBuf {
    PathBuf::from("dump.rdb")
}

/// Write the full RDB file for all databases.
fn write_rdb_file(path: &PathBuf) -> std::io::Result<()> {
    let mut file = fs::File::create(path)?;
    // Magic + version
    file.write_all(b"REDIS")?;
    file.write_all(&10u16.to_be_bytes())?;

    // Auxiliary fields
    file.write_all(&[0xFA])?;
    encode_string_to_writer(&mut file, "redis-ver")?;
    encode_string_to_writer(&mut file, env!("CARGO_PKG_VERSION"))?;
    encode_string_to_writer(&mut file, "redis-bits")?;
    encode_string_to_writer(&mut file, "64")?;
    let ctime_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;
    encode_string_to_writer(&mut file, "ctime")?;
    encode_string_to_writer(&mut file, &ctime_ms.to_string())?;

    // Write all non-empty databases
    for db_idx in 0..16 {
        let arc = DATABASES[db_idx]
            .get_or_init(|| Arc::new(std::sync::RwLock::new(std::collections::HashMap::new())))
            .clone();
        let guard = arc.read().unwrap();
        let has_keys = guard.iter().filter(|(_, v)| !v.is_expired()).count();
        if has_keys == 0 {
            continue;
        }
        // Database selector
        file.write_all(&[0xFE])?;
        encode_length_to_writer(&mut file, db_idx as u64)?;

        for (key, value) in guard.iter().filter(|(_, v)| !v.is_expired()) {
            if let Some(expire_at) = value.expire_at {
                file.write_all(&[0xFC])?;
                file.write_all(&expire_at.to_be_bytes())?;
            }
            encode_string_to_writer(&mut file, key)?;
            encode_redis_data_to_writer(&mut file, &value.data)?;
        }
    }

    // EOF + checksum placeholder
    file.write_all(&[0xFF, 0, 0, 0, 0, 0, 0, 0, 0, 0])?;
    file.flush()?;
    Ok(())
}

fn encode_string_to_writer<W: std::io::Write>(w: &mut W, s: &str) -> std::io::Result<()> {
    encode_length_to_writer(w, s.len() as u64)?;
    w.write_all(s.as_bytes())
}

fn encode_length_to_writer<W: std::io::Write>(w: &mut W, len: u64) -> std::io::Result<()> {
    if len < 64 {
        w.write_all(&[len as u8])
    } else if len < 16384 {
        let bits = (len << 2) | 0b01;
        w.write_all(&[0xC0 | ((bits >> 8) as u8), bits as u8])
    } else {
        let bits = (len << 2) | 0b10;
        w.write_all(&[
            0xC0 | ((bits >> 24) as u8),
            (bits >> 16) as u8,
            (bits >> 8) as u8,
            bits as u8,
        ])
    }
}

fn encode_redis_data_to_writer<W: std::io::Write>(
    w: &mut W,
    data: &crate::storage::data::RedisData,
) -> std::io::Result<()> {
    match data {
        crate::storage::data::RedisData::String(s) => {
            w.write_all(&[0])?;
            encode_string_to_writer(w, s)?;
        }
        crate::storage::data::RedisData::List(items) => {
            w.write_all(&[1])?;
            encode_length_to_writer(w, items.len() as u64)?;
            for item in items {
                encode_string_to_writer(w, item)?;
            }
        }
        crate::storage::data::RedisData::Hash(map) => {
            w.write_all(&[2])?;
            encode_length_to_writer(w, map.len() as u64)?;
            for (k, v) in map.iter() {
                encode_string_to_writer(w, k)?;
                encode_string_to_writer(w, v)?;
            }
        }
        crate::storage::data::RedisData::Set(items) => {
            w.write_all(&[3])?;
            encode_length_to_writer(w, items.len() as u64)?;
            for item in items {
                encode_string_to_writer(w, item)?;
            }
        }
        crate::storage::data::RedisData::SortedSet(zmap) => {
            w.write_all(&[4])?;
            encode_length_to_writer(w, zmap.len() as u64)?;
            for (member, score) in zmap.iter() {
                encode_string_to_writer(w, member)?;
                let mut ryu_buf = ryu::Buffer::new();
                let score_str = ryu_buf.format(*score);
                encode_string_to_writer(w, score_str)?;
            }
        }
        crate::storage::data::RedisData::Stream(stream) => {
            w.write_all(&[5])?;
            encode_length_to_writer(w, stream.len() as u64)?;
            for (id, entry) in stream.iter() {
                encode_string_to_writer(w, id)?;
                encode_length_to_writer(w, entry.fields.len() as u64)?;
                for (f, v) in &entry.fields {
                    encode_string_to_writer(w, f)?;
                    encode_string_to_writer(w, v)?;
                }
            }
        }
    }
    Ok(())
}

// ─── Commands ─────────────────────────────────────────────────────────────────

pub fn save(_args: &[Value]) -> CommandResult {
    let path = default_rdb_path();
    write_rdb_file(&path).map_err(|e| {
        CommandError::Generic(format!("ERR failed to save: {}", e))
    })?;
    BG_SAVE_COMPLETE_TIME.store(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        Ordering::SeqCst,
    );
    Ok(Value::SimpleString("OK".to_string()))
}

pub fn bgsave(args: &[Value]) -> CommandResult {
    if !args.is_empty() && args[0].as_str() == Some("LAzyfree") {
        // Async cleanup — no-op for now
    }

    if BG_SAVE_IN_PROGRESS.load(Ordering::SeqCst) {
        return Err(CommandError::Generic("ERR BGSAVE is already in progress".into()));
    }

    let path = default_rdb_path();
    BG_SAVE_IN_PROGRESS.store(true, Ordering::SeqCst);
    BG_SAVE_START_TIME.store(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        Ordering::SeqCst,
    );

    thread::spawn(move || {
        if let Err(e) = write_rdb_file(&path) {
            log::error!("BGSAVE failed: {}", e);
        }
        BG_SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
        BG_SAVE_COMPLETE_TIME.store(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_secs() as i64,
            Ordering::SeqCst,
        );
    });

    Ok(Value::SimpleString("Background saving started".to_string()))
}

pub fn lastsave(_args: &[Value]) -> CommandResult {
    let ts = BG_SAVE_COMPLETE_TIME.load(Ordering::SeqCst);
    // If never saved, return 0
    Ok(Value::Integer(ts))
}

pub fn bgrewriteaof(_args: &[Value]) -> CommandResult {
    // AOF rewrite — in Redis 7 this is integrated with the fork child.
    // For simplicity, just trigger a background save as the AOF equivalent.
    if BG_SAVE_IN_PROGRESS.load(Ordering::SeqCst) {
        return Err(CommandError::Generic(
            "ERR Background append only file rewriting already in progress".into(),
        ));
    }
    let path = PathBuf::from("dump.rdb");
    BG_SAVE_IN_PROGRESS.store(true, Ordering::SeqCst);
    thread::spawn(move || {
        if let Err(e) = write_rdb_file(&path) {
            log::error!("BGREWRITEAOF failed: {}", e);
        }
        BG_SAVE_IN_PROGRESS.store(false, Ordering::SeqCst);
    });
    Ok(Value::SimpleString(
        "Background append only file rewriting started".to_string(),
    ))
}

/// Perform a synchronous save. Used by SHUTDOWN.
pub fn do_save(path: &PathBuf) -> std::io::Result<()> {
    write_rdb_file(path)?;
    BG_SAVE_COMPLETE_TIME.store(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64,
        Ordering::SeqCst,
    );
    Ok(())
}
