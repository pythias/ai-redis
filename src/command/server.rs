//! Server commands: SHUTDOWN, ROLE, INFO, TIME, MIGRATE, FLUSHALL, CLIENT

use std::io::Write as IoWrite;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::memory::DATABASES;

/// Global flag to signal shutdown.
pub static SHUTDOWN_FLAG: AtomicBool = AtomicBool::new(false);

// ─── Commands ─────────────────────────────────────────────────────────────────

/// SHUTDOWN [NOSAVE|SAVE]
/// Performs a clean shutdown of the server.
pub fn shutdown(args: &[Value]) -> CommandResult {
    let save = if args.is_empty() {
        true
    } else {
        match args[0].as_str() {
            Some("NOSAVE") => false,
            Some("SAVE") => true,
            _ => return Err(CommandError::SyntaxError),
        }
    };

    log::info!("SHUTDOWN command received (save={})", save);

    if save {
        // Perform a synchronous save before exiting
        if let Err(e) = super::persistence::save(&[]) {
            log::error!("Failed to save during shutdown: {:?}", e);
        }
    }

    SHUTDOWN_FLAG.store(true, Ordering::SeqCst);
    // Return an error with "shutdown" message to signal to caller
    Err(CommandError::Generic("shutdown".to_string()))
}

/// ROLE
/// Returns the role of the server (master/slave/sentinel).
pub fn role(_args: &[Value]) -> CommandResult {
    // ai-redis only supports master mode for now
    let role_info = Value::Array(vec![
        Value::BulkString(Some("master".to_string())),
        Value::Integer(0),  // master_repl_offset
        Value::Array(vec![]), // empty list of slaves
    ]);
    Ok(role_info)
}

/// INFO [section]
/// Returns server information and statistics.
pub fn info(args: &[Value]) -> CommandResult {
    let section = args.first().and_then(|v| v.as_str()).unwrap_or("default");

    let config = super::config::get_config_map();

    // Count total keys across all databases
    let mut total_keys = 0u64;
    for db_idx in 0..16 {
        if let Some(arc) = DATABASES[db_idx].get() {
            if let Ok(guard) = arc.read() {
                total_keys += guard.iter().filter(|(_, v)| !v.is_expired()).count() as u64;
            }
        }
    }

    let stats = super::config::get_stats();

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let output = match section {
        "server" | "default" => format!(
            "# Server\n\
            redis_version:{}\n\
            redis_mode:standalone\n\
            os:rust\n\
            arch_bits:64\n\
            tcp_port:{}\n\
            uptime_in_seconds:{}\n\
            # Config\n\
            maxmemory:{}\n\
            maxmemory_policy:{}\n\
            ",
            env!("CARGO_PKG_VERSION"),
            config.get("port").cloned().unwrap_or_default(),
            now, // uptime
            config.get("maxmemory").cloned().unwrap_or_default(),
            config.get("maxmemory-policy").cloned().unwrap_or_default(),
        ),
        "clients" => "# Clients\nconnected_clients:0\nblocked_clients:0\n".to_string(),
        "memory" => format!(
            "# Memory\n\
            maxmemory:{}\n\
            used_memory:0\n\
            ",
            config.get("maxmemory").cloned().unwrap_or_default(),
        ),
        "stats" => format!(
            "# Stats\n\
            total_commands_processed:{}\n\
            keyspace_hits:{}\n\
            keyspace_misses:{}\n\
            ",
            stats.total_commands_processed,
            stats.keyspace_hits,
            stats.keyspace_misses,
        ),
        "keyspace" => format!(
            "# Keyspace\n\
            db0:keys={},expires=0,avg_ttl=0\n",
            total_keys,
        ),
        "replication" => "# Replication\nrole:master\nconnected_slaves:0\n".to_string(),
        _ => "".to_string(),
    };

    Ok(Value::BulkString(Some(output)))
}

/// TIME
/// Returns server current time as a 2-item array: [unix_timestamp, microseconds].
pub fn time(_args: &[Value]) -> CommandResult {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap();
    let secs = now.as_secs() as i64;
    let usecs = now.subsec_micros() as i64;
    Ok(Value::Array(vec![Value::Integer(secs), Value::Integer(usecs)]))
}

/// MIGRATE host port key timeout [COPY|REPLACE|DEL]
/// Atomically moves a key from the local server to another Redis server.
/// For simplicity, this implementation handles in-memory keys only.
pub fn migrate(args: &[Value]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArgs("MIGRATE".into()));
    }

    let host = args[0].as_str().ok_or(CommandError::WrongType)?;
    let _port: i64 = args[1]
        .as_int()
        .or_else(|| args[1].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let key = args[2].as_str().ok_or(CommandError::WrongType)?;
    let _timeout: i64 = args[3]
        .as_int()
        .or_else(|| args[3].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;

    let mut copy = false;
    let mut replace = false;
    let mut del = false;
    for i in 4..args.len() {
        match args[i].as_str() {
            Some("COPY") => copy = true,
            Some("REPLACE") => replace = true,
            Some("DEL") => del = true,
            _ => {}
        }
    }

    // In a real implementation, we would connect to the remote host and port,
    // serialize the key value, and send it. For now, just check key exists.
    let store = crate::storage::Storage::get();
    let guard = store.read().unwrap();
    let exists = guard.get(key).map(|v| !v.is_expired()).unwrap_or(false);

    if !exists {
        return Err(CommandError::Generic("ERR no such key".to_string()));
    }

    // Return not implemented for actual network migration
    let _ = (host, copy, replace, del);
    Err(CommandError::Generic("ERR MIGRATE not fully implemented (requires network)".to_string()))
}

/// FLUSHALL
/// Removes all keys from all databases.
pub fn flushall(_args: &[Value]) -> CommandResult {
    for db_idx in 0..16 {
        if let Some(arc) = DATABASES[db_idx].get() {
            if let Ok(mut guard) = arc.write() {
                guard.clear();
            }
        }
    }
    Ok(Value::SimpleString("OK".to_string()))
}

/// CLIENT subcommand dispatcher.
pub fn client(_args: &[Value]) -> CommandResult {
    if _args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CLIENT".into()));
    }
    // Just echo OK for now as client tracking is not implemented
    Ok(Value::SimpleString("OK".to_string()))
}
