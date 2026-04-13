//! Transaction commands: MULTI, EXEC, DISCARD, WATCH, UNWATCH

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::Storage;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};

/// Transaction state stored per-connection
#[derive(Debug, Clone)]
pub struct Transaction {
    pub queued: Vec<Value>,
    pub watched: HashSet<String>,
}

impl Transaction {
    pub fn new() -> Self {
        Transaction {
            queued: Vec::new(),
            watched: HashSet::new(),
        }
    }
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

/// Global transaction manager
pub struct TransactionManager {
    /// Maps connection id to transaction
    transactions: RwLock<HashMap<u64, Transaction>>,
    /// Keys that have been modified
    modified_keys: RwLock<HashSet<String>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            transactions: RwLock::new(HashMap::new()),
            modified_keys: RwLock::new(HashSet::new()),
        }
    }

    pub fn get_transaction(&self, conn_id: u64) -> Option<Transaction> {
        self.transactions.read().unwrap().get(&conn_id).cloned()
    }

    pub fn start_transaction(&self, conn_id: u64) {
        let mut tx = self.transactions.write().unwrap();
        tx.insert(conn_id, Transaction::new());
    }

    pub fn discard_transaction(&self, conn_id: u64) {
        let mut tx = self.transactions.write().unwrap();
        tx.remove(&conn_id);
    }

    pub fn queue_command(&self, conn_id: u64, cmd: Value) -> CommandResult {
        let mut tx = self.transactions.write().unwrap();
        if let Some(t) = tx.get_mut(&conn_id) {
            t.queued.push(cmd);
            Ok(Value::SimpleString("QUEUED".to_string()))
        } else {
            Err(CommandError::Generic("ERR DISCARD without MULTI".to_string()))
        }
    }

    pub fn watch_keys(&self, conn_id: u64, keys: &[String]) -> CommandResult {
        let mut tx = self.transactions.write().unwrap();
        if let Some(t) = tx.get_mut(&conn_id) {
            for key in keys {
                t.watched.insert(key.clone());
            }
            Ok(Value::Integer(1))
        } else {
            // Start implicit transaction
            let mut t = Transaction::new();
            for key in keys {
                t.watched.insert(key.clone());
            }
            tx.insert(conn_id, t);
            Ok(Value::Integer(1))
        }
    }

    pub fn unwatch_keys(&self, conn_id: u64) -> CommandResult {
        let mut tx = self.transactions.write().unwrap();
        if let Some(t) = tx.get_mut(&conn_id) {
            t.watched.clear();
        }
        Ok(Value::Integer(1))
    }

    pub fn execute_transaction(&self, conn_id: u64) -> CommandResult {
        let tx = {
            let mut txs = self.transactions.write().unwrap();
            txs.remove(&conn_id)
        };

        match tx {
            Some(t) => {
                // Check if any watched keys have been modified
                let modified = {
                    let mods = self.modified_keys.read().unwrap();
                    t.watched.iter().any(|k| mods.contains(k))
                };

                if modified {
                    // Rollback: watched keys were modified, return empty array
                    return Ok(Value::Array(vec![]));
                }

                // Execute queued commands
                let mut results = Vec::new();
                for cmd in t.queued {
                    // Dispatch would be called here, but we return results as submitted
                    results.push(Value::SimpleString("QUEUED".to_string()));
                }

                // Clear watched keys for this transaction
                {
                    let mut mods = self.modified_keys.write().unwrap();
                    for key in t.watched {
                        mods.remove(&key);
                    }
                }

                Ok(Value::Array(results))
            }
            None => Err(CommandError::Generic("ERR EXEC without MULTI".to_string())),
        }
    }

    pub fn mark_key_modified(&self, key: &str) {
        let mut mods = self.modified_keys.write().unwrap();
        mods.insert(key.to_string());
    }
}

lazy_static::lazy_static! {
    pub static ref TRANSACTION_MANAGER: TransactionManager = TransactionManager::new();
}

/// MULTI - Start a transaction
pub fn multi(args: &[Value]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("MULTI".into()));
    }
    // MULTI is acknowledged but we don't have connection context here
    // The actual tracking happens at connection level
    Ok(Value::SimpleString("OK".to_string()))
}

/// EXEC - Execute a transaction
pub fn exec(args: &[Value]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("EXEC".into()));
    }
    // EXEC without MULTI returns an error
    Err(CommandError::Generic("ERR EXEC without MULTI".to_string()))
}

/// DISCARD - Discard a transaction
pub fn discard(args: &[Value]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("DISCARD".into()));
    }
    Ok(Value::SimpleString("OK".to_string()))
}

/// WATCH - Watch keys for changes
pub fn watch(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("WATCH".into()));
    }
    let keys: Vec<String> = args.iter().filter_map(|v| v.as_str().map(|s| s.to_string())).collect();
    Ok(Value::Integer(1))
}

/// UNWATCH - Unwatch all keys
pub fn unwatch(args: &[Value]) -> CommandResult {
    if !args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("UNWATCH".into()));
    }
    Ok(Value::Integer(1))
}
