//! Thread-safe in-memory storage engine with 16 Redis databases (0–15).

use std::collections::HashMap;
use std::sync::{Arc, OnceLock, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::data::{RedisData, StoredValue};

/// Number of Redis databases (0–15).
const NUM_DATABASES: usize = 16;

thread_local! {
    static SELECTED_DB: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Global storage: an array of 16 OnceLock entries, each lazily initialized
/// to an Arc<RwLock<HashMap>> on first access.
pub static DATABASES: [OnceLock<Arc<RwLock<HashMap<String, StoredValue>>>>; NUM_DATABASES] = [
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
    const { OnceLock::new() },
];

/// Get the Arc<RwLock<HashMap>> for a specific DB index, initializing it
/// on first access.
fn get_db(db: usize) -> Arc<RwLock<HashMap<String, StoredValue>>> {
    DATABASES[db]
        .get_or_init(|| Arc::new(RwLock::new(HashMap::new())))
        .clone()
}

pub struct Storage;

#[allow(dead_code)]
impl Storage {
    /// Initialize the global storage.
    pub fn init() {
        // All DBs are initialized lazily on first access.
        for idx in 0..NUM_DATABASES {
            DATABASES[idx].get_or_init(|| Arc::new(RwLock::new(HashMap::new())));
        }
    }

    /// Get the current thread's selected database.
    pub fn get() -> Arc<RwLock<HashMap<String, StoredValue>>> {
        SELECTED_DB.with(|cell| get_db(cell.get()))
    }

    /// Switch the current thread's selected database.
    /// Panics if `db >= NUM_DATABASES`.
    pub fn select(db: usize) {
        assert!(db < NUM_DATABASES, "DB index out of range");
        SELECTED_DB.with(|cell| cell.set(db));
    }

    /// Returns the current thread's selected DB index.
    #[allow(dead_code)]
    pub fn current_db() -> usize {
        SELECTED_DB.with(|cell| cell.get())
    }

    /// Swap two databases globally (SWAPDB command).
    pub fn swapdb(db1: usize, db2: usize) {
        assert!(db1 < NUM_DATABASES && db2 < NUM_DATABASES, "DB index out of range");
        if db1 == db2 {
            return;
        }
        // Lock both HashMaps and swap their contents in-place.
        let arc1 = get_db(db1);
        let arc2 = get_db(db2);
        let mut g1 = arc1.write().unwrap();
        let mut g2 = arc2.write().unwrap();
        std::mem::swap(&mut *g1, &mut *g2);
    }

    /// Move a key from the current database to another database on the current
    /// thread (Redis MOVE semantics). Returns true if the key was moved.
    #[allow(dead_code)]
    pub fn move_key(key: &str, dest_db: usize) -> Option<(RedisData, Option<i64>)> {
        assert!(dest_db < NUM_DATABASES, "DB index out of range");
        let src_store = Self::get();
        let mut src_guard = src_store.write().unwrap();
        match src_guard.get(key) {
            Some(v) if !v.is_expired() => {
                let data = v.data.clone();
                let expire_at = v.expire_at;
                src_guard.remove(key);
                drop(src_guard);
                let dest_store = get_db(dest_db);
                let mut dest_guard = dest_store.write().unwrap();
                dest_guard.insert(
                    key.to_string(),
                    StoredValue::with_expiry(data.clone(), expire_at),
                );
                Some((data, expire_at))
            }
            _ => None,
        }
    }

    /// Execute a closure with the current database.
    fn with_db<F, R>(f: F) -> R
    where
        F: FnOnce(&Arc<RwLock<HashMap<String, StoredValue>>>) -> R,
    {
        let store = Self::get();
        f(&store)
    }

    // ─── Key operations ─────────────────────────────────────────────────────

    pub fn delete(&self, key: &str) -> bool {
        Self::with_db(|store| store.write().unwrap().remove(key).is_some())
    }

    pub fn exists(&self, key: &str) -> bool {
        Self::with_db(|store| {
            store
                .read()
                .unwrap()
                .get(key)
                .map(|v| !v.is_expired())
                .unwrap_or(false)
        })
    }

    pub fn r#type(&self, key: &str) -> String {
        Self::with_db(|store| match store.read().unwrap().get(key) {
            Some(v) if v.is_expired() => "none".to_string(),
            Some(v) => v.type_name().to_string(),
            None => "none".to_string(),
        })
    }

    // ─── TTL ────────────────────────────────────────────────────────────────

    pub fn expire(&self, key: &str, secs: i64) -> bool {
        Self::with_db(|store| {
            let mut guard = store.write().unwrap();
            if let Some(v) = guard.get_mut(key) {
                if v.is_expired() {
                    return false;
                }
                v.expire_at = Some(current_time_ms() + secs * 1000);
                true
            } else {
                false
            }
        })
    }

    pub fn expireat(&self, key: &str, timestamp: i64) -> bool {
        Self::with_db(|store| {
            let mut guard = store.write().unwrap();
            if let Some(v) = guard.get_mut(key) {
                if v.is_expired() {
                    return false;
                }
                v.expire_at = Some(timestamp * 1000);
                true
            } else {
                false
            }
        })
    }

    pub fn ttl(&self, key: &str) -> i64 {
        Self::with_db(|store| {
            let guard = store.read().unwrap();
            match guard.get(key) {
                Some(v) if v.is_expired() => -2,
                Some(v) => match v.expire_at {
                    Some(ts) => ((ts - current_time_ms()) / 1000).max(0),
                    None => -1,
                },
                None => -2,
            }
        })
    }

    pub fn pttl(&self, key: &str) -> i64 {
        Self::with_db(|store| {
            let guard = store.read().unwrap();
            match guard.get(key) {
                Some(v) if v.is_expired() => -2,
                Some(v) => match v.expire_at {
                    Some(ts) => (ts - current_time_ms()).max(0),
                    None => -1,
                },
                None => -2,
            }
        })
    }

    pub fn persist(&self, key: &str) -> bool {
        Self::with_db(|store| {
            let mut guard = store.write().unwrap();
            if let Some(v) = guard.get_mut(key) {
                if v.is_expired() {
                    return false;
                }
                v.expire_at = None;
                true
            } else {
                false
            }
        })
    }

    // ─── Key listing ────────────────────────────────────────────────────────

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        Self::with_db(|store| {
            store
                .read()
                .unwrap()
                .iter()
                .filter(|(_, v)| !v.is_expired())
                .map(|(k, _)| k.clone())
                .filter(|k| glob_match(pattern, k))
                .collect()
        })
    }

    pub fn all_keys(&self) -> Vec<String> {
        Self::with_db(|store| {
            store
                .read()
                .unwrap()
                .iter()
                .filter(|(_, v)| !v.is_expired())
                .map(|(k, _)| k.clone())
                .collect()
        })
    }

    pub fn dbsize(&self) -> usize {
        Self::with_db(|store| {
            store
                .read()
                .unwrap()
                .iter()
                .filter(|(_, v)| !v.is_expired())
                .count()
        })
    }

    pub fn flushdb(&self) {
        Self::with_db(|store| {
            let mut guard = store.write().unwrap();
            guard.retain(|_, v| !v.is_expired());
        });
    }

    // ─── String ──────────────────────────────────────────────────────────────

    pub fn set(&self, key: &str, value: String) {
        Self::with_db(|store| {
            store
                .write()
                .unwrap()
                .insert(key.to_string(), StoredValue::new(RedisData::String(value)));
        });
    }

    pub fn set_with_ttl(&self, key: &str, value: String, ttl_secs: i64) {
        Self::with_db(|store| {
            store.write().unwrap().insert(
                key.to_string(),
                StoredValue::with_ttl(RedisData::String(value), ttl_secs),
            );
        });
    }

    pub fn set_nx(&self, key: &str, value: String) -> bool {
        Self::with_db(|store| {
            let mut guard = store.write().unwrap();
            let exists = guard.get(key).map(|v| !v.is_expired()).unwrap_or(false);
            if !exists {
                guard.insert(key.to_string(), StoredValue::new(RedisData::String(value)));
                true
            } else {
                false
            }
        })
    }
}

pub fn current_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64
}

/// Glob pattern matcher using backtracking.
/// * matches any sequence (including empty), ? matches any single char.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    let pb = pattern.as_bytes();
    let nb = name.as_bytes();
    let mut i: usize = 0;
    let mut j: usize = 0;
    let mut star_i: isize = -1; // -1 = no star seen yet
    let mut star_j: usize = 0;

    while i < pb.len() || j < nb.len() {
        if i < pb.len() && j < nb.len() {
            let pc = pb[i];
            if pc == b'*' {
                star_i = i as isize + 1;
                star_j = j;
                i += 1;
            } else if pc == b'?' || pc == nb[j] {
                i += 1;
                j += 1;
            } else if star_i >= 0 {
                // Backtrack: star consumes one more name char
                star_j += 1;
                j = star_j;
                i = star_i as usize;
            } else {
                return false;
            }
        } else if i < pb.len() && pb[i] == b'*' {
            i += 1;
        } else if star_i >= 0 {
            if j < nb.len() {
                j += 1;
                i = star_i as usize;
            } else {
                // Name exhausted — remaining pattern must be all stars
                for &pc in &pb[i..] {
                    if pc != b'*' {
                        return false;
                    }
                }
                return true;
            }
        } else {
            return i == pb.len() && j == nb.len();
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_glob_match() {
        assert!(glob_match("*", "anything"));
        assert!(glob_match("foo*", "foobar"));
        assert!(glob_match("*bar", "foobar"));
        assert!(glob_match("foo*bar", "foobar"));
        assert!(glob_match("foo?bar", "fooXbar"));
        assert!(!glob_match("foo?bar", "fooXXbar"));
        assert!(glob_match("a*b*c", "axbxc"));
        assert!(glob_match("*:*", "u:1"));
        assert!(!glob_match("*:*", "user"));
    }
}
