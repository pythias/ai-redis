//! Thread-safe in-memory storage engine.

use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use super::data::{RedisData, StoredValue};

/// Global singleton storage.
static mut STORAGE: Option<Arc<RwLock<HashMap<String, StoredValue>>>> = None;

pub struct Storage;

impl Storage {
    /// Initialize the global storage.
    pub fn init() {
        unsafe {
            if STORAGE.is_none() {
                STORAGE = Some(Arc::new(RwLock::new(HashMap::new())));
            }
        }
    }

    /// Get the global storage Arc (associated function, no receiver).
    pub fn get() -> Arc<RwLock<HashMap<String, StoredValue>>> {
        unsafe {
            STORAGE.clone().expect("Storage not initialized — call Storage::init() first")
        }
    }

    fn with_storage<F, R>(f: F) -> R
    where
        F: FnOnce(&Arc<RwLock<HashMap<String, StoredValue>>>) -> R,
    {
        unsafe {
            let store = STORAGE.clone().expect("Storage not initialized — call Storage::init() first");
            f(&store)
        }
    }

    // ─── Key operations ─────────────────────────────────────────────────────

    pub fn delete(&self, key: &str) -> bool {
        Self::with_storage(|store| {
            store.write().unwrap().remove(key).is_some()
        })
    }

    pub fn exists(&self, key: &str) -> bool {
        Self::with_storage(|store| {
            store.read().unwrap().get(key).map(|v| !v.is_expired()).unwrap_or(false)
        })
    }

    pub fn r#type(&self, key: &str) -> String {
        Self::with_storage(|store| {
            match store.read().unwrap().get(key) {
                Some(v) if v.is_expired() => "none".to_string(),
                Some(v) => v.type_name().to_string(),
                None => "none".to_string(),
            }
        })
    }

    // ─── TTL ────────────────────────────────────────────────────────────────

    pub fn expire(&self, key: &str, secs: i64) -> bool {
        Self::with_storage(|store| {
            let mut guard = store.write().unwrap();
            if let Some(v) = guard.get_mut(key) {
                if v.is_expired() { return false; }
                v.expire_at = Some(current_time_ms() + secs * 1000);
                true
            } else { false }
        })
    }

    pub fn expireat(&self, key: &str, timestamp: i64) -> bool {
        Self::with_storage(|store| {
            let mut guard = store.write().unwrap();
            if let Some(v) = guard.get_mut(key) {
                if v.is_expired() { return false; }
                v.expire_at = Some(timestamp * 1000);
                true
            } else { false }
        })
    }

    pub fn ttl(&self, key: &str) -> i64 {
        Self::with_storage(|store| {
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
        Self::with_storage(|store| {
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
        Self::with_storage(|store| {
            let mut guard = store.write().unwrap();
            if let Some(v) = guard.get_mut(key) {
                if v.is_expired() { return false; }
                v.expire_at = None;
                true
            } else { false }
        })
    }

    // ─── Key listing ────────────────────────────────────────────────────────

    pub fn keys(&self, pattern: &str) -> Vec<String> {
        Self::with_storage(|store| {
            let guard = store.read().unwrap();
            guard.iter()
                .filter(|(_, v)| !v.is_expired())
                .map(|(k, _)| k.clone())
                .filter(|k| glob_match(pattern, k))
                .collect()
        })
    }

    pub fn all_keys(&self) -> Vec<String> {
        Self::with_storage(|store| {
            let guard = store.read().unwrap();
            guard.iter()
                .filter(|(_, v)| !v.is_expired())
                .map(|(k, _)| k.clone())
                .collect()
        })
    }

    pub fn dbsize(&self) -> usize {
        Self::with_storage(|store| {
            store.read().unwrap().iter()
                .filter(|(_, v)| !v.is_expired())
                .count()
        })
    }

    pub fn flushdb(&self) {
        Self::with_storage(|store| {
            let mut guard = store.write().unwrap();
            guard.retain(|_, v| !v.is_expired());
        });
    }

    // ─── String ──────────────────────────────────────────────────────────────

    pub fn set(&self, key: &str, value: String) {
        Self::with_storage(|store| {
            let mut guard = store.write().unwrap();
            guard.insert(key.to_string(), StoredValue::new(RedisData::String(value)));
        });
    }

    pub fn set_with_ttl(&self, key: &str, value: String, ttl_secs: i64) {
        Self::with_storage(|store| {
            let mut guard = store.write().unwrap();
            guard.insert(key.to_string(), StoredValue::with_ttl(RedisData::String(value), ttl_secs));
        });
    }

    pub fn set_nx(&self, key: &str, value: String) -> bool {
        Self::with_storage(|store| {
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
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64
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
                j += 1;
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
                i = star_i as usize;
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
    }
}
