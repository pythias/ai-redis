//! Blocking command support.
//! Blocking commands (BLPOP/BRPOP/BLMOVE/etc.) loop with sleeps waiting for keys.
//! When list/set/zset keys are modified, they notify blocked waiters.

use std::collections::HashMap;
use std::sync::{Arc, Condvar, Mutex, RwLock, LazyLock};
use std::time::{Duration, Instant};

/// Per-key blocked waiters. A connection blocks on a key until it's notified
/// or its timeout expires.
struct BlockedOn {
    /// Instant when this waiter times out
    #[allow(dead_code)]
    timeout: Instant,
    /// Set to true when another client modifies the key we're waiting on
    ready: Arc<Mutex<bool>>,
}

/// Global blocking registry — maps key -> blocked waiters
static BLOCKING: LazyLock<RwLock<HashMap<String, Vec<BlockedOn>>>, fn() -> RwLock<HashMap<String, Vec<BlockedOn>>>> =
    LazyLock::new(|| RwLock::new(HashMap::new()));
static BLOCKING_CV: Condvar = Condvar::new();

/// Milliseconds between poll iterations for blocking commands.
#[allow(dead_code)]
const BLOCK_TICK_MS: u64 = 50;

/// Register a waiter blocked on `key` with the given timeout.
/// Returns the Arc<Mutex<bool>> that will be set true when unblocked.
#[allow(dead_code)]
pub fn block_on(key: &str, timeout: Duration) -> Arc<Mutex<bool>> {
    let ready = Arc::new(Mutex::new(false));
    let blocked = BlockedOn {
        timeout: Instant::now() + timeout,
        ready: ready.clone(),
    };
    let mut g = BLOCKING.write().unwrap();
    g.entry(key.to_string()).or_default().push(blocked);
    drop(g);
    BLOCKING_CV.notify_all();
    ready
}

/// Called by LPUSH/RPUSH/etc. — wake all waiters on `key`.
pub fn notify_key(key: &str) {
    let mut g = BLOCKING.write().unwrap();
    if let Some(waiters) = g.get_mut(key) {
        for w in waiters.drain(..) {
            *w.ready.lock().unwrap() = true;
        }
    }
    drop(g);
    BLOCKING_CV.notify_all();
}

/// Check if a waiter is ready (key was modified).
#[allow(dead_code)]
pub fn is_ready(ready: &Arc<Mutex<bool>>) -> bool {
    *ready.lock().unwrap()
}

/// Remove expired waiters and clean up empty keys.
#[allow(dead_code)]
pub fn purge_expired() {
    let mut g = BLOCKING.write().unwrap();
    let now = Instant::now();
    g.retain(|_, waiters| {
        waiters.retain(|w| w.timeout > now);
        !waiters.is_empty()
    });
}

/// Sleep helper — accounts for elapsed time from `start`.
#[allow(dead_code)]
pub fn blocking_sleep(dur: Duration, start: &Instant) -> Duration {
    let elapsed = start.elapsed();
    if elapsed >= dur {
        return Duration::ZERO;
    }
    let remaining = dur - elapsed;
    std::thread::sleep(remaining.min(Duration::from_millis(BLOCK_TICK_MS)));
    remaining.min(Duration::from_millis(BLOCK_TICK_MS))
}

/// Try to pop from a list key. If empty/expired/missing, return None.
#[allow(dead_code)]
pub fn list_pop(key: &str, from_left: bool) -> Option<String> {
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            if let crate::storage::data::RedisData::List(list) = &mut v.data {
                if !list.is_empty() {
                    return if from_left { list.pop_front() } else { list.pop_back() };
                }
            }
        }
        _ => {}
    }
    None
}

/// Try to pop from a sorted set (ZPOPMIN/ZPOPMAX). Returns (member, score).
pub fn zset_pop(key: &str, min: bool) -> Option<(String, f64)> {
    let store = crate::storage::Storage::get();
    let mut guard = store.write().unwrap();
    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            if let crate::storage::data::RedisData::SortedSet(zset) = &mut v.data {
                if !zset.is_empty() {
                    let _idx = if min { 0 } else { zset.len() - 1 };
                    // BTreeMap doesn't have remove_index; use into_iter and reconstruct
                    let entry = if min {
                        zset.keys().next().cloned()
                    } else {
                        zset.keys().last().cloned()
                    };
                    if let Some(k) = entry {
                        let score = zset.get(&k).copied()?;
                        zset.remove(&k);
                        return Some((k, score));
                    }
                }
            }
        }
        _ => {}
    }
    None
}

/// Notify waiters blocked on a key (call after any list/zset modification).
pub fn notify_waiters(key: &str) {
    notify_key(key);
}

/// Block until ready or timeout. Returns true if ready, false if timed out.
#[allow(dead_code)]
pub fn wait_until_ready(ready: &Arc<Mutex<bool>>, timeout: Duration, start: &Instant) -> bool {
    let loop_timeout = *start + timeout;
    loop {
        if *ready.lock().unwrap() {
            return true;
        }
        let now = Instant::now();
        if now >= loop_timeout {
            return false;
        }
        let tick = Duration::from_millis(BLOCK_TICK_MS).min(loop_timeout - now);
        std::thread::sleep(tick);
    }
}
