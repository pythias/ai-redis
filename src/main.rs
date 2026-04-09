//! ai-redis — A Redis-compatible server written in Rust.

use std::env;

mod command;
mod network;
mod protocol;
mod storage;

use storage::Storage;
use network::server::run;

fn main() {
    env_logger::Builder::from_env(env_logger::Env::default().default_filter_or("info")).init();
    Storage::init();

    let addr = env::args().nth(1).unwrap_or_else(|| "127.0.0.1:6379".to_string());
    log::info!("ai-redis v0.1.0 — starting server on {}", addr);

    if let Err(e) = run(&addr) {
        eprintln!("server error: {}", e);
        std::process::exit(1);
    }
}
