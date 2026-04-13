//! TCP server that speaks RESP2.

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, Shutdown};
use std::thread;
use std::time::Duration;

use crate::command::router::Router;
use crate::command::server::SHUTDOWN_FLAG;
use crate::protocol::parse_command;
use crate::protocol::serializer::encode;
use crate::protocol::Value;

pub fn run(addr: &str) -> std::io::Result<()> {
    // Initialize config
    crate::command::config::init_config();

    let listener = TcpListener::bind(addr)?;
    listener.set_nonblocking(false)?;
    log::info!("ai-redis listening on {}", addr);

    for stream in listener.incoming() {
        // Check if shutdown was requested
        if SHUTDOWN_FLAG.load(std::sync::atomic::Ordering::SeqCst) {
            log::info!("Shutdown requested, stopping server");
            break;
        }

        match stream {
            Ok(stream) => {
                stream.set_nodelay(true).ok();
                thread::spawn(|| handle_client(stream));
            }
            Err(e) => {
                log::warn!("accept error: {}", e);
            }
        }
    }
    Ok(())
}

fn handle_client(mut stream: TcpStream) {
    let peer = stream.peer_addr().map(|a| a.to_string()).unwrap_or_else(|_| "?".to_string());
    log::info!("client connected: {}", peer);

    let router = Router::new();
    let mut buf = Vec::with_capacity(8192);
    let mut chunk = [0u8; 8192];

    loop {
        match stream.read(&mut chunk) {
            Ok(0) => {
                log::info!("client {} disconnected", peer);
                break;
            }
            Ok(n) => {
                buf.extend_from_slice(&chunk[..n]);

                loop {
                    let (consumed, cmd) = match parse_command(&buf) {
                        Ok((n, cmd)) => (n, cmd),
                        Err(_) => {
                            let err = Value::Error("Protocol error".to_string());
                            let _ = stream.write_all(&encode(&err));
                            let _ = stream.shutdown(Shutdown::Both);
                            return;
                        }
                    };

                    if consumed == 0 { break; }
                    buf.drain(..consumed);

                    let response = if let Some((cmd_name, cmd_args)) = cmd.split_first() {
                        let name = cmd_name.as_str().unwrap_or("").to_uppercase();
                        let args: Vec<Value> = cmd_args.to_vec();
                        match router.dispatch(&name, &args) {
                            Ok(v) => v,
                            Err(e) => {
                                // Check for shutdown signal
                                if let crate::command::CommandError::Generic(s) = &e {
                                    if s == "shutdown" {
                                        log::info!("Shutdown command received from client {}", peer);
                                        let _ = stream.write_all(&encode(&e.to_resp()));
                                        let _ = stream.shutdown(Shutdown::Both);
                                        // Signal global shutdown
                                        SHUTDOWN_FLAG.store(true, std::sync::atomic::Ordering::SeqCst);
                                        return;
                                    }
                                }
                                e.to_resp()
                            }
                        }
                    } else {
                        Value::Error("empty command".to_string())
                    };

                    if let Err(e) = stream.write_all(&encode(&response)) {
                        log::warn!("write error to {}: {}", peer, e);
                        let _ = stream.shutdown(Shutdown::Both);
                        return;
                    }

                    if buf.is_empty() { break; }
                }
            }
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                thread::sleep(Duration::from_millis(10));
            }
            Err(e) => {
                log::warn!("read error from {}: {}", peer, e);
                break;
            }
        }
    }

    let _ = stream.shutdown(Shutdown::Both);
}
