mod command;
mod net;
mod store;

use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;
use std::str;

use env_logger;
use log::{info, error};
use crate::command::protocol::Protocol;

fn main() {
    env_logger::init();

    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        thread::spawn(|| {
            let _ = handle_connection(stream);
        });
    }
}

fn handle_connection(mut stream: TcpStream) -> Result<(), String> {
    let mut buffer = [0; 512];

    loop {
        match stream.read(&mut buffer) {
            Ok(0) => {
                // Connection was closed
                return Ok(());
            }
            Ok(_) => {
                let mut protocol = Protocol::new();
                let requests = protocol.parse(&buffer)?;
                info!("Received request: {}", str::from_utf8(&buffer).unwrap_or("<Invalid UTF-8>"));

                let responses = protocol.handle_requests(requests)?;
                for response in responses {
                    info!("Sending response: {}", response);
                    stream.write(response.as_bytes()).unwrap();
                    stream.flush().unwrap();
                }
            }
            Err(e) => {
                // Log the error
                error!("Error reading from stream: {}", e);
                return Err(e.to_string());
            }
        }
    }
}