mod command;
mod net;
mod store;

use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;
use std::thread;

use crate::command::protocol::Protocol;

fn main() {
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
                protocol.parse(&buffer)?;
                let response = protocol.handle_command()?;

                stream.write(response.as_bytes()).unwrap();
                stream.flush().unwrap();
            }
            Err(e) => {
                return Err(e.to_string());
            }
        }
    }
}