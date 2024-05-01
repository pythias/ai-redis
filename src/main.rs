mod protocol;

use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;

use crate::protocol::Protocol;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:7878").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        let _ = handle_connection(stream);
    }
}

fn handle_connection(mut stream: TcpStream) -> Result<(), String> {
    let mut buffer = [0; 512];
    stream.read(&mut buffer).unwrap();

    println!("Received: {:?}", String::from_utf8_lossy(&buffer).trim_end_matches('\0'));

    let mut protocol = Protocol::new();
    protocol.parse(&buffer)?;
    let response = protocol.handle_command()?;

    println!("Response: {:?}", response);

    stream.write(response.as_bytes()).unwrap();
    stream.flush().unwrap();

    Ok(())
}