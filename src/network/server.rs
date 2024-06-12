// network/server.rs
use std::io::prelude::*;
use std::net::TcpStream;

pub fn handle_connection(mut stream: TcpStream) {
    let mut buffer = [0; 1024];

    stream.read(&mut buffer).unwrap();

    // TODO: Parse the Redis protocol from the buffer
    // TODO: Handle the command
    // TODO: Send the result back to the client
}