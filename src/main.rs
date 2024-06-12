// main.rs
use std::net::TcpListener;
use std::thread;

mod network;

fn main() {
    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        let stream = stream.unwrap();

        thread::spawn(|| {
            network::server::handle_connection(stream);
        });
    }
}