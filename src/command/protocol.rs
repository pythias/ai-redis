use std::str;
use crate::command::response::Response;

#[derive(Debug)]
pub struct Request {
    command: String,
    args: Vec<String>,
}

pub struct Protocol {
    result: Vec<String>, // 添加一个字段来存储解析的结果
}

impl Protocol {
    pub fn new() -> Self {
        Protocol {
            result: Vec::new(), // 初始化字段
        }
    }

    pub fn parse(&mut self, buffer: &[u8]) -> Result<Vec<Request>, String> {
        let mut requests = Vec::new();
        let mut iter = buffer.split(|&b| b == b'\n');
    
        while let Some(line) = iter.next() {
            if line.starts_with(b"*") {
                let count = str::from_utf8(&line[1..])
                    .map_err(|_| "Invalid UTF-8 sequence")?
                    .trim()
                    .parse::<usize>()
                    .map_err(|_| "Invalid number of arguments")?;
    
                let mut args = Vec::with_capacity(count);
    
                for _ in 0..count {
                    let line = iter.next().ok_or("Unexpected end of input")?;
                    if !line.starts_with(b"$") {
                        return Err("Expected '$'".into());
                    }
    
                    let len = str::from_utf8(&line[1..])
                        .map_err(|_| "Invalid UTF-8 sequence")?
                        .trim()
                        .parse::<usize>()
                        .map_err(|_| "Invalid argument length")?;
    
                    let arg = iter.next().ok_or("Unexpected end of input")?;
                    if arg.len() != len {
                        return Err("Argument length mismatch".into());
                    }
    
                    args.push(str::from_utf8(arg).map_err(|_| "Invalid UTF-8 sequence")?.into());
                }
    
                let command = args.remove(0); // Assuming the first argument is the command    
                requests.push(Request { command, args });
            }
        }
    
        Ok(requests)
    }

    pub fn handle_requests(&self, requests: Vec<Request>) -> Result<Vec<String>, String> {
        let mut responses = Vec::new();

        for request in requests {
            let response = self.handle_request(request)?;
            responses.push(response);
        }

        Ok(responses)
    }

    fn handle_request(&self, request: Request) -> Result<String, String> {
        println!("Handling request: {:?}", request);

        match request.command.to_uppercase().as_str() {
            "PING" => Ok(Response::SimpleString("PONG".to_string()).to_string()),
            "CONFIG" => {
                if request.args.len() >= 2 && request.args[0].to_uppercase() == "GET" {
                    match request.args[1].as_str() {
                        "save" => Ok(Response::Array(vec![
                            Response::BulkString(Some("save".to_string().into_bytes())),
                            Response::BulkString(Some("".to_string().into_bytes())),
                        ]).to_string()),
                        "appendonly" => Ok(Response::BulkString(Some("no".to_string().into_bytes())).to_string()),
                        "appendfsync" => Ok(Response::BulkString(Some("everysec".to_string().into_bytes())).to_string()),
                        _ => Ok(Response::BulkString(None).to_string()),  // Return an empty response for unknown config options
                    }
                } else {
                    Ok(Response::Array(vec![
                        Response::BulkString(Some("dbfilename".to_string().into_bytes())),
                        Response::BulkString(Some("dump.rdb".to_string().into_bytes())),
                        // Add more configuration options here
                    ]).to_string())
                }
            }
            _ => Err(Response::Error("Unknown command".to_string()).to_string()),
        }
    }
}