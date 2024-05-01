pub struct Protocol {
    result: Vec<String>, // 添加一个字段来存储解析的结果
}

impl Protocol {
    pub fn new() -> Self {
        Protocol {
            result: Vec::new(), // 初始化字段
        }
    }

    pub fn parse(&mut self, data: &[u8]) -> Result<(), String> {
        if data.is_empty() {
            return Err("Empty input".to_string());
        }

        match data[0] as char {
            '*' => {
                self.parse_aggregate(data)?; // 处理聚合类型
            }
            '+' => {
                let result = self.parse_simple_string(data)?;
                println!("Parsed simple string: {}", result);
            }
            _ => return Err("Unknown type".to_string()),
        }
        
        Ok(())
    }

    pub fn parse_aggregate(&mut self, data: &[u8]) -> Result<(), String> {
        let data = String::from_utf8_lossy(data);
        let mut lines = data.split("\r\n");
    
        let command_line = match lines.next() {
            Some(line) => line,
            None => return Err("No command line found".to_string()),
        };
    
        if !command_line.starts_with("*") {
            return Err("Invalid command line".to_string());
        }
    
        let count: usize = command_line[1..].parse().map_err(|_| "Invalid count".to_string())?;
    
        for _ in 0..count {
            let length_line = match lines.next() {
                Some(line) => line,
                None => return Err("No length line found".to_string()),
            };
    
            if !length_line.starts_with("$") {
                return Err("Invalid length line".to_string());
            }
    
            let length: usize = length_line[1..].parse().map_err(|_| "Invalid length".to_string())?;
    
            let data_line = match lines.next() {
                Some(line) => line,
                None => return Err("No data line found".to_string()),
            };
    
            if data_line.len() != length {
                return Err("Data line length mismatch".to_string());
            }
    
            self.result.push(data_line.to_string());
        }
    
        Ok(())
    }

    pub fn parse_simple_string(&mut self, data: &[u8]) -> Result<String, String> {
        let data_str = std::str::from_utf8(data).map_err(|_| "Invalid UTF-8 sequence")?;
        if !data_str.starts_with('+') {
            return Err("Not a simple string".to_string());
        }

        let end = data_str.find("\r\n").ok_or("No end of line")?;
        let content = &data_str[1..end];

        Ok(content.to_string())
    }

    pub fn handle_command(&self) -> Result<String, String> {
        println!("Handling command: {:?}", self.result);

        match self.result.first() {
            Some(command) if command == "PING" => Ok("+PONG\r\n".to_string()),
            _ => Err("Unknown command".to_string()),
        }
    }
}