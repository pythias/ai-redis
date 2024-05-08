pub enum Response {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Vec<Response>),
}

impl Response {
    pub fn to_string(&self) -> String {
        match self {
            Response::SimpleString(s) => format!("+{}\r\n", s),
            Response::Error(e) => format!("-{}\r\n", e),
            Response::Integer(i) => format!(":{}\r\n", i),
            Response::BulkString(Some(bytes)) => {
                let len = bytes.len();
                format!("${}\r\n{}\r\n", len, String::from_utf8_lossy(bytes))
            }
            Response::BulkString(None) => "$-1\r\n".to_string(),
            Response::Array(arr) => {
                let mut result = format!("*{}\r\n", arr.len());
                for item in arr {
                    result.push_str(&item.to_string());
                }
                result
            }
        }
    }
}