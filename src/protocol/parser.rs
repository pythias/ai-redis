//! RESP2 protocol parser.
//!
//! Frame types:
//!   +  Simple String : +Content\r\n
//!   -  Error        : -ERR message\r\n
//!   :  Integer      : :123\r\n
//!   $  Bulk String  : $5\r\nhello\r\n   (-1 = null)
//!   *  Array        : *3\r\n... frames ...  (-1 = null)

use thiserror::Error;

#[derive(Debug, Error)]
pub enum ParseError {
    #[error("incomplete frame, need {0} more bytes")]
    Incomplete(u64),

    #[error("invalid frame header: {0}")]
    InvalidHeader(String),

    #[error("invalid integer: {0}")]
    InvalidInteger(#[from] std::num::ParseIntError),

    #[error("negative bulk string length: {0}")]
    NegativeBulkLength(i64),

    #[error("negative array length: {0}")]
    NegativeArrayLength(i64),
}

/// RESP2 Value — any parsed frame.
#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<String>),
    Array(Vec<Value>),
    Null,
}

impl Value {
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Value::BulkString(Some(s)) => Some(s),
            Value::SimpleString(s) => Some(s),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&[Value]> {
        match self { Value::Array(v) => Some(v), _ => None }
    }

    pub fn as_int(&self) -> Option<i64> {
        match self { Value::Integer(i) => Some(*i), _ => None }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }
}

/// Parse one RESP frame from input. Returns (bytes_consumed, value).
/// Returns Err(Incomplete) if not enough data.
pub fn parse_frame(input: &[u8]) -> Result<(usize, Value), ParseError> {
    if input.is_empty() {
        return Err(ParseError::Incomplete(1));
    }
    match input[0] as char {
        '+' => parse_simple_string(input),
        '-' => parse_error(input),
        ':' => parse_integer(input),
        '$' => parse_bulk_string(input),
        '*' => parse_array(input),
        _ => Err(ParseError::InvalidHeader(format!("unknown type byte: {:?}", input[0]))),
    }
}

/// Parse a Redis command: an Array frame whose elements are the command + args.
pub fn parse_command(input: &[u8]) -> Result<(usize, Vec<Value>), ParseError> {
    let (consumed, value) = parse_frame(input)?;
    match value {
        Value::Array(arr) => Ok((consumed, arr)),
        Value::BulkString(Some(_)) => Ok((consumed, vec![value])),
        _ => Err(ParseError::InvalidHeader("command must be Array".into())),
    }
}

fn parse_simple_string(input: &[u8]) -> Result<(usize, Value), ParseError> {
    let end = find_crlf(input, 1)?;
    let content = String::from_utf8_lossy(&input[1..end]).to_string();
    Ok((end + 2, Value::SimpleString(content)))
}

fn parse_error(input: &[u8]) -> Result<(usize, Value), ParseError> {
    let end = find_crlf(input, 1)?;
    let content = String::from_utf8_lossy(&input[1..end]).to_string();
    Ok((end + 2, Value::Error(content)))
}

fn parse_integer(input: &[u8]) -> Result<(usize, Value), ParseError> {
    let end = find_crlf(input, 1)?;
    let s = String::from_utf8_lossy(&input[1..end]);
    let num: i64 = s.parse()?;
    Ok((end + 2, Value::Integer(num)))
}

fn parse_bulk_string(input: &[u8]) -> Result<(usize, Value), ParseError> {
    // $-1\r\n  → null
    // $5\r\nhello\r\n  → "hello"
    let end = find_crlf(input, 1)?;
    let s = String::from_utf8_lossy(&input[1..end]);
    let len: i64 = s.parse()?;

    if len < -1 {
        return Err(ParseError::NegativeBulkLength(len));
    }

    if len == -1 {
        return Ok((end + 2, Value::BulkString(None)));
    }

    let usize_len = len as usize;
    // Need: 1 ($) + digits + \r\n + len bytes + \r\n
    let needed = end + 2 + usize_len + 2;
    if input.len() < needed {
        return Err(ParseError::Incomplete(needed as u64 - input.len() as u64));
    }

    let content = String::from_utf8_lossy(&input[end + 2..end + 2 + usize_len]).to_string();
    Ok((end + 2 + usize_len + 2, Value::BulkString(Some(content))))
}

fn parse_array(input: &[u8]) -> Result<(usize, Value), ParseError> {
    // *3\r\n$3\r\nGET\r\n... → vec![...]
    // *-1\r\n → Null
    let end = find_crlf(input, 1)?;
    let s = String::from_utf8_lossy(&input[1..end]);
    let count: i64 = s.parse()?;

    if count < -1 {
        return Err(ParseError::NegativeArrayLength(count));
    }

    if count == -1 {
        return Ok((end + 2, Value::Null));
    }

    let count = count as usize;
    let mut pos = end + 2;
    let mut items = Vec::with_capacity(count);

    for _ in 0..count {
        let (consumed, item) = parse_frame(&input[pos..])?;
        items.push(item);
        pos += consumed;
    }

    Ok((pos, Value::Array(items)))
}

/// Find \r\n starting from byte index `start`. Returns index of \r.
fn find_crlf(input: &[u8], start: usize) -> Result<usize, ParseError> {
    if input.len() < start + 2 {
        return Err(ParseError::Incomplete((start + 2 - input.len()) as u64));
    }
    for i in start..input.len() - 1 {
        if input[i] == b'\r' && input[i + 1] == b'\n' {
            return Ok(i);
        }
    }
    Err(ParseError::Incomplete(2))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_simple_string() { assert_eq!(parse_frame(b"+OK\r\n").unwrap().1, Value::SimpleString("OK".into())); }
    #[test]
    fn test_error() { assert_eq!(parse_frame(b"-ERR test\r\n").unwrap().1, Value::Error("ERR test".into())); }
    #[test]
    fn test_integer() { assert_eq!(parse_frame(b":1000\r\n").unwrap().1, Value::Integer(1000)); }
    #[test]
    fn test_bulk_string() { assert_eq!(parse_frame(b"$5\r\nhello\r\n").unwrap().1, Value::BulkString(Some("hello".into()))); }
    #[test]
    fn test_null_bulk() { assert_eq!(parse_frame(b"$-1\r\n").unwrap().1, Value::BulkString(None)); }
    #[test]
    fn test_null_array() { assert_eq!(parse_frame(b"*-1\r\n").unwrap().1, Value::Null); }
    #[test]
    fn test_array() {
        let (_, v) = parse_frame(b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n").unwrap();
        let arr = v.as_array().unwrap();
        assert_eq!(arr.len(), 2);
        assert_eq!(arr[0].as_str(), Some("GET"));
        assert_eq!(arr[1].as_str(), Some("foo"));
    }
    #[test]
    fn test_command() {
        let (_, cmd) = parse_command(b"*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$5\r\nhello\r\n").unwrap();
        assert_eq!(cmd.len(), 3);
        assert_eq!(cmd[0].as_str(), Some("SET"));
    }
    #[test]
    fn test_incomplete() { assert!(matches!(parse_frame(b"+OK"), Err(ParseError::Incomplete(_)))); }
}
