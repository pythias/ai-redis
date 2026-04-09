//! RESP2 response serializer.

use super::parser::Value;

pub fn encode(value: &Value) -> Vec<u8> {
    match value {
        Value::SimpleString(s) => {
            let mut r = Vec::with_capacity(s.len() + 3);
            r.push(b'+');
            r.extend_from_slice(s.as_bytes());
            r.extend_from_slice(b"\r\n");
            r
        }
        Value::Error(s) => {
            let mut r = Vec::with_capacity(s.len() + 3);
            r.push(b'-');
            r.extend_from_slice(s.as_bytes());
            r.extend_from_slice(b"\r\n");
            r
        }
        Value::Integer(n) => format!(":{}\r\n", n).into_bytes(),
        Value::BulkString(Some(s)) => {
            let len = s.len();
            let mut r = Vec::with_capacity(len + 10);
            r.extend_from_slice(format!("${}\r\n", len).as_bytes());
            r.extend_from_slice(s.as_bytes());
            r.extend_from_slice(b"\r\n");
            r
        }
        Value::BulkString(None) => b"$-1\r\n".to_vec(),
        Value::Array(arr) => {
            let mut r = Vec::new();
            r.extend_from_slice(format!("*{}\r\n", arr.len()).as_bytes());
            for v in arr {
                r.extend_from_slice(&encode(v));
            }
            r
        }
        Value::Null => b"*-1\r\n".to_vec(),
    }
}

pub fn ok() -> Value { Value::SimpleString("OK".to_string()) }
pub fn error(msg: impl Into<String>) -> Value { Value::Error(msg.into()) }
pub fn integer(n: i64) -> Value { Value::Integer(n) }
pub fn bulk(s: impl Into<String>) -> Value { Value::BulkString(Some(s.into())) }
pub fn null_bulk() -> Value { Value::BulkString(None) }
pub fn array(values: Vec<Value>) -> Value { Value::Array(values) }
pub fn nil() -> Value { Value::Null }

#[cfg(test)]
mod tests {
    use super::super::parser::Value;

    #[test]
    fn test_encode_simple_string() {
        let v = Value::SimpleString("OK".to_string());
        assert_eq!(super::encode(&v), b"+OK\r\n");
    }

    #[test]
    fn test_encode_bulk_string() {
        let v = Value::BulkString(Some("hello".to_string()));
        assert_eq!(super::encode(&v), b"$5\r\nhello\r\n");
    }

    #[test]
    fn test_encode_array() {
        let v = Value::Array(vec![
            Value::BulkString(Some("GET".to_string())),
            Value::BulkString(Some("foo".to_string())),
        ]);
        assert_eq!(super::encode(&v), b"*2\r\n$3\r\nGET\r\n$3\r\nfoo\r\n");
    }
}
