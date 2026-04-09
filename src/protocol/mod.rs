//! RESP (REdis Serialization Protocol) implementation.

pub mod parser;
pub mod serializer;

pub use parser::{Value, ParseError, parse_command};
