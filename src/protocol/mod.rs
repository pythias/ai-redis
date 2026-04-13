//! RESP (REdis Serialization Protocol) implementation.

pub mod parser;
pub mod serializer;

pub use parser::{Value, parse_command};
