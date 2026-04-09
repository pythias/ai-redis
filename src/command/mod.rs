//! Command handling — routing, dispatch, and implementations.

pub mod router;
pub mod generic;
pub mod connection;
pub mod string;
pub mod hash;
pub mod list;
pub mod set;
pub mod sorted_set;
pub mod bitmap;

use crate::protocol::Value;

/// Result type for command handlers.
pub type CommandResult = Result<Value, CommandError>;

#[derive(Debug)]
pub enum CommandError {
    WrongNumberOfArgs(String),
    NotImplemented(String),
    WrongType,
    ValueTooLarge,
    InvalidInt,
    InvalidFloat,
    IndexOutOfRange,
    UnknownCommand(String),
    Generic(String),
}

impl From<std::convert::Infallible> for CommandError {
    fn from(_: std::convert::Infallible) -> Self {
        CommandError::Generic("?".to_string())
    }
}

impl CommandError {
    pub fn to_resp(&self) -> Value {
        let msg = match self {
            CommandError::WrongNumberOfArgs(cmd) => format!("wrong number of arguments for '{}' command", cmd),
            CommandError::NotImplemented(cmd) => format!("command '{}' not implemented", cmd),
            CommandError::WrongType => "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
            CommandError::ValueTooLarge => "ERR string exceeds maximum allowed size".to_string(),
            CommandError::InvalidInt => "ERR value is not an integer".to_string(),
            CommandError::InvalidFloat => "ERR value is not a float".to_string(),
            CommandError::IndexOutOfRange => "ERR index out of range".to_string(),
            CommandError::UnknownCommand(cmd) => format!("ERR unknown command '{}'", cmd),
            CommandError::Generic(s) => s.clone(),
        };
        Value::Error(msg)
    }
}
