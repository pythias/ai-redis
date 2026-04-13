//! Lua scripting commands stub (EVAL, EVALSHA, FCALL, SCRIPT)
//! This is a placeholder - full Lua support requires the mlua crate

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;

/// EVAL script numkeys [key ...] [arg ...]
pub fn eval(args: &[Value]) -> CommandResult {
    Err(CommandError::NotImplemented("EVAL".into()))
}

/// EVALSHA sha1 numkeys [key ...] [arg ...]
pub fn evalsha(args: &[Value]) -> CommandResult {
    Err(CommandError::NotImplemented("EVALSHA".into()))
}

/// FCALL function_name numkeys [key ...] [arg ...]
pub fn fcall(args: &[Value]) -> CommandResult {
    Err(CommandError::NotImplemented("FCALL".into()))
}

/// SCRIPT LOAD script
pub fn script_load(args: &[Value]) -> CommandResult {
    Err(CommandError::NotImplemented("SCRIPT".into()))
}
