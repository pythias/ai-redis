//! Connection-related commands: CLIENT LIST, CLIENT SETNAME, CLIENT KILL, etc.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;

/// CLIENT command — handles subcommands: LIST, GETNAME, SETNAME, KILL, INFO.
pub fn client(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CLIENT".into()));
    }
    let sub = args[0].as_str().unwrap_or("").to_uppercase();
    let rest = &args[1..];
    match sub.as_str() {
        "LIST" => client_list(rest),
        "GETNAME" => client_getname(rest),
        "SETNAME" => client_setname(rest),
        "KILL" => client_kill(rest),
        "INFO" => client_info(rest),
        _ => Err(CommandError::UnknownCommand(format!("CLIENT {}", sub))),
    }
}

fn client_list(_args: &[Value]) -> CommandResult {
    // Return basic info about connected clients
    // Format: adr=127.0.0.1:6379 fd=6 name= age=0 idle=0 cmd=client
    Ok(Value::BulkString(Some("id=0 addr=127.0.0.1:6379 laddr=127.0.0.1:6379 user=default".to_string())))
}

fn client_getname(_args: &[Value]) -> CommandResult {
    Ok(Value::BulkString(None)) // no name set by default
}

fn client_setname(args: &[Value]) -> CommandResult {
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CLIENT SETNAME".into()));
    }
    // Name is stored per-connection; for now just return OK
    let _name = args[0].as_str().unwrap_or("");
    Ok(Value::SimpleString("OK".to_string()))
}

fn client_kill(args: &[Value]) -> CommandResult {
    // CLIENT KILL [ADDR:port] — for server-side client disconnection
    if args.is_empty() {
        return Err(CommandError::WrongNumberOfArgs("CLIENT KILL".into()));
    }
    Ok(Value::SimpleString("OK".to_string()))
}

fn client_info(_args: &[Value]) -> CommandResult {
    Ok(Value::BulkString(Some("id=0 name=0000000000000000000000000000000000000000 lib-name=ai-redis lib-ver=0.1.0".to_string())))
}
