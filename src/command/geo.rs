//! Geospatial Redis commands.

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::{RedisData, StoredValue};
use crate::storage::Storage;
use std::collections::HashMap;

pub fn geoadd(args: &[Value]) -> CommandResult {
    if args.len() < 4 || (args.len() - 1) % 3 != 0 {
        return Err(CommandError::WrongNumberOfArgs("GEOADD".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let members: Vec<(&str, f64, f64)> = parse_geo_args(&args[1..])?;

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Hash(h) => {
                    let mut added = 0;
                    for (member, lon, lat) in members {
                        let key_str = format!("{}:{}:{}", member, lon, lat);
                        if !h.contains_key(member) {
                            added += 1;
                        }
                        h.insert(member.to_string(), key_str);
                    }
                    Ok(Value::Integer(added))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => {
            let mut h = HashMap::new();
            let mut added = 0;
            for (member, lon, lat) in members {
                let key_str = format!("{}:{}:{}", member, lon, lat);
                h.insert(member.to_string(), key_str);
                added += 1;
            }
            guard.insert(key.to_string(), StoredValue::new(RedisData::Hash(h)));
            Ok(Value::Integer(added))
        }
    }
}

pub fn geopos(args: &[Value]) -> CommandResult {
    if args.len() < 2 { return Err(CommandError::WrongNumberOfArgs("GEOPOS".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let members: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let positions: Vec<Value> = members.iter().map(|m| {
                        match h.get(*m) {
                            Some(pos_str) => {
                                let parts: Vec<&str> = pos_str.split(':').collect();
                                if parts.len() >= 3 {
                                    let lon: f64 = parts[1].parse().unwrap_or(0.0);
                                    let lat: f64 = parts[2].parse().unwrap_or(0.0);
                                    Value::Array(vec![
                                        Value::BulkString(Some(format!("{:.6}", lon))),
                                        Value::BulkString(Some(format!("{:.6}", lat))),
                                    ])
                                } else {
                                    Value::BulkString(None)
                                }
                            }
                            None => Value::BulkString(None),
                        }
                    }).collect();
                    Ok(Value::Array(positions))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(members.iter().map(|_| Value::BulkString(None)).collect())),
    }
}

pub fn geodist(args: &[Value]) -> CommandResult {
    if args.len() != 3 && args.len() != 4 {
        return Err(CommandError::WrongNumberOfArgs("GEODIST".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let member1 = args[1].as_str().ok_or(CommandError::WrongType)?;
    let member2 = args[2].as_str().ok_or(CommandError::WrongType)?;
    let unit = if args.len() == 4 {
        args[3].as_str().unwrap_or("m")
    } else {
        "m"
    };

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let pos1 = h.get(member1);
                    let pos2 = h.get(member2);
                    
                    match (pos1, pos2) {
                        (Some(p1), Some(p2)) => {
                            let parts1: Vec<&str> = p1.split(':').collect();
                            let parts2: Vec<&str> = p2.split(':').collect();
                            
                            if parts1.len() >= 3 && parts2.len() >= 3 {
                                let lon1: f64 = parts1[1].parse().unwrap_or(0.0);
                                let lat1: f64 = parts1[2].parse().unwrap_or(0.0);
                                let lon2: f64 = parts2[1].parse().unwrap_or(0.0);
                                let lat2: f64 = parts2[2].parse().unwrap_or(0.0);
                                
                                let dist = haversine_distance(lat1, lon1, lat2, lon2);
                                let multiplier = match unit {
                                    "km" => 1.0,
                                    "m" => 1000.0,
                                    "mi" => 0.621371,
                                    "ft" => 3.28084 * 1000.0,
                                    _ => 1.0,
                                };
                                
                                Ok(Value::BulkString(Some(format!("{:.2}", dist * multiplier / 1000.0))))
                            } else {
                                Ok(Value::BulkString(None))
                            }
                        }
                        _ => Ok(Value::BulkString(None)),
                    }
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::BulkString(None)),
    }
}

pub fn georadius(args: &[Value]) -> CommandResult {
    if args.len() < 5 { return Err(CommandError::WrongNumberOfArgs("GEORADIUS".into())); }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let lon: f64 = args[1].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let lat: f64 = args[2].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let radius: f64 = args[3].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
    let unit = args.get(4).and_then(|v| v.as_str()).unwrap_or("m");
    
    let withdist = args.len() > 5 && args[5].as_str().map(|s| s.to_uppercase()) == Some("WITHDIST".to_string());
    let withcoord = args.len() > 6 && args[6].as_str().map(|s| s.to_uppercase()) == Some("WITHCOORD".to_string());

    let multiplier = match unit {
        "km" => 1.0,
        "m" => 1000.0,
        "mi" => 0.621371,
        "ft" => 3.28084 * 1000.0,
        _ => 1.0,
    };

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let mut results = Vec::new();
                    for (member, pos_str) in h {
                        let parts: Vec<&str> = pos_str.split(':').collect();
                        if parts.len() >= 3 {
                            let mlon: f64 = parts[1].parse().unwrap_or(0.0);
                            let mlat: f64 = parts[2].parse().unwrap_or(0.0);
                            let dist = haversine_distance(lat, lon, mlat, mlon) * multiplier / 1000.0;
                            
                            if dist <= radius {
                                if withdist && withcoord {
                                    results.push(Value::Array(vec![
                                        Value::BulkString(Some(member.clone())),
                                        Value::BulkString(Some(format!("{:.2}", dist))),
                                        Value::Array(vec![
                                            Value::BulkString(Some(format!("{:.6}", mlon))),
                                            Value::BulkString(Some(format!("{:.6}", mlat))),
                                        ]),
                                    ]));
                                } else if withdist {
                                    results.push(Value::Array(vec![
                                        Value::BulkString(Some(member.clone())),
                                        Value::BulkString(Some(format!("{:.2}", dist))),
                                    ]));
                                } else if withcoord {
                                    results.push(Value::Array(vec![
                                        Value::BulkString(Some(member.clone())),
                                        Value::Array(vec![
                                            Value::BulkString(Some(format!("{:.6}", mlon))),
                                            Value::BulkString(Some(format!("{:.6}", mlat))),
                                        ]),
                                    ]));
                                } else {
                                    results.push(Value::BulkString(Some(member.clone())));
                                }
                            }
                        }
                    }
                    Ok(Value::Array(results))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

fn parse_geo_args(args: &[Value]) -> Result<Vec<(&str, f64, f64)>, CommandError> {
    let mut members = Vec::new();
    let mut i = 0;
    while i + 2 < args.len() {
        let member = args[i].as_str().ok_or(CommandError::WrongType)?;
        let lon: f64 = args[i + 1].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
        let lat: f64 = args[i + 2].as_str().and_then(|s| s.parse().ok()).ok_or(CommandError::InvalidFloat)?;
        members.push((member, lon, lat));
        i += 3;
    }
    Ok(members)
}

fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_M: f64 = 6371000.0;
    
    let lat1_rad = lat1.to_radians();
    let lat2_rad = lat2.to_radians();
    let delta_lat = (lat2 - lat1).to_radians();
    let delta_lon = (lon2 - lon1).to_radians();
    
    let a = (delta_lat / 2.0).sin().powi(2) 
        + lat1_rad.cos() * lat2_rad.cos() * (delta_lon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();
    
    EARTH_RADIUS_M * c
}
