//! Stream and Geospatial extensions: XCLAIM, XTRIM, GEORADIUSBYMEMBER, GEOHASH

use crate::command::{CommandError, CommandResult};
use crate::protocol::Value;
use crate::storage::data::RedisData;
use crate::storage::Storage;

/// XCLAIM - Claim ownership of a message
pub fn xclaim(args: &[Value]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArgs("XCLAIM".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let _group = args[1].as_str().ok_or(CommandError::WrongType)?;
    let _consumer = args[2].as_str().ok_or(CommandError::WrongType)?;
    let _min_idle_time: i64 = args[3].as_int()
        .or_else(|| args[3].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;
    let ids: Vec<&str> = args[4..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Stream(s) => {
                    let mut claimed = Vec::new();
                    for id in ids {
                        if let Some(entry) = s.get_mut(id) {
                            // In a real implementation, would check min_idle_time
                            // and update last_delivery_info
                            claimed.push(Value::Array(vec![
                                Value::BulkString(Some(id.to_string())),
                                Value::Array(entry.fields.iter().map(|(k, val)| {
                                    Value::Array(vec![
                                        Value::BulkString(Some(k.clone())),
                                        Value::BulkString(Some(val.clone())),
                                    ])
                                }).collect()),
                            ]));
                        }
                    }
                    Ok(Value::Array(claimed))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

/// XTRIM - Trim a stream to max length
pub fn xtrim(args: &[Value]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArgs("XTRIM".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let max_len: i64 = args[1].as_int()
        .or_else(|| args[1].as_str().and_then(|s| s.parse().ok()))
        .ok_or(CommandError::InvalidInt)?;

    // Check for optional approximate flag
    let _approx = args.get(2).and_then(|v| v.as_str())
        .map(|s| s.to_uppercase() == "APPROXIMATE")
        .unwrap_or(false);

    let store = Storage::get();
    let mut guard = store.write().unwrap();

    match guard.get_mut(key) {
        Some(v) if !v.is_expired() => {
            match &mut v.data {
                RedisData::Stream(s) => {
                    let before = s.len();
                    // Keep only the newest entries
                    let mut ids: Vec<String> = s.keys().cloned().collect();
                    ids.sort();
                    
                    while ids.len() > max_len as usize && !ids.is_empty() {
                        s.remove(&ids.remove(0));
                    }
                    
                    Ok(Value::Integer((before - s.len()) as i64))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Integer(0)),
    }
}

/// GEORADIUSBYMEMBER - Find members within radius of a member
pub fn georadiusbymember(args: &[Value]) -> CommandResult {
    if args.len() < 4 {
        return Err(CommandError::WrongNumberOfArgs("GEORADIUSBYMEMBER".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let member = args[1].as_str().ok_or(CommandError::WrongType)?;
    let radius: f64 = args[2].as_str()
        .and_then(|s| s.parse().ok())
        .ok_or(CommandError::InvalidFloat)?;
    let unit = args.get(3).and_then(|v| v.as_str()).unwrap_or("m");

    let withdist = args.len() > 4 && args[4].as_str().map(|s| s.to_uppercase()) == Some("WITHDIST".to_string());
    let withcoord = args.len() > 5 && args[5].as_str().map(|s| s.to_uppercase()) == Some("WITHCOORD".to_string());
    let count = args.iter().position(|v| {
        v.as_str().map(|s| s.to_uppercase() == "COUNT").unwrap_or(false)
    }).and_then(|i| {
        args.get(i + 1).and_then(|v| v.as_int())
    });

    let multiplier = match unit {
        "km" => 1.0,
        "m" => 1000.0,
        "mi" => 0.621371,
        "ft" => 3.28084 * 1000.0,
        _ => 1.0,
    };

    let store = Storage::get();
    let guard = store.read().unwrap();

    // First get the member's position
    let (member_lon, member_lat) = match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    match h.get(member) {
                        Some(pos_str) => {
                            let parts: Vec<&str> = pos_str.split(':').collect();
                            if parts.len() >= 3 {
                                let lon: f64 = parts[1].parse().unwrap_or(0.0);
                                let lat: f64 = parts[2].parse().unwrap_or(0.0);
                                (lon, lat)
                            } else {
                                return Ok(Value::Array(vec![]));
                            }
                        }
                        None => return Ok(Value::Array(vec![])),
                    }
                }
                _ => return Err(CommandError::WrongType),
            }
        }
        _ => return Ok(Value::Array(vec![])),
    };

    // Now find all members within the radius
    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let mut results = Vec::new();
                    for (m, pos_str) in h {
                        let parts: Vec<&str> = pos_str.split(':').collect();
                        if parts.len() >= 3 {
                            let mlon: f64 = parts[1].parse().unwrap_or(0.0);
                            let mlat: f64 = parts[2].parse().unwrap_or(0.0);
                            let dist = haversine_distance(member_lat, member_lon, mlat, mlon) * multiplier / 1000.0;

                            if dist <= radius {
                                if withdist && withcoord {
                                    results.push(Value::Array(vec![
                                        Value::BulkString(Some(m.clone())),
                                        Value::BulkString(Some(format!("{:.2}", dist))),
                                        Value::Array(vec![
                                            Value::BulkString(Some(format!("{:.6}", mlon))),
                                            Value::BulkString(Some(format!("{:.6}", mlat))),
                                        ]),
                                    ]));
                                } else if withdist {
                                    results.push(Value::Array(vec![
                                        Value::BulkString(Some(m.clone())),
                                        Value::BulkString(Some(format!("{:.2}", dist))),
                                    ]));
                                } else if withcoord {
                                    results.push(Value::Array(vec![
                                        Value::BulkString(Some(m.clone())),
                                        Value::Array(vec![
                                            Value::BulkString(Some(format!("{:.6}", mlon))),
                                            Value::BulkString(Some(format!("{:.6}", mlat))),
                                        ]),
                                    ]));
                                } else {
                                    results.push(Value::BulkString(Some(m.clone())));
                                }
                            }
                        }
                    }
                    // Apply count limit if specified
                    if let Some(c) = count {
                        results.truncate(c as usize);
                    }
                    Ok(Value::Array(results))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(vec![])),
    }
}

/// GEOHASH - Return geohash strings for one or more members
pub fn geohash(args: &[Value]) -> CommandResult {
    if args.len() < 2 {
        return Err(CommandError::WrongNumberOfArgs("GEOHASH".into()));
    }
    let key = args[0].as_str().ok_or(CommandError::WrongType)?;
    let members: Vec<&str> = args[1..].iter().filter_map(|v| v.as_str()).collect();

    let store = Storage::get();
    let guard = store.read().unwrap();

    match guard.get(key) {
        Some(v) if !v.is_expired() => {
            match &v.data {
                RedisData::Hash(h) => {
                    let hashes: Vec<Value> = members.iter().map(|m| {
                        match h.get(*m) {
                            Some(pos_str) => {
                                let parts: Vec<&str> = pos_str.split(':').collect();
                                if parts.len() >= 3 {
                                    let lon: f64 = parts[1].parse().unwrap_or(0.0);
                                    let lat: f64 = parts[2].parse().unwrap_or(0.0);
                                    // Generate geohash string
                                    let hash = encode_geohash(lat, lon, 11);
                                    Value::BulkString(Some(hash))
                                } else {
                                    Value::BulkString(None)
                                }
                            }
                            None => Value::BulkString(None),
                        }
                    }).collect();
                    Ok(Value::Array(hashes))
                }
                _ => Err(CommandError::WrongType),
            }
        }
        _ => Ok(Value::Array(members.iter().map(|_| Value::BulkString(None)).collect())),
    }
}

/// Encode latitude and longitude into a geohash string
fn encode_geohash(lat: f64, lon: f64, precision: usize) -> String {
    const BASE32: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    
    let mut result = String::new();
    let mut lat_range = (-90.0, 90.0);
    let mut lon_range = (-180.0, 180.0);
    let mut is_lon = true;
    let mut bit = 0;
    let mut ch = 0;

    while result.len() < precision {
        let range;
        let val;
        if is_lon {
            range = lon_range;
            val = lon;
        } else {
            range = lat_range;
            val = lat;
        }

        let mid = (range.0 + range.1) / 2.0;
        if val >= mid {
            ch |= 1 << (4 - bit);
            if is_lon {
                lon_range.0 = mid;
            } else {
                lat_range.0 = mid;
            }
        } else {
            if is_lon {
                lon_range.1 = mid;
            } else {
                lat_range.1 = mid;
            }
        }

        is_lon = !is_lon;
        bit += 1;

        if bit == 5 {
            result.push(BASE32[ch as usize] as char);
            bit = 0;
            ch = 0;
        }
    }

    result
}

/// Calculate distance using Haversine formula
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
