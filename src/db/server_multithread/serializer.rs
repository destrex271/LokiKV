use serde::Serialize;
use serde_json;

pub fn serialize<T: Serialize>(data: &T) -> Result<String, String> {
    serde_json::to_string(data).map_err(|e| format!("Serialization error: {}", e))
}