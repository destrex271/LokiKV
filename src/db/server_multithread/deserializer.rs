use serde::Deserialize;
use serde_json::{self, Result};

#[derive(Debug, Deserialize)]
pub struct Request {
    pub query: String,
}

pub fn deserialize(input: &str) -> Result<Request> {
    serde_json::from_str(input)
}