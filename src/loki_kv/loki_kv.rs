use core::{f32, panic};
use std::any::{Any, TypeId};
use std::collections::{self, HashMap};
use std::fmt::Debug;
use std::mem;
use std::ptr::null;

#[derive(Debug, Clone)]
pub enum ValueObject {
    StringData(String),
    IntData(isize),
    BoolData(bool),
    Phantom,
    DecimalData(f64),
    OutputString(String),
    BlobData(Vec<u8>),
}

// Primary Store Structs to store data into page, uses a hashmap
pub struct LokiKV {
    store: HashMap<String, ValueObject>,
}

impl LokiKV {
    pub fn new() -> Self {
        let store: HashMap<String, ValueObject> = HashMap::new();
        LokiKV { store }
    }

    // Inserts Data
    pub fn put(&mut self, key: String, value: ValueObject) -> bool {
        let stat = self.store.insert(key, value);
        match stat {
            Some(stat) => {
                println!("{:?}", stat);
                true
            }
            None => false,
        }
    }

    // Gets data
    pub fn get(&self, key: String) -> Result<&ValueObject, String> {
        let data = self.store.get(&key);

        match data {
            Some(dt) => Ok(dt),
            None => Err("no data found!".to_string()),
        }
    }

    pub fn incr(&mut self, key: String) -> Result<(), &str> {
        let val = self.store.get(&key).unwrap();

        match val {
            ValueObject::IntData(data) => {
                self.store.insert(key, ValueObject::IntData(data + 1));
                Ok(())
            }
            ValueObject::DecimalData(data) => {
                self.store.insert(key, ValueObject::DecimalData(data + 1.0));
                Ok(())
            }
            _ => Err("incr not supported for data type"),
        }
    }

    pub fn decr(&mut self, key: String) -> Result<(), &str> {
        let val = self.store.get(&key).unwrap();

        match val {
            ValueObject::IntData(data) => {
                self.store.insert(key, ValueObject::IntData(data - 1));
                Ok(())
            }
            ValueObject::DecimalData(data) => {
                self.store.insert(key, ValueObject::DecimalData(data - 1.0));
                Ok(())
            }
            _ => Err("incr not supported for data type"),
        }
    }

    // Displays all keys and values
    pub fn display_collection(&self) -> String {
        let mut data = String::new();
        for (key, val) in self.store.iter() {
            println!("|{:?}\t\t\t\t|\t\t\t\t{:?}|", key, val);
            data += &format!("|{:?}\t\t\t\t|\t\t\t\t{:?}|", key, val);
        }
        data += &format!("-------------------------------------------------------------------");
        return data;
    }
}
