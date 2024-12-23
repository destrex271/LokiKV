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


// Equivalent to a table
pub struct Collection{
    store: HashMap<String, ValueObject>,
}

impl Collection{
    fn new() -> Self{
        let store: HashMap<String, ValueObject> = HashMap::new();
        Collection{
            store
        }
    }
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
            data += &format!("{:?} -> {:?}", key, val);
        }
        return data;
    }
}

pub struct LokiKV {
    collections: HashMap<String, Collection>,
    current_collection: String,
}

impl LokiKV {
    pub fn new() -> Self {
        let mut collections: HashMap<String, Collection> = HashMap::new();
        collections.insert("default".to_string(), Collection::new());
        LokiKV {
            collections,
            current_collection: "default".to_string()
        }
    }

    pub fn create_collection(&mut self, collection_name: String){
        self.collections.insert(collection_name, Collection::new());
    }

    pub fn select_collection(&mut self, key: String){
        if self.collections.contains_key(&key){
        self.current_collection = key;
        }else{
            panic!("Collection not found!")
        }
    }

    pub fn get_current_collection_name(&self) -> String{
        self.current_collection.clone()
    }

    pub fn get_current_collection_mut(&mut self) -> &mut Collection{
        self.collections.get_mut(&self.current_collection).expect("Current collection missing..")
    }

    pub fn get_current_collection(&self) -> &Collection{
        self.collections.get(&self.current_collection).expect("Current collection missing..")
    }

    // Inserts Data
    pub fn put(&mut self, key: String, value: ValueObject) -> bool {
        self.get_current_collection_mut().put(key, value)
    }

    // Gets data
    pub fn get(&self, key: String) -> Result<&ValueObject, String> {
        self.get_current_collection().get(key)
    }

    pub fn incr(&mut self, key: String) -> Result<(), &str> {
        self.get_current_collection_mut().incr(key)
    }

    pub fn decr(&mut self, key: String) -> Result<(), &str> {
        self.get_current_collection_mut().decr(key)
    }

    // Displays all keys and values
    pub fn display_collection(&self) -> String {
        self.get_current_collection().display_collection()
    }

    pub fn get_all_collection_names(&self) -> String{
        let mut res: String = String::new();
        for (key, _) in self.collections.iter(){
            res += &key.clone();
            res += "\n";
        }
        res
    }
}
