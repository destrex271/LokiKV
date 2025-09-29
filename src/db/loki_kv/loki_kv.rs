use core::{f32, panic};
use std::any::{Any, TypeId};
use std::collections::{self, BTreeMap, HashMap};
use std::env::VarError;
use std::fmt::Debug;
use std::ptr::null;
use std::time::{SystemTime, UNIX_EPOCH};
use std::{env, mem};

use clap::builder::StringValueParser;
use serde::{Deserialize, Serialize};
use tokio::fs::File;

use crate::loki_kv::wal::WALManager;
use crate::utils::error_string;

use super::data_structures::btree::btree::BTree;
use super::data_structures::hyperloglog::HLL;
use super::persist::Persistor;
use paris::Logger;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ValueObject {
    StringData(String),
    IntData(isize),
    BoolData(bool),
    Phantom,
    DecimalData(f64),
    OutputString(String),
    BlobData(Vec<u8>),
    ListData(Vec<ValueObject>),
    #[serde(skip_serializing, skip_deserializing)]
    HLLPointer(HLL),
}

pub trait CollectionProps {
    fn new() -> Self
    where
        Self: Sized; // Move Sized to this method only
    fn put(&mut self, key: &str, value: ValueObject) -> bool;
    fn get(&self, key: &str) -> Option<&ValueObject>;
    fn key_exists(&self, key: &str) -> bool;
    fn incr(&mut self, key: &str) -> Result<(), &str>;
    fn decr(&mut self, key: &str) -> Result<(), &str>;
    fn display_collection(&self) -> String;
    fn generate_pairs(&self) -> Vec<(String, ValueObject)>;
    fn bulk_put(&mut self, pairs: Vec<(String, ValueObject)>);
}

// Table structure with btree as internal store
#[derive(Clone)]
pub struct CollectionBTree {
    store: BTreeMap<String, ValueObject>,
}

impl CollectionProps for CollectionBTree {
    fn new() -> Self {
        let store: BTreeMap<String, ValueObject> = BTreeMap::new();
        CollectionBTree { store }
    }

    fn put(&mut self, key: &str, value: ValueObject) -> bool {
        let stat = self.store.insert(key.to_string(), value);
        match stat {
            Some(stat) => true,
            None => false,
        }
    }

    fn bulk_put(&mut self, pairs: Vec<(String, ValueObject)>) {
        for a in pairs {
            self.put(a.0.as_str(), a.1);
        }
    }

    // Gets data
    fn get(&self, key: &str) -> Option<&ValueObject> {
        self.store.get(key)
    }

    fn key_exists(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    fn incr(&mut self, key: &str) -> Result<(), &str> {
        let val = self.store.get(key).unwrap();

        match val {
            ValueObject::IntData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::IntData(data + 1));
                Ok(())
            }
            ValueObject::DecimalData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::DecimalData(data + 1.0));
                Ok(())
            }
            _ => Err("incr not supported for data type"),
        }
    }

    fn decr(&mut self, key: &str) -> Result<(), &str> {
        let val = self.store.get(key).unwrap();

        match val {
            ValueObject::IntData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::IntData(data - 1));
                Ok(())
            }
            ValueObject::DecimalData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::DecimalData(data - 1.0));
                Ok(())
            }
            _ => Err("incr not supported for data type"),
        }
    }

    // Displays all keys and values
    fn display_collection(&self) -> String {
        let mut data = String::new();
        for (key, val) in self.store.iter() {
            data += &format!("{:?} -> {:?}", key, val);
        }
        return data;
    }

    // generate key-value pairs to write to pages
    fn generate_pairs(&self) -> Vec<(String, ValueObject)> {
        let mut data: Vec<(String, ValueObject)> = vec![];
        for (key, val) in self.store.iter() {
            data.push((key.to_string(), val.clone()));
        }
        return data;
    }
}

// Custom BTree Implementation
#[derive(Clone)]
pub struct CollectionBTreeCustom {
    store: BTree,
    option_val: Option<ValueObject>,
}

impl CollectionProps for CollectionBTreeCustom {
    fn new() -> Self {
        let store: BTree = BTree::new();
        CollectionBTreeCustom {
            store,
            option_val: None,
        }
    }

    fn put(&mut self, key: &str, value: ValueObject) -> bool {
        self.store.insert(key.to_string(), value);
        return true;
        // let stat = self.store.insert(key, value);
        // match stat {
        //     Some(stat) => {
        //         println!("{:?}", stat);
        //         true
        //     }
        //     None => false,
        // }
    }

    fn bulk_put(&mut self, pairs: Vec<(String, ValueObject)>) {
        for a in pairs {
            self.put(a.0.as_str(), a.1);
        }
    }

    fn key_exists(&self, key: &str) -> bool {
        match self.store.search(key.to_string()) {
            None => false,
            Some(_) => true,
        }
    }

    // Gets data
    fn get(&self, key: &str) -> Option<&ValueObject> {
        let data = match self.store.search(key.to_string()) {
            Some(value) => Some(value), // This won't work! See explanation below
            None => None,
        };

        data
    }

    fn incr(&mut self, key: &str) -> Result<(), &str> {
        let val = self.get(key.clone()).unwrap();

        match val {
            ValueObject::IntData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::IntData(data + 1));
                Ok(())
            }
            ValueObject::DecimalData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::DecimalData(data + 1.0));
                Ok(())
            }
            _ => Err("incr not supported for data type"),
        }
    }

    fn decr(&mut self, key: &str) -> Result<(), &str> {
        let val = self.get(key.clone()).unwrap();

        match val {
            ValueObject::IntData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::IntData(data - 1));
                Ok(())
            }
            ValueObject::DecimalData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::DecimalData(data - 1.0));
                Ok(())
            }
            _ => Err("incr not supported for data type"),
        }
    }

    // Displays all keys and values
    fn display_collection(&self) -> String {
        return self.store.print_tree();
    }

    fn generate_pairs(&self) -> Vec<(String, ValueObject)> {
        let mut data: Vec<(String, ValueObject)> = vec![];
        self.store.generate_pairs(0, data.as_mut());
        return data;
    }
}

// Equivalent to a table
#[derive(Clone)]
pub struct Collection {
    store: HashMap<String, ValueObject>,
}

impl CollectionProps for Collection {
    fn new() -> Self {
        let store: HashMap<String, ValueObject> = HashMap::new();
        Collection { store }
    }
    fn put(&mut self, key: &str, value: ValueObject) -> bool {
        let stat = self.store.insert(key.to_string(), value);
        match stat {
            Some(stat) => true,
            None => false,
        }
    }

    fn bulk_put(&mut self, pairs: Vec<(String, ValueObject)>) {
        for a in pairs {
            self.put(a.0.as_str(), a.1);
        }
    }

    fn key_exists(&self, key: &str) -> bool {
        self.store.contains_key(key)
    }

    // Gets data
    fn get(&self, key: &str) -> Option<&ValueObject> {
        let data = self.store.get(key);
        data
    }

    fn incr(&mut self, key: &str) -> Result<(), &str> {
        let val = self.store.get(key).unwrap();

        match val {
            ValueObject::IntData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::IntData(data + 1));
                Ok(())
            }
            ValueObject::DecimalData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::DecimalData(data + 1.0));
                Ok(())
            }
            _ => Err("incr not supported for data type"),
        }
    }

    fn decr(&mut self, key: &str) -> Result<(), &str> {
        let val = self.store.get(key).unwrap();

        match val {
            ValueObject::IntData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::IntData(data - 1));
                Ok(())
            }
            ValueObject::DecimalData(data) => {
                self.store
                    .insert(key.to_string(), ValueObject::DecimalData(data - 1.0));
                Ok(())
            }
            _ => Err("incr not supported for data type"),
        }
    }

    // Displays all keys and values
    fn display_collection(&self) -> String {
        let mut data = String::new();
        for (key, val) in self.store.iter() {
            data += &format!("{:?} -> {:?}", key, val);
        }
        return data;
    }

    // generate key-value pairs to write to pages
    fn generate_pairs(&self) -> Vec<(String, ValueObject)> {
        let mut data: Vec<(String, ValueObject)> = vec![];
        for (key, val) in self.store.iter() {
            data.push((key.to_string(), val.clone()));
        }
        return data;
    }
}

pub fn get_data_directory() -> String {
    match env::var("PERSIST_DIR") {
        Ok(s) => s,
        _ => "data".to_string(),
    }
}

pub fn get_checkpoint_directory() -> String {
    match env::var("CHECKPOINT_DIR") {
        Ok(s) => s,
        _ => "checkpoints".to_string(),
    }
}

pub fn get_current_timestamp() -> String {
    let now = SystemTime::now();
    let duration_since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
    let timestamp = duration_since_epoch.as_secs(); // u64
    let timestamp_str = timestamp.to_string();
    return timestamp_str;
}

pub fn get_current_timestamp_as_u64() -> u64 {
    let now = SystemTime::now();
    let duration_since_epoch = now.duration_since(UNIX_EPOCH).unwrap();
    let timestamp = duration_since_epoch.as_secs(); // u64
    return timestamp;
}

pub struct LokiKV {
    collections_hmap: HashMap<String, Collection>,
    collections_bmap: HashMap<String, CollectionBTree>,
    collections_bmap_cust: HashMap<String, CollectionBTreeCustom>,
    current_collection: String,
    wal_manager: WALManager,
}

impl LokiKV {
    pub fn new() -> Self {
        let mut collections_hmap: HashMap<String, Collection> = HashMap::new();
        let mut collections_bmap: HashMap<String, CollectionBTree> = HashMap::new();
        let mut collections_bmap_cust: HashMap<String, CollectionBTreeCustom> = HashMap::new();
        let control_file_path = "/home/akshat/lokikv/control.toml";
        collections_hmap.insert("default".to_string(), Collection::new());
        LokiKV {
            collections_hmap,
            collections_bmap,
            collections_bmap_cust,
            current_collection: "default".to_string(),
            wal_manager: WALManager::new_without_toml(),
        }
    }

    pub fn create_hmap_collection(&mut self, collection_name: String) {
        self.collections_hmap
            .insert(collection_name, Collection::new());
    }

    pub fn create_bmap_collection(&mut self, collection_name: String) {
        self.collections_bmap
            .insert(collection_name, CollectionBTree::new());
    }

    pub fn create_custom_bcol(&mut self, collection_name: String) {
        self.collections_bmap_cust
            .insert(collection_name, CollectionBTreeCustom::new());
    }

    pub fn append_custom_bcol(&mut self, collection_name: String, col: CollectionBTreeCustom) {
        self.collections_bmap_cust.insert(collection_name, col);
    }

    pub fn append_bcol(&mut self, collection_name: String, col: CollectionBTree) {
        self.collections_bmap.insert(collection_name, col);
    }

    pub fn append_hmap(&mut self, collection_name: String, col: Collection) {
        self.collections_hmap.insert(collection_name, col);
    }

    pub fn remove_collection(&mut self, collection_name: String) {
        self.collections_bmap.remove(collection_name.as_str());
        self.collections_bmap_cust.remove(collection_name.as_str());
        self.collections_hmap.remove(collection_name.as_str());
    }

    pub fn select_collection(&mut self, key: &str) {
        if self.collections_hmap.contains_key(key) {
            self.current_collection = key.to_string();
        } else if self.collections_bmap.contains_key(key) {
            self.current_collection = key.to_string();
        } else if self.collections_bmap_cust.contains_key(key) {
            self.current_collection = key.to_string();
        } else {
            panic!("Collection not found!")
        }
    }

    pub fn get_current_collection_name(&self) -> String {
        self.current_collection.clone()
    }

    pub fn get_current_collection_mut(&mut self) -> &mut dyn CollectionProps {
        self.get_collection_by_name_mut(&self.current_collection.clone())
    }

    pub fn get_collection_by_name(&self, name: &str) -> &dyn CollectionProps {
        if let Some(x) = self.collections_hmap.get(name) {
            return x;
        }

        if let Some(x) = self.collections_bmap.get(name) {
            return x;
        }

        if let Some(x) = self.collections_bmap_cust.get(name) {
            return x;
        }

        panic!("Collection does not exist!")
    }

    pub fn get_collection_by_name_mut(&mut self, name: &str) -> &mut dyn CollectionProps {
        if let Some(x) = self.collections_hmap.get_mut(name) {
            return x;
        }

        if let Some(x) = self.collections_bmap.get_mut(name) {
            return x;
        }

        if let Some(x) = self.collections_bmap_cust.get_mut(name) {
            return x;
        }

        error_string("Collection does not exist!".to_string());
        panic!()
    }

    pub fn get_current_collection(&self) -> &dyn CollectionProps {
        return self.get_collection_by_name(self.current_collection.clone().as_str());
    }

    // Inserts Data
    pub fn put(&mut self, key: &str, value: ValueObject) -> bool {
        self.get_current_collection_mut().put(key, value)
    }

    pub fn put_in_collection(&mut self, collection_name: &str, key: &str, value: ValueObject) {
        self.wal_manager
            .append_record(collection_name.to_string(), key.to_string(), value.clone());
        self.get_collection_by_name_mut(collection_name)
            .put(key, value);
    }

    // Gets data
    pub fn get(&self, key: &str) -> Option<&ValueObject> {
        self.get_current_collection().get(key)
    }

    pub fn incr(&mut self, key: &str) -> Result<(), &str> {
        self.get_current_collection_mut().incr(key)
    }

    pub fn decr(&mut self, key: &str) -> Result<(), &str> {
        self.get_current_collection_mut().decr(key)
    }

    // Displays all keys and values
    pub fn display_collection(&self) -> String {
        self.get_current_collection().display_collection()
    }

    pub fn get_all_collection_names(&self) -> String {
        let mut res: String = String::new();
        for (key, _) in self.collections_hmap.iter() {
            res += &key.clone();
            res += "\n";
        }
        for (key, _) in self.collections_bmap.iter() {
            res += &key.clone();
            res += "\n";
        }
        for (key, _) in self.collections_bmap_cust.iter() {
            res += &key.clone();
            res += "\n";
        }
        res
    }

    pub fn checkpoint(&mut self) {
        let checkpoint_id = get_current_timestamp_as_u64();

        for col in self.collections_bmap_cust.iter() {
            let pth = format!("{}/{}", get_checkpoint_directory(), checkpoint_id);
            let pairs = col.1.clone().generate_pairs();
            if pairs.len() == 0 {
                continue;
            }
            let persistor = Persistor::new(pth);
            persistor.persist(pairs, col.0.clone());
        }

        for col in self.collections_bmap.iter() {
            let pth = format!("{}/{}", get_checkpoint_directory(), checkpoint_id);
            let pairs = col.1.clone().generate_pairs();
            if pairs.len() == 0 {
                continue;
            }
            let persistor = Persistor::new(pth);
            persistor.persist(pairs, col.0.clone());
        }

        for col in self.collections_hmap.iter() {
            let pth = format!("{}/{}", get_checkpoint_directory(), checkpoint_id);
            let pairs = col.1.clone().generate_pairs();
            if pairs.len() == 0 {
                continue;
            }
            let persistor = Persistor::new(pth);
            persistor.persist(pairs, col.0.clone());
        }

        self.wal_manager.dump_records(checkpoint_id);
    }

    pub fn display_wal(&self) -> String {
        let res = self.wal_manager.display_wal();
        res
    }
}
