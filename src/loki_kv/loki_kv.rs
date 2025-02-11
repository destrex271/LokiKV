use core::{f32, panic};
use std::any::{Any, TypeId};
use std::collections::{self, BTreeMap, HashMap};
use std::fmt::Debug;
use std::mem;
use std::ptr::null;

use super::btree::btree::BTree;

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

// Table structure with btree as internal store
pub struct CollectionBTree{
    store: BTreeMap<String, ValueObject>
}

impl CollectionBTree{
    fn new() -> Self{
        let store: BTreeMap<String, ValueObject> = BTreeMap::new();
        CollectionBTree{
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

// Custom BTree Implementation
pub struct CollectionBTreeCustom{
    store: BTree
}

impl CollectionBTreeCustom{
    fn new() -> Self{
        let store: BTree = BTree::new();
        CollectionBTreeCustom{
            store
        }
    }

    pub fn put(&mut self, key: String, value: ValueObject) -> bool {
        self.store.insert(key, value);
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

    // Gets data
    pub fn get(&self, key: String) -> Result<ValueObject, String> {
        let data = self.store.search(key);

        match data {
            Some(dt) => Ok(dt),
            None => Err("no data found!".to_string()),
        }
    }

    pub fn incr(&mut self, key: String) -> Result<(), &str> {
        let val = self.get(key.clone()).unwrap();

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
        let val = self.get(key.clone()).unwrap();

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
        return self.store.print_tree();
    }
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
    collections_hmap: HashMap<String, Collection>,
    collections_bmap: HashMap<String, Collection>,
    collections_bmap_cust: HashMap<String, Collection>,
    current_collection: String,
}

impl LokiKV {
    pub fn new() -> Self {
        let mut collections_hmap: HashMap<String, Collection> = HashMap::new();
        let mut collections_bmap: HashMap<String, Collection> = HashMap::new();
        let mut collections_bmap_cust: HashMap<String, Collection> = HashMap::new();
        collections_hmap.insert("default".to_string(), Collection::new());
        LokiKV {
            collections_hmap,
            collections_bmap,
            collections_bmap_cust,
            current_collection: "default".to_string()
        }
    }

    pub fn create_hmap_collection(&mut self, collection_name: String){
        self.collections_hmap.insert(collection_name, Collection::new());
    }

    pub fn create_bmap_collection(&mut self, collection_name: String){
        self.collections_bmap.insert(collection_name, Collection::new());
    }

    pub fn create_custom_bcol(&mut self, collection_name: String){
        self.collections_bmap_cust.insert(collection_name, Collection::new());
    }

    pub fn select_collection(&mut self, key: String){
        if self.collections_hmap.contains_key(&key){
            self.current_collection = key;
        }else if self.collections_bmap.contains_key(&key){
            self.current_collection = key;
        }else if self.collections_bmap_cust.contains_key(&key){
            self.current_collection = key;
        }else{
            panic!("Collection not found!")
        }
    }

    pub fn get_current_collection_name(&self) -> String{
        self.current_collection.clone()
    }

    pub fn get_current_collection_mut(&mut self) -> &mut Collection{
        match self.collections_hmap.get_mut(&self.current_collection){
            Some(x) => {
                x
            },
            None => {
                match self.collections_bmap.get_mut(&self.current_collection){
                    Some(x) => x,
                    None => {
                        match self.collections_bmap_cust.get_mut(&self.current_collection){
                            Some(x) => x,
                            None => panic!("Collection does not exist!")
                        }
                    } 
                }
            }
        }
    }

    pub fn get_current_collection(&self) -> &Collection{
        match self.collections_hmap.get(&self.current_collection){
            Some(x) => {
                x
            },
            None => {
                match self.collections_bmap.get(&self.current_collection){
                    Some(x) => x,
                    None => {
                        match self.collections_bmap_cust.get(&self.current_collection){
                            Some(x) => x,
                            None => panic!("Not found!")
                        }
                    }
                }
            }
        }
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
        for (key, _) in self.collections_hmap.iter(){
            res += &key.clone();
            res += "\n";
        }
        for (key, _) in self.collections_bmap.iter(){
            res += &key.clone();
            res += "\n";
        }
        for (key, _) in self.collections_bmap_cust.iter(){
            res += &key.clone();
            res += "\n";
        }
        res
    }
}
