use serde::ser::SerializeTupleVariant;

use super::loki_kv::{Collection, LokiKV};
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
// read-only function for collection that writes to a .lkvq file

const FILE_EXTENSION: &str = ".lkqv";

pub struct StorageEngine{
    DEFAULT_BASE_DIRECTORY: String,
    collection_name: String
}

impl StorageEngine{
    pub fn new(def_base_dir: String, collection_name: String) -> Self{
        StorageEngine{
            DEFAULT_BASE_DIRECTORY: def_base_dir,
            collection_name
        }
    }

    pub fn persist_hmap(&self, db_instance: Arc<RwLock<LokiKV>>){
        let cur_time = SystemTime::now();
        let epoch_time = cur_time
            .duration_since(UNIX_EPOCH)
            .expect("Time before epoch"); // Most unlikely
        let filename = format!("{}<{}>.lkvq", self.collection_name.clone(), epoch_time.as_nanos()).to_string();
        
        let db_read = db_instance.read().unwrap();

        let collection = db_read.get_collection_by_name(self.collection_name.clone());
        let data = collection.display_collection();
        println!("{} {}", filename.clone(), data);
    }
}