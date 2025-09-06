use tokio::sync::RwLock;

// To persist data on disk
use crate::loki_kv::loki_kv::{
    Collection, CollectionBTree, CollectionBTreeCustom, CollectionProps, LokiKV, ValueObject,
};
use crate::utils::{error, info, info_string};
use std::fs;
use std::fs::{create_dir_all, File};
use std::io::Write;
use std::path::Path;
use std::sync::Arc;

const FILE_EXTENSION: &str = ".lktbl";
const HARD_END_LIMIT: usize = 8000;

struct StoragePage {
    content: Vec<(String, ValueObject)>,
    chunk_start_idx: usize,
    pwd: String,
}

impl StoragePage {
    fn new(content: Vec<(String, ValueObject)>, start_idx: usize, pwd: String) -> Self {
        StoragePage {
            content,
            chunk_start_idx: start_idx,
            pwd,
        }
    }

    fn flush_to_disk(&self) {
        let path_disp = format!("{}/{}_{}.lqlpage", self.pwd, "chunk", self.chunk_start_idx);
        let path = Path::new(&path_disp);
        info_string(format!("Persisting to page at {}", path.display()));

        let mut file = match File::create(&path) {
            Ok(file) => file,
            Err(e) => panic!("failed to create {}", e),
        };

        let data = bincode::serialize(&self.content).unwrap();
        file.write_all(&data);
    }
}

// Object to save collection to disk
#[derive(Clone)]
pub struct Persistor {
    directory_name: String,
}

impl Persistor {
    pub fn new(directory_name: String) -> Self {
        let _ = match create_dir_all(directory_name.clone()) {
            Ok(_) => info("Created new directory"),
            Err(e) => println!("got some error, lets ignore it for now...{}", e),
        };
        Persistor { directory_name }
    }

    pub fn load_to_btree(&self, collection_name: String) -> (String, CollectionBTreeCustom) {
        let fin_path = format!("{}/{}", self.directory_name, collection_name);
        let dir = fs::read_dir(&fin_path).expect("Failed to read directory");

        let mut col = CollectionBTreeCustom::new();
        for entry in dir {
            let entry = entry.expect("Invalid directory entry");
            let path = entry.path();

            let bytes = fs::read(&path).expect("Unable to read file");
            let dc: Vec<(String, ValueObject)> =
                bincode::deserialize(&bytes).expect("Failed to deserialize bincode");

            col.bulk_put(dc);
        }

        return (collection_name, col);
    }

    pub fn load_to_btree_def(&self, collection_name: String) -> (String, CollectionBTree) {
        let fin_path = format!("{}/{}", self.directory_name, collection_name);
        let dir = fs::read_dir(&fin_path).expect("Failed to read directory");

        let mut col = CollectionBTree::new();
        for entry in dir {
            let entry = entry.expect("Invalid directory entry");
            let path = entry.path();

            let bytes = fs::read(&path).expect("Unable to read file");
            let dc: Vec<(String, ValueObject)> =
                bincode::deserialize(&bytes).expect("Failed to deserialize bincode");

            col.bulk_put(dc);
        }

        return (collection_name, col);
    }

    pub fn load_to_hmap(&self, collection_name: String) -> (String, Collection) {
        let fin_path = format!("{}/{}", self.directory_name, collection_name);
        let dir = fs::read_dir(&fin_path).expect("Failed to read directory");

        let mut col = Collection::new();
        for entry in dir {
            let entry = entry.expect("Invalid directory entry");
            let path = entry.path();

            let bytes = fs::read(&path).expect("Unable to read file");
            let dc: Vec<(String, ValueObject)> =
                bincode::deserialize(&bytes).expect("Failed to deserialize bincode");

            col.bulk_put(dc);
        }

        return (collection_name, col);
    }

    pub fn persist(&self, content: Vec<(String, ValueObject)>, collection_name: String) {
        let mut cnt = 0;
        let fin_path = self.directory_name.clone() + "/" + collection_name.as_str();
        let _ = match create_dir_all(fin_path.clone()) {
            Ok(_) => info("Created new directory"),
            Err(..) => error("got some error, lets ignore it for now..."),
        };
        for idx in (0..content.len()).step_by(HARD_END_LIMIT) {
            let mut end_idx = idx + HARD_END_LIMIT;
            if end_idx >= content.len() {
                end_idx = content.len();
            }
            info_string(format!("WRITING {}...", fin_path));
            let cur_page = StoragePage::new(content[idx..end_idx].to_vec(), cnt, fin_path.clone());
            cur_page.flush_to_disk();
            cnt += 1;
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::loki_kv::loki_kv::Collection;

    use super::*;

    #[test]
    fn test_persistor_hmap_collection() {
        let mut dc = Collection::new();
        for val in (1..1000000) {
            dc.put(
                &val.to_string(),
                crate::loki_kv::loki_kv::ValueObject::IntData(val.clone()),
            );
        }
        let my_persistor = Persistor::new("hii".to_string());
        my_persistor.persist(dc.generate_pairs(), "testCollection".to_string());
    }
}
