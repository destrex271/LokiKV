// To persist data on disk
use crate::loki_kv::loki_kv::CollectionProps;
use std::fs::{File, create_dir_all};
use std::fs;
use std::path::Path;

use super::loki_kv::ValueObject;

const FILE_EXTENSION: &str = ".lktbl";
const HARD_END_LIMIT: usize = 8000;

struct StoragePage{
    content: Vec<(String, ValueObject)>,
    chunk_start_idx:usize,
    pwd: String
}

impl StoragePage{
    fn new(content: Vec<(String, ValueObject)>, start_idx: usize, pwd: String) -> Self{
        StoragePage{
            content,
            chunk_start_idx: start_idx,
            pwd
        }
    }

    fn flush_to_disk(&self) {
        let path_disp = format!("{}/{}_{}.lqlpage", self.pwd, "chunk", self.chunk_start_idx);
        let path = Path::new(&path_disp);
        println!("Persisting to page at {}", path.display());

        let mut file = match File::create(&path) {
            Ok(file) => file, 
            Err(_) => panic!("failed to create")
        };

        match fs::write(path, format!("{:?}", self.content).as_bytes()){
            Err(err) => panic!("Error: {}", err),
            Ok(_) => println!("written to file")
        };
    }
}

// Object to save collection to disk
pub struct Persistor<T: CollectionProps>{
    data: Box<T>,
    directory_name: String
}

impl<T: CollectionProps> Persistor<T>{
    fn new(data: T, directory_name: String) -> Self{
        let _ = match create_dir_all(directory_name.clone()){
            Ok(_) => println!("Created new directory"),
            Err(..) => println!("got some error, lets ignore it for now...")
        };
        println!("created directory");
        Persistor{
            data: Box::new(data),
            directory_name
        }
    }

    fn persist(&self){
        let content = self.data.generate_pairs();
        println!("{:?}", content);

        let mut cnt = 0;
        for idx in (0..content.len()).step_by(HARD_END_LIMIT){
            let mut end_idx = idx + HARD_END_LIMIT;
            if end_idx >= content.len(){
                end_idx = content.len();
            }
            let cur_page = StoragePage::new(content[idx..end_idx].to_vec(), cnt, self.directory_name.clone());
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
    fn test_persistor_hmap_collection(){

        let mut dc = Collection::new();
        for val in (1..1000000){
            dc.put(&val.to_string(), crate::loki_kv::loki_kv::ValueObject::IntData(val.clone()));
        }
        let my_persistor = Persistor::new(dc.clone(), "hii".to_string());
        my_persistor.persist();
    }
}