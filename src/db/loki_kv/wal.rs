use std::path::{PathBuf};
use std::io::Write;
use std::fs::File;
use std::time::SystemTime;
use std::fs::OpenOptions;
use std::collections::HashSet;

pub struct WALManager{
    filename: String,
    directory: String,
    full_path: PathBuf,
    buffer: HashSet<WALObject>,
    DUMP_THRESHOLD: usize
}

#[derive(Eq, Hash, PartialEq,Clone)]
struct WALObject{
    collection_name: String,
    key: String,
    value: String,
}

fn get_cur_timestamp_as_str() -> String {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => format!("{:?}", n),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

fn dump_hash_set(buffer: HashSet<WALObject>, collection_name: String) -> Result<String, String>{
    Ok(String::from("hello"))
}

impl WALManager{
    fn new(&self, instance_id: String, directory: String, dump_threshold: usize) -> WALManager{
        let filename = format!("{}_{}.wal",instance_id, get_cur_timestamp_as_str());

        let full_path = match directory.len(){
            0 => Path::new(format!("./{}", filename).as_str()),
            _ => Path::new(format!("{}/{}", directory, filename).as_str())
        };

        let hashset: HashSet<WALObject> = HashSet::new();
        WALManager{
            filename,
            directory,
            full_path: full_path.to_owned(),
            buffer: hashset,
            DUMP_THRESHOLD: dump_threshold
        }
    }
    fn append_to_log(&mut self, colname: String, key: String, value: String, typ: String) -> bool{
        self.buffer.insert(

        );
        self.buffer.insert(format!("[colname: {:?}\tkey: {:?}\tvalue: {:?}\ttype: {:?}]", colname, key, value, typ));    
        if self.buffer.len() > self.DUMP_THRESHOLD{
            dump_hash_set(self.buffer.clone(), colname);
            self.buffer.clear();
        }

        return true;
    }

    fn read_log(&self) -> bool{
        true
    }
}


mod tests {
    use super::*;

    #[test]
    fn try_wal() {
        let mut mgr = WALManager::new("dummy-wal", "");
        mgr.append_to_log("data", "12", "Int");
        mgr.append_to_log("data", "12", "Int");
        mgr.append_to_log("data", "12", "Int");
        mgr.append_to_log("data", "12", "Int");
        // validate file?
    }

}
