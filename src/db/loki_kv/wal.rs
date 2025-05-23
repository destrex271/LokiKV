use std::path::Path;
use std::io::Write;
use std::fs::File;
use std::time::SystemTime;
use std::fs::OpenOptions;

pub struct WALManager{
    filename: String,
    directory: String,
    full_path: Path,
    buffer: HashSet<usize>,
    DUMP_THRESHOLD: usize
}

struct WALObject{
    collection_name: String,
    key: String,
    value: String,
}

fn get_cur_timestamp_as_str() -> String {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => format!("1970-01-01 00:00:00 UTC"),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

fn dump_hash_set(buffer: HashSet<WALObject>)

impl WALManager{
    fn new(&self, instance_id: String, directory: String, dump_threshold: usize) -> WALManager{
        let filename = format!("{}_{}.wal",instance_id, get_cur_timestamp_as_str());

        let full_path = match directory.len(){
            0 => Path::new(format!("./{}", filename)),
            _ => Path::new(format!("{}/{}", directory, filename))
        };

        let hashset: HashSet<WALObject> = HashSet::new();
        WALManager{
            filename,
            directory,
            full_path,
            buffer: hashset,
            DUMP_THRESHOLD: dump_threshold
        }
    }
    fn append_to_log(&mut self, colname: String, key: String, value: String, typ: String) -> bool{
        self.hashset.insert(format!("[colname: {:?}\tkey: {:?}\tvalue: {:?}\ttype: {:?}]", colname, key, value, typ));    
        if hashset.len() > self.DUMP_THRESHOLD{
            dump_hash_set(self.hashset.clone());
            self.hashset.clear();
        }
    }

    fn read_log(&self) -> bool{}
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
