use std::path::{PathBuf, Path};
use std::io::Write;
use std::fs::{File, OpenOptions};
use std::time::SystemTime;
use std::collections::HashSet;

pub struct WALManager {
    filename: String,
    directory: String,
    full_path: PathBuf,
    buffer: HashSet<WALObject>,
    DUMP_THRESHOLD: usize
}

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
struct WALObject {
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

fn dump_hash_set(buffer: HashSet<WALObject>, collection_name: String) -> Result<String, String> {
    let filename = format!("{}.wal", collection_name);
    println!("Dump data from {}", filename);
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&filename)
        .map_err(|e| format!("Failed to open file: {}", e))?;

    for obj in buffer.iter() {
        let line = format!(
            "collection: {}\tkey: {}\tvalue: {}\n",
            obj.collection_name, obj.key, obj.value
        );
        file.write_all(line.as_bytes())
            .map_err(|e| format!("Failed to write to file: {}", e))?;
    }

    Ok(filename)
}

impl WALManager {
    pub fn new(instance_id: &str, directory: &str, dump_threshold: usize) -> WALManager {
        let filename = format!("{}_{}.wal", instance_id, get_cur_timestamp_as_str());

        let path_str = if directory.is_empty() {
            format!("./{}", filename)
        } else {
            format!("{}/{}", directory, filename)
        };

        let full_path = Path::new(&path_str).to_path_buf();

        let hashset: HashSet<WALObject> = HashSet::new();
        WALManager {
            filename,
            directory: directory.to_string(),
            full_path: full_path.to_owned(),
            buffer: hashset,
            DUMP_THRESHOLD: dump_threshold,
        }
    }

    pub fn append_to_log(&mut self, colname: &str, key: &str, value: &str, _typ: &str) -> bool {
        self.buffer.insert(WALObject {
            collection_name: colname.to_string(),
            key: key.to_string(),
            value: value.to_string(),
        });

        println!("{:?}", self.buffer);

        if self.buffer.len() > self.DUMP_THRESHOLD {
            let _ = dump_hash_set(self.buffer.clone(), colname.to_string());
            self.buffer.clear();
        }

        true
    }

    pub fn read_log(&self) -> bool {
        // send data to db. ShouldI send with TCP or directly write?
        // how can we do that?
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_wal() {
        let mut mgr = WALManager::new("dummy-wal", "./", 3);
        println!("WAL TEST");
        mgr.append_to_log("data", "12", "Int", "insert");
        mgr.append_to_log("data1", "13", "Int", "insert");
        mgr.append_to_log("data2", "14", "Int", "insert");
        mgr.append_to_log("data3", "16", "Int", "insert");
    }
}

