use std::path::Path;
use std::io::Write;
use std::fs::File;
use std::time::SystemTime;

pub struct WALManager{
    filename: String,
    directory: String,
    full_path: Box<Path>
}

fn get_cur_timestamp_as_str() -> String {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => format!("1970-01-01 00:00:00 UTC"),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

impl WALManager{
    fn new(instance_id: String, directory: String) -> WALManager{
        let filename = format!("{}_{}.wal",instance_id, get_cur_timestamp_as_str());

        let full_path = match directory.len(){
            0 => Path::new(format!("./{}", filename)),
            _ => Path::new(format!("{}/{}", directory, filename))
        };

        WALManager{
            filename,
            directory,
            full_path
        }
    }
    fn append_to_log(&self, command: String) -> bool{
        let mut file: File = match File::options().append(true).open(self.full_path) {
            Ok(file) => file,
            Err(_) => panic!("failed to create"),
        };
        
        match writeln!(&mut file, "{}", command){
            Ok(a) => {
                println!("{:?}", a);
                return true;
            }
            Err(err) => false 
        }
    }
    fn read_log(&self) -> bool{
        false
    }
}

#[cfg(test)]
mod tests {
    use crate::loki_kv::wal::WALManager;
    use super::*;

    #[test]
    fn test_wal_write() {
        let file_content = vec!["SET data 12", "SET data1 <BLOB_BEGINS>blob data<BLOB_ENDS>"];
        let wal_mgr = WALManager::new(String::from("12"), String::from(""));

        for item in file_content{
            wal_mgr.append_to_log(String::from(item));
        }
    }
}
