use std::Path;

pub struct WALManager{
    filename: String,
    directory: String,
    full_path: Path
}

fn get_cur_timestamp_as_str() -> String {
    match SystemTime::now().duration_since(SystemTime::UNIX_EPOCH) {
        Ok(n) => format!("1970-01-01 00:00:00 UTC"),
        Err(_) => panic!("SystemTime before UNIX EPOCH!"),
    }
}

impl WALManager{
    fn new(&self, instance_id: String, directory: String) -> WALManager{
        let filename = format!("{}_{}.wal",instance_id, get_cur_timestamp_as_str());

        let full_path = match self.directory.len(){
            0 => Path::new(format("./{}", filename)),
            _ => Path::new(format!("{}/{}", self.directory, filename))
        };

        WALManager{
            filename,
            directory,
            full_path
        }
    }
    fn append_to_log(&self, command: String) -> bool{

    }
    fn read_log(&self) -> bool{}
}