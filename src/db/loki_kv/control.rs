use std::{fs::File, path::Path};

pub struct ControlFile{
    last_wal_timeline: u64,
    last_checkpoint_id: u64,
    checkpoint_directory_path: String,
    wal_directory_path: String,
}

impl ControlFile{
    fn write(path: String, last_wal_timeline: u64, last_checkpoint_id: u64, checkpoint_directory_path: String, wal_directory_path: String) -> Self{
        let file = ControlFile{
            last_wal_timeline,
            last_checkpoint_id,
            checkpoint_directory_path,
            wal_directory_path,
        };

        // Take lock on control file
        let path = Path::new(path.as_str());
        // Write to control file
        let file = match File::open(&path){
            Ok(_) => !("Opened Control File.."),
        };
    }
}
