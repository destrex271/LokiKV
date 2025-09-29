use bincode;
use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Write},
    path::Path,
};

#[derive(Serialize, Deserialize)]
pub struct ControlFile {
    last_wal_timeline: u64,
    last_checkpoint_id: u64,
    checkpoint_directory_path: String,
    wal_directory_path: String,
}

impl ControlFile {
    pub fn write(
        path: String,
        last_wal_timeline: u64,
        last_checkpoint_id: u64,
        checkpoint_directory_path: String,
        wal_directory_path: String,
    ) -> Result<String, String> {
        let ctrl_file = ControlFile {
            last_wal_timeline,
            last_checkpoint_id,
            checkpoint_directory_path,
            wal_directory_path,
        };

        // Take lock on control file
        let path = Path::new(path.as_str());
        // Write to control file
        match File::open(&path) {
            Ok(mut file) => {
                // take a lock on the file
                match File::try_lock(&file) {
                    Ok(_) => {
                        let vc: Vec<u8> = bincode::serialize(&ctrl_file).unwrap();
                        file.write_all(&vc);
                        Ok(format!("written to control file : {}", path.display()))
                    }
                    Err(err) => Err(err.to_string()),
                }
            }
            Err(err) => return Err(err.to_string()),
        }
    }

    pub fn read_from_file_path(path_string: String) -> Result<ControlFile, String> {
        let path = Path::new(path_string.as_str());
        match File::open(&path) {
            Ok(mut file) => {
                // take a lock on the file
                match File::try_lock(&file) {
                    Ok(_) => {
                        let mut buffer = Vec::new();
                        file.read_to_end(&mut buffer).unwrap();
                        let ctrl_file: ControlFile = bincode::deserialize(&buffer).unwrap();
                        println!("Read control file: {:?}", path_string);
                        Ok(ctrl_file)
                    }
                    Err(err) => return Err(format!("Failed to lock file: {}", err)),
                }
            }
            Err(err) => Err(format!("Failed to open file: {}", err)),
        }
    }
}
