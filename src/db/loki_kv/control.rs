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
    pub fn get_next_checkpoint_id(&self) -> u64 {
        self.last_checkpoint_id + 1
    }
    pub fn get_next_timeline_id(&self) -> u64 {
        self.last_wal_timeline + 1
    }
    pub fn get_wal_directory_path(&self) -> &str {
        &self.wal_directory_path
    }
    pub fn write(
        path: String,
        last_wal_timeline: u64,
        last_checkpoint_id: u64,
        checkpoint_directory_path: String,
        wal_directory_path: String,
    ) -> Result<ControlFile, String> {
        let ctrl_file = ControlFile {
            last_wal_timeline,
            last_checkpoint_id,
            checkpoint_directory_path,
            wal_directory_path,
        };

        // Take lock on control file
        let path = Path::new(path.as_str());
        // Write to control file
        match File::create(&path) {
            Ok(mut file) => {
                let toml_string = toml::to_string(&ctrl_file).unwrap();
                println!("{}", toml_string);
                file.write_all(toml_string.as_bytes()).unwrap();
                Ok(ctrl_file)
            }
            Err(err) => return Err(err.to_string()),
        }
    }

    pub fn read_from_file_path(path_string: String) -> Result<ControlFile, String> {
        let path = Path::new(path_string.as_str());
        println!("{:?}", path);
        match File::open(&path) {
            Ok(mut file) => {
                // take a lock on the file
                let mut buffer = String::new();
                file.read_to_string(&mut buffer).unwrap();
                toml::from_str(&buffer).unwrap()
            }
            Err(err) => Err(format!("Failed to open file: {}", err)),
        }
    }

    pub fn set_new_params(&mut self, checkpoint_id: u64) {
        self.last_wal_timeline += 1;
        self.last_checkpoint_id = checkpoint_id;
        self.update();
    }

    fn update(&mut self) -> Result<(), String> {
        let path = Path::new(self.get_wal_directory_path());
        match File::create(&path) {
            Ok(mut file) => {
                let toml_string = toml::to_string(&self).unwrap();
                println!("{}", toml_string);
                file.write_all(toml_string.as_bytes()).unwrap();
                Ok(())
            }
            Err(err) => return Err(err.to_string()),
        }
    }
}
