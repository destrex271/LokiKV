use bincode;
use serde::{Deserialize, Serialize};
use std::{
    fs::{create_dir_all, File},
    io::{Read, Write},
    path::Path,
};

use crate::utils::info_string;

#[derive(Serialize, Deserialize, Clone)]
pub struct ControlFile {
    last_wal_timeline: u64,
    last_checkpoint_id: u64,
    checkpoint_directory_path: String,
    wal_directory_path: String,
    current_leader_value: Option<u64>,
    self_identifier: Option<u64>,
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

    pub fn get_checkpoint_directory_path(&self) -> &str {
        &self.checkpoint_directory_path
    }

    pub fn get_current_leader_identifier(&self) -> Option<u64>{
        self.current_leader_value.clone()
    }

    pub fn get_self_identifier(&self) -> Option<u64>{
        self.self_identifier.clone()
    }

    pub fn set_current_leader_identifier(&mut self, current_leader_value: u64){
        self.current_leader_value = Some(current_leader_value.clone());
    }

    pub fn set_self_identifier(&mut self, id: u64) {
        self.self_identifier = Some(id.clone());
    }

    pub fn is_leader(&self) -> bool{
        if self.self_identifier.is_some() && self.current_leader_value.is_some(){
            if self.self_identifier.unwrap() == self.current_leader_value.unwrap(){
                return true;
            }
        }
        return false;
    }
    pub fn write(
        path: String,
        last_wal_timeline: u64,
        last_checkpoint_id: u64,
        checkpoint_directory_path: String,
        wal_directory_path: String,
    ) -> Result<ControlFile, String> {
        // Create the WAL and checkpoint directories
        let wal_dir = Path::new(&wal_directory_path);
        let checkpoint_dir = Path::new(&checkpoint_directory_path);
        match create_dir_all(wal_dir) {
            Ok(_) => info_string("Created WAL directory".to_string()),
            Err(err) => return Err(err.to_string()),
        }
        match create_dir_all(checkpoint_dir) {
            Ok(_) => info_string("Created checkpoint directory".to_string()),
            Err(err) => return Err(err.to_string()),
        }

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
                let op = toml::from_str::<ControlFile>(&buffer).unwrap();
                return Ok(op);
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
