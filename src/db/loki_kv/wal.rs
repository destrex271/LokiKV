use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use serde::{Deserialize, Serialize};

use crate::loki_kv::control::ControlFile;
use crate::loki_kv::loki_kv::ValueObject;

#[derive(Serialize, Deserialize)]
pub struct WALRecord {
    timestamp: u64,
    collection_name: String,
    key: String,
    value: ValueObject,
}

impl WALRecord {
    fn new(timestamp: u64, collection_name: String, key: String, value: ValueObject) -> Self {
        WALRecord {
            timestamp: timestamp,
            collection_name: collection_name,
            key: key,
            value: value,
        }
    }
}

// ----------- WAL Record Manager ---------------------
// Responsible for routing WAL records to timeline buffer
// Once a timeline is flushed, the timeline reference is
// kept for future reference.
pub struct WALManager {
    control_file: ControlFile,
    wal_records: Vec<WALRecord>,
    cur_timeline: u64,
}

impl WALManager {
    pub fn new(ctrl_file_path: String) -> Self {
        let control_file: ControlFile = ControlFile::read_from_file_path(ctrl_file_path).unwrap();
        let timeline = control_file.get_next_timeline_id();
        WALManager {
            control_file,
            wal_records: Vec::new(),
            cur_timeline: timeline,
        }
    }

    pub fn new_without_toml() -> Self {
        let control_file = ControlFile::write(
            "/home/akshat/lokikv/control.toml".to_string(),
            0 as u64,
            0 as u64,
            "/home/akshat/lokikv/checkpoints".to_string(),
            "/home/akshat/lokikv/wal".to_string(),
        )
        .unwrap();
        let timeline = control_file.get_next_timeline_id();
        WALManager {
            control_file,
            wal_records: Vec::new(),
            cur_timeline: timeline,
        }
    }

    pub fn append_record(&mut self, collection_name: String, key: String, value: ValueObject) {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap();
        let record = WALRecord::new(now.as_secs(), collection_name, key, value);
        self.wal_records.push(record);
    }

    pub fn dump_records(&mut self, checkpoint_id: u64) {
        let wal_file_path = format!(
            "{}/{}.wal",
            self.control_file.get_wal_directory_path(),
            self.cur_timeline.to_string()
        );
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(false)
            .open(wal_file_path)
            .unwrap();
        for record in &self.wal_records {
            let data = bincode::serialize(record).unwrap();
            file.write_all(&data).unwrap();
        }

        self.wal_records.clear();
        self.control_file.set_new_params(checkpoint_id);
    }

    pub fn replay_records(&self) -> Result<Vec<(String, ValueObject)>, String> {
        let wal_file_path = format!(
            "{}/{}.wal",
            self.control_file.get_wal_directory_path(),
            self.cur_timeline.to_string()
        );
        let mut file = OpenOptions::new().read(true).open(wal_file_path).unwrap();
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let mut records: Vec<(String, ValueObject)> = Vec::new();

        for line in buffer.lines() {
            match bincode::deserialize(line.as_bytes()) {
                Ok(record) => records.push(record),
                Err(e) => return Err(format!("Failed to deserialize WAL record: {}", e)),
            }
        }

        Ok(records)
    }

    pub fn display_wal(&self) -> String {
        let wal_file_path = format!(
            "{}/{}.wal",
            self.control_file.get_wal_directory_path(),
            self.cur_timeline.to_string()
        );
        let mut file = OpenOptions::new().read(true).open(wal_file_path).unwrap();
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        buffer
    }
}
