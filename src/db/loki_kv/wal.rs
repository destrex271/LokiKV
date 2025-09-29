use std::collections::HashSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use crate::loki_kv::control::ControlFile;
use crate::loki_kv::loki_kv::ValueObject;

pub struct WALRecord {
    timestamp: u64,
    key: String,
    value: ValueObject,
}

impl WALRecord {
    fn new(timeline_id: u32, key: String, value: ValueObject) -> Self {
        WALRecord {
            timestamp: timeline_id,
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
    wal_records: HashSet<WALRecord>,
    cur_timeline: u32,
}

impl WALManager {
    fn new(ctrl_file_path: String) -> Self {
        let control_file: ControlFile = ControlFile::read_from_file_path(ctrl_file_path).unwrap();
        WALManager {
            control_file,
            wal_records: HashSet::new(),
            cur_timeline: 0,
        }
    }
}
