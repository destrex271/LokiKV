use std::path::{PathBuf, Path};
use std::io::Write;
use std::fs::{File, OpenOptions};
use std::time::SystemTime;
use std::collections::HashSet;

use crate::loki_kv::loki_kv::ValueObject;

pub struct WALRecord{
    timeline_id: u64,
    key: String,
    value: ValueObject,
}

impl WALRecord{
    fn new(timeline_id: u64, key: String, value: ValueObject) -> Self{
        WALRecord { timeline_id: timeline_id, key: key, value: value}
    }
}


// ----------- WAL Record Manager ---------------------
// Responsible for routing WAL records to timeline buffer
// Once a timeline is flushed, the timeline reference is 
// kept for future reference.
// pub struct WALManager{
//     control_file: ControlFile,
// }

