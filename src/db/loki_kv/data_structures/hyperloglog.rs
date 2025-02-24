use std::{collections::HashMap, hash::{DefaultHasher, Hash, Hasher}};

use bit_set::BitSet;

pub struct HLL{
    // leading zeros -> Count of elements
    streams: HashMap<u32, usize>
}

fn calculate_hash<T: Hash>(t: &T) -> u64{
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

impl HLL{
    pub fn new() -> Self{
        let streams = HashMap::new();
        HLL{
            streams
        }
    }
    
    fn get_leading_zeros(&mut self, entry: String) -> u32{
        calculate_hash(&entry).leading_zeros()
    }

    pub fn add_item(&mut self, entries: Vec<String>){
        for entry in entries.iter(){
            let leading_zeros = self.get_leading_zeros(entry.clone());
            let data = self.streams.get(&leading_zeros).unwrap_or(&0);
            self.streams.insert(leading_zeros, *data + 1);
        }
    }

    pub fn display_streams(&self){
        for (k, v) in self.streams.iter(){
            println!("{}: {}", k, v);
        }
    }
}

#[cfg(test)]
mod tests{
    use super::*;

    #[test]
    fn try_hll() {
        let mut hll = HLL::new();
        let vec_data = vec!["Hello".to_string(), "Akshat".to_string(), "sdaysduyad".to_string()];
        hll.add_item(vec_data);
        hll.display_streams();
    }
}
