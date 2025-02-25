use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

use bit_set::BitSet;
const P_BITS: u32 = 4;
const M: usize = 2_i32.pow(P_BITS) as usize;

pub struct HLL {
    // leading zeros -> Count of elements
    streams: HashMap<u64, usize>
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn get_first_pbits(hashed_val: u64) -> u64{
    let mask = (1 << P_BITS) - 1;
    (hashed_val & mask)
}

impl HLL {
    pub fn new() -> Self {
        let streams = HashMap::new();
        HLL { streams }
    }

    pub fn add_item(&mut self, entries: Vec<String>) {
        for entry in entries.iter() {
            let hashed_value = calculate_hash(&entry);
            let leading_zeros = hashed_value.leading_zeros() as usize;
            let first_p_bits = get_first_pbits(hashed_value);
            
            println!("Leading zeros for {} : {} {}", entry, leading_zeros, hashed_value);
            
            let mut data = self.streams.get(&first_p_bits).unwrap_or(&0);
            if *data < leading_zeros{
                data = &leading_zeros;
            }
            self.streams.insert(first_p_bits, *data);
        }
    }

    fn get_empty_registers(&self) -> usize{
        M - self.streams.len() 
    }

    pub fn calculate_cardinality(&self) -> f64{
        let mut indicator = 0.0;
        let mut sum = 0.0;
        for (_, v) in self.streams.iter(){
            let val = 2i32.pow(*v as u32);
            sum += 1.0/(val as f64);
        }
        indicator = 1.0/sum;
        // println!("indicator -> {}", indicator);

        let mut alpha = 0.673;
        if M >= 128{
            alpha = 0.7213/(1.0 + 1.079/M as f64);
        }else if M >= 64{
            alpha = 0.709;
        }else if M >= 32{
            alpha = 0.697;
        }

        let mut fin_res = alpha * M as f64 * M as f64 * indicator;

        if fin_res <= 2.5 * M as f64{
            println!("min threshold hit!");
            let v = self.get_empty_registers();
            if v != 0{
                println!("started linear counting!");
                fin_res = M as f64 * f64::ln((M/v) as f64);
            }
        } else if fin_res > (1.0/30.0) * 2_f64.powf(32.0){
            println!("upper threshld..");
            fin_res = -(2f64.powf(32.0)) * f64::ln(1.0 - fin_res/2f64.powf(32.0));
        } else {
            println!("no changes..");
        }

        fin_res
    }

    pub fn display_streams(&self) {
        for (k, v) in self.streams.iter() {
            println!("{}: {}", k, v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_hll() {
        let mut hll = HLL::new();
        let count = 100000;
        let low_lim = count as f64 - count as f64 * 0.05;
        let mut large_test: Vec<String> = (0..count)
        .map(|i| String::from(format!("user_{}", i)))
        .collect(); 
        hll.add_item(large_test);
        hll.display_streams();
        let cardinality = hll.calculate_cardinality();
        println!("{}", cardinality);
        assert!(cardinality < count as f64 && cardinality > low_lim, "{}", format!("error rate -> {}", (count as f64 - cardinality)/(count as f64)));
    }
}
