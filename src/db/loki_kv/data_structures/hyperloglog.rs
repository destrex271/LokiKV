use std::{
    collections::HashMap,
    fmt::Debug,
    hash::{DefaultHasher, Hash, Hasher},
};

use crate::loki_kv::loki_kv::ValueObject;

const P_BITS: u32 = 16;
const M: usize = 2_i32.pow(P_BITS) as usize;

#[derive(Debug, Clone)]
pub struct HLL {
    // leading zeros -> Count of elements
    streams: Vec<usize>,
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn get_first_pbits(hashed_val: u64) -> u64 {
    let mask = (1 << P_BITS) - 1;
    hashed_val & mask
}

impl HLL {
    pub fn new() -> Self {
        let streams = vec![0; M];
        HLL { streams }
    }

    pub fn add_item<T: Debug + std::fmt::Display + Hash>(&mut self, entry: T) {
        let hashed_value = calculate_hash(&entry);
        let remaining_bits = hashed_value >> P_BITS;
        let leading_zeros = remaining_bits.leading_zeros() as usize + 1;
        let first_p_bits = get_first_pbits(hashed_value) as usize;

        if leading_zeros > self.streams[first_p_bits] {
            self.streams[first_p_bits] = leading_zeros;
        }
    }

    fn get_empty_registers(&self) -> usize {
        self.streams.iter().filter(|&&x| x == 0).count()
    }

    pub fn calculate_cardinality(&self) -> f64 {
        let mut sum = 0.0;

        for reg in 0..M {
            let reg_val = self.streams[reg];
            sum += 1.0 / (2_f64.powf(reg_val as f64));
        }

        // println!("indicator -> {}", indicator);

        let alpha = match M {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / M as f64),
        };

        let raw_estimate = (alpha * M as f64) / sum;

        if raw_estimate <= 2.5 * M as f64 {
            // Correction for small range
            println!("min threshold hit!");
            let v = self.get_empty_registers();
            if v != 0 {
                println!("started linear counting! {} {}", M, v);
                M as f64 * f64::ln(M as f64 / v as f64)
            } else {
                raw_estimate
            }
        } else if raw_estimate > (1.0 / 30.0) * 2_f64.powf(32.0) {
            // correction for large range
            println!("upper threshld..");
            -(2f64.powf(32.0)) * f64::ln(1.0 - raw_estimate / 2_f64.powf(32.0))
        } else {
            println!("no stuff");
            raw_estimate
        }
    }

    pub fn display_streams(&self) {
        for v in self.streams.iter() {
            println!("{}", v);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_hll() {
        let mut hll = HLL::new();
        let count = 800957;
        let low_lim = count as f64 - count as f64 * 0.05;
        let high_lim = count as f64 + count as f64 * 0.05;
        let mut large_test: Vec<String> = (0..count)
            .map(|i| String::from(format!("user_{}", i)))
            .collect();
        for entry in large_test.iter() {
            hll.add_item(entry);
        }
        // hll.display_streams();
        let cardinality = hll.calculate_cardinality();
        // println!("Result -> {} {}", cardinality, count);
        assert!(
            cardinality > low_lim && cardinality < high_lim,
            "{}",
            (cardinality - count as f64) / 100.0
        )
    }

    #[test]
    fn try_hll_non_unique() {
        let mut hll = HLL::new();
        let count = 800000;
        let low_lim = (1000.0) - (1000.0) * 0.05;
        let high_lim = (1000.0) + (1000.0) * 0.05;
        let large_test: Vec<String> = (0..count).map(|i| format!("user_{}", i % 1000)).collect();

        for entry in large_test.iter() {
            hll.add_item(entry);
        }

        let cardinality = hll.calculate_cardinality();
        println!(
            "Result -> {} {} {} {}",
            cardinality, count, low_lim, high_lim
        );
        assert!(
            cardinality > low_lim && cardinality < high_lim,
            "{} {} {} {} {}",
            cardinality,
            (cardinality - 1000.0) / 100.0,
            low_lim,
            high_lim,
            count / 1000
        );
    }
}
