use std::{
    collections::HashMap,
    hash::{DefaultHasher, Hash, Hasher},
};

const P_BITS: u32 = 16;
const M: usize = 2_i32.pow(P_BITS) as usize;

pub struct HLL {
    // leading zeros -> Count of elements
    streams: HashMap<u64, usize>,
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
        let streams = HashMap::new();
        HLL { streams }
    }

    pub fn add_item(&mut self, entries: Vec<String>) {
        for entry in entries.iter() {
            let hashed_value = calculate_hash(&entry);
            let remaining_bits = hashed_value >> P_BITS;
            let leading_zeros = remaining_bits.leading_zeros() as usize + 1;
            let first_p_bits = get_first_pbits(hashed_value);

            println!(
                "Leading zeros for {} : {} {}",
                entry, leading_zeros, hashed_value
            );

            let cur_max = *self.streams.get(&first_p_bits).unwrap_or(&0);
            if leading_zeros > cur_max {
                self.streams.insert(first_p_bits, leading_zeros);
            }
        }
    }

    fn get_empty_registers(&self) -> usize {
        M - self.streams.len()
    }

    pub fn calculate_cardinality(&self) -> f64 {
        let mut sum = 0.0;

        for reg in 0..M {
            let reg_val = *self.streams.get(&(reg as u64)).unwrap_or(&0);
            sum += 1.0 / (2_f64.powf(reg_val as f64));
        }

        // println!("indicator -> {}", indicator);

        let alpha = match M {
            16 => 0.673,
            32 => 0.697,
            64 => 0.709,
            _ => 0.7213 / (1.0 + 1.079 / M as f64),
        };

        let raw_estimate = (alpha * M as f64 * M as f64) / sum;

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
            -2f64.powf(32.0) * f64::ln(1.0 - raw_estimate / 2_f64.powf(32.0))
        } else {
            raw_estimate
        }
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
        println!("Result -> {}", cardinality);
    }
}
