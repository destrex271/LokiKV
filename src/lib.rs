use core::{f32, panic};
use std::collections::{self, HashMap};
use std::mem;
use std::ptr::null;


// Root level memory, contains value data as bytes, does not know anything about the key
struct Page{
    data: Vec<Vec<u8>>,
    total_size: usize
}

impl Page{
    fn new() -> Self{
        let data = Vec::new();
        Page{
            data,
            total_size: 0 as usize
        }
    }

    fn add_value(&mut self, data: Vec<u8>) -> usize{
        self.data.push(data);
        return self.data.len() - 1;
    }

    fn get_value(&self, index: usize) -> Vec<u8>{
        let v = self.data[index].clone(); 
        v
    }
}

// Primary Store Structs to store data into page, uses a hashmap
pub struct Collection{
    map: HashMap<Vec<u8>, usize>,
    value_page: Page
}

trait ToConvBytes{
    fn to_bytestream(&self) -> Vec<u8>;
}

// Implementing Trait for all types
impl ToConvBytes for String{
    fn to_bytestream(&self) -> Vec<u8> {
        let mut data: Vec<u8> = Vec::new();
        let binding = self.to_owned().to_string();
        let bytes = binding.bytes();
        for byte in bytes.into_iter(){
            data.push(byte);
        }
        data
    }
}

impl ToConvBytes for isize{
    fn to_bytestream(&self) -> Vec<u8> {
        // self.to_string().to_bytestream()
        self.to_be_bytes().to_vec()
    }
}

impl ToConvBytes for usize{
    fn to_bytestream(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl ToConvBytes for f32{
    fn to_bytestream(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl ToConvBytes for f64{
    fn to_bytestream(&self) -> Vec<u8> {
        self.to_be_bytes().to_vec()
    }
}

impl ToConvBytes for bool{
    fn to_bytestream(&self) -> Vec<u8> {
        let mut vc: Vec<u8> = Vec::new();
        if *self{
            vc.push(1);
        }else{
            vc.push(0);
        }
        vc
    }
}

impl ToConvBytes for char{
    fn to_bytestream(&self) -> Vec<u8> {
        self.to_string().to_bytestream()
    }
}


impl Collection{
    fn new() -> Self{
        let map: HashMap<Vec<u8>, usize> = HashMap::new();
        let value_page = Page::new();
        Collection{
            map,
            value_page
        }
    }

    fn insertElement(&mut self, key: Vec<u8>, value: Vec<u8>) -> bool{
        let val_index = self.value_page.add_value(value);
        self.map.insert(key, val_index);
        return true;
    }

    fn getValue(&self, key: Vec<u8>) -> Vec<u8>{
        let index = self.map.get(&key).unwrap_or_else(|| panic!("FAILED AT RETRIEVING INDEX"));
        println!("Index is: {:?}", index);
        self.value_page.get_value(*index)
    }
}

// Primary Store

pub struct LokiKV{
    collection: Collection
}

impl LokiKV{
    pub fn new() -> Self{
        LokiKV{
           collection: Collection::new()
        }
    }

    pub fn put(&mut self, key: Vec<u8>, value: Vec<u8>){
        self.collection.insertElement(key, value);
    }

    pub fn put_generic<K:ToConvBytes, V:ToConvBytes>(&mut self, key: &K, value: &V){
        self.collection.insertElement(key.to_bytestream(), value.to_bytestream());
    }

    pub fn display_collection(&mut self){
        self.collection.display()
    }

    pub fn get_value<K: ToConvBytes>(&mut self, key: &K) -> String{
        let val = self.collection.getValue(key.to_bytestream());
        String::from_utf8(val).unwrap()
    }
}
