use lokikv::LokiKV;

fn main() {
    let mut store = LokiKV::new();
    let a: String = String::from("Hello");

    let mut values: Vec<isize> = Vec::new();

    for i in 1..=10000000{
        values.push(i);
    }

    let mut kv = LokiKV::new();
    let key = String::from("HEllo");
    // Check updates speed
    for val in values.into_iter(){
        println!("Adding: {:?}", val);
        let v = val.clone();
        let k = key.clone();
        kv.put_generic(&v, &v);
    }

    kv.display_collection();
}
