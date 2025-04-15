// To persist data on disk
use crate::loki_kv::loki_kv::CollectionProps;

const FILE_EXTENSION: &str = ".lktbl";

// Object to save collection to disk
pub struct Persistor{
    data: Box<dyn CollectionProps>,
    file_name: String
}

impl Persistor{
    fn new(data: &dyn CollectionProps, file_name: String) -> Persistor{
        Persistor{
            data: Box::new(data),
            file_name
        }
    }

    fn persist(&self){
        let data = self.data.display_collection();
        println!("{:?}", data);
    }
}

#[cfg(test)]
mod tests {
    use crate::loki_kv::loki_kv::Collection;

    use super::*;
    
    #[test]
    fn test_persistor_hmap_collection(){

        let mut dc = Collection::new();
        let data_map = vec![1,2,3,4,5,6,7];
        for val in data_map.iter(){
            dc.put(&val.to_string(), crate::loki_kv::loki_kv::ValueObject::IntData(val.clone()));
        }
        let my_persistor = Persistor::new(&dc, "hii".to_string());
        my_persistor.persist();
    }
}

