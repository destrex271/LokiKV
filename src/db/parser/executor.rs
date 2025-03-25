use std::process;
use std::sync::{Arc, RwLock};

use crate::loki_kv::data_structures::hyperloglog::HLL;
use crate::{
    loki_kv::loki_kv::{LokiKV, ValueObject},
    parser::parser::QLCommands,
};

use super::parser::{QLValues, AST};

pub enum OpMode {
    Read,
    Write,
    Phantom,
    Append,
    AppendHLL 
}

pub struct Executor {
    database: Arc<RwLock<LokiKV>>,
    asts: Vec<Option<AST>>,
}

fn convert_to_value_object(list_data: Vec<QLValues>) -> Vec<ValueObject>{
    let mut data: Vec<ValueObject> = vec![];
    for item in list_data{
        match item{
            QLValues::QLInt(a) => data.push(ValueObject::IntData(a)),
            QLValues::QLBool(a) => data.push(ValueObject::BoolData(a)),
            QLValues::QLFloat(a) => data.push(ValueObject::DecimalData(a)),
            QLValues::QLString(a) => data.push(ValueObject::StringData(a)),
            QLValues::QLBlob(a) => data.push(ValueObject::BlobData(a)),
            _ => println!("no conversions available")
        }
    }
    data
}

impl Executor {
    // Generates a new executor
    pub fn new(db: Arc<RwLock<LokiKV>>, asts: Vec<Option<AST>>) -> Self {
        Executor { database: db, asts }
    }

    // Execute AST
    pub fn execute(&mut self) -> Vec<ValueObject> {
        let db = &self.database;
        let mut responses: Vec<ValueObject> = vec![];
        for ast in self.asts.iter_mut() {
            let vc = match ast {
                Some(v) => v,
                None => panic!("Empty node"),
            };

            // Since we already know that we are going to get a phantom value
            if let Some(left_node) = vc.get_left_child() {
                let response_d = execute_rec(left_node, db, OpMode::Phantom, None);
                match response_d {
                    Some(res) => {
                        responses.push(res);
                    }
                    None => {}
                }
            };

            if let Some(right_node) = vc.get_right_child() {
                let response_d = execute_rec(right_node, db, OpMode::Phantom, None);
                match response_d {
                    Some(res) => {
                        responses.push(res);
                    }
                    None => {}
                }
            }
        }
        return responses;
    }
}

fn execute_rec(
    node: &AST,
    db: &Arc<RwLock<LokiKV>>,
    mode: OpMode,
    key: Option<String>,
) -> Option<ValueObject> {
    println!("{:?}", key);
    let val = node.get_value();
    let mut local_key = String::new();

    match val {
        QLValues::QLCommand(cmd) => {
            // println!("Command is -> {:?}", cmd);
            match cmd {
                QLCommands::SET => {
                    let key_node = node.get_left_child();
                    let value_node = node.get_right_child();

                    println!("Set {:?} {:?}", key_node, value_node);

                    if let Some(node) = key_node {
                        let v = execute_rec(node, db, OpMode::Phantom, None);
                        println!("{:?}", v);
                        match v {
                            Some(vc) => {
                                if let ValueObject::OutputString(val) = vc {
                                    local_key = val;
                                }
                            }
                            None => panic!("No Key!"),
                        }
                    };

                    if let Some(node) = value_node {
                        execute_rec(node, db, OpMode::Write, Some(local_key));
                    };
                    Some(ValueObject::OutputString("SET".to_string()))
                }
                QLCommands::ADDHLL => {
                    let key_node = node.get_left_child();
                    let value_node = node.get_right_child();

                    println!("Set {:?} {:?}", key_node, value_node);

                    if let Some(node) = key_node {
                        let v = execute_rec(node, db, OpMode::Phantom, None);
                        println!("{:?}", v);
                        match v {
                            Some(vc) => {
                                if let ValueObject::OutputString(val) = vc {
                                    local_key = val;
                                }
                            }
                            None => panic!("No Key!"),
                        }
                    };

                    if let Some(node) = value_node {
                        execute_rec(node, db, OpMode::AppendHLL, Some(local_key));
                    };
                    Some(ValueObject::OutputString("SET".to_string()))
                }
                QLCommands::COUNTHLL => {
                    let key_node = node.get_left_child();

                    let mut val: ValueObject = ValueObject::Phantom;
                    println!("getting node -> {:?}", key_node);

                    if let Some(node) = key_node {
                        let _ = match execute_rec(node, db, OpMode::Read, None) {
                            Some(vc) => {
                                if let ValueObject::OutputString(data) = vc {
                                    local_key = data
                                }
                            }
                            None => panic!("Unable to parse key"),
                        };
                        let ins = db.read().unwrap();
                        if let Some(vd) = ins.get(&local_key){
                            match vd{
                                ValueObject::HLLPointer(hll_obj) => {
                                    val = ValueObject::DecimalData(hll_obj.calculate_cardinality());
                                }
                                _ => val = ValueObject::OutputString("ERROR: Value is not of type HLL".to_string())
                            }
                        }
                    };
                    Some(val)
                }
                QLCommands::GET => {
                    let key_node = node.get_left_child();
                    let mut val: ValueObject = ValueObject::Phantom;
                    println!("getting node -> {:?}", key_node);

                    if let Some(node) = key_node {
                        let _ = match execute_rec(node, db, OpMode::Read, None) {
                            Some(vc) => {
                                if let ValueObject::OutputString(data) = vc {
                                    local_key = data
                                }
                            }
                            None => panic!("Unable to parse key"),
                        };
                        let ins = db.read().unwrap();
                        val = ins.get(&local_key).unwrap().clone();
                    };
                    Some(val)
                }
                QLCommands::CREATEBCOL => {
                    let table_node = node.get_left_child();

                    if let Some(node) = table_node {
                        let _ = match execute_rec(node, db, OpMode::Read, None) {
                            Some(vc) => {
                                if let ValueObject::OutputString(data) = vc {
                                    local_key = data
                                }
                            }
                            None => panic!("Unable to parse key"),
                        };
                        let mut ins = db.write().unwrap();
                        ins.create_bmap_collection(local_key);
                    };
                    Some(ValueObject::OutputString(
                        "CREATE B-TREE MAP COLLECTION".to_string(),
                    ))
                }
                QLCommands::CREATEBCUST => {
                    let table_node = node.get_left_child();

                    if let Some(node) = table_node {
                        let _ = match execute_rec(node, db, OpMode::Read, None) {
                            Some(vc) => {
                                if let ValueObject::OutputString(data) = vc {
                                    local_key = data
                                }
                            }
                            None => panic!("Unable to parse key"),
                        };
                        let mut ins = db.write().unwrap();
                        ins.create_custom_bcol(local_key);
                    };
                    Some(ValueObject::OutputString(
                        "CREATE CUSTOM B-TREE MAP COLLECTION".to_string(),
                    ))
                }
                QLCommands::CREATEHCOL => {
                    let table_node = node.get_left_child();

                    if let Some(node) = table_node {
                        let _ = match execute_rec(node, db, OpMode::Read, None) {
                            Some(vc) => {
                                if let ValueObject::OutputString(data) = vc {
                                    local_key = data
                                }
                            }
                            None => panic!("Unable to parse key"),
                        };
                        let mut ins = db.write().unwrap();
                        ins.create_hmap_collection(local_key);
                    };
                    Some(ValueObject::OutputString(
                        "CREATE CUSTOM H-MAP COLLECTION".to_string(),
                    ))
                }
                QLCommands::SELCOL => {
                    let table_node = node.get_left_child();
                    if let Some(node) = table_node {
                        let _ = match execute_rec(node, db, OpMode::Read, None) {
                            Some(vc) => {
                                if let ValueObject::OutputString(data) = vc {
                                    local_key = data
                                }
                            }
                            None => panic!("Unable to parse key"),
                        };
                        let mut ins = db.write().unwrap();
                        ins.select_collection(&local_key);
                    };
                    Some(ValueObject::OutputString("SELECT COLUMN".to_string()))
                }
                QLCommands::INCR => {
                    let key_node = node.get_left_child();

                    if let Some(node) = key_node {
                        let _ = match execute_rec(node, db, OpMode::Read, None) {
                            Some(vc) => {
                                if let ValueObject::OutputString(data) = vc {
                                    local_key = data
                                }
                            }
                            None => panic!("Unable to parse key"),
                        };
                        let mut ins = db.write().unwrap();
                        ins.incr(&local_key);
                    };
                    Some(ValueObject::OutputString("INCR".to_string()))
                }
                QLCommands::DECR => {
                    let key_node = node.get_left_child();

                    if let Some(node) = key_node {
                        let _ = match execute_rec(node, db, OpMode::Read, None) {
                            Some(vc) => {
                                if let ValueObject::OutputString(data) = vc {
                                    local_key = data
                                }
                            }
                            None => panic!("Unable to parse key"),
                        };
                        let mut ins = db.write().unwrap();
                        ins.decr(&local_key);
                    };
                    Some(ValueObject::OutputString("DECR".to_string()))
                }
                QLCommands::DISPLAY => {
                    let ins = db.read().unwrap();
                    let data = ins.display_collection();
                    Some(ValueObject::OutputString(data))
                }
                QLCommands::CURCOLNAME => {
                    let ins = db.read().unwrap();
                    let data = ins.get_current_collection_name();
                    Some(ValueObject::OutputString(data))
                }
                QLCommands::LISTCOLNAMES => {
                    let ins = db.read().unwrap();
                    let data = ins.get_all_collection_names();
                    Some(ValueObject::OutputString(data))
                }
                QLCommands::SHUTDOWN => {
                    process::exit(1);
                }
            }
        }
        QLValues::QLId(key_val) => Some(ValueObject::OutputString(key_val)),
        QLValues::QLList(list_value) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                match key {
                    Some(kv) => {
                        println!("setting {} to {:?}", kv, list_value);
                        ins.put(&kv, ValueObject::ListData(convert_to_value_object(list_value)));
                        None
                    },
                    _ => None
                }
            },
            // OpMode::AppendHLL => {
            //     let mut ins = db.write().unwrap();
            //     match key{
            //         Some(kv) => {
            //             // get value at key
            //             println!("HLL here -> {}", kv);
            //             if let ValueObject::HLLPointer(hll_obj) = ins.get(&kv).unwrap(){
            //                 hll_obj.add_item(entry);
            //                 ins.put(&kv, hll_obj);
            //             }else{
            //                 // Since hll pointer not found at key, we will change the value at that key
            //                 let mut hll = HLL::new();
            //                 hll.add_item(val);
            //                 ins.put(&kv, ValueObject::HLLPointer(hll));
            //             }
            //         }
            //     }
            //     None
            // }
            OpMode::Append => {
                let mut ins = db.write().unwrap();
                println!("Appending to hll...");
                match key {
                    Some(kv) => {
                        if let Some(cur_list) = ins.get(&local_key){
                            if let ValueObject::ListData(new_vec) = cur_list.clone(){
                                println!("new data -> {:?}", new_vec);
                            }
                        }
                        None
                    }
                    _ => None
                }
            }
            _ => None
        }
        QLValues::QLBool(bool_val) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                match key {
                    Some(kv) => {
                        println!("setting {} to  {}", kv, bool_val);
                        ins.put(&kv, ValueObject::BoolData(bool_val));
                    }
                    None => {}
                }
                None
            }
            OpMode::AppendHLL => {
                let mut ins = db.write().unwrap();
                match key{
                    Some(kv) => {
                        // get value at key
                        println!("HLL here -> {}", kv);
                        if let Some(vb) = ins.get(&kv){
                            match vb{
                                ValueObject::HLLPointer(hll_obj) => {
                                    let mut mhll_obj = hll_obj.clone();
                                    mhll_obj.add_item(bool_val);
                                    ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
                                }
                                _ => {}
                            }
                        }else{
                            // Since hll pointer not found at key, we will change the value at that key
                            let mut mhll_obj = HLL::new();
                            mhll_obj.add_item(bool_val);
                            ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
                        }

                    }
                    _ => {}
                }
                None
            }
            _ => None,
        },
        QLValues::QLInt(int_v) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                match key {
                    Some(kv) => {
                        println!("setting {} to  {}", kv, int_v);
                        ins.put(&kv, ValueObject::IntData(int_v));
                    }
                    _ => {},
                }
                None
            }
            OpMode::AppendHLL => {
                let mut ins = db.write().unwrap();
                match key{
                    Some(kv) => {
                        // get value at key
                        println!("HLL here -> {}", kv);
                        if let Some(vb) = ins.get(&kv){
                            match vb{
                                ValueObject::HLLPointer(hll_obj) => {
                                    let mut mhll_obj = hll_obj.clone();
                                    mhll_obj.add_item(int_v);
                                    ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
                                }
                                _ => {}
                            }
                        }else{
                            // Since hll pointer not found at key, we will change the value at that key
                            let mut mhll_obj = HLL::new();
                            mhll_obj.add_item(int_v);
                            ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
                        }

                    }
                    _ => {}
                }
                None
            }
            _ => None,
        },
        QLValues::QLFloat(fl_v) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                match key {
                    Some(kv) => {
                        println!("setting {} to  {}", kv, fl_v);
                        ins.put(&kv, ValueObject::DecimalData(fl_v));
                    }
                    _ => {}
                }
                None
            }
            // OpMode::AppendHLL => {
            //     let mut ins = db.write().unwrap();
            //     match key{
            //         Some(kv) => {
            //             // get value at key
            //             println!("HLL here -> {}", kv);
            //             if let Some(vb) = ins.get(&kv){
            //                 match vb{
            //                     ValueObject::HLLPointer(hll_obj) => {
            //                         let mut mhll_obj = hll_obj.clone();
            //                         mhll_obj.add_item(fl_v);
            //                         ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
            //                     }
            //                     _ => {}
            //                 }
            //             }else{
            //                 // Since hll pointer not found at key, we will change the value at that key
            //                 let mut mhll_obj = HLL::new();
            //                 mhll_obj.add_item(fl_v);
            //                 ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
            //             }

            //         }
            //         _ => {}
            //     }
            //     None
            // }
            _ => None,
        },
        QLValues::QLString(st_v) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                match key {
                    Some(kv) => {
                        println!("setting {} to  {}", kv, st_v);
                        ins.put(&kv, ValueObject::StringData(st_v));
                    }
                    _ => {}
                }
                None
            }
            OpMode::AppendHLL => {
                let mut ins = db.write().unwrap();
                match key{
                    Some(kv) => {
                        // get value at key
                        println!("HLL here -> {}", kv);
                        if let Some(vb) = ins.get(&kv){
                            match vb{
                                ValueObject::HLLPointer(hll_obj) => {
                                    let mut mhll_obj = hll_obj.clone();
                                    mhll_obj.add_item(st_v);
                                    ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
                                }
                                _ => {}
                            }
                        }else{
                            // Since hll pointer not found at key, we will change the value at that key
                            let mut mhll_obj = HLL::new();
                            mhll_obj.add_item(st_v);
                            ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
                        }

                    }
                    _ => {}
                }
                None
            }
            _ => None            
        },
        QLValues::QLBlob(data) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                match key {
                    Some(kv) => {
                        println!("setting {} to  {:?}", kv, data);
                        ins.put(&kv, ValueObject::BlobData(data));
                    }
                    _ => {}
                }
                None
            }
            // OpMode::AppendHLL => {
            //     let mut ins = db.write().unwrap();
            //     match key{
            //         Some(kv) => {
            //             // get value at key
            //             println!("HLL here -> {}", kv);
            //             if let Some(vb) = ins.get(&kv){
            //                 match vb{
            //                     ValueObject::HLLPointer(hll_obj) => {
            //                         let mut mhll_obj = hll_obj.clone();
            //                         mhll_obj.add_item(data);
            //                         ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
            //                     }
            //                     _ => {}
            //                 }
            //             }else{
            //                 // Since hll pointer not found at key, we will change the value at that key
            //                 let mut mhll_obj = HLL::new();
            //                 mhll_obj.add_item(data);
            //                 ins.put(&kv, ValueObject::HLLPointer(mhll_obj));
            //             }

            //         }
            //         _ => {}
            //     }
            //     None
            // }
            
            _ => None,
        },
        _ => {
            // println!("Value is -> {:?}", val);
            None
        }
    }
}
