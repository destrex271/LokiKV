use std::sync::{Arc, RwLock};

use crate::{
    loki_kv::loki_kv::{LokiKV, ValueObject},
    parser::parser::QLCommands,
};

use super::parser::{QLValues, AST};

pub enum OpMode {
    Read,
    Write,
    Phantom,
}

pub struct Executor {
    database: Arc<RwLock<LokiKV>>,
    asts: Vec<Option<AST>>,
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
                    None
                }
                QLCommands::GET => {
                    let key_node = node.get_left_child();
                    let mut val: ValueObject = ValueObject::Phantom;

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
                        val = ins.get(local_key).unwrap().clone();
                    };
                    Some(val)
                }
                QLCommands::CREATECOL => {
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
                        ins.create_collection(local_key);
                    };
                    None
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
                        ins.select_collection(local_key);
                    };
                    None
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
                        ins.incr(local_key);
                    };
                    None
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
                        ins.decr(local_key);
                    };
                    None
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
            }
        }
        QLValues::QLId(key_val) => Some(ValueObject::OutputString(key_val)),
        QLValues::QLBool(bool_val) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                match key {
                    Some(kv) => {
                        println!("setting {} to  {}", kv, bool_val);
                        ins.put(kv, ValueObject::BoolData(bool_val));
                    }
                    None => {}
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
                        ins.put(kv, ValueObject::IntData(int_v));
                    }
                    None => {}
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
                        ins.put(kv, ValueObject::DecimalData(fl_v));
                    }
                    None => {}
                }
                None
            }
            _ => None,
        },
        QLValues::QLString(st_v) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                match key {
                    Some(kv) => {
                        println!("setting {} to  {}", kv, st_v);
                        ins.put(kv, ValueObject::StringData(st_v));
                    }
                    None => {}
                }
                None
            }
            _ => None,
        },
        QLValues::QLBlob(data) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                match key {
                    Some(kv) => {
                        println!("setting {} to  {:?}", kv, data);
                        ins.put(kv, ValueObject::BlobData(data));
                    }
                    None => {}
                }
                None
            }
            _ => None,
        },
        _ => {
            // println!("Value is -> {:?}", val);
            None
        }
    }
}
