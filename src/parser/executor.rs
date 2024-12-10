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
    pub fn execute(&mut self) -> Vec<Option<ValueObject>> {
        let db = &self.database;
        let mut responses: Vec<Option<ValueObject>> = vec![];
        for ast in self.asts.iter_mut() {
            let vc = match ast {
                Some(v) => v,
                None => panic!("Empty node"),
            };

            // Since we already know that we are going to get a phantom value
            if let Some(left_node) = vc.get_left_child() {
                let response = execute_rec(left_node, db, OpMode::Phantom);
                responses.push(response);
            };

            if let Some(right_node) = vc.get_right_child() {
                let response = execute_rec(right_node, db, OpMode::Phantom);
                responses.push(response);
            }
        }
        return responses;
    }
}

fn execute_rec(node: &AST, db: &Arc<RwLock<LokiKV>>, mode: OpMode) -> Option<ValueObject> {
    let val = node.get_value();
    let mut key = String::new();

    match val {
        QLValues::QLCommand(cmd) => {
            // println!("Command is -> {:?}", cmd);
            match cmd {
                QLCommands::SET => {
                    let key_node = node.get_left_child();
                    let value_node = node.get_right_child();

                    if let Some(node) = key_node {
                        execute_rec(node, db, OpMode::Phantom);
                    };

                    if let Some(node) = value_node {
                        execute_rec(node, db, OpMode::Write);
                    };
                    None
                }
                QLCommands::GET => {
                    let key_node = node.get_left_child();
                    let mut val: ValueObject = ValueObject::Phantom;

                    if let Some(node) = key_node {
                        execute_rec(node, db, OpMode::Read);
                        let ins = db.read().unwrap();
                        val = ins.get(key).unwrap().clone();
                        // println!("{:?}", val);
                    };
                    Some(val)
                }
                QLCommands::INCR => {
                    let key_node = node.get_left_child();

                    if let Some(node) = key_node {
                        execute_rec(node, db, OpMode::Read);
                        let mut ins = db.write().unwrap();
                        ins.incr(key);
                    };
                    None
                }
                QLCommands::DECR => {
                    let key_node = node.get_left_child();

                    if let Some(node) = key_node {
                        execute_rec(node, db, OpMode::Write);
                        let mut ins = db.write().unwrap();
                        ins.decr(key);
                    };
                    None
                }
                QLCommands::DISPLAY => {
                    let ins = db.read().unwrap();
                    let data = ins.display_collection();
                    Some(ValueObject::OutputString(data))
                }
            }
        }
        QLValues::QLKey(key_val) => {
            // println!("Key is -> {:?}", key_val);
            key = key_val;
            None
        }
        QLValues::QLBool(bool_val) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                ins.put(key, ValueObject::BoolData(bool_val));
                None
            }
            _ => {
                // println!("No need for writing");
                None
            }
        },
        QLValues::QLInt(int_v) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                ins.put(key, ValueObject::IntData(int_v));
                None
            }
            _ => {
                // println!("No need for writing");
                None
            }
        },
        QLValues::QLFloat(fl_v) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                ins.put(key, ValueObject::DecimalData(fl_v));
                None
            }
            _ => {
                // println!("No need for writing");
                None
            }
        },
        QLValues::QLString(st_v) => match mode {
            OpMode::Write => {
                let mut ins = db.write().unwrap();
                ins.put(key, ValueObject::StringData(st_v));
                None
            }
            _ => {
                // println!("No need for writing");
                None
            }
        },
        _ => {
            // println!("Value is -> {:?}", val);
            None
        }
    }
}
