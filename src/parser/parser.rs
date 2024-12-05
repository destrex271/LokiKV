use std::ops::Deref;

use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

#[derive(Parser)]
#[grammar = "./parser/lokiql.pest"]
pub struct LokiQLParser;

enum QLCommands {
    SET,
    GET,
    INCR,
    DECR,
}

enum QLValues {
    QLBool(bool),
    QLInt(isize),
    QLFloat(f64),
    QLString(String),
    QLKey(String),
    QLCommand(QLCommands),
    QLPhantom,
}

struct AST {
    val: QLValues,
    children: Vec<Box<AST>>, // Can only have 2 children at max
}

impl AST {
    fn new(val: QLValues) -> Self {
        let children: Vec<Box<AST>> = vec![];
        AST { val, children }
    }

    fn add_child(&mut self, val: QLValues) {
        if self.children.len() == 2 {
            eprintln!("Only 2 children allowed per node!")
        }
        let new_node = Box::new(AST::new(val));
        self.children.push(new_node);
    }

    fn get_left_child(&mut self) -> Option<&mut Box<AST>> {
        if self.children.len() == 0 {
            return None;
        }
        return self.children.get_mut(0);
    }

    fn get_right_child(&mut self) -> Option<&mut Box<AST>> {
        if self.children.len() < 2 {
            return None;
        }
        return self.children.get_mut(1);
    }
}

pub fn parse_lokiql(ql: &str) {
    println!("Data -> {:?}", ql);
    let result = LokiQLParser::parse(Rule::LOKIQL_FILE, ql).unwrap();
    println!("{:?}", result);
    for pair in result {
        println!("HERE -----> {:?}", pair);
        match pair.as_rule() {
            // Parse Each command
            Rule::COMMAND => {
                println!("Sending {:?}", pair);
                parse_vals(pair, None);
            }
            _ => {
                println!("Something not for sending -> {:?}", pair)
            }
        }
    }
}

pub fn parse_vals(pair: Pair<Rule>, ast_node: Option<&mut Box<AST>>) -> Option<AST> {
    match pair.as_rule() {
        Rule::DUO_COMMAND => {
            println!("Duo command here -> {:?}", pair.as_str());
            let mut node = QLValues::QLPhantom;
            match pair.as_str() {
                "SET" => {
                    node = QLValues::QLCommand(QLCommands::SET);
                    ast_node.unwrap().add_child(node);
                    None
                }

                _ => panic!("Command not supported yet!"),
            }
        }
        Rule::UNI_COMMAND => {
            let mut node = QLValues::QLPhantom;
            match pair.as_str() {
                "GET" => {
                    node = QLValues::QLCommand(QLCommands::GET);
                    ast_node.unwrap().add_child(node);
                    None
                }
                "INCR" => {
                    node = QLValues::QLCommand(QLCommands::INCR);
                    ast_node.unwrap().add_child(node);
                    None
                }
                "DECR" => {
                    node = QLValues::QLCommand(QLCommands::DECR);
                    ast_node.unwrap().add_child(node);
                    None
                }
                _ => panic!("Command not supported yet!"),
            }
        }
        Rule::FLOAT => {
            let node_val = QLValues::QLFloat(pair.as_str().parse().unwrap());
            ast_node.unwrap().add_child(node_val);
            println!("Float here -> {:?}", pair.as_str());
            None
        }
        Rule::INT => {
            let node_val = QLValues::QLInt(pair.as_str().parse().unwrap());
            ast_node.unwrap().add_child(node_val);
            println!("Int here -> {:?}", pair.as_str());
            None
        }
        Rule::STRING => {
            let node_val = QLValues::QLString(pair.as_str().to_string());
            ast_node.unwrap().add_child(node_val);
            println!("String here -> {:?}", pair.as_str());
            None
        }
        Rule::BOOL => {
            let node_val = QLValues::QLString(pair.as_str().parse().unwrap());
            ast_node.unwrap().add_child(node_val);
            println!("Bool here -> {:?}", pair.as_str());
            None
        }
        Rule::KEY => {
            let node_val = QLValues::QLKey(pair.as_str().to_string());
            ast_node.unwrap().add_child(node_val);
            println!("KEy here -> {:?}", pair.as_str());
            None
        }
        Rule::COMMAND => {
            println!("Command -> {:?}", pair);
            let mut pair_in = pair.clone().into_inner();
            let mut root = Box::new(AST::new(QLValues::QLPhantom));
            let mut root_ast = &mut root;
            if let Some(command) = pair_in.next() {
                parse_vals(command, Some(&mut root_ast));
                root_ast = root_ast.get_left_child().unwrap();
            };
            if let Some(key) = pair_in.next() {
                parse_vals(key, root_ast.get_left_child());
                root_ast = root_ast.get_left_child().unwrap();
            };
            if let Some(value) = pair_in.next() {
                parse_vals(value, root_ast.get_left_child().unwrap().get_left_child());
            };
            return Some(*root);
        }
        _ => panic!("Something..."),
    }
}
