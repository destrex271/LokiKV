use std::ops::Deref;

use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;

use crate::loki_kv::data_structures::hyperloglog::HLL;

#[derive(Parser)]
#[grammar = "./db/parser/lokiql.pest"]
pub struct LokiQLParser;

#[derive(Clone, Copy, Debug)]
pub enum QLCommands {
    SET,
    ADDHLL,
    GET,
    INCR,
    DECR,
    DISPLAY,
    CREATEHCOL,
    CREATEBCOL,
    CREATEBCUST,
    SELCOL,
    CURCOLNAME,
    LISTCOLNAMES,
    SHUTDOWN,
    COUNTHLL,
    PERSIST,
}

#[derive(Clone, Debug)]
pub enum QLValues {
    QLBool(bool),
    QLInt(isize),
    QLFloat(f64),
    QLString(String),
    QLId(String),
    QLCommand(QLCommands),
    QLPhantom,
    QLBlob(Vec<u8>),
    QLList(Vec<QLValues>),
    QLHLL(HLL),
}

#[derive(Debug)]
pub struct AST {
    val: QLValues,
    children: Vec<Box<AST>>, // Can only have 2 children at max
}

impl AST {
    fn new(val: QLValues) -> Self {
        let children: Vec<Box<AST>> = vec![];
        AST { val, children }
    }

    fn add_child(&mut self, val: QLValues) {
        println!("adding -> {:?}", val);
        let new_node = Box::new(AST::new(val));
        println!("new node -> {:?}", new_node);
        self.children.push(new_node);
    }

    pub fn get_value(&self) -> QLValues {
        self.val.clone()
    }

    pub fn get_left_child(&self) -> Option<&Box<AST>> {
        if self.children.len() == 0 {
            return None;
        }
        return self.children.get(0);
    }

    pub fn get_right_child(&self) -> Option<&Box<AST>> {
        if self.children.len() < 2 {
            return None;
        }
        return self.children.get(1);
    }

    pub fn get_left_child_mut(&mut self) -> Option<&mut Box<AST>> {
        if self.children.len() == 0 {
            return None;
        }
        return self.children.get_mut(0);
    }

    pub fn get_right_child_mut(&mut self) -> Option<&mut Box<AST>> {
        if self.children.len() < 2 {
            return None;
        }
        return self.children.get_mut(1);
    }
}

pub fn parse_lokiql(ql: &str) -> Vec<Option<AST>> {
    println!("Data -> {:?}", ql);
    let result = LokiQLParser::parse(Rule::LOKIQL_FILE, ql).unwrap();
    println!("{:?}", result);

    let mut asts: Vec<Option<AST>> = vec![];
    for pair in result {
        // println!("HERE -----> {:?}", pair);
        match pair.as_rule() {
            // Parse Each command
            Rule::COMMAND => {
                // println!("Sending {:?}", pair);
                let ast = parse_vals(pair, None);
                asts.push(ast);
            }
            // Rule::EOI => println!("End of File"),
            _ => {
                // println!("Something not for sending -> {:?}", pair)
            }
        }
    }

    return asts;
}

pub fn parse_individual_item_asql(pair: Pair<Rule>) -> QLValues {
    match pair.as_rule() {
        Rule::FLOAT => QLValues::QLFloat(pair.as_str().parse().unwrap()),
        Rule::INT => QLValues::QLInt(pair.as_str().parse().unwrap()),
        Rule::STRING => QLValues::QLString(pair.as_str().to_string()),
        Rule::BOOL => QLValues::QLBool(pair.as_str().parse().unwrap()),
        Rule::BLOB => {
            let mut val: String = pair.as_str().parse().unwrap();
            val = val.replace("<BLOB_BEINGS>", "");
            val = val.replace("<BLOB_ENDS>", "");
            QLValues::QLBlob(val.as_bytes().to_vec())
        }
        _ => panic!("primitive not added"),
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
                "ADDHLL" => {
                    println!("in set hll");
                    node = QLValues::QLCommand(QLCommands::ADDHLL);
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
                "HLLCOUNT" => {
                    node = QLValues::QLCommand(QLCommands::COUNTHLL);
                    ast_node.unwrap().add_child(node);
                    None
                }
                "/c_hcol" => {
                    node = QLValues::QLCommand(QLCommands::CREATEHCOL);
                    ast_node.unwrap().add_child(node);
                    None
                }
                "/c_bcol" => {
                    node = QLValues::QLCommand(QLCommands::CREATEBCOL);
                    ast_node.unwrap().add_child(node);
                    None
                }
                "/c_bcust" => {
                    node = QLValues::QLCommand(QLCommands::CREATEBCUST);
                    ast_node.unwrap().add_child(node);
                    None
                }
                "/selectcol" => {
                    node = QLValues::QLCommand(QLCommands::SELCOL);
                    ast_node.unwrap().add_child(node);
                    None
                }
                "PERSIST" => {
                    node = QLValues::QLCommand(QLCommands::PERSIST);
                    ast_node.unwrap().add_child(node);
                    None
                }
                _ => panic!("Command not supported yet!"),
            }
        }
        Rule::SOLO_COMMAND => match pair.as_str() {
            "DISPLAY" => {
                let node = QLValues::QLCommand(QLCommands::DISPLAY);
                ast_node.unwrap().add_child(node);
                None
            }
            "/getcur_colname" => {
                let node = QLValues::QLCommand(QLCommands::CURCOLNAME);
                ast_node.unwrap().add_child(node);
                None
            }
            "/listcolnames" => {
                let node = QLValues::QLCommand(QLCommands::LISTCOLNAMES);
                ast_node.unwrap().add_child(node);
                None
            }
            "SHUTDOWN" => {
                let node = QLValues::QLCommand(QLCommands::SHUTDOWN);
                ast_node.unwrap().add_child(node);
                None
            }
            _ => panic!("Support for command not added"),
        },
        Rule::FLOAT => {
            let node_val = QLValues::QLFloat(pair.as_str().parse().unwrap());
            ast_node.unwrap().add_child(node_val);
            // println!("Float here -> {:?}", pair.as_str());
            None
        }
        Rule::INT => {
            let node_val = QLValues::QLInt(pair.as_str().parse().unwrap());
            ast_node.unwrap().add_child(node_val);
            // println!("Int here -> {:?}", pair.as_str());
            None
        }
        Rule::STRING => {
            let node_val = QLValues::QLString(pair.as_str().to_string());
            ast_node.unwrap().add_child(node_val);
            // println!("String here -> {:?}", pair.as_str());
            None
        }
        Rule::BOOL => {
            let node_val = QLValues::QLBool(pair.as_str().parse().unwrap());
            ast_node.unwrap().add_child(node_val);
            // println!("Bool here -> {:?}", pair.as_str());
            None
        }
        Rule::BLOB => {
            let mut val: String = pair.as_str().parse().unwrap();
            val = val.replace("<BLOB_BEINGS>", "");
            val = val.replace("<BLOB_ENDS>", "");
            let node_val = QLValues::QLBlob(val.as_bytes().to_vec());
            ast_node.unwrap().add_child(node_val);
            None
        }
        Rule::ID => {
            let node_val = QLValues::QLId(pair.as_str().to_string());
            println!("Key -> {:?}", node_val);
            ast_node.unwrap().add_child(node_val);
            None
        }
        Rule::LIST => {
            let values: Vec<QLValues> = pair.into_inner().map(parse_individual_item_asql).collect();
            println!("Value -> {:?}", values);
            ast_node.unwrap().add_child(QLValues::QLList(values));
            None
        }
        Rule::EOI => None,
        Rule::COMMAND => {
            // println!("Command -> {:?}", pair);
            let mut pair_in = pair.clone().into_inner();
            let mut root = Box::new(AST::new(QLValues::QLPhantom));
            let mut root_ast = &mut root;
            if let Some(command) = pair_in.next() {
                parse_vals(command, Some(&mut root_ast));
                root_ast = root_ast.get_left_child_mut().unwrap();
                // println!("\nPARSED COMMAND\n next is {:?}", root_ast);
            };
            if let Some(key) = pair_in.next() {
                parse_vals(key, Some(root_ast));
                // println!("\nPARSED KEY\n next is {:?}", root_ast);
            };
            if let Some(value) = pair_in.next() {
                parse_vals(value, Some(root_ast));
                // println!("\nPARSED VALUE\n next is {:?}", root_ast);
            };
            return Some(*root);
        }
        _ => panic!("Something..."),
    }
}
