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
    QLCommand(QLCommands),
    QLPhantom,
}

struct AST {
    val: QLValues,
    children: Vec<Box<AST>>,
}

impl AST {
    fn new(val: QLValues) -> Self {
        let children: Vec<Box<AST>> = vec![];
        AST { val, children }
    }

    fn add_child(&mut self, val: QLValues) {
        let new_node = Box::new(AST::new(val));
        self.children.push(new_node);
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

pub fn parse_vals(pair: Pair<Rule>, ast_node: Option<&mut Box<AST>>) {
    match pair.as_rule() {
        Rule::DUO_COMMAND => {
            println!("Duo command here -> {:?}", pair.as_str());
            let mut node = QLValues::QLPhantom;
            match pair.as_str() {
                "SET" => {
                    node = QLValues::QLCommand(QLCommands::SET);
                    ast_node.unwrap().add_child(node);
                }
                "GET" => {
                    node = QLValues::QLCommand(QLCommands::GET);
                    ast_node.unwrap().add_child(node);
                }
                "INCR" => {
                    node = QLValues::QLCommand(QLCommands::INCR);
                    ast_node.unwrap().add_child(node);
                }
                "DECR" => {
                    node = QLValues::QLCommand(QLCommands::DECR);
                    ast_node.unwrap().add_child(node);
                }
                _ => panic!("Command not supported yet!"),
            }
        }
        Rule::UNI_COMMAND => {
            println!("Uni command here -> {:?}", pair.as_str());
        }
        Rule::FLOAT => {
            println!("Float here -> {:?}", pair.as_str());
        }
        Rule::INT => {
            println!("Int here -> {:?}", pair.as_str());
        }
        Rule::STRING => {
            println!("String here -> {:?}", pair.as_str());
        }
        Rule::BOOL => {
            println!("Bool here -> {:?}", pair.as_str());
        }
        Rule::KEY => {
            println!("KEy here -> {:?}", pair.as_str());
        }
        Rule::COMMAND => {
            println!("Command -> {:?}", pair);
            let mut pair_in = pair.clone().into_inner();
            let mut root_ast = Box::new(AST::new(QLValues::QLPhantom));
            if let Some(command) = pair_in.next() {
                parse_vals(command, Some(&mut root_ast));
            };
            if let Some(key) = pair_in.next() {
                parse_vals(key, Some(&mut root_ast));
            };
            if let Some(value) = pair_in.next() {
                parse_vals(value, Some(&mut root_ast));
            };
        }
        _ => println!("Something..."),
    }
}
