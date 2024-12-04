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
                parse_vals(pair);
            }
            _ => {
                println!("Something not for sending -> {:?}", pair)
            }
        }
    }
}

pub fn parse_vals(pair: Pair<Rule>) {
    match pair.as_rule() {
        Rule::DUO_COMMAND => {
            println!("Duo command here -> {:?}", pair.as_str());
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
            if let Some(command) = pair_in.next() {
                parse_vals(command);
            };
            if let Some(key) = pair_in.next() {
                parse_vals(key);
            };
            if let Some(value) = pair_in.next() {
                parse_vals(value);
            };
        }
        _ => println!("Something..."),
    }
}
