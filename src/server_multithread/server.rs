use crate::loki_kv::loki_kv::{LokiKV, ValueObject};
use crate::parser::executor::Executor;
use crate::parser::parser::parse_lokiql;
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock},
};
use tokio::{
    io::{self, AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

// Server Logic
pub struct LokiServer {
    tcp_listener: TcpListener,
    host: String,
    port: u16,
    thread_count: usize,
    db_instance: Arc<RwLock<LokiKV>>,
}
//
async fn handle_connection(
    stream: TcpStream,
    db_instance: Arc<RwLock<LokiKV>>,
) -> Result<(), String> {
    println!("Starting handle....");
    let (mut rd, mut wr) = io::split(stream);
    let mut buf = vec![0; 128];
    let n = rd.read(&mut buf).await.unwrap();
    if n == 0 {
        println!("Connection closed!");
        return Err(String::from("connection closed"));
    }

    let request_line = String::from_utf8(buf[..n].to_vec())
        .map_err(|e| format!("Invalid UTF-8 data: {}", e))
        .unwrap();

    let asts = parse_lokiql(&request_line);
    let mut ast_exector = Executor::new(db_instance, asts);
    let responses = ast_exector.execute();

    let mut resp_str = String::from("----Query Results----\n");
    for response in responses.iter() {
        resp_str += &format!("{:?}\n", response);
    }
    println!("{}", resp_str);
    wr.write_all(resp_str.as_bytes()).await;
    Ok(())
}

// fn get_command_type_and_args(cmd_string: &str) -> Vec<&str> {
//     let mut tokens = cmd_string.split_whitespace();
//     let cmd = tokens.nth(0);

//     let op_type = match cmd {
//         Some("SET") => "wr",
//         Some("INCR") => "wr",
//         Some("DECR") => "wr",
//         _ => "rd",
//     };

//     let mut res = vec![op_type];
//     for (_, k) in tokens.enumerate() {
//         res.push(k);
//     }
//     res
// }

// // Uses a immutable reference for reads
// fn handle_db_command_rd(db_instance: &LokiKV, cmd_string: String) -> Result<String, String> {
//     let mut buffer_value = String::new();
//     let mut tokens = cmd_string.split_whitespace();
//     // println!("{:?}", tokens);
//     let cmd = tokens.nth(0);

//     match cmd.unwrap() {
//         "GET" => {
//             let key_wrap = tokens.nth(0);
//             let key = match key_wrap {
//                 Some(data) => data.to_string(),
//                 _ => "".to_string(),
//             };
//             if key.len() == 0 {
//                 panic!("No key provided");
//             }
//             let value = db_instance.get(key).unwrap();
//             match value {
//                 ValueObject::StringData(data) => buffer_value = format!("{data}"),
//                 _ => panic!("Unsupported data type"),
//             }
//         }
//         "ECHOBUFF" => {
//             println!("{:?}", buffer_value);
//         }
//         "DISPLAY" => {
//             db_instance.display_collection();
//         }
//         _ => println!("Not a valid command"),
//     }

//     Ok(buffer_value)
// }

// // Uses a mutable reference for writes
// fn handle_db_command_wr(db_instance: &mut LokiKV, cmd_string: String) -> Result<String, String> {
//     let mut buffer_value = String::new();
//     let mut tokens = cmd_string.split_whitespace();
//     // println!("{:?}", tokens);
//     let cmd = tokens.nth(0);

//     match cmd.unwrap() {
//         "SET" => {
//             // Check valid set command
//             let key: String = tokens.nth(0).unwrap().to_string();
//             buffer_value = format!("Key: {key}");
//             let val: String = tokens.nth(0).unwrap().to_string();

//             // Handle String type
//             if val.starts_with("'") && val.ends_with("'") {
//                 db_instance.put(key, ValueObject::StringData(val));
//             } else if val.eq("true") || val.eq("false") {
//                 db_instance.put(key, ValueObject::BoolData(val.eq("true")));
//             } else if val.contains(".") {
//                 let vl = val.parse::<f64>();
//                 match vl {
//                     Ok(data) => {
//                         db_instance.put(key, ValueObject::DecimalData(data));
//                     }
//                     _ => eprintln!("Data type not supported yet"),
//                 }
//             } else {
//                 let vl = val.parse::<isize>();
//                 match vl {
//                     Ok(data) => {
//                         db_instance.put(key, ValueObject::IntData(data));
//                     }
//                     _ => eprintln!("Data type not supported yet"),
//                 }
//             }
//         }
//         "ECHOBUFF" => {
//             println!("{:?}", buffer_value);
//         }
//         "INCR" => {
//             // Increases value at key
//             let key = tokens.nth(0).unwrap().to_string();
//             let resp = db_instance.incr(key);
//             match resp {
//                 Err(st) => eprintln!("{:?}", st),
//                 _ => print!(""),
//             }
//         }
//         "DECR" => {
//             let key = tokens.nth(0).unwrap().to_string();
//             let resp = db_instance.incr(key);
//             match resp {
//                 Err(st) => eprintln!("{:?}", st),
//                 _ => print!(""),
//             }
//         }
//         _ => println!("Not a valid command"),
//     }

//     Ok(buffer_value)
// }

impl LokiServer {
    pub async fn new(host: String, port: u16, thread_count: usize) -> Self {
        let addr = format!("{}:{}", host, port);
        println!("Trying to start server at -> {}", addr);
        let tcp_listener = TcpListener::bind(addr).await;

        match tcp_listener {
            Ok(tcp_list) => {
                println!("Started Sevrer at {}:{}", host, port);
                let db_instance = LokiKV::new();
                LokiServer {
                    tcp_listener: tcp_list,
                    host,
                    port,
                    thread_count,
                    db_instance: Arc::new(RwLock::new(db_instance)),
                }
            }
            Err(_) => {
                panic!("Unable to Create new Server at {}:{}", host, port);
            }
        }
    }

    pub async fn start_event_loop(&mut self) {
        loop {
            match self.tcp_listener.accept().await {
                Ok((socket, _)) => {
                    let db = self.db_instance.clone();

                    tokio::spawn(async move {
                        if let Err(e) = handle_connection(socket, db).await {
                            eprintln!("Error handling connection: {}", e);
                        }
                    });
                }
                _ => panic!("error accepting connection"),
            };
        }
    }
}
