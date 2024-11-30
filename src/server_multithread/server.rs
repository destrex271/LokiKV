use std::{borrow::Borrow, ops::{Deref, DerefMut}, sync::{Arc, Mutex, RwLock}, time::Duration};
use crate::loki_kv::loki_kv::LokiKV;
// use crate::server_multithread::thread_pool::ThreadPool;
use rayon::{scope, ThreadPool, ThreadPoolBuilder};
use tokio::{io::{self, AsyncReadExt, AsyncWriteExt}, net::{TcpListener, TcpStream}, time::sleep};

// Server Logic
pub struct LokiServer{
    tcp_listener: TcpListener,
    host: String,
    port: u16,
    thread_count: usize,
    db_instance: Arc<RwLock<LokiKV>>
}
//
async fn handle_connection(stream: TcpStream, db_instance: Arc<RwLock<LokiKV>>) -> Result<(), String>{
    println!("Starting handle....");
    let (mut rd, mut wr) = io::split(stream);
    let mut buf = vec![0; 128];
    let n = rd.read(&mut buf).await.unwrap();
    if n == 0{
        println!("Connection closed!");
        return Err(String::from("connection closed"))
    }

    let request_line = String::from_utf8(buf[..n].to_vec())
        .map_err(|e| format!("Invalid UTF-8 data: {}", e)).unwrap();

    // Handle various commands
    println!("{request_line}");
    let cmd = get_command_type_and_args(&request_line);
    
    match cmd[0]{
        "wr" => {
            let mut ins = db_instance.write().unwrap();
            let res = handle_db_command_wr(ins.deref_mut(), request_line.clone()).unwrap();
            println!("{res}");
            let _ = wr.write_all(res.as_bytes());
            return Ok(())
        },
        "rd" => {
            let ins = db_instance.read().unwrap();
            let res = handle_db_command_rd(ins.deref(), request_line.clone()).unwrap();
            println!("{res}");
            let _ = wr.write_all(res.as_bytes());
            return Ok(())
        },
        _ => {
            Err("No opr defined!".to_string())
        }
    }
   
}

fn get_command_type_and_args(cmd_string: &str) -> Vec<&str>{
    let mut tokens = cmd_string.split_whitespace();
    let cmd = tokens.nth(0);

    let op_type = match cmd{
        Some("SET") => "wr",
        Some("INCR") => "wr",
        Some("DECR") => "wr",
        _ => "rd"
    };

    let mut res = vec![op_type];
    for (_, k) in tokens.enumerate(){
        res.push(k);
    }
    res

}

// Uses a immutable reference for reads
fn handle_db_command_rd(db_instance: &LokiKV, cmd_string: String) -> Result<String, String>{
    let mut buffer_value = String::new();
    let mut tokens = cmd_string.split_whitespace();
    // println!("{:?}", tokens);
    let cmd = tokens.nth(0);

    match cmd.unwrap(){
        "GET" => {
            let key_wrap = tokens.nth(0);
            let key = match key_wrap{
                Some(data) => data.to_string(),
                _ => "".to_string()
            };
            if key.len() == 0{
                panic!("No key provided");
            }
            let value = db_instance.get_value(&key);
            buffer_value = format!("{value}");
        },
        "ECHOBUFF" => {
            println!("{:?}", buffer_value);
        },
        "DISPLAY" => {
            db_instance.display_collection();
        }
        _ => println!("Not a valid command")
    }

    Ok(buffer_value)
}

// Uses a mutable reference for writes
fn handle_db_command_wr(db_instance: &mut LokiKV, cmd_string: String) -> Result<String, String>{
    let mut buffer_value = String::new();
    let mut tokens = cmd_string.split_whitespace();
    // println!("{:?}", tokens);
    let cmd = tokens.nth(0);

    match cmd.unwrap(){
        "SET" => {
            // Check valid set command
            let key: String = tokens.nth(0).unwrap().to_string();
            buffer_value = format!("Key: {key}");
            let val: String = tokens.nth(0).unwrap().to_string();
            db_instance.put_generic(&key, &val);
        },
        "GET" => {
            let key_wrap = tokens.nth(0);
            let key = match key_wrap{
                Some(data) => data.to_string(),
                _ => "".to_string()
            };
            if key.len() == 0{
                panic!("No key provided");
            }
            let value = db_instance.get_value(&key);
            buffer_value = format!("{value}");
        },
        "ECHOBUFF" => {
            println!("{:?}", buffer_value);
        },
        "INCR" => {
            // Increases value at key
            let key = tokens.nth(0).unwrap().to_string();
            let val = db_instance.get_value(&key);

            // Try to convert variable to integer
            match val.parse::<f64>(){
                Ok(n) => {
                    let dc: f64 = n + 1.0;
                    db_instance.put_generic(&key, &dc.to_string());
                },
                Err(err) => {
                    panic!("{:?}", err);
                }
            }
        },
        "DECR" => {
            let key = tokens.nth(0).unwrap().to_string();
            let val = db_instance.get_value(&key);

            // Try to convert variable to integer
            match val.parse::<f64>(){
                Ok(n) => {
                    // Update
                    let dc: f64 = n - 1.0;
                    println!("{dc}");
                    db_instance.put_generic(&key, &dc.to_string());
                },
                Err(err) => {
                    panic!("{:?}", err);
                }
            }

        },
        "DISPLAY" => {
            db_instance.display_collection();
        }
        _ => println!("Not a valid command")
    }

    Ok(buffer_value)
}

// async fn handle_conn_dummy(socket: TcpStream, db_instance: Arc<RwLock<LokiKV>>) {
//     println!("HI!");
//     let mut ins = db_instance.write().unwrap();
//     let res = handle_db_command_wr(ins.deref_mut(), String::from("SET data 12"));
//     println!("DONE!");
// }


impl LokiServer{
    pub async fn new(host: String, port: u16, thread_count: usize) -> Self{
        let addr = format!("{}:{}", host, port);
        println!("Trying to start server at -> {}", addr);
        let tcp_listener = TcpListener::bind(addr).await;

        match tcp_listener{
            Ok(tcp_list) => {
                println!("Started Sevrer at {}:{}", host, port);
                let db_instance = LokiKV::new();
                LokiServer{
                    tcp_listener: tcp_list,
                    host,
                    port,
                    thread_count,
                    db_instance: Arc::new(RwLock::new(db_instance))
                }
            },
            Err(_) =>{
                panic!("Unable to Create new Server at {}:{}", host, port);
            }
        }
    }


    pub async fn start_event_loop(&mut self){
        loop{
            match self.tcp_listener.accept().await{
                Ok((socket, _)) => {
                    let db = self.db_instance.clone();

                    tokio::spawn(async move{
                        if let Err(e) = handle_connection(socket, db).await{
                            eprintln!("Error handling connection: {}", e);
                        }
                    });
                },
                _ => panic!("error accepting connection")
            };
        }
    }
}
