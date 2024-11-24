use std::{io::{self, BufRead, BufReader, Read, Write}, net::{TcpListener, TcpStream}, ops::{Deref, DerefMut}, sync::{Arc, Mutex}};
use lokikv::LokiKV;
use crate::server_multithread::thread_pool::ThreadPool;

// Server Logic
pub struct LokiServer{
    tcp_listener: TcpListener,
    host: String,
    port: u16,
    thread_count: usize,
    db_instance: Arc<Mutex<LokiKV>>
}

pub trait LokiServerFunctions{
    fn new(host: String, port: u16, thread_count: usize) -> Self;
    fn start_event_loop(&mut self);
}

fn handle_connection(mut stream: TcpStream, db_instance: Arc<Mutex<LokiKV>>) {
    let buf_reader = BufReader::new(&mut stream);
    let request_line = buf_reader.lines().next().unwrap().unwrap();

    // Handle various commands
    println!("{request_line}");

    let mut db_instance = db_instance.lock().unwrap();
    let res = handle_db_command(db_instance.deref_mut(), request_line.clone()).unwrap();
    println!("{res}");
    
    let _ = stream.write_all(res.as_bytes());
}

fn handle_db_command(db_instance: &mut LokiKV, cmd_string: String) -> Result<String, String>{
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

impl LokiServerFunctions for LokiServer{
    fn new(host: String, port: u16, thread_count: usize) -> Self{
        let addr = format!("{}:{}", host, port);
        println!("Trying to start server at -> {}", addr);
        let tcp_listener = TcpListener::bind(addr);

        match tcp_listener{
            Ok(tcp_list) => {
                println!("Started Sevrer at {}:{}", host, port);
                let db_instance = LokiKV::new();
                LokiServer{
                    tcp_listener: tcp_list,
                    host,
                    port,
                    thread_count,
                    db_instance: Arc::new(Mutex::new(db_instance))
                }
            },
            Err(_) =>{
                panic!("Unable to Create new Server at {}:{}", host, port);
            }
        }
    }


    fn start_event_loop(&mut self){
        let pool = ThreadPool::new(self.thread_count);
        for stream in self.tcp_listener.incoming(){
            let sr = stream.unwrap();
            let ins = Arc::clone(&self.db_instance);
            pool.execute(move || {
                handle_connection(sr, ins);
            });
        }
        println!("Shutting Down")
    }
}
