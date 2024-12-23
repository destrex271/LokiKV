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

    let mut resp_str = String::new();
    // Improve output result
    for response in responses.iter() {
        if let val = response {
            resp_str += &format!("{:?}\n", val);
        };
    }
    println!("{}", resp_str);
    wr.write_all(resp_str.as_bytes()).await;
    Ok(())
}

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
