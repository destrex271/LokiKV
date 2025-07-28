use crate::loki_kv::loki_kv::{LokiKV, ValueObject};
use crate::parser::executor::Executor;
use crate::parser::parser::parse_lokiql;
use std::env;
use std::time::{Duration, Instant};
use std::{
    ops::{Deref, DerefMut},
    sync::{Arc, RwLock},
};
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::select;
use tokio::time::{interval, sleep};
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
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    let mut buf = String::new();

    loop {
        buf.clear();
        let n = reader.read_line(&mut buf).await.unwrap();
        if n == 0 {
            println!("Connection closed!");
            return Err(String::from("connection closed"));
        }

        let request_line = buf.trim().to_string();
        // let request_line = String::from_utf8(buf[..n].to_vec())
        //     .map_err(|e| format!("Invalid UTF-8 data: {}", e))
        //     .unwrap();

        println!("Got {:?}", request_line);

        let asts = parse_lokiql(&request_line);
        let mut ast_exector = Executor::new(db_instance.clone(), asts);
        let responses = ast_exector.execute();

        let mut resp_str = String::new();
        // Improve output result
        for response in responses.iter() {
            if let val = response {
                resp_str += &format!("{:?}\n", val);
            };
        }

        resp_str += "<END_OF_RESPONSE>\n";
        println!("RESPONSE: {}", resp_str);
        let _ = wr.write_all(resp_str.as_bytes()).await;
        let _ = wr.flush().await;
        // println!("Wrote bytes {}", resp_str);
    }
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
        let checkpoint_itr: u64 = match env::var("CHECKPOINT_INTERVAL"){
            Ok(n) => n.parse().unwrap(),
            _ => 120 as u64
        };

        let mut checkpoint_timer = interval(Duration::from_secs(checkpoint_itr));


        loop {
            select! {
                accept_result = self.tcp_listener.accept() => {
                    match accept_result {
                        Ok((socket, _)) => {
                            let db = self.db_instance.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(socket, db).await {
                                    eprintln!("Error handling connection: {}", e);
                                }
                            });
                        }
                        Err(e) => {
                            eprintln!("Error accepting connection: {}", e);
                        }
                    }
                }

                _ = checkpoint_timer.tick() => {
                    println!("Checkpointing...");
                    let ins = self.db_instance.clone();
                    tokio::spawn(async move {
                        let db = ins.read().unwrap();
                        db.checkpoint();
                    });
                }
            }
        }
    }

    pub async fn start_checkpointing(&self) {
        loop{
            let ins = self.db_instance.read().unwrap();
            ins.checkpoint();
            sleep(Duration::from_secs(10)).await;
        }
    }
}
