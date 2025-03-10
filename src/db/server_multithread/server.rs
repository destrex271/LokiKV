use crate::loki_kv::loki_kv::LokiKV;
use crate::parser::executor::Executor;
use crate::parser::parser::parse_lokiql;
use crate::server_multithread::serializer::serialize;
use crate::server_multithread::deserializer::deserialize;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    sync::{Arc, RwLock},
};
use tokio::io::{AsyncBufReadExt, BufReader, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

#[derive(Debug, Serialize, Deserialize)]
pub struct Request {
    query: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Response {
    result: Vec<String>,
}

// Server Logic
pub struct LokiServer {
    tcp_listener: TcpListener,
    host: String,
    port: u16,
    thread_count: usize,
    db_instance: Arc<RwLock<LokiKV>>,
}

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
        println!("Got {:?}", request_line);

        // Fix deserialization
        match deserialize(&request_line) {
            Ok(query) => {
                println!("Executing query: {:?}", query);

                let asts = parse_lokiql(&query.query);
                let mut ast_exector = Executor::new(db_instance.clone(), asts);
                let responses = ast_exector.execute();

                let mut resp_str = String::new();
                for response in responses.iter() {
                    if let val = response {
                        resp_str += &format!("{:?}\n", val);
                    };
                }

                resp_str += "<END_OF_RESPONSE>\n";
                println!("RESPONSE: {}", resp_str);
                let _ = wr.write_all(resp_str.as_bytes()).await;
                let _ = wr.flush().await;
            }
            Err(e) => {
                eprintln!("Deserialization error: {}", e);
                let _ = wr.write_all(b"Deserialization error\n").await;
                let _ = wr.flush().await;
            }
        }
    }
}

impl LokiServer {
    pub async fn new(host: String, port: u16, thread_count: usize) -> Self {
        let addr = format!("{}:{}", host, port);
        println!("Trying to start server at -> {}", addr);
        let tcp_listener = TcpListener::bind(addr).await.expect("Unable to create server");

        println!("Started Server at {}:{}", host, port);
        let db_instance = LokiKV::new();
        LokiServer {
            tcp_listener,
            host,
            port,
            thread_count,
            db_instance: Arc::new(RwLock::new(db_instance)),
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
                Err(e) => eprintln!("Error accepting connection: {}", e),
            };
        }
    }
}
