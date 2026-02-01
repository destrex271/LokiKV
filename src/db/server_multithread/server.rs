use crate::loki_kv::control::ControlFile;
use crate::loki_kv::loki_kv::{get_control_file_path, LokiKV, ValueObject};
use crate::parser::executor::Executor;
use crate::parser::parser::parse_lokiql;
use crate::server_multithread::paxos::PaxosNode;
use crate::utils::{error_string, info, info_string, warning};
use rand;
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
    control_file: ControlFile,
}

fn decide_random_bool() -> bool {
    let x: u8 = rand::random();
    x % 2 == 0
}

//
async fn handle_connection(
    stream: TcpStream,
    db_instance: Arc<RwLock<LokiKV>>,
) -> Result<(), String> {
    info("Starting handle....");
    let (rd, mut wr) = stream.into_split();
    let mut reader = BufReader::new(rd);
    let mut buf = String::new();

    loop {
        buf.clear();
        let n = reader.read_line(&mut buf).await.unwrap();
        if n == 0 {
            warning("Connection closed!");
            return Err(String::from("connection closed"));
        }

        let request_line = buf.trim().to_string();
        // let request_line = String::from_utf8(buf[..n].to_vec())
        //     .map_err(|e| format!("Invalid UTF-8 data: {}", e))
        //     .unwrap();

        let mut resp_str = String::new();

        let asts = parse_lokiql(&request_line);
        if asts.len() == 0 {
            // Query was wrong.. lets tell it to the user
            resp_str += "Invalid command.. Pls try again\n";
        } else {
            let mut ast_exector = Executor::new(db_instance.clone(), asts);
            let responses = ast_exector.execute();

            // Improve output result
            for response in responses.iter() {
                if let val = response {
                    resp_str += &format!("{:?}\n", val);
                };
            }
        }

        resp_str += "<END_OF_RESPONSE>\n";

        wr.write_all(resp_str.as_bytes())
            .await
            .map_err(|e| format!("Failed to write response: {}", e))?;

        wr.flush()
            .await
            .map_err(|e| format!("Failed to flush writer: {}", e))?;

        info_string(format!("Sent response: {} bytes", resp_str));
    }
}

impl LokiServer {
    pub async fn new(thread_count: usize) -> Self {
        let control_file = ControlFile::read_from_file_path(get_control_file_path()).unwrap();
        let host: String = control_file.get_hostname();
        let port: u16 = control_file.get_port();
        let addr = format!(
            "{}:{}",
            control_file.get_hostname(),
            control_file.get_port()
        );
        info_string(format!("Trying to start server at -> {}", addr));
        let tcp_listener = TcpListener::bind(addr).await;

        match tcp_listener {
            Ok(tcp_list) => {
                info_string(format!("Started Sevrer at {}:{}", host, port));
                let db_instance = LokiKV::new();
                LokiServer {
                    tcp_listener: tcp_list,
                    host,
                    port,
                    thread_count,
                    db_instance: Arc::new(RwLock::new(db_instance)),
                    control_file: control_file,
                }
            }
            Err(_) => {
                panic!("Unable to Create new Server at {}:{}", host, port);
            }
        }
    }

    pub async fn start_event_loop(&mut self) {
        let checkpoint_itr: u64 = self.control_file.get_checkpoint_timer_interval();
        let paxos_itr: u64 = self.control_file.get_paxos_timer_interval();

        let mut checkpoint_timer = interval(Duration::from_secs(checkpoint_itr * 60));
        let mut paxos_gossip_broadcast_timer = interval(Duration::from_secs(paxos_itr * 30));
        // let mut paxos_gossip_consumer_timer = interval(Duration::from_secs(paxos_itr*60));

        let paxos_node: Arc<tokio::sync::RwLock<PaxosNode>> =
            Arc::new(tokio::sync::RwLock::new(PaxosNode::new_node()));

        let mut should_broadcast = decide_random_bool();

        loop {
            select! {
                accept_result = self.tcp_listener.accept() => {
                    match accept_result {
                        Ok((socket, _)) => {
                            let db = self.db_instance.clone();
                            tokio::spawn(async move {
                                if let Err(e) = handle_connection(socket, db).await {
                                    error_string(format!("Error handling connection: {}", e));
                                }
                            });
                        }
                        Err(e) => {
                            error_string(format!("Error accepting connection: {}", e));
                        }
                    }
                }


                _ = checkpoint_timer.tick() => {
                    info("Checkpointing...");
                    let ins = self.db_instance.clone();
                    tokio::spawn(async move {
                        let mut db = ins.write().unwrap();
                        db.checkpoint();
                    });
                }

                _ = paxos_gossip_broadcast_timer.tick() => {
                    let node = paxos_node.clone();
                    tokio::spawn( async move{
                        // Send out self information 10 times
                        if should_broadcast{
                            for _ in 0..10{
                                info("Gossiping self information with strangers!");
                                let res = node.write().await.gossip().await;
                                match res{
                                    Ok(_) => info("Successfully shared gossip information."),
                                    Err(err) => info_string(format!("Failed to share gossip information {}", err))
                                };
                            }
                            sleep(Duration::from_secs(10)).await;
                        }else{
                            // Consume gossip data
                            info("Consuming gossip from strangers..(for now)");
                            node.write().await.gossip_consume().await;
                        }

                    });
                    should_broadcast = !should_broadcast;
                }
            }
        }
    }
}
