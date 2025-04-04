mod loki_kv;
mod parser;
mod server_multithread;
use tokio::net::TcpListener;

use crate::server_multithread::server::LokiServer;

#[tokio::main]
async fn main() {
    let serv = LokiServer::new("localhost".to_string(), 8765, 16);
    serv.await.start_event_loop().await;
}
