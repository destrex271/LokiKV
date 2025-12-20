mod loki_kv;
mod parser;
mod server_multithread;
mod utils;

use crate::server_multithread::server::LokiServer;

#[tokio::main]
async fn main() {
    let serv = LokiServer::new(16);
    serv.await.start_event_loop().await;
}
