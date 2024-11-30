mod server_multithread;
mod loki_kv;
use tokio::net::TcpListener;

use crate::server_multithread::server::LokiServer;

#[tokio::main]
async fn main(){
    let serv = LokiServer::new("localhost".to_string(), 8765, 16);
    serv.await.start_event_loop().await;
    // let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
    //
    // loop{
    //     // let (mut socket, _) = self.tcp_listener.accept().await.unwrap();
    //     let (mut socket, _) = listener.accept().await.unwrap();
    //     // let db = self.db_instance.clone();
    //
    //     tokio::spawn(async move{
    //         let mut buf = vec![0; 1024];
    //         // socket.read(&mut buf).await.unwrap();
    //         // socket.write_all("Hello".as_bytes()).await.unwrap();
    //     });
    // }
}
