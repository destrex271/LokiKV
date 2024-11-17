use std::net::TcpListener;

pub struct LokiServer{
    tcp_listener: TcpListener,
    host: String,
    port: u16
}

pub trait LokiServerFunctions{
    fn new(host: String, port: u16) -> Self;
    fn start_event_loop(&self);
}

impl LokiServerFunctions for LokiServer{
    fn new(host: String, port: u16) -> Self{
        let addr = format!("{}:{}", host, port);
        println!("Trying to start server at -> {}", addr);
        let tcp_listener = TcpListener::bind(addr);

        match tcp_listener{
            Ok(tcp_list) => {
                println!("Started Sevrer at {:?}:{:?}", host, port);
                LokiServer{
                    tcp_listener: tcp_list,
                    host,
                    port
                }
            },
            Err(_) =>{
                panic!("Unable to Create new Server at {:?}:{:?}", host, port);
            }
        }
    }


    fn start_event_loop(&self){
        for stream in self.tcp_listener.incoming(){
            let data = stream.unwrap();
            println!("Data: {:?}", data);
        }
    }
}
