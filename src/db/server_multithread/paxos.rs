use std::net::{SocketAddr};

use tokio::net::UdpSocket;

use crate::{loki_kv::{control::ControlFile, loki_kv::get_control_file_path}, utils::info_string};

pub struct ServiceManager {
    udp_socket_send: UdpSocket,
    udp_socket_recv: UdpSocket,
    BROADCAST_ADDRESS: SocketAddr,
}

impl ServiceManager {
    pub fn new() -> Self {
        let control_file = ControlFile::read_from_file_path(get_control_file_path()).unwrap();
        let listen_addr: SocketAddr = control_file.get_listen_addr().parse().unwrap();
        let std_socket = std::net::UdpSocket::bind(listen_addr).unwrap();

        let consume_addr: SocketAddr = control_file.get_consume_addr().parse().unwrap();
        let std_consumer_socket = std::net::UdpSocket::bind(consume_addr).unwrap();

        std_socket.set_broadcast(true);

        ServiceManager {
            udp_socket_send: UdpSocket::from_std(std_socket).unwrap(),
            udp_socket_recv: UdpSocket::from_std(std_consumer_socket).unwrap(),
            BROADCAST_ADDRESS: "255.255.255.255:8080".parse().unwrap(),
        }
    }

    pub async fn broadcast_message(&self, msg: &str) -> Result<(), String> {
        self.udp_socket_send.send_to(msg.as_bytes(), self.BROADCAST_ADDRESS).await.unwrap();
        Ok(())
    }

    pub async fn start_consumption(self) -> Result<(), ()> {
        loop{
            // TODO: Add consumption logic
            // Somehitng like a go-routine treatment here?
            let mut msg_bytes: Vec<u8> = vec![];
            self.udp_socket_recv.recv_from(&mut msg_bytes);

            tokio::spawn(
                async move {
                    // Log message
                    info_string(format!("Recieved the following message: {:?}", msg_bytes));
                }
            );

            break;
        }
        Ok(())
    }
}

pub struct PaxosNode {
    ctrl_file: ControlFile,
    service_manager: ServiceManager,
}

impl PaxosNode {
    pub fn propose(&self) {
        let value = self.ctrl_file.get_self_identifier();

        // Broadcast to network
        match value{
           Some(val) => {
               let msg = format!("PROPOSE {}", val);
               self.service_manager.broadcast_message(msg.as_str());
           },
           None => panic!("No value to broadcast!")
        }
    }

    pub fn accept() {}
    pub fn learn() {}
}
