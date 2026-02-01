use local_ip_address::local_ip;
use std::{
    collections::{HashMap, HashSet},
    io::Split,
    net::SocketAddr,
};

use tokio::time::Duration;
use tokio::{net::UdpSocket, time::timeout};

use crate::{
    loki_kv::{control::ControlFile, loki_kv::get_control_file_path},
    utils::{info_string, warning, warning_string},
};

// ---------------------------- SERVICE MANAGER -------------------------------------------

pub struct ServiceManager {
    udp_socket_send: UdpSocket,
    udp_socket_recv: UdpSocket,
    node_directory: HashSet<(String, String)>, // Hashset of node_id, address(ip + port)
    BROADCAST_ADDRESS: SocketAddr,
}

impl ServiceManager {
    pub fn new() -> Self {
        let control_file = ControlFile::read_from_file_path(get_control_file_path()).unwrap();
        let listen_addr: SocketAddr = control_file.get_send_addr().parse().unwrap();
        let std_socket = std::net::UdpSocket::bind(listen_addr).unwrap();

        let consume_addr: SocketAddr = control_file.get_consume_addr().parse().unwrap();
        let std_consumer_socket = std::net::UdpSocket::bind(consume_addr).unwrap();

        let mut node_directory: HashSet<(String, String)> = HashSet::new();

        std_socket.set_broadcast(true);

        ServiceManager {
            udp_socket_send: UdpSocket::from_std(std_socket).unwrap(),
            udp_socket_recv: UdpSocket::from_std(std_consumer_socket).unwrap(),
            node_directory: node_directory,
            BROADCAST_ADDRESS: "255.255.255.255:8080".parse().unwrap(),
        }
    }

    pub async fn broadcast_message(&self, msg: &str) -> Result<(), String> {
        self.udp_socket_send
            .send_to(msg.as_bytes(), self.BROADCAST_ADDRESS)
            .await
            .unwrap();
        Ok(())
    }

    pub async fn start_consumption(&self) -> Result<(), ()> {
        loop {
            // TODO: Add consumption logic
            // Somehitng like a go-routine treatment here?
            let mut msg_bytes: Vec<u8> = vec![];
            self.udp_socket_recv.recv_from(&mut msg_bytes);

            tokio::spawn(async move {
                // Log message
                info_string(format!("Recieved the following message: {:?}", msg_bytes));
            });

            break;
        }
        Ok(())
    }

    pub fn update_node_directory(&mut self, node_id: String, node_addr: String) {
        self.node_directory.insert((node_id, node_addr));
    }
}

// ---------------- PAXOS NODE ----------------------------

fn get_ip_addr(addr: String) -> String {
    let my_local_ip = local_ip().unwrap();
    let mut tks = addr.split(":");
    let ip = format!(
        "{}:{}",
        my_local_ip.to_string(),
        tks.nth(1).unwrap().to_string()
    );
    return ip;
}

pub struct PaxosNode {
    ctrl_file: ControlFile,
    service_manager: ServiceManager,
}

impl PaxosNode {
    pub fn new_node() -> Self {
        let control_file = ControlFile::read_from_file_path(get_control_file_path()).unwrap();
        PaxosNode {
            ctrl_file: control_file,
            service_manager: ServiceManager::new(),
        }
    }
    pub async fn propose(&self) {
        let value = self.ctrl_file.get_self_identifier();

        // Broadcast to network
        match value {
            Some(val) => {
                let msg = format!("PROPOSE {}", val);
                self.service_manager.broadcast_message(msg.as_str());
            }
            None => panic!("No value to broadcast!"),
        }
    }

    pub fn accept() {}
    pub fn learn() {}

    // Gossip for node discovery
    pub async fn gossip(&self) -> Result<(), String> {
        let node_id = self.ctrl_file.get_self_identifier().unwrap();
        let data = format!(
            "{}~{}",
            node_id.to_string(),
            get_ip_addr(self.ctrl_file.get_consume_addr().to_string())
        );
        info_string(format!("Sending -> {}", data));
        let result = self.service_manager.broadcast_message(data.as_str()).await;
        return result;
    }

    pub async fn gossip_consume(&mut self) {
        let MAX_GOSSIP_CONSUMPTION = 10;
        for i in 0..MAX_GOSSIP_CONSUMPTION {
            info_string(format!("{} gossip trial", i));
            // Somehitng like a go-routine treatment here?
            let mut msg_bytes: Vec<u8> = vec![];
            match timeout(
                Duration::from_secs(self.ctrl_file.get_gossip_timeout()),
                self.service_manager
                    .udp_socket_recv
                    .recv_from(&mut msg_bytes),
            )
            .await
            {
                Ok(Ok((_, _))) => {
                    let data = String::from_utf8(msg_bytes).unwrap();
                    let mut tokens;
                    if data.contains("~") {
                        tokens = data.as_str().split("~");
                    } else {
                        let msg = format!("Token does not contain any ~ {:?}, skipping..", data);
                        warning(msg.as_str());
                        continue;
                    }

                    // Log message
                    info_string(format!("Recieved the following message: {:?}", tokens));
                    self.service_manager.update_node_directory(
                        tokens.nth(0).unwrap().to_string(),
                        tokens.nth(1).unwrap().to_string(),
                    );
                }
                Ok(Err(e)) => panic!("{}", e),
                Err(e) => panic!("{}", e),
            };
        }
    }
}
