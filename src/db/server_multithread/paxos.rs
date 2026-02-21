use serde::{Deserialize, Serialize};
use socket2::{Domain, SockAddr, Socket, Type};
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::RwLock;
use tokio::net::UdpSocket;
use crate::loki_kv::loki_kv::ValueObject;
use crate::loki_kv::control::ControlFile;
use crate::loki_kv::loki_kv::get_control_file_path;
use crate::utils::info_string;

pub struct ServiceManager {
    udp_socket_send: UdpSocket,
    udp_socket_recv: UdpSocket,
    node_directory: HashSet<(String, String)>,
    broadcast_address: SocketAddr,
}

impl ServiceManager {
    pub fn new() -> Self {
        let control_file = ControlFile::read_from_file_path(get_control_file_path()).unwrap();
        let listen_addr: SocketAddr = control_file.get_send_addr().parse().unwrap();
        let soc2_listen_socket = Socket::new(Domain::IPV4, Type::DGRAM, None).unwrap();
        soc2_listen_socket.set_broadcast(true).unwrap();
        soc2_listen_socket.bind(&SockAddr::from(listen_addr)).unwrap();
        let std_socket: std::net::UdpSocket = soc2_listen_socket.into();

        let consume_addr: SocketAddr = control_file.get_consume_addr().parse().unwrap();
        let soc2_raw_consumer_socket = Socket::new(Domain::IPV4, Type::DGRAM, None).unwrap();
        soc2_raw_consumer_socket.set_reuse_address(true).unwrap();
        soc2_raw_consumer_socket.bind(&SockAddr::from(consume_addr)).unwrap();
        let std_consumer_socket: std::net::UdpSocket = soc2_raw_consumer_socket.into();

        let node_directory: HashSet<(String, String)> = HashSet::new();
        let broadcast_ip_string = format!("255.255.255.255:{}", consume_addr.port());

        ServiceManager {
            udp_socket_send: UdpSocket::from_std(std_socket).unwrap(),
            udp_socket_recv: UdpSocket::from_std(std_consumer_socket).unwrap(),
            node_directory,
            broadcast_address: broadcast_ip_string.parse().unwrap(),
        }
    }

    pub async fn broadcast_message(&self, msg: &str) -> Result<(), String> {
        self.udp_socket_send
            .send_to(msg.as_bytes(), self.broadcast_address)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn send_msg_to_node(&self, msg: &str, node_ip: SocketAddr) -> Result<(), String> {
        self.udp_socket_send
            .send_to(msg.as_bytes(), node_ip)
            .await
            .map_err(|e| e.to_string())?;
        Ok(())
    }

    pub async fn receive_message(&mut self) -> Result<Vec<u8>, String> {
        let mut buf = vec![0u8; 65536];
        let (len, _) = self.udp_socket_recv.recv_from(&mut buf)
            .await
            .map_err(|e| e.to_string())?;
        buf.truncate(len);
        Ok(buf)
    }

    pub fn update_node_directory(&mut self, node_id: String, node_addr: String) {
        self.node_directory.insert((node_id, node_addr));
    }

    pub fn get_peers(&self) -> HashSet<u64> {
        self.node_directory.iter()
            .filter_map(|(id, _)| id.parse().ok())
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BallotNumber {
    pub n: u64,
    pub node_id: u64,
}

impl BallotNumber {
    pub fn new(n: u64, node_id: u64) -> Self {
        Self { n, node_id }
    }

    pub fn zero() -> Self {
        Self { n: 0, node_id: 0 }
    }
}

impl PartialOrd for BallotNumber {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BallotNumber {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.n.cmp(&other.n).then_with(|| self.node_id.cmp(&other.node_id))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PaxosMessage {
    Prepare {
        ballot: BallotNumber,
        log_index: u64,
    },
    Promise {
        ballot: BallotNumber,
        accepted_ballot: Option<BallotNumber>,
        accepted_value: Option<ValueObject>,
        log_index: u64,
        from: u64,
    },
    Accept {
        ballot: BallotNumber,
        value: ValueObject,
        log_index: u64,
    },
    Accepted {
        ballot: BallotNumber,
        log_index: u64,
        from: u64,
    },
    Nack {
        ballot: BallotNumber,
        log_index: u64,
        from: u64,
    },
    LeaderHeartbeat {
        leader_id: u64,
        ballot: BallotNumber,
    },
    RequestVote {
        candidate_id: u64,
        last_log_index: u64,
        last_log_term: u64,
    },
    VoteResponse {
        voter_id: u64,
        vote_granted: bool,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogEntry {
    pub term: u64,
    pub value: Option<ValueObject>,
    pub index: u64,
    pub committed: bool,
}

impl LogEntry {
    pub fn new(index: u64, term: u64, value: Option<ValueObject>) -> Self {
        Self {
            term,
            value,
            index,
            committed: false,
        }
    }
}

pub struct PaxosState {
    pub node_id: u64,
    pub current_term: u64,
    pub voted_for: Option<u64>,
    pub log: Vec<LogEntry>,
    pub commit_index: u64,
    pub last_applied: u64,
    
    pub promised_ballot: BallotNumber,
    pub accepted_ballot: HashMap<u64, BallotNumber>,
    pub accepted_value: HashMap<u64, ValueObject>,
    
    pub quorum_size: usize,
    pub peers: HashSet<u64>,
    
    pub leader_id: Option<u64>,
    pub pending_prepares: HashMap<u64, Vec<BallotNumber>>,
    pub pending_accepts: HashMap<u64, HashSet<u64>>,
}

impl Clone for PaxosState {
    fn clone(&self) -> Self {
        PaxosState {
            node_id: self.node_id,
            current_term: self.current_term,
            voted_for: self.voted_for,
            log: self.log.clone(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            promised_ballot: self.promised_ballot,
            accepted_ballot: self.accepted_ballot.clone(),
            accepted_value: self.accepted_value.clone(),
            quorum_size: self.quorum_size,
            peers: self.peers.clone(),
            leader_id: self.leader_id,
            pending_prepares: self.pending_prepares.clone(),
            pending_accepts: self.pending_accepts.clone(),
        }
    }
}

impl PaxosState {
    pub fn new(node_id: u64, quorum_size: usize, peers: HashSet<u64>) -> Self {
        Self {
            node_id,
            current_term: 0,
            voted_for: None,
            log: vec![LogEntry::new(0, 0, None)],
            commit_index: 0,
            last_applied: 0,
            promised_ballot: BallotNumber::zero(),
            accepted_ballot: HashMap::new(),
            accepted_value: HashMap::new(),
            quorum_size,
            peers,
            leader_id: None,
            pending_prepares: HashMap::new(),
            pending_accepts: HashMap::new(),
        }
    }

    pub fn generate_ballot(&self) -> BallotNumber {
        BallotNumber::new(self.current_term, self.node_id)
    }

    pub fn get_log_entry(&self, index: u64) -> Option<&LogEntry> {
        self.log.get(index as usize)
    }

    pub fn append_log(&mut self, value: ValueObject) -> u64 {
        let index = self.log.len() as u64;
        let entry = LogEntry::new(index, self.current_term, Some(value));
        self.log.push(entry);
        index
    }

    pub fn has_majority(&self, votes: &HashSet<u64>) -> bool {
        votes.len() >= self.quorum_size
    }
}

pub struct MultiPaxos {
    state: Arc<RwLock<PaxosState>>,
}

impl MultiPaxos {
    pub fn new(node_id: u64, peers: HashSet<u64>) -> Self {
        let quorum_size = (peers.len() + 1) / 2 + 1;
        let state = PaxosState::new(node_id, quorum_size, peers);
        Self {
            state: Arc::new(RwLock::new(state)),
        }
    }

    pub async fn handle_message(&self, msg: PaxosMessage) -> Option<PaxosMessage> {
        match msg {
            PaxosMessage::Prepare { ballot, log_index } => {
                self.handle_prepare(ballot, log_index).await
            }
            PaxosMessage::Promise { .. } => {
                self.handle_promise(msg).await
            }
            PaxosMessage::Accept { ballot, value, log_index } => {
                self.handle_accept(ballot, value, log_index).await
            }
            PaxosMessage::Accepted { .. } => {
                self.handle_accepted(msg).await
            }
            PaxosMessage::Nack { .. } => {
                self.handle_nack(msg).await
            }
            PaxosMessage::LeaderHeartbeat { leader_id, ballot } => {
                self.handle_leader_heartbeat(leader_id, ballot).await
            }
            PaxosMessage::RequestVote { .. } => {
                self.handle_request_vote(msg).await
            }
            PaxosMessage::VoteResponse { .. } => {
                self.handle_vote_response(msg).await
            }
        }
    }

    async fn handle_prepare(&self, ballot: BallotNumber, log_index: u64) -> Option<PaxosMessage> {
        let mut state = self.state.write().await;
        
        if ballot > state.promised_ballot {
            state.promised_ballot = ballot;
            
            let accepted_ballot = state.accepted_ballot.get(&log_index).cloned();
            let accepted_value = state.accepted_value.get(&log_index).cloned();
            
            Some(PaxosMessage::Promise {
                ballot,
                accepted_ballot,
                accepted_value,
                log_index,
                from: state.node_id,
            })
        } else {
            Some(PaxosMessage::Nack {
                ballot,
                log_index,
                from: state.node_id,
            })
        }
    }

    async fn handle_promise(&self, msg: PaxosMessage) -> Option<PaxosMessage> {
        if let PaxosMessage::Promise { ballot, accepted_ballot, accepted_value, log_index, from } = msg {
            let mut state = self.state.write().await;
            
            let promises = state.pending_prepares.entry(log_index).or_insert_with(Vec::new);
            
            if let Some(entry) = promises.iter().position(|b| b == &ballot) {
                promises.remove(entry);
            }
            promises.push(ballot);
            
            if promises.len() >= state.quorum_size {
                let value = accepted_value.unwrap_or_else(|| ValueObject::Phantom);
                
                return Some(PaxosMessage::Accept {
                    ballot,
                    value,
                    log_index,
                });
            }
        }
        None
    }

    async fn handle_accept(&self, ballot: BallotNumber, value: ValueObject, log_index: u64) -> Option<PaxosMessage> {
        let mut state = self.state.write().await;
        
        if ballot >= state.promised_ballot {
            state.promised_ballot = ballot;
            state.accepted_ballot.insert(log_index, ballot);
            state.accepted_value.insert(log_index, value.clone());
            
            if log_index >= state.log.len() as u64 {
                state.append_log(value);
            }
            
            Some(PaxosMessage::Accepted {
                ballot,
                log_index,
                from: state.node_id,
            })
        } else {
            Some(PaxosMessage::Nack {
                ballot,
                log_index,
                from: state.node_id,
            })
        }
    }

    async fn handle_accepted(&self, msg: PaxosMessage) -> Option<PaxosMessage> {
        if let PaxosMessage::Accepted { ballot, log_index, from } = msg {
            let mut state = self.state.write().await;
            
            let accepted_nodes = state.pending_accepts.entry(log_index).or_insert_with(HashSet::new);
            accepted_nodes.insert(from);
            
            if accepted_nodes.len() >= state.quorum_size {
                if let Some(entry) = state.log.get_mut(log_index as usize) {
                    entry.committed = true;
                }
                state.commit_index = state.commit_index.max(log_index);
                
                return Some(PaxosMessage::Accepted {
                    ballot,
                    log_index,
                    from: state.node_id,
                });
            }
        }
        None
    }

    async fn handle_nack(&self, msg: PaxosMessage) -> Option<PaxosMessage> {
        if let PaxosMessage::Nack { ballot, log_index, .. } = msg {
            let mut state = self.state.write().await;
            
            if ballot.n > state.current_term {
                state.current_term = ballot.n;
                state.voted_for = None;
                state.promised_ballot = BallotNumber::zero();
            }
        }
        None
    }

    async fn handle_leader_heartbeat(&self, leader_id: u64, ballot: BallotNumber) -> Option<PaxosMessage> {
        let mut state = self.state.write().await;
        
        if ballot > state.promised_ballot {
            state.promised_ballot = ballot;
            state.leader_id = Some(leader_id);
        }
        None
    }

    async fn handle_request_vote(&self, msg: PaxosMessage) -> Option<PaxosMessage> {
        if let PaxosMessage::RequestVote { candidate_id, last_log_index, last_log_term } = msg {
            let mut state = self.state.write().await;
            
            let last_log = state.log.last();
            let last_index = state.log.len() as u64 - 1;
            let last_term = last_log.map(|e| e.term).unwrap_or(0);
            
            let vote_granted = (state.voted_for.is_none() || state.voted_for == Some(candidate_id))
                && (last_log_term > last_log_term || (last_log_term == last_log_term && last_index >= last_log_index));
            
            if vote_granted {
                state.voted_for = Some(candidate_id);
            }
            
            return Some(PaxosMessage::VoteResponse {
                voter_id: state.node_id,
                vote_granted,
            });
        }
        None
    }

    async fn handle_vote_response(&self, msg: PaxosMessage) -> Option<PaxosMessage> {
        if let PaxosMessage::VoteResponse { voter_id, vote_granted } = msg {
            let mut state = self.state.write().await;
            
            if vote_granted {
                state.peers.insert(voter_id);
                if state.peers.len() >= state.quorum_size {
                    state.leader_id = Some(state.node_id);
                }
            }
        }
        None
    }

    pub async fn propose(&self, value: ValueObject) -> Result<u64, String> {
        let ballot = {
            let mut state = self.state.write().await;
            state.current_term += 1;
            state.generate_ballot()
        };
        
        let log_index = {
            let mut state = self.state.write().await;
            state.append_log(value.clone())
        };
        
        Some(PaxosMessage::Prepare {
            ballot,
            log_index,
        });
        
        Ok(log_index)
    }

    pub async fn get_committed_value(&self, index: u64) -> Option<ValueObject> {
        let state = self.state.read().await;
        state.log.get(index as usize).and_then(|e| e.value.clone())
    }

    pub async fn get_state(&self) -> PaxosState {
        self.state.read().await.clone()
    }

    pub async fn gossip(&self) -> Result<String, String> {
        let state = self.state.read().await;
        Ok(format!("NODE~{}~{}", state.node_id, state.current_term))
    }

    pub async fn start_consumption(&self, data: String) -> Result<(), String> {
        if data.contains("~") {
            let parts: Vec<&str> = data.split("~").collect();
            if parts.len() >= 2 {
                if let Ok(node_id) = parts[1].parse::<u64>() {
                    let mut state = self.state.write().await;
                    state.peers.insert(node_id);
                }
            }
        }
        Ok(())
    }

    pub async fn is_leader(&self) -> bool {
        let state = self.state.read().await;
        state.leader_id == Some(state.node_id)
    }

    pub async fn get_leader(&self) -> Option<u64> {
        let state = self.state.read().await;
        state.leader_id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ballot_ordering() {
        let b1 = BallotNumber::new(1, 1);
        let b2 = BallotNumber::new(2, 1);
        let b3 = BallotNumber::new(1, 2);
        
        assert!(b2 > b1);
        assert!(b3 > b1);
        assert!(b2 > b3);
    }

    #[tokio::test]
    async fn test_propose_creates_entry() {
        let peers = vec![1, 2, 3].into_iter().collect();
        let paxos = MultiPaxos::new(1, peers);
        
        let result = paxos.propose(ValueObject::StringData("test".to_string())).await;
        assert!(result.is_ok());
    }
}
