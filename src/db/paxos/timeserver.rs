use chrono::{DateTime, Local};
use rsntp::SntpClient;

struct TTInterval {
    earliest: usize,
    latest: usize,
}

trait timeserver {
    fn new() -> Self;
    fn now() -> Option<TTInterval>;
    fn after() -> bool;
    fn before() -> bool;
}

struct NTPTimeServer{
    identifier: String,
    client: SntpClient
}

impl timeserver for NTPTimeServer{
    fn new() -> Self{
        let client = SntpClient::new();
        NTPTimeServer { identifier: "id-TODO", client }
    }
    fn now() -> Option<TTInterval>{}
    fn after() -> bool{}
    fn before() -> bool{}
}