use chrono::{DateTime, Local};

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
