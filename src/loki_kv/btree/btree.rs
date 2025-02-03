use crate::loki_kv::{self, loki_kv::ValueObject};

const capacity = 4

struct BTreeNode{
    // Tuples -> only key if internal node, key + value if both
    key_start: isize,
    val_start: Option<usize>,
    num_tups: usize,
    children: Vec<Option<usize>>,
    is_leaf: bool,
    to_right: Option<usize>
}

struct BTree{
    keys: Vec<String>,
    vals: Vec<ValueObject>,
    nodes: Vec<Option<BTreeNode>>
}

impl BTreeNode{
    fn new() -> Self{
        BTreeNode{
            key_start: -1,
            val_start: None,
            num_tups: 0,
            children: Vec::with_capacity(capacity),
            is_leaf: false,
            to_right: None
        }
    }
}

impl BTree{
    fn new() -> Self{
        BTree{
            keys: Vec::new(),
            vals: Vec::new(),
            nodes: Vec::new()
        }
    }
}
