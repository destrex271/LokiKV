use crate::loki_kv::{self, loki_kv::ValueObject};
use std::boxed::Box;

const CAP: usize = 4;

#[derive(Debug)]
struct BTreeNode {
    keys: Vec<String>,
    val_start: Option<usize>,
    num_keys: usize,
    children: Vec<Option<usize>>,
    is_leaf: bool,
    to_right: Option<usize>
}

impl Clone for BTreeNode {
    fn clone(&self) -> Self {
        BTreeNode {
            keys: self.keys.clone(),
            val_start: self.val_start,
            num_keys: self.num_keys,
            children: self.children.clone(),
            is_leaf: self.is_leaf,
            to_right: self.to_right,
        }
    }
}

struct BTree {
    root_index: usize,
    vals: Vec<ValueObject>,
    nodes: Vec<BTreeNode>
}

impl BTreeNode {
    fn new() -> Self {
        BTreeNode {
            keys: vec!["".to_string(); CAP-1],
            val_start: None,
            num_keys: 0,
            children: vec![None; CAP],
            is_leaf: true,
            to_right: None
        }
    }

    fn is_node_full(&self) -> bool {
        self.num_keys == CAP - 1
    }    
}

impl BTree {
    fn new() -> Self {
        let mut tree = BTree {
            root_index: 0,
            vals: Vec::new(),
            nodes: Vec::new()
        };
        tree.root_index = tree.add_node(BTreeNode::new()).unwrap();
        tree
    }

    fn get_node(&self, idx: usize) -> BTreeNode {
        self.nodes[idx].clone()
    }
    
    fn add_node(&mut self, node: BTreeNode) -> Option<usize> {
        self.nodes.push(node);
        Some(self.nodes.len() - 1)
    }

    fn is_node_full(&mut self, node_idx: usize) -> bool {
        self.get_node(node_idx).is_node_full()
    }

    fn replace_node(&mut self, node: BTreeNode, idx: usize) {
        self.nodes[idx] = node;
    }

    fn split_child(&mut self, node_idx: usize, child_idx: usize) {
        let mut parent = self.get_node(node_idx);
        let child_node_idx = parent.children[child_idx].unwrap();
        let mut child = self.get_node(child_node_idx);
        
        let mut new_node = BTreeNode::new();
        new_node.is_leaf = child.is_leaf;
        
        let mid = CAP / 2;
        
        // Move keys to new node
        for i in mid..(CAP-1) {
            new_node.keys[i-mid] = child.keys[i].clone();
            child.keys[i] = "".to_string();
        }
        
        // Move children if internal node
        if !child.is_leaf {
            for i in mid..CAP {
                new_node.children[i-mid] = child.children[i];
                child.children[i] = None;
            }
        }
        
        new_node.num_keys = CAP/2 - 1;
        child.num_keys = mid - 1;
        
        // Insert new node
        let new_node_idx = self.add_node(new_node).unwrap();
        
        // Update parent
        for i in (child_idx..parent.num_keys).rev() {
            parent.keys[i+1] = parent.keys[i].clone();
            parent.children[i+2] = parent.children[i+1];
        }
        
        parent.keys[child_idx] = child.keys[mid-1].clone();
        child.keys[mid-1] = "".to_string();
        parent.children[child_idx+1] = Some(new_node_idx);
        parent.num_keys += 1;
        
        self.replace_node(child, child_node_idx);
        self.replace_node(parent, node_idx);
    }

    fn insert_nonfull(&mut self, node_idx: usize, key: String) {
        let mut node = self.get_node(node_idx);
        
        if node.is_leaf {
            let mut pos = node.num_keys;
            while pos > 0 && node.keys[pos-1] > key {
                node.keys[pos] = node.keys[pos-1].clone();
                pos -= 1;
            }
            node.keys[pos] = key;
            node.num_keys += 1;
            self.replace_node(node, node_idx);
        } else {
            let mut child_idx = node.num_keys;
            while child_idx > 0 && node.keys[child_idx-1] > key {
                child_idx -= 1;
            }
            
            let next_child_idx = node.children[child_idx].unwrap();
            if self.is_node_full(next_child_idx) {
                self.split_child(node_idx, child_idx);
                node = self.get_node(node_idx);
                if node.keys[child_idx] < key {
                    child_idx += 1;
                }
            }
            self.insert_nonfull(node.children[child_idx].unwrap(), key);
        }
    }

    fn insert(&mut self, key: String) {
        if self.is_node_full(self.root_index) {
            let new_root = BTreeNode {
                keys: vec!["".to_string(); CAP-1],
                val_start: None,
                num_keys: 0,
                children: {
                    let mut v = vec![None; CAP];
                    v[0] = Some(self.root_index);
                    v
                },
                is_leaf: false,
                to_right: None
            };
            let new_root_idx = self.add_node(new_root).unwrap();
            self.root_index = new_root_idx;
            self.split_child(new_root_idx, 0);
        }
        self.insert_nonfull(self.root_index, key);
    }

    fn print_tree(&self) -> String {
        let mut result = String::new();
        self.display_tree(self.root_index, 0, &mut result);
        result
    }

    fn display_tree(&self, node_idx: usize, level: usize, result: &mut String) {
        let node = self.get_node(node_idx);
        let keys: Vec<_> = node.keys[..node.num_keys].to_vec();
        
        result.push_str(&format!("{:indent$}[Level {}] Keys: {:?}\n", "", level, keys, indent = level * 2));

        if !node.is_leaf {
            for i in 0..=node.num_keys {
                if let Some(child_idx) = node.children[i] {
                    self.display_tree(child_idx, level + 1, result);
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_btree() {
        let mut tree = BTree::new();
        let data = vec!["test1", "test2", "test3", "test4", "test5", "test6", "test7", "test0", "0000", "test-1"];

        let mut k = String::new();
        for item in data {
            tree.insert(item.to_string());
            let a = tree.print_tree();
            k += &a;
            println!("{}", a);
        }

    }
}
