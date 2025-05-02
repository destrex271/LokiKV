use crate::loki_kv::{self, loki_kv::ValueObject};
use std::boxed::Box;

const CAP: usize = 4;

#[derive(Debug)]
struct BTreeNode {
    keys: Vec<String>,
    values: Vec<ValueObject>,
    num_keys: usize,
    children: Vec<Option<usize>>,
    is_leaf: bool,
    to_right: Option<usize>,
}

impl Clone for BTreeNode {
    fn clone(&self) -> Self {
        BTreeNode {
            keys: self.keys.clone(),
            values: self.values.clone(),
            num_keys: self.num_keys,
            children: self.children.clone(),
            is_leaf: self.is_leaf,
            to_right: self.to_right,
        }
    }
}

#[derive(Clone)]
pub struct BTree {
    root_index: usize,
    vals: Vec<ValueObject>,
    nodes: Vec<BTreeNode>,
}

impl BTreeNode {
    pub fn new() -> Self {
        BTreeNode {
            keys: vec!["".to_string(); CAP - 1],
            values: vec![ValueObject::Phantom; CAP - 1],
            num_keys: 0,
            children: vec![None; CAP],
            is_leaf: true,
            to_right: None,
        }
    }

    pub fn is_node_full(&self) -> bool {
        // println!("{} --> {}", self.num_keys, CAP-1);
        self.num_keys == CAP - 1
    }
}

impl BTree {
    pub fn new() -> Self {
        let mut tree = BTree {
            root_index: 0,
            vals: Vec::new(),
            nodes: Vec::new(),
        };
        tree.root_index = tree.add_node(BTreeNode::new()).unwrap();
        tree
    }

    pub fn get_node(&self, idx: usize) -> BTreeNode {
        self.nodes[idx].clone()
    }

    pub fn get_node_ref(&self, idx: usize) -> &BTreeNode {
        &self.nodes[idx]
    }

    pub fn add_node(&mut self, node: BTreeNode) -> Option<usize> {
        self.nodes.push(node);
        Some(self.nodes.len() - 1)
    }

    pub fn is_node_full(&mut self, node_idx: usize) -> bool {
        self.get_node(node_idx).is_node_full()
    }

    pub fn replace_node(&mut self, node: BTreeNode, idx: usize) {
        self.nodes[idx] = node;
    }

    pub fn split_child(&mut self, node_idx: usize, child_idx: usize) {
        let mut parent = self.get_node(node_idx);
        let child_node_idx = parent.children[child_idx].unwrap();
        let mut child = self.get_node(child_node_idx);

        let mut new_node = BTreeNode::new();
        new_node.is_leaf = child.is_leaf;

        let mid = CAP / 2;

        // Move keys to new node
        for i in 0..(mid - 1) {
            new_node.keys[i] = child.keys[i + mid].clone();
            new_node.values[i] = child.values[mid + i].clone();
            child.keys[i + mid] = "".to_string();
        }

        // Move children if internal node
        if !child.is_leaf {
            for i in 0..mid {
                new_node.children[i] = child.children[i + mid];
                child.children[i + mid] = None;
            }
        }

        new_node.num_keys = mid - 1;
        child.num_keys = mid;

        // Insert new node
        let new_node_idx = self.add_node(new_node).unwrap();
        child.to_right = Some(new_node_idx);

        // Update parent
        for i in (child_idx..parent.num_keys).rev() {
            parent.keys[i + 1] = parent.keys[i].clone();
            parent.values[i + 1] = parent.values[i].clone();
            parent.children[i + 2] = parent.children[i + 1];
        }

        parent.keys[child_idx] = child.keys[mid].clone();
        parent.values[child_idx] = child.values[mid].clone();
        parent.children[child_idx + 1] = Some(new_node_idx);
        parent.num_keys += 1;

        child.keys[mid] = "".to_string();
        child.values[mid] = ValueObject::Phantom;

        self.replace_node(child, child_node_idx);
        self.replace_node(parent, node_idx);
    }

    pub fn insert_nonfull(&mut self, node_idx: usize, key: String, value: ValueObject) {
        let mut node = self.get_node(node_idx);

        if node.is_leaf {
            let mut pos = node.num_keys;
            while pos > 0 && node.keys[pos - 1] > key {
                node.keys[pos] = node.keys[pos - 1].clone();
                node.values[pos] = node.values[pos - 1].clone();
                pos -= 1;
            }
            // println!("{:?}", node.values);
            node.keys[pos] = key;
            node.values[pos] = value;
            node.num_keys += 1;
            self.replace_node(node, node_idx);
        } else {
            let mut child_idx = node.num_keys;
            while child_idx > 0 && node.keys[child_idx - 1] > key {
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
            self.insert_nonfull(node.children[child_idx].unwrap(), key, value);
        }
    }

    pub fn insert(&mut self, key: String, value: ValueObject) {
        if self.is_node_full(self.root_index) {
            let new_root = BTreeNode {
                keys: vec!["".to_string(); CAP - 1],
                values: vec![ValueObject::Phantom; CAP - 1],
                num_keys: 0,
                children: {
                    let mut v = vec![None; CAP];
                    v[0] = Some(self.root_index);
                    v
                },
                is_leaf: false,
                to_right: None,
            };
            let new_root_idx = self.add_node(new_root).unwrap();
            self.root_index = new_root_idx;
            self.split_child(new_root_idx, 0);
        }
        self.insert_nonfull(self.root_index, key, value);
    }

    pub fn print_tree(&self) -> String {
        let mut result = String::new();
        self.display_tree(self.root_index, &mut result);
        result
    }

    pub fn display_tree(&self, node_idx: usize, result: &mut String) {
        let node = self.get_node(node_idx);

        // Append key -> value pairs
        for i in 0..node.num_keys {
            result.push_str(&format!("{} -> {:?}\n", node.keys[i], node.values[i]));
        }

        // Recursively traverse child nodes if not a leaf
        if !node.is_leaf {
            for i in 0..=node.num_keys {
                if let Some(child_idx) = node.children[i] {
                    self.display_tree(child_idx, result);
                }
            }
        }
    }

    pub fn generate_pairs(&self, node_idx: usize, result: &mut Vec<(String, ValueObject)>) {
        let node = self.get_node(node_idx);

        // Append key -> value pairs
        for i in 0..node.num_keys {
            result.push((node.keys[i].clone(), node.values[i].clone()));
        }

        // Recursively traverse child nodes if not a leaf
        if !node.is_leaf {
            for i in 0..=node.num_keys {
                if let Some(child_idx) = node.children[i] {
                    self.generate_pairs(child_idx, result);
                }
            }
        }
    }

    fn search_at_idx(&self, idx: usize, key: String) -> Option<&ValueObject> {
        let mut i = 0;
        let root = self.get_node_ref(idx);
        while i < root.num_keys && key > root.keys[i] {
            i += 1;
        }

        if i <= root.num_keys && key == root.keys[i] {
            return Some(&root.values[i]);
        } else if root.is_leaf {
            return None;
        } else {
            return self.search_at_idx(root.children[i].unwrap(), key);
        }
    }

    pub fn search(&self, key: String) -> Option<&ValueObject> {
        return self.search_at_idx(self.root_index, key);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_btree() {
        let mut tree = BTree::new();
        let data = vec![
            "test1", "test2", "test3", "test4", "test5", "test6", "test7", "test0", "0000",
            "test-1",
        ];

        let mut k = String::new();
        for item in data {
            tree.insert(item.to_string(), ValueObject::StringData(item.to_string()));
            let a = tree.print_tree();
            k += &a;
            println!("{}", a);
        }
    }
}
