use crate::loki_kv::{self, loki_kv::ValueObject};
use std::boxed::Box;

const CAP: usize = 4;

#[derive(Debug)]
struct BTreeNode{
    // Tuples -> only key if internal node, key + value if both
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

struct BTree{
    root_index: usize,
    vals: Vec<ValueObject>,
    nodes: Vec<BTreeNode>
}

impl BTreeNode{

    fn new() -> Self{
        BTreeNode{
            keys: Vec::with_capacity(CAP - 1),
            val_start: None,
            num_keys: 0,
            children: Vec::with_capacity(CAP),
            is_leaf: false,
            to_right: None
        }
    }

    fn is_node_full(&self) -> bool{
        if self.keys.len() == CAP{
            return true
        }
        false
    }    
}

impl BTree{
    fn new() -> Self{
        let mut tree = BTree{
            root_index: 0,
            vals: Vec::new(),
            nodes: Vec::new()
        };

        let root_node = BTreeNode::new();
        tree.root_index = tree.add_node(root_node).unwrap();
        tree
    }

    fn get_node(&self, idx: usize) -> BTreeNode{
        return self.nodes[idx].clone()
    }
    
    fn add_node(&mut self, node: BTreeNode) -> Option<usize>{
        self.nodes.push(node);
        Some(self.nodes.len() - 1)
    }

    fn is_node_full(&mut self, node_idx: usize) -> bool{
        self.get_node(node_idx).is_node_full()
    }

    fn replace_node(&mut self, node: BTreeNode, idx: usize){
        self.nodes[idx] = node;
    }

    fn get_val(&self, idx: usize) -> Option<ValueObject>{
        if idx >= self.vals.len(){
            return None
        }
        Some(self.vals[idx].clone())
    }

    fn add_val(&mut self, val_obj: ValueObject) -> Option<usize>{
        self.vals.push(val_obj);
        Some(self.vals.len() - 1)
    }

    fn split_child(&mut self, node_idx: usize, child_idx: usize){
        let mut node = self.get_node(node_idx);
        let tru_child_idx = node.children[child_idx].unwrap();
        let mut child = self.get_node(tru_child_idx);
        
        // Allocate new node
        let mut sibling = BTreeNode::new();
        sibling.is_leaf = child.is_leaf; 

        let mid = CAP/2;

        for i in 0..=mid-1{
            sibling.keys[i] = child.keys[i + mid].clone();
        }
        sibling.num_keys = mid - 1;

        if !child.is_leaf{
            for j in 0..=mid{
                sibling.children[j] = child.children[j + mid];
            }
        }
        child.num_keys = mid - 1;

        // Commit Sibling to list
        let sibling_idx = self.add_node(sibling);
        node.children[child_idx + 1] = sibling_idx;

        for j in (child_idx..=mid-1).rev(){
            node.keys[j+1] = node.keys[j].clone();
        }

        node.keys[child_idx] = child.keys[mid].clone();        
        node.num_keys += 1;
        
        // Commit Changes
        self.replace_node(node, node_idx);
        self.replace_node(child, tru_child_idx);
    }

    fn split_root(&mut self){
        let mut new_root = BTreeNode::new();
        new_root.children[0] = Some(self.root_index);
        self.root_index = self.add_node(new_root).unwrap();
        self.split_child(self.root_index, 0);
    }

    fn insert_nonfull(&mut self, node_idx: usize, key: String){
        let mut node = self.get_node(node_idx);
        if node.is_leaf{
            // Node used in write mode
            let mut i: isize = node.num_keys as isize - 1;
            loop{
                if i < 0 && key >= node.keys[i as usize]{
                    break;
                }
                node.keys[i as usize +1] = node.keys[i as usize].clone();
                i -= 1;
            }
            node.keys[i as usize +1] = key;
            node.num_keys+=1;
            self.replace_node(node, node_idx);
        }else{
            // node used in read-only mode
            let mut i: isize = node.num_keys as isize;
            loop{
                println!("{}", i);
                if i < 0 && key >= node.keys[i as usize]{
                    break;
                }
                i -= 1;
            }
            i += 1;
            let child = self.get_node(i as usize);
            if child.num_keys == CAP - 1{
                self.split_child(node_idx, i as usize);
                if key > node.keys[i as usize]{
                    i += 1;
                }
            }
            self.insert_nonfull(i as usize, key);
        }
    }

    fn insert(&mut self, key: String){
        if self.is_node_full(self.root_index){
            self.split_root();
        }
        self.insert_nonfull(self.root_index, key);
    }

    fn display_tree(&self, node_idx: usize, level: usize) {
        let node = self.get_node(node_idx);

        // Print the current node's keys
        println!("{:indent$}[Level {}] Node: {:?}", "", level, node.keys, indent = level * 2);

        if !node.is_leaf {
            // Recursively display the children if not a leaf node
            for i in 0..=node.num_keys {
                if let Some(child_idx) = node.children[i] {
                    self.display_tree(child_idx, level + 1);
                }
            }
        }
    }

    // Public function to start the tree display from the root
    fn print_tree(&self) {
        self.display_tree(self.root_index, 0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn try_btree() {
        let mut tree = BTree::new();
        tree.print_tree();
        let data = vec!["test1", "test2", "test3", "test4", "test5", "test6", "test7"];

        for item in data{
            println!("Inserting ... {}", item);
            tree.insert(item.to_string());
        }

        tree.print_tree();
    }
}