struct BTreeNode{
    // Tuples -> only key if internal node, key + value if both
    tuples: Vec<(usize, Option<usize>)>,
    num_tups: usize,
    children: Vec<Option<BTreeNode>>,
    is_leaf: bool,
    to_right: Option<usize>
}
