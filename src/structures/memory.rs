pub struct Node {
    prev: Vec<Box<Node>>,
    next: Vec<Box<Node>>,
    data: [u8],
}
