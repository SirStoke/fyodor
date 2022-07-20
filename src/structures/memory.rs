use rand::Rng;
use std::rc::Rc;

/// A Skip List Node
pub struct Node<K, V> {
    prev: Vec<Rc<Node<K, V>>>,
    next: Vec<Rc<Node<K, V>>>,
    key: K,
    value: V,
}

#[derive(Clone)]
struct FingerNode<K, V>(Option<Rc<Node<K, V>>>);

impl<K, V> FingerNode<K, V> {
    #[inline]
    pub fn some(node: Rc<Node<K, V>>) -> FingerNode<K, V> {
        FingerNode(Some(node))
    }

    #[inline]
    pub fn empty() -> FingerNode<K, V> {
        FingerNode(None)
    }
}

/// A data structure that stores an array of levels
struct Finger<K, V> {
    prev: Vec<FingerNode<K, V>>,
    next: Vec<FingerNode<K, V>>,
}

impl<K, V> Finger<K, V> {
    fn empty(levels: usize) -> Finger<K, V> {
        Finger {
            prev: Vec::with_capacity(levels),
            next: Vec::with_capacity(levels),
        }
    }

    /// Creates a finger with the same capacity as the provided Node, and that references the same
    /// next and prev entries
    fn from_node(node: &Node<K, V>) -> Finger<K, V> {
        let levels = node.prev.len();

        let mut finger = Finger::empty(levels);

        for i in 0..levels {
            finger.prev[i] = FingerNode::some(node.prev[i].clone());
            finger.next[i] = FingerNode::some(node.next[i].clone());
        }

        finger
    }

    /// Returns a [Finger] that brackets the provided key. In case the key is already present, it
    /// returns the contents of that node. If the key is supposed to be before the first node,
    /// then prev is empty. If the key is supposed to be after the last node, then next is empty.
    pub fn bracketing_finger(list: &Rc<Node<K, V>>, key: &K) -> Finger<K, V>
    where
        K: Ord + Clone,
        V: Clone,
    {
        use std::cmp::Ordering::*;

        let mut level = list.next.len() - 1;

        let mut finger = Finger::empty(list.next.len());

        if key.cmp(&list.key) == Less {
            finger.next.fill(FingerNode::some(list.clone()));

            return finger;
        }

        let mut node = list;

        while level != 0 {
            let mut curr_order = Equal;
            let mut next_order = Equal;

            while curr_order != Less && next_order != Greater {
                curr_order = node.key.cmp(key);

                if curr_order == Equal {
                    return Finger::from_node(&node);
                }

                next_order = if let Some(next) = node.next.get(level) {
                    next.key.cmp(key)
                } else {
                    finger.prev[level] = FingerNode::some(node.clone());
                    finger.next[level] = FingerNode::empty();

                    break;
                };

                if next_order == Equal {
                    return Finger::from_node(&node.next[level]);
                }

                node = &node.next[level];
            }

            finger.next[level] = FingerNode::some(node.clone());
            finger.prev[level] = FingerNode::some(node.prev[level].clone());

            level -= 1;
        }

        finger
    }
}

const MAX_HEIGHT: u8 = 12;

impl<K, V> Node<K, V>
where
    K: Ord,
{
    /// Creates a new unlinked node
    pub fn new(key: K, value: V) -> Node<K, V> {
        Node {
            prev: vec![],
            next: vec![],
            key,
            value,
        }
    }

    /// Inserts a new entry in the list
    pub fn insert(key: K, value: V, list: &mut Node<K, V>) -> Node<K, V> {
        let node = Node::new(key, value);
        let mut rng = rand::thread_rng();
        let mut levels = 0;

        // Use 1/4th scaling
        while rng.gen_range(1..4) == 1 as u8 && levels < MAX_HEIGHT {
            levels += 1;
        }

        for level in levels..=0 {}

        todo!()
    }
}
