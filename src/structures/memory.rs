use rand::Rng;
use std::rc::Rc;
use crossbeam::atomic::AtomicCell;

/// A Skip List Node
#[allow(dead_code)]
pub struct Node<K, V> {
    prev: Vec<AtomicCell<Rc<Node<K, V>>>>,
    next: Vec<AtomicCell<Rc<Node<K, V>>>>,
    key: K,
    value: V,
}

#[derive(Clone)]
struct FingerNode<K, V>(Option<Rc<Node<K, V>>>);

impl<K, V> FingerNode<K, V> {
    #[inline]
    fn some(node: Rc<Node<K, V>>) -> FingerNode<K, V> {
        FingerNode(Some(node))
    }

    #[inline]
    fn empty() -> FingerNode<K, V> {
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
            // SAFETY: `as_ref()` invariants must all hold for this Node to be valid
            unsafe {
                finger.prev[i] = FingerNode(node.prev[i].as_ptr().as_ref().map(Rc::clone));
                finger.next[i] = FingerNode(node.next[i].as_ptr().as_ref().map(Rc::clone));
            }
        }

        finger
    }

    /// Returns a [Finger] that brackets the provided key. In case the key is already present, it
    /// returns the contents of that node. If the key is supposed to be before the first node,
    /// then prev is empty. If the key is supposed to be after the last node, then next is empty.
    ///
    /// SAFETY: there are many unsafe blocks in this function. They are valid because
    /// data inside "Node" is actually never mutated (except, of course, the other AtomicCells), only
    /// the pointer inside the AtomicCell is. In other words, all &Rc<Node<K, V>> actually alias to
    /// immutable data, and the only data that mutates is a field inside the AtomicCell.
    fn bracketing_finger(list: &Rc<Node<K, V>>, key: &K) -> Finger<K, V>
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

        let mut node = list.clone();

        while level != 0 {
            let mut curr_order = Equal;
            let mut next_order = Equal;

            while curr_order != Less && next_order != Greater {
                curr_order = node.key.cmp(key);

                if curr_order == Equal {
                    return Finger::from_node(node.as_ref());
                }

                next_order = if let Some(next) = node.next.get(level) {
                    // SAFETY: data inside Node is never mutated (the AtomicCell's content is)
                    unsafe { (*next.as_ptr()).clone().key.cmp(key) }
                } else {
                    finger.prev[level] = FingerNode::some(node.clone());
                    finger.next[level] = FingerNode::empty();

                    break;
                };

                // SAFETY: data inside Node is never mutated (the AtomicCell's content is)
                let next_node = unsafe { (&*node.next[level].as_ptr()).clone() };

                if next_order == Equal {
                    return Finger::from_node(next_node.as_ref());
                }

                node = next_node;
            }

            finger.next[level] = FingerNode::some(node.clone());

            // SAFETY: data inside Node is never mutated (the AtomicCell's content is)
            unsafe {
                finger.prev[level] = FingerNode::some((*node.prev[level].as_ptr()).clone());
            }

            level -= 1;
        }

        finger
    }
}

const MAX_HEIGHT: u8 = 12;

impl<K, V> Node<K, V>
where
    K: Ord + Clone,
    V: Clone,
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
    pub fn insert(key: K, value: V, list: Rc<Node<K, V>>) -> Node<K, V> {
        let node = Node::new(key, value);
        let mut rng = rand::thread_rng();
        let mut levels = 0;

        // Use 1/4th scaling
        while rng.gen_range(1..4) == 1_u8 && levels < MAX_HEIGHT {
            levels += 1;
        }

        let finger = Finger::bracketing_finger(&list, &node.key);

        println!("{:?}", finger.prev.len());

        for _level in levels..=0 {}

        todo!()
    }
}

#[cfg(test)]
mod tests {
    use std::rc::Rc;
    use crossbeam::atomic::AtomicCell;
    use crate::structures::memory::Node;

    #[test]
    fn atomic_cell_doesnt_lock() {
        assert!(AtomicCell::<Rc<Node<&str, &str>>>::is_lock_free());
    }
}
