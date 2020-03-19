use std::rc::Rc;
use std::cell::RefCell;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use super::{Index,IndexStream};

pub type Node = u32;
pub type Edge = (Node, Node);

/// Handles to the forward and reverse graph indices.
pub struct GraphStreamIndexHandle<T> {
    forward: Rc<RefCell<Index<Node,T>>>,
    reverse: Rc<RefCell<Index<Node,T>>>,
}

impl<T: Ord+Clone+::std::fmt::Debug> GraphStreamIndexHandle<T> {
    /// Merges both handles up to the specified time, compacting their representations.
    pub fn merge_to(&self, time: &T) {
        self.forward.borrow_mut().merge_to(time);
        self.reverse.borrow_mut().merge_to(time);
    }
}

/// Indices and updates for a graph stream.
pub struct GraphStreamIndex<G: Scope, H1: Fn(Node)->u64, H2: Fn(Node)->u64>
    where G::Timestamp: Ord+::std::hash::Hash {
    pub forward: IndexStream<Node, H1, G::Timestamp>,
    pub reverse: IndexStream<Node, H2, G::Timestamp>,
    pub updates: Stream<G, (Vec<Node>, i32)>,
}

impl<G: Scope, H1: Fn(Node)->u64+'static, H2: Fn(Node)->u64+'static> GraphStreamIndex<G, H1, H2> where G::Timestamp: Ord+::std::hash::Hash {
    /// Constructs a new graph stream index from initial edges and an update stream.
    pub fn from(initially: Stream<G, Edge>,
                updates: Stream<G, (Edge, i32)>, hash1: H1, hash2: H2) -> (Self, GraphStreamIndexHandle<G::Timestamp>) {
        let forward = IndexStream::from(hash1, &initially, &updates, true);
        let reverse = IndexStream::from(hash2, &initially.map(|(src, dst)| (dst, src)),
                                        &updates.map(|((src, dst), wgt)| ((dst, src), wgt)), false);
        let updates = updates.map(|((src, dst),wgt)|(vec![src, dst], wgt));
        let index = GraphStreamIndex {
            forward: forward,
            reverse: reverse,
            updates: updates,
        };
        let handles = GraphStreamIndexHandle {
            forward: index.forward.index.clone(),
            reverse: index.reverse.index.clone(),
        };
        (index, handles)
    }
}