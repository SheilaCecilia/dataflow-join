pub mod plan;
pub mod graph_stream;
pub mod dir_reader;

use timely::dataflow::*;

use timely::ExchangeData;
use timely::Data;

pub use super::Indexable;

pub use self::graph_stream::GraphStreamIndex;

pub use self::plan::{Plan};
pub use self::dir_reader::DirReader;
pub use super::wings_rule::{Index, IndexStream, advance, StreamPrefixExtender, StreamPrefixIntersector, Intersection, GenericJoin, IntersectOnly};

pub type Node = u32;
pub type Edge = (Node, Node);


pub trait ExtendEdges<G: Scope, P: Data>{
    fn extend_attributes<'a, H1: Fn(Node)->u64 + 'static, H2: Fn(Node)->u64 + 'static>(&self, graph: &GraphStreamIndex<G, H1, H2>, attributes: &[(usize, bool)])
                                                                                       -> Stream<G, (P, Vec<Node>, i32)>
        where G: 'a,
              P: ::std::fmt::Debug+ExchangeData+Indexable<Node>;

    fn intersect_attributes<'a, H1: Fn(Node)->u64 + 'static, H2: Fn(Node)->u64 + 'static>(&self, graph: &GraphStreamIndex<G, H1, H2>, attributes: &[(usize, usize)])
                                                                                          -> Stream<G, (P, i32)>
        where G: 'a,
              P: ::std::fmt::Debug+ExchangeData+Indexable<Node>;
}

impl<G: Scope, P: ::std::fmt::Debug+ExchangeData+Indexable<Node>> ExtendEdges<G, P> for Stream<G, (P, i32)>{

    fn extend_attributes<'a, H1: Fn(Node)->u64 + 'static, H2: Fn(Node)->u64 + 'static>(&self, graph: &GraphStreamIndex<G, H1, H2>, attributes: &[(usize, bool)]) -> Stream<G, (P, Vec<Node>, i32)>
        where G: 'a,
              P: ::std::fmt::Debug+ExchangeData+Indexable<Node> {
        let mut extenders: Vec<Box<StreamPrefixExtender<G, i32, Prefix=P, Extension=Node>+'a>> = vec![];
        for &(attribute, is_forward) in attributes {
            extenders.push(match is_forward {
                true    => Box::new(graph.forward.extend_using(move |x: &P| x.index(attribute))),
                false   => Box::new(graph.reverse.extend_using(move |x: &P| x.index(attribute))),
            });
        }
        self.extend(extenders)
    }

    fn intersect_attributes<'a, H1: Fn(Node)->u64 + 'static, H2: Fn(Node)->u64 + 'static>(&self, graph: &GraphStreamIndex<G, H1, H2>, attributes: &[(usize, usize)]) -> Stream<G, (P, i32)>
        where G: 'a,
              P: ::std::fmt::Debug+ExchangeData+Indexable<Node>{
        let mut intersectors: Vec<Box<StreamPrefixIntersector<G, Prefix=P>+'a>> = vec![];
        for &(src, dst) in attributes{
            intersectors.push(Box::new(graph.forward.intersect_using(move |x: &P| x.index(src), move |x: &P| x.index(dst))));
        }
        self.intersect_only(intersectors)
    }
}
