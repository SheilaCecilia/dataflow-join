use std::rc::Rc;
use std::cell::RefCell;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::fmt::Debug;
use std::hash::Hash;
//use timely::Data;

use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::Operator;
use timely::dataflow::channels::pact::Exchange;
use timely::progress::Timestamp;
use timely::dataflow::operators::probe::Handle as ProbeHandle;

use super::{Index, IndexStream};
use super::StreamPrefixIntersector;

pub struct IndexIntersector<K, T, P, L, L1, L2, L3, H>
    where
        K: Ord+Hash+Clone,
        T: Timestamp,
        L: Fn(&P)->K,
        L1: Fn(&P)->K,
        L2: Fn(&P)->K,
        L3: Fn(&P)->K,
        H: Fn(K)->u64,
{
    handle: ProbeHandle<T>,
    index: Rc<RefCell<Index<K, T>>>,
    hash: Rc<H>,
    logic1: Rc<L>,
    logic2: Rc<L1>,
    get_src: Rc<L2>,
    get_dst: Rc<L3>,
    is_forward: bool,
    phantom: PhantomData<P>,
}

pub trait IntersectOnly<K: Ord+Hash+Clone, H: Fn(K)->u64, T: Timestamp+Ord>
{
    fn intersect_using<P, L, L1, L2, L3>(&self, logic1: L, logic2: L1, get_src: L2, get_dst: L3) -> Rc<IndexIntersector<K, T, P, L, L1, L2, L3, H>>
        where
            L: Fn(&P)->K+'static,
            L1: Fn(&P)->K+'static,
            L2: Fn(&P)->K+'static,
            L3: Fn(&P)->K+'static;
}

impl<K: Ord+Hash+Clone, H: Fn(K)->u64, T: Timestamp+Ord> IntersectOnly<K, H, T> for IndexStream<K, H, T> {
    fn intersect_using<P, L, L1, L2, L3>(&self, logic1: L, logic2: L1, get_src: L2, get_dst: L3) -> Rc<IndexIntersector<K, T, P, L, L1, L2, L3, H>>
        where
            L: Fn(&P)->K+'static,
            L1: Fn(&P)->K+'static,
            L2: Fn(&P)->K+'static,
            L3: Fn(&P)->K+'static,
    {
        Rc::new(IndexIntersector {
            handle: self.handle.clone(),
            index: self.index.clone(),
            hash: self.hash.clone(),
            is_forward: self.is_forward,
            logic1: Rc::new(logic1),
            logic2: Rc::new(logic2),
            get_src: Rc::new(get_src),
            get_dst: Rc::new(get_dst),
            phantom: PhantomData,
        })
    }
}

impl<K, G, P, L, L1, L2, L3, H> StreamPrefixIntersector<G> for Rc<IndexIntersector<K, G::Timestamp, P, L, L1, L2, L3, H>>
    where
        K: Ord+Hash+Clone+ExchangeData,
        G: Scope,
        G::Timestamp: Timestamp+Ord+Clone,//+::std::hash::Hash+Ord,
        P: ExchangeData+Debug,
        L: Fn(&P)->K+'static,
        L1: Fn(&P)->K+'static,
        L2: Fn(&P)->K+'static,
        L3: Fn(&P)->K+'static,
        H: Fn(K)->u64+'static,
{
    type Prefix = P;

    fn intersect_only(&self, stream: Stream<G, (Self::Prefix, i32)>) -> Stream<G, (Self::Prefix, i32)>{
        let hash = self.hash.clone();
        let logic1 = self.logic1.clone();
        let logic1_2 = self.logic1.clone();
        let logic2 = self.logic2.clone();
        let get_src = self.get_src.clone();
        let get_dst = self.get_dst.clone();
        let is_forward = self.is_forward;
        let index = self.index.clone();
        let handle = self.handle.clone();

        let mut buffer = Vec::new();
        let mut blocked = HashMap::new();
        let exch = Exchange::new(move |&(ref x,_)| (*hash)((*logic1_2)(x)));

        stream.unary(exch, "Intersect_only", move |_,_| move |input, output| {

            input.for_each(|time, data| {
                data.swap(&mut buffer);
                blocked.entry(time.retain())
                    .or_insert(Vec::new())
                    .extend(buffer.drain(..))
            });

            for (time, data) in blocked.iter_mut() {

                // ok to process if no further updates less or equal to `time`.
                if !handle.less_equal(time.time()) {
                    (*index).borrow_mut().intersect_only(data, &*logic1, &*logic2, is_forward, &time.time(), &*get_src, &*get_dst);
                    output.session(&time).give_iterator(data.drain(..));
                }
            }

            blocked.retain(|_, data| data.len() > 0);
        })
    }
}
