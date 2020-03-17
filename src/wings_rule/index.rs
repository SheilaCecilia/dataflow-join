use super::advance;

use std::hash::Hash;
use std::collections::HashMap;

use self::edge_list_neu::EdgeList;
use self::compact::CompactIndex;
use self::unsorted::Unsorted;
use ::Indexable;

/// A multiversion multimap from `Key` to `Val`.
///
/// An `Index` represents a multiversion `(Key, Val)` relation keyed on the first field.
/// It presently assumes that the keys are dense, and so uses a `Vec<State>` to maintain
/// per-key state. This could be generalized (and may need to be) to index structures
/// such as e.g. `HashMap`.
pub struct Index<Key: Ord+Hash, T> {
    /// Optionally, a pair of (key, end) and (val) lists, representing compacted accumulation.
    // compact: (Vec<(Key, usize)>, Vec<u32>),
    compact: CompactIndex<Key,Key>,
    /// An index of committed but un-compacted updates.
    edges: HashMap<Key, EdgeList<Key>>,
    /// A sorted list of un-committed updates.
    // diffs: Vec<(Key, u32, T, i32)>,
    diffs: Unsorted<Key, Key, T>,
}

mod compact {

    use super::advance;

    pub struct CompactIndex<K,V> {
        keys: Vec<(K, usize)>,
        vals: Vec<V>,
    }

    impl<K: Ord, V: Ord> CompactIndex<K, V> {

        /// Allocates a new `CompactIndex`.
        pub fn new() -> Self {
            CompactIndex {
                keys: Vec::new(),
                vals: Vec::new(),
            }
        }

        /// Load a `CompactIndex` from an ordered sequence of key-value pairs.
        pub fn load<I: Iterator<Item = (K, V)>>(&mut self, length: usize, iterator: I) {

            self.keys.clear();
            self.vals.clear();
            self.vals.reserve(length);

            for (key, val) in iterator {
                self.vals.push(val);
                if self.keys.last().map(|x| &x.0) != Some(&key) {
                    self.keys.push((key, self.vals.len()));
                }
                else {
                    let idx = self.keys.len();
                    self.keys[idx-1].1 = self.vals.len();
                }
            }
        }

        /// Reveal the slice for `key` starting from (and updating) `key_cursor`.
        #[inline(always)]
        pub fn values_from<'a>(&'a self, key: &K, key_cursor: &mut usize) -> &'a [V] {

            if *key_cursor < self.keys.len() {

                *key_cursor += advance(&self.keys[*key_cursor..], |x| &x.0 < key);

                if self.keys.get(*key_cursor).map(|x| &x.0) == Some(key) {
                    let lower = if *key_cursor == 0 { 0 } else { self.keys[*key_cursor-1].1 };
                    let upper = self.keys[*key_cursor].1;

                    assert!(lower < upper);

                    *key_cursor += 1;
                    &self.vals[lower .. upper]
                }
                else { &[] }
            }
            else { &[] }
        }
    }
}

mod edge_list_neu {

    use super::advance;

    /// A LSM-style list of updates.
    ///
    /// The `values` field contains sorted runs of updates, whose boundaries are recorded
    /// in the `bounds` field. In the not-uncommon case that there is one sorted run, the
    /// `bounds` field can be an empty vector with no backing allocation.
    ///
    /// We often work with the tail of `values` and `offsets`, when we push new updates
    /// and merge relatively similarly sized runs.
    ///
    /// The `effort` field records cumulative effort to be paid towards the cost of merging
    /// runs that may not otherwise need to be merged, in service of maintaining a small
    /// amortized cost for reads.
    ///
    /// The `count` field tracks the sum of all updates in `values`, for constant-time
    /// reference when required.
    pub struct EdgeList<V: Ord> {
        bounds: Vec<usize>,
        values: Vec<(V, i32)>,
        effort: u32,
        count: i32,     // accumulated diffs; could be negative
    }

    impl<V: Ord> EdgeList<V> {

        /// Allocates a new empty `EdgeList`.
        #[inline(always)]
        pub fn new() -> Self {
            EdgeList {
                bounds: Vec::new(),
                values: Vec::new(),
                effort: 0,
                count: 0,
            }
        }

        #[inline(always)]
        pub fn count(&self) -> i32 { self.count }

        // The next methods are, annoyingly, in support of pushing updates into the LSM.
        // Because insertion is a bit interactive, with tests on timestamps and setting
        // of weights for moved records, this is not supplied as an iterator to use for
        // extending. Instead, the user is expected to call `position`, call `push` as
        // many times as they like, and then call `seal_from` with the position they got
        // from the first call. Sorry!

        /// Reports the current position of the LSM write cursor.
        #[inline(always)]
        pub fn position(&self) -> usize { self.values.len() }

        #[inline(always)]
        pub fn push(&mut self, update: (V, i32)) {
            self.count += update.1;
            self.values.push(update);
        }

        /// Seal an ordered sequence of pushed updates.
        ///
        /// This method is called after a series of `push` calls, and is a moment
        /// for reflection on whether the most recent sorted run of updates is
        /// large enough that we should merge it with prior runs.
        #[inline(always)]
        pub fn seal_from(&mut self, position: usize) {

            // only if values have been pushed.
            if self.values.len() > position {

                // we will push `position` only if there are already values, and
                // the new run is shorter than half the second most recent run.
                let prev_run = position - self.bounds.last().map(|&x| x).unwrap_or(0);
                if self.values.len() - position < prev_run / 2 {
                    self.bounds.push(position);
                }
                else {

                    // we must merge the most recent run, and we must now determine
                    // how many sorted runs to merge. we do this by popping elements
                    // from `self.bounds` as long as they separate regions that
                    // should be merged.

                    // while the last region is greater than half the second-to-last
                    // region (a sorted run), remove the boundary between them.
                    while self.bounds.len() >= 2 && (self.bounds[self.bounds.len()-2] - self.bounds[self.bounds.len()-1] < 2 * (self.values.len() - self.bounds[self.bounds.len()-1])) {
                        self.bounds.pop();
                    }

                    // if the final boundary should be removed, do that too.
                    if self.bounds.len() == 1 && self.bounds[0] < self.values.len() / 2 {
                        self.bounds = Vec::new();
                    }

                    self.consolidate_tail();
                }
            }
        }

        #[inline(always)]
        pub fn proposals(&mut self) -> &[(V, i32)] {
            if self.bounds.len() > 0 {
                self.bounds = Vec::new();
                self.consolidate_tail();
            }
            &self.values[..]
        }

        fn consolidate_tail(&mut self) {
            let bound = self.bounds.last().map(|&x| x).unwrap_or(0);
            self.values[bound ..].sort_by(|x,y| x.0.cmp(&y.0));

            let mut cursor = bound;
            for index in (bound + 1) .. self.values.len() {
                if self.values[index].0 == self.values[cursor].0 {
                    self.values[cursor].1 += self.values[index].1;
                }
                else {
                    if self.values[cursor].1 != 0 {
                        cursor += 1;
                    }
                    self.values.swap(cursor, index);
                }
            }
            if self.values[cursor].1 != 0 {
                cursor += 1;
            }

            self.values.truncate(cursor);

        }

        /// Indicate that a certain amount of effort will be expended.
        ///
        /// This gives the `EdgeList` a chance to simplify its representation in response to work
        /// that is about to be done. If a great deal of work will be done, it may make sense to
        /// consolidate the edge list to simplify that work.
        #[inline(always)]
        pub fn expend(&mut self, effort: u32) {
            if self.bounds.len() > 0 {
                self.effort += effort;
                if (self.effort as usize) > self.values.len() {
                    self.bounds = Vec::new();
                    self.consolidate_tail();
                }
                self.effort = 0;
            }
        }

        /// Accumulates counts for each value in `values` into `temp`.
        /// Populates `temp` with accumulated counts for corresponding elements in `values`.
        ///
        /// This method is used to assist with intersection testing, by reporting accumulated
        /// counts for each element of the supplied `values`.
        #[inline(never)]
        pub fn intersect(&self, values: &[V], temp: &mut Vec<i32>) {

            assert!(temp.len() == values.len());
            assert!(temp.iter().all(|&x| x == 0));

            let mut slice = &self.values[..];

            // for each bound, process the subsequent sorted run.
            for &bound in self.bounds.iter().rev() {
                EdgeList::intersect_helper(values, &slice[bound ..], &mut temp[..]);
                slice = &slice[..bound];
            }

            // process the first run, with no leading bound.
            EdgeList::intersect_helper(values, slice, &mut temp[..]);
        }

        // to simplify things, this accumulates updates
        fn intersect_helper(source: &[V], updates: &[(V, i32)], counts: &mut [i32]) {

            use std::cmp::Ordering;

            let mut s_cursor = 0;
            let mut u_cursor = 0;

            // merge by galloping
            while s_cursor < source.len() && u_cursor < updates.len() {
                match source[s_cursor].cmp(&updates[u_cursor].0) {
                    Ordering::Less => {
                        let step = 1 + advance(&source[(s_cursor+1)..], |x| x < &updates[u_cursor].0);
                        debug_assert!(step > 0);
                        s_cursor += step;
                    },
                    Ordering::Equal => {
                        counts[s_cursor] += updates[u_cursor].1;
                        s_cursor += 1;
                        u_cursor += 1;
                    },
                    Ordering::Greater => {
                        let step = 1 + advance(&updates[(u_cursor+1)..], |x| x.0 < source[s_cursor]);
                        debug_assert!(step > 0);
                        u_cursor += step;
                    },
                }
            }
        }
    }
}

mod unsorted {

    use super::advance;

    pub struct Unsorted<K, V, T> {
        pub updates: Vec<(K, V, T, i32)>,
        pub min_time: Option<T>,
    }

    impl<K: Ord, V: Ord, T: Ord+Clone> Unsorted<K, V, T> {

        pub fn new() -> Self { Unsorted { updates: Vec::new(), min_time: None } }

        pub fn values_from<'a>(&'a self, key: &K, key_cursor: &mut usize) -> &'a [(K, V, T, i32)] {
            *key_cursor += advance(&self.updates[*key_cursor ..], |x| &x.0 < key);
            let step = advance(&self.updates[*key_cursor ..], |x| &x.0 <= key);
            let result = &self.updates[*key_cursor..][..step];
            *key_cursor += step;
            result
        }

        pub fn extend<I: Iterator<Item=((K, V), i32)>>(&mut self, time: T, iterator: I) {
            self.updates.extend(iterator.map(|((k,v),d)| (k, v, time.clone(), d)));
            self.updates.sort_by(|x,y| (&x.0, &x.1).cmp(&(&y.0, &y.1)));

            if self.min_time == None || self.min_time.as_ref().unwrap() > &time {
                self.min_time = Some(time);
            }
        }
    }
}

impl<Key: Ord+Hash+Clone, T: Ord+Clone> Index<Key, T> {

    /// Allocates a new empty index.
    pub fn new() -> Self {
        Index {
            compact: CompactIndex::new(),
            edges: HashMap::new(),
            diffs: Unsorted::new(),
        }
    }

    /// Updates entries of `data` to reflect counts in the index.
    ///
    /// This method may overwrite entries in `data` to replace the second and third fields with
    /// the count of extensions this index would propose and `ident`, respectively. This overwrite
    /// happens if the counts proposed here would be smaller than what is currently recorded in the
    /// tuple.

    #[inline(never)]
    pub fn count<P,K,W>(&mut self, data: &mut Vec<(P, u64, u64, W)>, func: &K, start_time: &T, ident: u64)
        where K: Fn(&P)->Key{

        // sort data by key, to share work for the same key.
        data.sort_by(|x,y| func(&x.0).cmp(&(func(&y.0))));

        // cursors into `self.compact` and `self.diffs`.
        let mut c_cursor = 0;
        let mut d_cursor = 0;

        let possible_diffs = self.diffs.min_time.as_ref().map(|t| t <= &start_time).unwrap_or(false);

        let mut index = 0;
        while index < data.len() {

            let mut count = 0u64;
            let key = func(&data[index].0);

            // (ia) update `count` by the number of values in `self.compact`.
            count += self.compact.values_from(&key, &mut c_cursor).len() as u64;

            // (ib) update `count` by values in `self.edges`.
            count += self.edges.get(&key).map(|entry| entry.count() as u64).unwrap_or(0);

            // (ic) update `count` by values in `self.diffs`. (an over-estimate)
            if possible_diffs {
                count += self.diffs.values_from(&key, &mut d_cursor).len() as u64;
            }

            // (ii) we may have multiple records with the same key, do them all.
            while index < data.len() && func(&data[index].0) == key {

                // if the count improves, retain the count and the identifier of the index.
                if count < data[index].1 {
                    data[index].1 = count;
                    data[index].2 = ident;
                }

                index += 1;
            }
        }
    }

    #[inline(never)]
    pub fn forward_propose<P, K, W>(&mut self, data: &mut Vec<(P, Vec<Key>, W)>, func: &K, start_time: &T)
        where K: Fn(&P) -> Key,
              P: Indexable<Key>,
    {
        // sorting allows us to re-use computation for the same key, and simplifies the searching
        // of self.compact and self.diffs.
        data.sort_unstable_by(|x,y| (func(&x.0), x.0.get_src(), x.0.get_dst()).cmp(&(func(&y.0), y.0.get_src(), y.0.get_dst())));

        // fingers into compacted data and uncommited updates.
        let mut offset_cursor = 0;
        let mut diffs_cursor = 0;
        // let mut diffs = &self.diffs[..];

        // temporary array to stage proposals
        let mut proposals = Vec::<(Key, i32)>::new();

        // current position in `data`.
        let mut index = 0;
        while index < data.len() {

            let key = func(&data[index].0);
            proposals.clear();

            // (ia): incorporate updates from `self.compact`.
            let values = self.compact.values_from(&key, &mut offset_cursor);
            proposals.extend(values.iter().map(|v| (v.clone(), 1)));

            // (ib): incorporate updates from `self.edges`.
            self.edges.get_mut(&key).map(|entry| proposals.extend_from_slice(entry.proposals()));

            // (ic): incorporate updates from `self.diffs`.
            let values = self.diffs.values_from(&key, &mut diffs_cursor);

            for &(ref _key, ref val, ref time, wgt) in values.iter() {
                if time < start_time{
                    proposals.push((val.clone(), wgt));
                }
            }

            //propose key -> extension, if (key < src) || (key == src && extension < dst)
            let mut dst_cursor = 0;
            while index < data.len() && key == func(&data[index].0) {

                let src = data[index].0.get_src();

                if src < key {
                    // (id): consolidate all the counts that we added in, keep positive counts.
                    consolidate_proposals(&mut proposals);

                    //for all src with src < key, in self.diffs only edges with less timestamp can be seen, propose them all
                    while index < data.len() && func(&data[index].0) == key && data[index].0.get_src() < key {
                        for &(ref val, cnt) in &proposals {
                            for _ in 0..cnt {
                                if !data[index].0.find(val){
                                    data[index].1.push(val.clone());
                                }
                            }
                        }
                        index += 1;
                    }
                }
                else if src == key {
                    //re-use computation for the same key, src
                    while index < data.len() && func(&data[index].0) == key && data[index].0.get_src() == src {
                        let dst = data[index].0.get_dst();

                        //for src with src == key, except edges with less timestamp, edges (key -> extension) with the same timestamp
                        //and extension < dst can be seen
                        while dst_cursor < values.len() && values[dst_cursor].1 < dst {
                            if values[dst_cursor].2 == *start_time {
                                proposals.push((values[dst_cursor].1.clone(), values[dst_cursor].3));
                            }
                            dst_cursor += 1;
                        }
                        // (id): consolidate all the counts that we added in, keep positive counts.
                        consolidate_proposals(&mut proposals);

                        //propose for those with the same key, src and dst
                        while index < data.len() && func(&data[index].0) == key && data[index].0.get_src() == src && data[index].0.get_dst() == dst {
                            for &(ref val, cnt) in &proposals {
                                for _ in 0..cnt {
                                    if !data[index].0.find(val){
                                        data[index].1.push(val.clone());
                                    }
                                }
                            }
                            index += 1;
                        }
                    }
                }
                else if src > key {
                    //all edges with less equal timestamp can be seen,re-use computation from (src == key)
                    while dst_cursor < values.len() {
                        if values[dst_cursor].2 == *start_time{
                            proposals.push((values[dst_cursor].1.clone(), values[dst_cursor].3));
                        }
                        dst_cursor += 1;
                    }

                    // (id): consolidate all the counts that we added in, keep positive counts.
                    consolidate_proposals(&mut proposals);

                    //propose for all with the same key and src > key
                    while index < data.len() && func(&data[index].0) == key{
                        for &(ref val, cnt) in &proposals {
                            for _ in 0..cnt {
                                if !data[index].0.find(val){
                                    data[index].1.push(val.clone());
                                }
                            }
                        }
                        index += 1;
                    }
                }
            }
        }
    }

    pub fn reverse_propose<P, K, W>(&mut self, data: &mut Vec<(P, Vec<Key>, W)>, func: &K, start_time: &T)
        where K: Fn(&P) -> Key,
              P: Indexable<Key>,
    {
        data.sort_unstable_by(|x,y| (func(&x.0), x.0.get_src(), x.0.get_dst()).cmp(&(func(&y.0), y.0.get_src(), y.0.get_dst())));

        // fingers into compacted data and uncommited updates.
        let mut offset_cursor = 0;
        let mut diffs_cursor = 0;
        // let mut diffs = &self.diffs[..];

        // temporary array to stage proposals
        let mut proposals = Vec::<(Key, i32)>::new();

        // current position in `data`.
        let mut index = 0;
        while index < data.len() {

            let key = func(&data[index].0);
            proposals.clear();

            // (ia): incorporate updates from `self.compact`.
            let values = self.compact.values_from(&key, &mut offset_cursor);
            proposals.extend(values.iter().map(|v| (v.clone(), 1)));

            // (ib): incorporate updates from `self.edges`.
            self.edges.get_mut(&key).map(|entry| proposals.extend_from_slice(entry.proposals()));

            // (ic): incorporate updates from `self.diffs`.
            let values = self.diffs.values_from(&key, &mut diffs_cursor);

            for &(ref _key, ref val, ref time, wgt) in values.iter() {
                if time < &start_time{
                    proposals.push((val.clone(), wgt));
                }
            }

            //propose key <- extension, if (extension < src) || (extension == src && key < dst)
            let mut src_cursor = 0;

            while index < data.len() && key == func(&data[index].0) {

                let dst = data[index].0.get_dst();
                let src = data[index].0.get_src();

                if dst <= key {// propose extension < src

                    while src_cursor < values.len() && values[src_cursor].1 < src {
                        if values[src_cursor].2 == *start_time{
                            proposals.push((values[src_cursor].1.clone(), values[src_cursor].3));
                        }
                        src_cursor += 1;
                    }

                    // (id): consolidate all the counts that we added in, keep positive counts.
                    consolidate_proposals(&mut proposals);

                    // propose for all with the same key, src and dst (dst <= key)
                    while index < data.len() && func(&data[index].0) == key && data[index].0.get_src() == src && data[index].0.get_dst() <= key {
                        for &(ref val, cnt) in &proposals {
                            for _ in 0..cnt {
                                if !data[index].0.find(val){
                                    data[index].1.push(val.clone());
                                }
                            }
                        }
                        index += 1;
                    }
                }
                else if dst > key { // propose extension <= src

                    while src_cursor < values.len()&& values[src_cursor].1 <= src {
                        if values[src_cursor].2 == *start_time{
                            proposals.push((values[src_cursor].1.clone(), values[src_cursor].3));
                        }
                        src_cursor += 1;
                    }

                    // (id): consolidate all the counts that we added in, keep positive counts.
                    consolidate_proposals(&mut proposals);

                    // propose for all with the same key ,src and dst (dst > key)
                    while index < data.len() && func(&data[index].0) == key && data[index].0.get_src() == src {
                        for &(ref val, cnt) in &proposals {
                            for _ in 0..cnt {
                                if !data[index].0.find(val){
                                    data[index].1.push(val.clone());
                                }
                            }
                        }
                        index += 1;
                    }
                }
            }
        }
    }

    #[inline(never)]
    pub fn intersect<P, F, W>(&mut self, data: &mut Vec<(P, Vec<Key>, W)>, func: &F, is_forward: bool, start_time: &T)
        where F: Fn(&P)->Key,
              P: Indexable<Key>,
    {
        // sorting data by key allows us to re-use some work / compact representations.
        data.sort_unstable_by(|x,y| func(&x.0).cmp(&(func(&y.0))));

        // counts for each value to validate
        let mut temp = Vec::new();

        // fingers into compacted data and uncommited updates.
        let mut offset_cursor = 0;
        let mut diffs_cursor = 0;
        // let mut diffs = &self.diffs[..];

        let mut index = 0;
        while index < data.len() {

            let key = func(&data[index].0);

            // consider the amount of effort we are about to invest:
            let mut effort = 16;
            let mut temp_index = index;
            while temp_index < data.len() && func(&data[temp_index].0) == key {
                effort += data[temp_index].1.len();
                temp_index += 1;
            }

            // (i) position `self.compact` cursor so that we can re-use it.
            let compact_slice = self.compact.values_from(&key, &mut offset_cursor);

            // (ii) prepare non-compact updates. if our effort level is large, consolidate.
            let mut entry = self.edges.get_mut(&key);
            entry.as_mut().map(|x| x.expend(effort as u32));

            // (iii) position `self.diffs` cursor so that we can re-use it.
            let diffs_slice = self.diffs.values_from(&key, &mut diffs_cursor);


            // we may have multiple records with the same key, do them all.
            while index < data.len() && func(&data[index].0) == key {

                let src = data[index].0.get_src();
                let dst = data[index].0.get_dst();

                // in this context, we only worry about the proposals of the record.
                let proposals = &mut data[index].1;

                // Our plan is to take the list of proposals (record.1) and populate
                // a corresponding vector of `i32` counts for each proposal, from each
                // of our sources of changes.

                // set `temp` to be a vector of initially zero counts.
                temp.clear();
                temp.resize(proposals.len(), 0);

                // (ia) update `temp` counts based on `self.edges[key]`, if it exists.
                entry.as_mut().map(|x| x.intersect(proposals, &mut temp));

                // (ib, ic) update `temp` counts based on `self.compact` and `self.diffs`.
                let mut c_cursor = 0;
                let mut d_cursor = 0;

                // walk proposals linearly (could gallop, if we felt strongly enough).
                for (proposal, count) in proposals.iter().zip(temp.iter_mut()) {

                    // move c_cursor to where `proposal` would start ..
                    c_cursor += advance(&compact_slice[c_cursor..], |x| x < proposal);
                    while compact_slice.get(c_cursor) == Some(proposal) {
                        *count += 1;
                        c_cursor += 1;
                    }

                    // move d_cursor to where `proposal` would start ..
                    d_cursor += advance(&diffs_slice[d_cursor..], |x| &x.1 < proposal);

                    while diffs_slice.get(d_cursor).map(|x| &x.1) == Some(proposal) {
                        if (start_time > &diffs_slice[d_cursor].2)
                        ||((start_time == &diffs_slice[d_cursor].2)&&
                            ((is_forward && ((key < src)||(key == src && proposal < &dst)))
                                ||(!is_forward && ((proposal < &src)||(proposal == &src && key < dst))))) {
                            *count += diffs_slice[d_cursor].3;
                        }
                        d_cursor += 1;
                    }

                }

                // (ii) remove elements whose count is not strictly positive.
                let mut cursor = 0;
                for i in 0 .. temp.len() {
                    if temp[i] > 0 {
                        proposals.swap(cursor, i);
                        cursor += 1;
                    }
                }
                proposals.truncate(cursor);

                index += 1;
            }
        }
    }

    pub fn intersect_only<P,K1,K2,W>(&mut self, data: &mut Vec<(P, W)>, func1: &K1, func2: &K2, is_forward:bool, start_time: &T)
        where K1: Fn(&P)->Key,
              K2: Fn(&P)->Key,
              P: Indexable<Key>,
    {

        // sorting data by key allows us to re-use some work / compact representations.
        //data.sort_unstable_by(|x,y| func(&x.0).cmp(&(func(&y.0))));
        data.sort_unstable_by(|x,y| (func1(&x.0),func2(&x.0)).cmp(&(func1(&y.0),func2(&y.0))));

        // counts for each value to validate
        let mut temp = Vec::new();

        // fingers into compacted data and uncommited updates.
        let mut offset_cursor = 0;
        let mut diffs_cursor = 0;

        let mut index = 0;

        let mut r_cursor = 0;

        while index < data.len() {
            let key = func1(&data[index].0);
            //let key2 = func2(&data[index].0);

            // consider the amount of effort we are about to invest:
            let mut effort = 16;

            let temp_index = index + advance(&data[index..],|x|func1(&x.0)<= key);
            effort += temp_index - index;

            // (i) position `self.compact` cursor so that we can re-use it.
            let compact_slice = self.compact.values_from(&key, &mut offset_cursor);

            // (ii) prepare non-compact updates. if our effort level is large, consolidate.
            let mut entry = self.edges.get_mut(&key);
            entry.as_mut().map(|x| x.expend(effort as u32));

            // (iii) position `self.diffs` cursor so that we can re-use it.
            let diffs_slice = self.diffs.values_from(&key, &mut diffs_cursor);

            let mut proposals = Vec::new();
            for i in index .. temp_index{
                proposals.push(func2(&data[i].0));
            }

            // set `temp` to be a vector of initially zero counts.
            temp.clear();
            temp.resize(proposals.len(), 0);

            // (ia) update `temp` counts based on `self.edges[key]`, if it exists.
            entry.as_mut().map(|x| x.intersect(&proposals[..], &mut temp));

            // (ib, ic) update `temp` counts based on `self.compact` and `self.diffs`.
            let mut c_cursor = 0;
            let mut d_cursor = 0;

            // walk proposals linearly (could gallop, if we felt strongly enough).
            for (proposal, count) in proposals.iter().zip(temp.iter_mut()) {

                // move c_cursor to where `proposal` would start ..
                c_cursor += advance(&compact_slice[c_cursor..], |x| x < proposal);
                while compact_slice.get(c_cursor) == Some(proposal) {
                    *count += 1;
                    c_cursor += 1;
                }

                // move d_cursor to where `proposal` would start ..
                d_cursor += advance(&diffs_slice[d_cursor..], |x| &x.1 < proposal);

                let src = data[index].0.get_src();
                let dst = data[index].0.get_dst();

                while diffs_slice.get(d_cursor).map(|x| &x.1) == Some(proposal) {
                    if (start_time > &diffs_slice[d_cursor].2)
                        ||((start_time == &diffs_slice[d_cursor].2)&&
                        ((is_forward && ((key < src)||(key == src && proposal < &dst)))
                            ||(!is_forward && ((proposal < &src)||(proposal == &src && key < dst))))) {
                        *count += diffs_slice[d_cursor].3;
                    }
                    d_cursor += 1;
                }
            }
            //remove absent prefixes
            let mut t_cursor = 0;

            while index < temp_index{
                if temp[t_cursor] != 0||(r_cursor > 0 && func1(&data[index].0) == func1(&data[r_cursor - 1].0) && func2(&data[index].0) == func2(&data[r_cursor - 1].0)){
                    data.swap(r_cursor,index);
                    r_cursor += 1;
                }
                t_cursor += 1;
                index += 1;
            }
        }
        data.truncate(r_cursor);
    }

    /// Commits updates up to and including `time`.
    ///
    /// This merges any differences with time less or equal to `time`, and should probably only be called
    /// once the user is certain to never require such a distinction again. These differences are not yet
    /// compacted, they've just had their times stripped off.
    ///
    /// This operation is important to ensure that `self.diffs` doesn't grow too large, as our strategy
    /// for keeping it sorted is to re-sort it whenever we add data. If it grew without bound this would
    /// be pretty horrible. In principle, this operation also allows us to consolidate the representation,
    /// if we have updates which update the same value (potentially cancelling).
    #[inline(never)]
    pub fn merge_to(&mut self, time: &T) {

        let mut index = 0;
        while index < self.diffs.updates.len() {

            let key_index = index;
            let entry = self.edges.entry(self.diffs.updates[key_index].0.clone()).or_insert(EdgeList::new());
            let prior_position = entry.position();

            while self.diffs.updates.get(index).map(|x| &x.0) == self.diffs.updates.get(key_index).map(|x| &x.0) {
                if self.diffs.updates[index].2.le(time) {
                    entry.push((self.diffs.updates[index].1.clone(), self.diffs.updates[index].3));
                    self.diffs.updates[index].3 = 0;
                }
                index += 1;
            }

            entry.seal_from(prior_position);
        }

        // remove committed updates
        self.diffs.updates.retain(|x| x.3 != 0);
        self.diffs.min_time = self.diffs.updates.iter().map(|x| x.2.clone()).min();
    }

    /// Introduces a collection of updates at various times.
    ///
    /// These updates will now be reflected in all queries against the index, at or after the
    /// indicated logical time.
    #[inline(never)]
    pub fn update(&mut self, time: T, updates: &mut Vec<((Key, Key), i32)>) {
        self.diffs.extend(time, updates.drain(..));
    }

    /// Sets an initial collection of positive counts, which we can compact.
    #[inline(never)]
    pub fn initialize(&mut self, initial: &mut Vec<Vec<(Key, Key)>>) {
        let length = initial.iter().map(|x| x.len()).sum();
        self.compact.load(length, initial.drain(..).flat_map(|x| x.into_iter()));
    }
}

fn consolidate_proposals<Val: Ord>(proposals: &mut Vec<(Val, i32)>){
    if proposals.len() > 0 {
        proposals.sort_by(|x, y| x.0.cmp(&y.0));
        for cursor in 0..proposals.len() - 1 {
            if proposals[cursor].0 == proposals[cursor + 1].0 {
                proposals[cursor + 1].1 += proposals[cursor].1;
                proposals[cursor].1 = 0;
            }
        }
        proposals.retain(|x| x.1 > 0);
    }
}