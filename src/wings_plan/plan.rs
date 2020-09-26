use std::rc::Rc;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use std::sync::{Arc, Mutex, RwLock};
use timely::dataflow::{ProbeHandle};
use timely::dataflow::operators::{Exchange, Inspect, Probe};
use timely::{ExchangeData};

use std::collections::HashMap;
use std::io::BufReader;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;

use super::Indexable;

use super::graph_stream::GraphStreamIndex;
use wings_plan::ExtendEdges;

pub type Node = u32;
pub type Edge = (Node, Node);
type Label = u32;

#[derive(Debug, Default)]
pub struct PlanNode{
    // edges[edge_start_idx, edge_start_idx + num_edges] are the out edges of this node
    edge_start_idx: usize,
    num_edges: usize,
    subgraph_num_vertices: usize,
    is_query: bool,
    idx: usize
}

#[derive(Debug, Default)]
pub struct PlanOperation{
    src_key: usize,
    dst_key: usize,
    is_forward: bool,
}

#[derive(Debug, Default)]
pub struct PlanEdge{
    src: Rc<PlanNode>,
    dst: Rc<PlanNode>,
    operations: Vec<PlanOperation>,
    extensions: Vec<PlanOperation>,
    intersections: Vec<PlanOperation>,
}

#[derive(Debug, Default)]
pub struct Plan{
    edges: Vec<PlanEdge>,
    nodes: Vec<Rc<PlanNode>>,
    root_node_id: usize,
}

impl Plan{
    pub fn track_motif<H1, H2, G: Scope>(&self, graph: &GraphStreamIndex<G, H1, H2>, probe: &mut ProbeHandle<G::Timestamp>, counter: Arc<Mutex<u64>>, labeled_counters: Arc<RwLock<HashMap<(usize,Vec<u32>),Mutex<u64>>>>, vertex_id_label_map: Arc<HashMap<u32, u32>>)
        where H1: Fn(Node)->u64 + 'static,
              H2: Fn(Node)->u64 + 'static
    {
        let root = self.nodes[self.root_node_id].clone();
        self.execute_node(root, &graph.updates, graph, probe, counter, labeled_counters, vertex_id_label_map);
    }

    fn execute_node<H1, H2, G: Scope, P>(&self, root: Rc<PlanNode>, stream: &Stream<G, (P, i32)>, graph: &GraphStreamIndex<G, H1, H2>, probe: &mut ProbeHandle<G::Timestamp>, counter: Arc<Mutex<u64>>, labeled_counters: Arc<RwLock<HashMap<(usize,Vec<u32>),Mutex<u64>>>>, vertex_id_label_map: Arc<HashMap<u32, u32>>)
        where H1: Fn(Node)->u64 + 'static,
              H2: Fn(Node)->u64 + 'static,
              P: ::std::fmt::Debug+ExchangeData+Indexable<Node>,

    {
        let start_idx = root.edge_start_idx;
        let end_idx = root.edge_start_idx + root.num_edges;

        for index in start_idx .. end_idx{
            let child = self.edges[index].dst.clone();
            let child1 = self.edges[index].dst.clone();

            let counter1 = counter.clone();
            let counter2 = counter.clone();
            let labeled_counters = labeled_counters.clone();
            let vertex_id_label_map1 = vertex_id_label_map.clone();
            let vertex_id_label_map2 = vertex_id_label_map.clone();

            let plan_edge = &self.edges[index];
            let intersect_attributes = plan_edge.get_intersect_attributes();
            let extend_attributes = plan_edge.get_extend_attributes();

            let output = if plan_edge.extensions.is_empty(){
                stream.intersect_attributes(graph, &intersect_attributes)
            }
            else if !plan_edge.intersections.is_empty(){
                stream.intersect_attributes(graph, &intersect_attributes)
                    .extend_attributes(graph, &extend_attributes)
                    .flat_map(|(p, es, w)|
                        es.into_iter().map(move |e|  {
                            let mut clone = p.clone();
                            clone.push(e);
                            (clone, w)
                        }))
            } else {
                stream.extend_attributes(graph, &extend_attributes)
                    .flat_map(|(p, es, w)|
                        es.into_iter().map(move |e|  {
                            let mut clone = p.clone();
                            clone.push(e);
                            (clone, w)
                        }))
            };
            let child_counters = labeled_counters.clone();
            if child.is_query{
                output.probe_with(probe);
                output.exchange(|x| (x.0).index(0) as u64)
                    .inspect_batch(move |_,xs| {
                        let mut batch_query_count = HashMap::new();
                        let vertex_id_label_map3 = vertex_id_label_map2.clone();
                        for x in xs.iter(){
                            let labeled_query = label_matching(&x.0, vertex_id_label_map3.clone());
                            let counter =  batch_query_count.entry((child.idx, labeled_query)).or_insert(0 as u64);
                            *counter += 1;
                        }

                        for (query, count) in batch_query_count.into_iter() {
                            let counters = labeled_counters.read().expect("Mutex poisoned");

                            if let Some(counter) = counters.get(&query) {
                                let mut counter = counter.lock().expect("Mutex poisoned");
                                *counter += count;
                                println!("Labeled {:?}: {}", query, *counter);
                                continue;
                            }

                            drop(counters);
                            let mut counters = labeled_counters.write().expect("RwLock poisoned");
                            println!("Labeled {:?}: {}", query, count);
                            counters.entry(query).or_insert_with(||Mutex::new(count));

                        }
                    })
                    //.inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                    .count()
                    //.inspect_batch(move |t,x| println!("{:?}: {:?}", t, x))
                    .inspect_batch(move |_,x| {
                        if let Ok(mut bound) = counter1.lock() {
                            *bound += x[0] as u64;
                        }
                        //Find count for each labeled query

                    });
            }
            self.execute_node(child1, &output, graph, probe, counter2, child_counters, vertex_id_label_map1);
        }
    }

    fn initialize(&mut self){
        for edge in &mut self.edges {
            edge.initialize();
        }
    }
}

impl PlanEdge{
    fn initialize(&mut self){
        for operation in &self.operations{
            if operation.dst_key == self.src.subgraph_num_vertices{
                self.extensions.push(PlanOperation{src_key: operation.src_key, dst_key: operation.dst_key, is_forward: operation.is_forward });
            } else {
                self.intersections.push(PlanOperation{src_key: operation.src_key, dst_key: operation.dst_key, is_forward: operation.is_forward });
            }
        }
    }



    fn get_extend_attributes(&self) -> Vec<(usize, bool)>{
        let mut constraints = vec![];

        for &ref operation in &self.extensions{
            constraints.push((operation.src_key, operation.is_forward));
        }

        constraints
    }

    fn get_intersect_attributes(&self) -> Vec<(usize, usize)>{
        let mut constraints = vec![];

        for &ref operation in &self.intersections{
            if operation.is_forward{
                constraints.push((operation.src_key, operation.dst_key));
            }else{
                constraints.push((operation.dst_key, operation.src_key));
            }
        }

        constraints
    }
}


pub fn read_plan(filename:&str) -> Plan{
    let path = Path::new(filename);
    let display = path.display();

    let file = match File::open(&path){
        Err(why) => {
            panic!("EXCEPTION: couldn't open {}: {}",
                    display,
                    Error::description(&why))
        }
        Ok(file) => file,
    };

    let mut reader = BufReader::new(file);
    let mut plan:Plan = Default::default();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    plan.root_node_id = line.trim().parse().unwrap();

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let nodes: usize = line.trim().parse().unwrap();

    for idx in 0 .. nodes {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let elts: Vec<&str> = line[..].split_whitespace().collect();
        let edge_start_idx:usize = elts[0].parse().unwrap();
        let num_edges: usize = elts[1].parse().unwrap();
        let subgraph_num_vertices: usize = elts[2].parse().unwrap();
        let is_query: usize = elts[3].parse().unwrap();
        let is_query = if is_query == 1 { true } else {false};
        plan.nodes.push(Rc::new(PlanNode{ edge_start_idx, num_edges, subgraph_num_vertices, is_query, idx}));
    }

    let mut line = String::new();
    reader.read_line(&mut line).unwrap();
    let edges: usize = line.trim().parse().unwrap();

    for _i in 0 .. edges {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        let elts: Vec<&str> = line[..].split_whitespace().collect();
        let src: usize = elts[0].parse().unwrap();
        let dst: usize = elts[1].parse().unwrap();
        let num_operations: usize = elts[2].parse().unwrap();

        let mut operations = vec![];

        for _j in 0 .. num_operations {
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let elts: Vec<&str> = line[..].split_whitespace().collect();
            let src_key: usize = elts[0].parse().unwrap();
            let dst_key: usize = elts[1].parse().unwrap();
            let is_forward: usize = elts[2].parse().unwrap();
            let is_forward = if is_forward == 1 { true } else {false};
            operations.push(PlanOperation{src_key, dst_key, is_forward});
        }

        plan.edges.push(PlanEdge{
            src: plan.nodes[src].clone(),
            dst: plan.nodes[dst].clone(),
            extensions: Vec::new(),
            intersections: Vec::new(),
            operations,
        })
    }

    plan.initialize();

    plan
}

fn label_matching<P: Indexable<Node>>(matching: &P, vertex_id_label_map: Arc<HashMap<Node, Label>>) -> Vec<Label> {
    let mut labels = Vec::new();

    for i in 0..matching.length() {
        labels.push(vertex_id_label_map[&matching.index(i)])
    }

    labels
}
