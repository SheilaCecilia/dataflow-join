use std::rc::Rc;

use timely::dataflow::*;
use timely::dataflow::operators::*;

use std::sync::{Arc, Mutex};
use timely::dataflow::{ProbeHandle};
use timely::dataflow::operators::{Exchange, Inspect, Probe};
use timely::{Data, ExchangeData};

use super::Indexable;

use super::graph_stream::GraphStreamIndex;
use wings_plan::ExtendEdges;

pub type Node = u32;
pub type Edge = (Node, Node);

//#[derive(Debug, Default)]
pub struct PlanNode{
    // edges[edge_start_idx, edge_start_idx + num_edges] are the out edges of this node
    edge_start_idx: usize,
    num_edges: usize,
    subgraph_num_vertices: usize,
    is_query: bool,
}

//#[derive(Debug, Default)]
pub struct PlanOperation{
    src_key: usize,
    dst_key: usize,
    is_forward: bool,
}

//#[derive(Debug, Default)]
pub struct PlanEdge{
    src: Rc<PlanNode>,
    dst: Rc<PlanNode>,
    operations: Vec<PlanOperation>,
}

//#[derive(Debug, Default)]
pub struct Plan{
    edges: Vec<PlanEdge>,
    nodes: Vec<Rc<PlanNode>>,
    root_node_id: usize,
}

impl Plan{
    pub fn track_motif<H1, H2, G: Scope>(&self, graph: &GraphStreamIndex<G, H1, H2>, probe: &mut ProbeHandle<G::Timestamp>, counter: Arc<Mutex<usize>>)
        where H1: Fn(Node)->u64 + 'static,
              H2: Fn(Node)->u64 + 'static
    {
        let root = self.nodes[self.root_node_id].clone();
        self.execute_node(root, &graph.updates, graph, probe, counter);
    }

    fn execute_node<H1, H2, G: Scope, P>(&self, root: Rc<PlanNode>, stream: &Stream<G, (P, i32)>, graph: &GraphStreamIndex<G, H1, H2>, probe: &mut ProbeHandle<G::Timestamp>, counter: Arc<Mutex<usize>>)
        where H1: Fn(Node)->u64 + 'static,
              H2: Fn(Node)->u64 + 'static,
              P: ::std::fmt::Debug+ExchangeData+Indexable<Node>,

    {
        let start_idx = root.edge_start_idx;
        let end_idx = root.edge_start_idx + root.num_edges;

        for index in start_idx .. end_idx{
            let child = self.edges[index].dst.clone();
            let output:Stream<G, (P, i32)>;
            let counter1 = counter.clone();
            let counter2 = counter.clone();
            if root.subgraph_num_vertices < child.subgraph_num_vertices{
                let attributes = self.edges[index].get_extend_attributes();
                output = stream.extend_attributes(graph, &attributes)
                    .flat_map(|(p, es, w)|
                        es.into_iter().map(move |e|  {
                            let mut clone = p.clone();
                            clone.push(e);
                            (clone, w)
                        }));
            }else{
                let attributes = self.edges[index].get_intersect_attributes();
                output = stream.intersect_attributes(graph, &attributes);
            }
            if child.is_query{
                output.probe_with(probe);
                output.exchange(|x| (x.0).index(0) as u64)
                    // .inspect_batch(|t,x| println!("{:?}: {:?}", t, x))
                    .count()
                    .inspect_batch(move |t,x| println!("{:?}: {:?}", t, x))
                    .inspect_batch(move |_,x| {
                        if let Ok(mut bound) = counter1.lock() {
                            *bound += x[0];
                        }
                    });
            }
            self.execute_node(child, &output, graph, probe, counter2);
        }
    }
}

impl PlanEdge{
    fn get_extend_attributes(&self) -> Vec<(usize, bool)>{
        let mut constraints = vec![];

        for &ref operation in &self.operations{
            constraints.push((operation.src_key, operation.is_forward));
        }

        constraints
    }

    fn get_intersect_attributes(&self) -> Vec<(usize, usize)>{
        let mut constraints = vec![];

        for &ref operation in &self.operations{
            if operation.is_forward{
                constraints.push((operation.src_key, operation.dst_key));
            }else{
                constraints.push((operation.dst_key, operation.src_key));
            }
        }

        println!("get_intersect_attributes: {:?}", constraints);

        constraints
    }
}

pub fn get_test_extend_plan() -> Plan{
    let mut plan = Plan{edges: vec![], nodes: vec![], root_node_id: 0};

    plan.root_node_id = 0;

    plan.nodes = vec![
        Rc::new(PlanNode{
            edge_start_idx: 0,
            num_edges: 3,
            subgraph_num_vertices: 2,
            is_query: false,
        }),
        Rc::new(PlanNode{
            edge_start_idx: 3,
            num_edges: 0,
            subgraph_num_vertices: 3,
            is_query: true,
        }),
        Rc::new(PlanNode{
            edge_start_idx: 3,
            num_edges: 0,
            subgraph_num_vertices: 3,
            is_query: true,
        }),
        Rc::new(PlanNode{
            edge_start_idx:3,
            num_edges: 0,
            subgraph_num_vertices: 3,
            is_query: true,
        })];

    plan.edges = vec![
        PlanEdge{
            src: plan.nodes[0].clone(),
            dst: plan.nodes[1].clone(),
            operations: vec![
                PlanOperation{src_key: 0, dst_key: 2, is_forward: true},
                PlanOperation{src_key: 1, dst_key: 2, is_forward: true}
            ]
        },
        PlanEdge{
            src: plan.nodes[0].clone(),
            dst: plan.nodes[2].clone(),
            operations: vec![
                PlanOperation{src_key: 0, dst_key: 2, is_forward: true},
                PlanOperation{src_key: 1, dst_key: 2, is_forward: false}
            ]
        },
        PlanEdge{
            src: plan.nodes[0].clone(),
            dst: plan.nodes[3].clone(),
            operations: vec![
                PlanOperation{src_key: 0, dst_key: 2, is_forward: false},
                PlanOperation{src_key: 1, dst_key: 2, is_forward: false}
            ]
        }];

    plan
}

pub fn get_test_intersect_plan() -> Plan{
    let mut plan = Plan{edges: vec![], nodes: vec![], root_node_id: 0};

    plan.root_node_id = 0;

    plan.nodes = vec![
        Rc::new(PlanNode{
            edge_start_idx: 0,
            num_edges: 3,
            subgraph_num_vertices: 2,
            is_query: false,
        }),
        Rc::new(PlanNode{
            edge_start_idx: 3,
            num_edges: 1,
            subgraph_num_vertices: 3,
            is_query: false,
        }),
        Rc::new(PlanNode{
            edge_start_idx: 4,
            num_edges: 0,
            subgraph_num_vertices: 3,
            is_query: true,
        }),
        Rc::new(PlanNode{
            edge_start_idx:4,
            num_edges: 0,
            subgraph_num_vertices: 3,
            is_query: true,
        }),
        Rc::new(PlanNode{
            edge_start_idx:4,
            num_edges: 0,
            subgraph_num_vertices: 3,
            is_query: true,
        })
    ];

    plan.edges = vec![
        PlanEdge{
            src: plan.nodes[0].clone(),
            dst: plan.nodes[1].clone(),
            operations: vec![
                //PlanOperation{src_key: 0, dst_key: 2, is_forward: true},
                PlanOperation{src_key: 1, dst_key: 2, is_forward: true}
            ]
        },
        PlanEdge{
            src: plan.nodes[0].clone(),
            dst: plan.nodes[2].clone(),
            operations: vec![
                PlanOperation{src_key: 0, dst_key: 2, is_forward: true},
                PlanOperation{src_key: 1, dst_key: 2, is_forward: false}
            ]
        },
        PlanEdge{
            src: plan.nodes[0].clone(),
            dst: plan.nodes[3].clone(),
            operations: vec![
                PlanOperation{src_key: 0, dst_key: 2, is_forward: false},
                PlanOperation{src_key: 1, dst_key: 2, is_forward: false}
            ]
        },
        PlanEdge{
            src: plan.nodes[1].clone(),
            dst: plan.nodes[4].clone(),
            operations: vec![
                PlanOperation{src_key: 0, dst_key: 2, is_forward: false},
                //PlanOperation{src_key: 1, dst_key: 2, is_forward: false}
            ]
        }

    ];

    plan
}