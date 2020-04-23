extern crate timely;
extern crate graph_map;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex};

use alg3_dynamic::wings_plan::*;

use timely::dataflow::{ProbeHandle};
use timely::dataflow::operators::*;

use graph_map::GraphMMap;

#[allow(non_snake_case)]
fn main () {

    let start = ::std::time::Instant::now();

    let send = Arc::new(Mutex::new(0));
    let send2 = send.clone();

    let inspect = ::std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args(), move |root| {

        let send = send.clone();

        // used to partition graph loading
        let index = root.index();
        let peers = root.peers();

        let plan_filename = std::env::args().nth(3).unwrap();
        let plan = plan::read_plan(&plan_filename);

        // handles to input and probe, but also both indices so we can compact them.
        let (mut input, forward_probe, reverse_probe, probe, handles) = root.dataflow::<u32,_,_>(|builder| {

            // Please see triangles for more information on "graph" and dG.
            let (graph, dG) = builder.new_input::<((u32, u32), i32)>();
            //let dG_init = dG.map(|((src,dst),wgt)| (vec![src,dst],wgt));
            // Our query is K3 = A(x,y) B(x,z) C(y,z): triangles..

            let (graph_index, handles) = GraphStreamIndex::from(Vec::new().to_stream(builder), dG, |k| k as u64, |k| k as u64);

            let mut probe = ProbeHandle::new();

            plan.track_motif(&graph_index, &mut probe, send);

            (graph, graph_index.forward.handle , graph_index.reverse.handle, probe, handles)
        });

        // load fragment of input graph into memory to avoid io while running.
        let filename = std::env::args().nth(1).unwrap();
        let graph = GraphMMap::new(&filename);

        let nodes = graph.nodes();
        let mut edges = Vec::new();

        for node in 0 .. graph.nodes() {
            if node % peers == index {
                edges.push(graph.edges(node).to_vec());
            }
        }

        drop(graph);

        // synchronize with other workers.
        let prev = input.time().clone();
        input.advance_to(prev.inner + 1);
        root.step_while(|| probe.less_than(input.time()));

        // number of nodes introduced at a time
        let batch: usize = std::env::args().nth(2).unwrap().parse().unwrap();

        // start the experiment!
        let start = ::std::time::Instant::now();
        let mut batch_start = ::std::time::Instant::now();
        let mut batch_mid: std::time::Instant;
        let mut batch_end: std::time::Instant;
        for node in 0 .. nodes {

            // introduce the node if it is this worker's responsibility
            if node % peers == index {
                for &edge in &edges[node / peers] {
                    if node as u32 != edge{
                        input.send(((node as u32, edge), 1));
                    }
                }
            }

            // if at a batch boundary, advance time and do work.
            if node % batch == (batch - 1) {
                let prev = input.time().clone();
                input.advance_to(prev.inner + 1);

                root.step_while(|| forward_probe.less_than(inputQ.time()) ||reverse_probe.less_than(inputQ.time()));
                batch_mid = ::std::time::Instant::now();

                root.step_while(|| probe.less_than(input.time()));
                batch_end = ::std::time::Instant::now();

                println!("After io: {:?}", batch_mid.duration_since(batch_start));
                println!("After batch: {:?}", batch_end.duration_since(batch_start));

                batch_start = ::std::time::Instant::now();
                // merge all of the indices we maintain.
                handles.merge_to(&prev);


            }
        }

        input.close();
        while root.step() { }

        if inspect {
            println!("worker {} elapsed: {:?}", index, start.elapsed());
        }

    }).unwrap();

    let total = if let Ok(lock) = send2.lock() {
        *lock
    }
    else { 0 };

    if inspect {
        println!("elapsed: {:?}\ttotal triangles at this process: {:?}", start.elapsed(), total);
    }
}
