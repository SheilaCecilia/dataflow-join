extern crate timely;
extern crate graph_map;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex, RwLock};

use alg3_dynamic::wings_plan::*;

use timely::communication::{Configuration};
use timely::dataflow::{ProbeHandle};
use timely::dataflow::operators::*;

use std::io::BufReader;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
use std::collections::HashMap;

type Label = u32;

#[allow(non_snake_case)]
fn main () {
    //datasetFile  batchSize  numBatch  baseSize  planFile vertexLabelFile
    let start_main = ::std::time::Instant::now();

    let send = Arc::new(Mutex::new(0));
    let send2 = send.clone();
    let labeled_query_count = Arc::new(RwLock::new(HashMap::new()));

    let inspect = ::std::env::args().find(|x| x == "inspect").is_some();

    //read vertex label
    let vertex_label_filename = std::env::args().nth(6).unwrap();
    let vertex_id_label_map = Arc::new(read_vertex_id_label_mapping(&vertex_label_filename));

    timely::execute_from_args(std::env::args(), move |root| {
        
        let start_dataflow = ::std::time::Instant::now();
        let send = send.clone();
        let counters = labeled_query_count.clone();
        let vertex_id_label_map = vertex_id_label_map.clone();

        // used to partition graph loading
        let index = root.index() as u32;
        let peers = root.peers() as u32;

        let configuration = Configuration::from_args(std::env::args()).unwrap();
        let num_threads = match configuration {
            Configuration::Thread => 1,
            Configuration::Process(threads) => threads,
            Configuration::Cluster(threads, _, _, _, _) => threads,
        };
        let local_index = index % num_threads as u32;

        let plan_filename = std::env::args().nth(5).unwrap();
        let plan = count_vertex_labeled_query_plan::read_plan(&plan_filename);

        // handles to input and probe, but also both indices so we can compact them.
        let (mut inputG, mut inputQ, forward_probe, reverse_probe, probe, handles) = root.dataflow::<u32,_,_>(|builder| {

            // Please see triangles for more information on "graph" and dG.
            let (graph, dG) = builder.new_input::<(u32, u32)>();
            let (query, dQ) = builder.new_input::<((u32, u32), i32)>();
            // Our query is K3 = A(x,y) B(x,z) C(y,z): triangles..

            let (graph_index, handles) = GraphStreamIndex::from(dG, dQ, |k| k as u64, |k| k as u64);

            let mut probe = ProbeHandle::new();

            plan.track_motif(&graph_index, &mut probe, send, counters, vertex_id_label_map);

            (graph, query, graph_index.forward.handle , graph_index.reverse.handle, probe, handles)
        });
        let end_dataflow = ::std::time::Instant::now();
        println!("worker {} build dataflow: {:?}", index, end_dataflow.duration_since(start_dataflow));

        let start_read_base = ::std::time::Instant::now();
        // number of nodes introduced at a time
        let num_processes = peers as usize / num_threads;
        let batch_size: usize = std::env::args().nth(2).unwrap().parse().unwrap();
        let batch_size = batch_size / num_processes;
        let num_batches: usize = std::env::args().nth(3).unwrap().parse().unwrap();
        let base_size: usize = std::env::args().nth(4).unwrap().parse().unwrap();
        let limit = (base_size / num_processes) as usize;

        let mut reader_option: Option<BufReader<File>> = None;
        let mut edges = Vec::new();

        if local_index == 0 {
            let graph_filename = std::env::args().nth(1).unwrap();
            let path = Path::new(&graph_filename);
            let display = path.display();

            // Open the path in read-only mode, returns `io::Result<File>`
            let file = match File::open(&path) {
                // The `description` method of `io::Error` returns a string that describes the error
                Err(why) => {
                    panic!("EXCEPTION: couldn't open {}: {}",
                           display,
                           Error::description(&why))
                }
                Ok(file) => file,
            };

            reader_option = Some(BufReader::new(file));
            let reader = reader_option.as_mut().unwrap();

            let mut num_edges = 0;

            while num_edges < limit {
                let mut line = String::new();
                reader.read_line(&mut line).unwrap();
                if !line.starts_with('#') && line.len() > 0 {
                    let elts: Vec<&str> = line[..].split_whitespace().collect();
                    let src: u32 = elts[0].parse().ok().expect("malformed src");
                    let dst: u32 = elts[1].parse().ok().expect("malformed dst");
                    edges.push((src, dst));
                    num_edges += 1;
                }
            }
        }

        // synchronize with other workers.
        let prevG = inputG.time().clone();
        inputG.advance_to(prevG.inner + 1);
        inputQ.advance_to(prevG.inner + 1);
        root.step_while(|| probe.less_than(inputG.time()));
        let end_read_base = ::std::time::Instant::now();
        println!("worker {} read base graph: {:?}", index, end_read_base.duration_since(start_read_base));

        // start the experiment!
        let start = ::std::time::Instant::now();

        if local_index == 0{
            // load graph to data flow
            inputG.send_batch(&mut edges);
        }

        let prevG = inputG.time().clone();
        inputG.advance_to(prevG.inner + 1);
        inputQ.advance_to(prevG.inner + 1);
        root.step_while(|| probe.less_than(inputG.time()));

        if inspect {
            println!("{:?}\t[worker {}]\tdata loaded", start.elapsed(), index);
        }

        // merge all of the indices we maintain.
        let prevG = inputG.time().clone();
        handles.merge_to(&prevG);

        if inspect {
            println!("{:?}\t[worker {}]\tindices merged", start.elapsed(), index);
        }

        let prevG = inputG.time().clone();
        inputG.advance_to(prevG.inner + 1);
        inputQ.advance_to(prevG.inner + 1);
        root.step_while(|| probe.less_than(inputG.time()));

        let mut batch_index = 0 as usize;
        let mut read_start = ::std::time::Instant::now();
        let mut batch_start = ::std::time::Instant::now();
        let mut batch_mid: std::time::Instant;
        let mut batch_end: std::time::Instant;

//        let mut read_edge_time =  Vec::new();
//        let mut update_index_time = Vec::new();
//        let mut pattern_matching_time = Vec::new();

        while batch_index < num_batches {
            let mut edgesQ = Vec::new();
            if local_index == 0 {
                read_start = ::std::time::Instant::now();
                edgesQ = read_batch_edges(&mut reader_option.as_mut().unwrap(), batch_size, index);
                batch_start = ::std::time::Instant::now();
            }

            let prevG = inputG.time().clone();
            inputG.advance_to(prevG.inner + 1);

            if local_index == 0 {
                inputQ.send_batch(&mut edgesQ);
            }

            let prev = inputQ.time().clone();
            inputQ.advance_to(prev.inner + 1);

            root.step_while(|| forward_probe.less_than(inputQ.time()) ||reverse_probe.less_than(inputQ.time()));
            batch_mid = ::std::time::Instant::now();

            root.step_while(|| probe.less_than(inputQ.time()));

            // merge all of the indices we maintain.
            handles.merge_to(&prev);
            batch_end = ::std::time::Instant::now();

            if local_index == 0{
                println!("Batch {} read edge time: {:?}", batch_index, batch_start.duration_since(read_start));
                println!("Batch {} update index time: {:?}", batch_index, batch_mid.duration_since(batch_start));
                println!("Batch {} pattern matching time: {:?}", batch_index, batch_end.duration_since(batch_mid));
            }

//            if local_index == 0{
//                read_edge_time.push(batch_start.duration_since(read_start));
//                update_index_time.push(batch_mid.duration_since(batch_start));
//                pattern_matching_time.push(batch_end.duration_since(batch_mid));
//
//                if (batch_index + 1) % 100 == 0 {
//                    let idx_start = batch_index - 99;
//                    let idx_end = batch_index + 1;
//                    for idx in idx_start..idx_end {
//                        println!("Batch {} read edge time: {:?}", idx, read_edge_time[idx - idx_start]);
//                        println!("Batch {} update index time: {:?}", idx, update_index_time[idx - idx_start]);
//                        println!("Batch {} pattern matching time: {:?}", idx, pattern_matching_time[idx - idx_start]);
//                    }
//                    read_edge_time.clear();
//                    update_index_time.clear();
//                    pattern_matching_time.clear();
//                }
//            }

            batch_index += 1;
        }

        inputG.close();
        inputQ.close();
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
        println!("elapsed: {:?}\ttotal matchings at this process: {:?}", start_main.elapsed(), total);
    }
}

fn read_batch_edges(reader: &mut BufReader<File>, batch: usize, index: u32) -> Vec<((u32, u32), i32)>{
    println!("Worker {} start: Read batch with {} edges", index, batch);

    let mut batch_edges = Vec::new();
    let mut num_edges = 0;

    while num_edges < batch {
        let mut line = String::new();
        reader.read_line(&mut line).unwrap();
        if !line.starts_with('#') && line.len() > 0 {
            let elts: Vec<&str> = line[..].split_whitespace().collect();
            let src: u32 = elts[0].parse().ok().expect("malformed src");
            let dst: u32 = elts[1].parse().ok().expect("malformed dst");
            batch_edges.push(((src, dst),1));
            num_edges += 1;
        }
    }

    println!("Worker {} end: Read batch with {} edges", index, batch);

    batch_edges
}

fn read_vertex_id_label_mapping(filename: &String) -> HashMap<u32, u32> {
    let mut vertex_label_map = HashMap::new();

    let path = Path::new(&filename);
    let display = path.display();
    let file = match File::open(&path) {
        // The `description` method of `io::Error` returns a string that describes the error
        Err(why) => {
            panic!("EXCEPTION: couldn't open {}: {}",
                   display,
                   Error::description(&why))
        }
        Ok(file) => file,
    };

    let reader = BufReader::new(file);

    for line in reader.lines() {
        let good_line = line.ok().expect("EXCEPTION: read error");
        if good_line.len() > 0 {
            let elts: Vec<&str> = good_line[..].split_whitespace().collect();
            let node: Node = elts[0].parse().ok().expect("malformed node");
            let label: Label = elts[1].parse().ok().expect("malformed label");
            vertex_label_map.insert(node, label);
        }
    }

    vertex_label_map
}

