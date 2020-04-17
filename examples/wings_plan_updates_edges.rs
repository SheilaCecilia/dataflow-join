extern crate timely;
extern crate graph_map;
extern crate alg3_dynamic;

use std::sync::{Arc, Mutex};

use alg3_dynamic::wings_plan::*;

use timely::communication::{Configuration};
use timely::dataflow::{ProbeHandle};
use timely::dataflow::operators::*;

#[allow(non_snake_case)]
fn main () {
    //datasetDirectory  batchSize  numBatch  baseSize  planFile
    let start = ::std::time::Instant::now();

    let send = Arc::new(Mutex::new(0));
    let send2 = send.clone();

    let inspect = ::std::env::args().find(|x| x == "inspect").is_some();

    timely::execute_from_args(std::env::args(), move |root| {

        let send = send.clone();

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
        let plan = plan::read_plan(&plan_filename);

        // handles to input and probe, but also both indices so we can compact them.
        let (mut inputG, mut inputQ, forward_probe, reverse_probe, probe, handles) = root.dataflow::<u32,_,_>(|builder| {

            // Please see triangles for more information on "graph" and dG.
            let (graph, dG) = builder.new_input::<(u32, u32)>();
            let (query, dQ) = builder.new_input::<((u32, u32), i32)>();
            // Our query is K3 = A(x,y) B(x,z) C(y,z): triangles..

            let (graph_index, handles) = GraphStreamIndex::from(dG, dQ, |k| k as u64, |k| k as u64);

            let mut probe = ProbeHandle::new();

            plan.track_motif(&graph_index, &mut probe, send);

            (graph, query, graph_index.forward.handle , graph_index.reverse.handle, probe, handles)
        });

        // number of nodes introduced at a time
        let num_processes = peers as usize / num_threads;
        let batch_size: usize = std::env::args().nth(2).unwrap().parse().unwrap();
        let batch_size = batch_size / num_processes;
        let num_batches: usize = std::env::args().nth(3).unwrap().parse().unwrap();
        let base_size: usize = std::env::args().nth(4).unwrap().parse().unwrap();
        let base_size = base_size / num_processes;

        let mut dir_reader_option: Option<DirReader> = None;

       //let mut reader_option: Option<BufReader<File>> = None;
        let mut edges = Vec::new();

        if local_index == 0 {
            let graph_dirname = std::env::args().nth(1).unwrap();
            dir_reader_option = Some(DirReader::new(&graph_dirname));

            let reader = dir_reader_option.as_mut().unwrap();

            edges = reader.read_edges(base_size);
        }

        // synchronize with other workers.
        let prevG = inputG.time().clone();
        inputG.advance_to(prevG.inner + 1);
        inputQ.advance_to(prevG.inner + 1);
        root.step_while(|| probe.less_than(inputG.time()));

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

        if local_index == 0 && inspect {
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

        while batch_index < num_batches {
            let mut edgesQ = Vec::new();
            if local_index == 0 {
                read_start = ::std::time::Instant::now();
                edgesQ = dir_reader_option.as_mut().unwrap().read_edges(batch_size).iter().map(|&(src, dst)| ((src, dst), 1)).collect::<Vec<_>>();
                //edgesQ = read_batch_edges(&mut reader_option.as_mut().unwrap(), batch_size);
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
                println!("Batch {} pm time: {:?}", batch_index, batch_end.duration_since(batch_mid));
            }

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
        println!("elapsed: {:?}\ttotal triangles at this process: {:?}", start.elapsed(), total);
    }
}

