use std::io::BufReader;
use std::fs::File;
use std::io::prelude::*;
use std::path::{PathBuf};
use std::vec::IntoIter;
use std::collections::HashMap;

use std::{fs, io};

pub struct DirReader {
    reader: BufReader<File>,
    paths: IntoIter<PathBuf>,
}

impl DirReader {
    pub fn new(dirname: &str) -> DirReader {
//        let mut paths = fs::read_dir(dirname).unwrap()
//            .map(|res| res.map(|e| e.path()))
//            .collect::<Result<Vec<_>, io::Error>>().unwrap().into_iter();
        let mut paths = fs::read_dir(dirname).unwrap()
            .map(|res| res.map(|e| e.path()))
            .collect::<Result<Vec<_>, io::Error>>().unwrap();

        paths.sort();

        let mut paths = paths.into_iter();

        let path = paths.next().unwrap();
        let file = File::open(path).unwrap();
        let reader = BufReader::new(file);

        DirReader{reader, paths}
    }


    pub fn read_edges (&mut self, num_edges: usize) -> Vec<(u32, u32)>{
        let mut edges = Vec::new();

        let mut edge_index = 0;
        let mut discard = false;
        while edge_index < num_edges {
            let mut line = String::new();
            if self.reader.read_line(&mut line).unwrap() == 0 {
                let path: PathBuf;
                if let Some(p) = self.paths.next() {
                    path = p;
                }else {
                    return edges;
                }

                let file = File::open(path).unwrap();
                self.reader = BufReader::new(file);
                continue;
            }
            if !discard && !line.starts_with('#') && line.len() > 0 {
                let elts: Vec<&str> = line[..].split_whitespace().collect();
                let src: u32 = elts[0].parse().ok().expect("malformed src");
                let dst: u32 = elts[1].parse().ok().expect("malformed dst");
                edges.push((src, dst));
                edge_index += 1;
            }
            discard = !discard;
        }

        edges
    }

    pub fn read_vertex_labels(&mut self) -> HashMap<u32, u32> {
        let mut vertex_label_map = HashMap::new();

        while true {
            let mut line = String::new();
            if self.reader.read_line(&mut line).unwrap() == 0 {
                let path: PathBuf;
                if let Some(p) = self.paths.next() {
                    path = p;
                } else {
                    return vertex_label_map;
                }
                let file = File::open(path).unwrap();
                self.reader = BufReader::new(file);
                continue;
            }
            if line.len() > 0 {
                let elts: Vec<&str> = line[..].split_whitespace().collect();
                let node: u32 = elts[0].parse().ok().expect("malformed node");
                let label: u32 = elts[1].parse().ok().expect("malformed label");
                vertex_label_map.insert(node, label);
            }
        }
        vertex_label_map
    }
}