#include <iostream>
#include <vector>
#include <queue>

#include "plan.hpp"

#include "boost/algorithm/string.hpp"
#include "boost/functional/hash.hpp"
#include "boost/graph/adjacency_list.hpp"
#include "boost/graph/connected_components.hpp"
#include "boost/graph/copy.hpp"
#include <boost/graph/mcgregor_common_subgraphs.hpp>
#include "boost/graph/vf2_sub_graph_iso.hpp"
#include "boost/property_map/property_map.hpp"

using EdgeProperty = boost::property<boost::edge_name_t, unsigned int>;
using VertexProperty = boost::property<boost::vertex_name_t, unsigned int, boost::property<boost::vertex_index_t, int> >;
using Graph = boost::adjacency_list<boost::vecS, boost::vecS, boost::bidirectionalS, VertexProperty, EdgeProperty>;

std::ostream& operator<<(std::ostream& out, const Graph& g) {
    //out<<"---Print Graph Start ----"<< std::endl;
    auto labelling_vertex = boost::get(boost::vertex_name, g);
    out << boost::num_vertices(g)<< " " << boost::num_edges(g) << std::endl;
    for (unsigned i = 0; i < boost::num_vertices(g); i++) {
      out << labelling_vertex[i] << " ";
    }
    out << std::endl;
    for (auto ep = boost::edges(g); ep.first != ep.second; ++ep.first) {
      unsigned int source_ = boost::source(*ep.first, g);
      unsigned int target_ = boost::target(*ep.first, g);
      out << source_ << " " << target_ << std::endl;
    }
    //out<<"---Print Graph End----"<< std::endl;
    return out;
}

struct GraphHash {
  std::size_t operator()(Graph const &Graph) const;
};

std::size_t GraphHash::operator()(Graph const &Graph) const {
  std::size_t res = 0;
  auto labelling_vertex = boost::get(boost::vertex_name, Graph);
  auto labelling_edge = boost::get(boost::edge_name, Graph);
  unsigned int edge_xor = 1;
  unsigned int vertex_xor = 1;
  for (auto ep = boost::edges(Graph); ep.first != ep.second; ++ep.first) {
    unsigned int source = boost::source(*ep.first, Graph);
    unsigned int target = boost::target(*ep.first, Graph);
    edge_xor = edge_xor ^ labelling_edge[*ep.first];
    vertex_xor = vertex_xor ^ labelling_vertex[source] ^ labelling_vertex[target];
  }
  unsigned int multi = edge_xor + vertex_xor;
  boost::hash_combine(res, multi);
  boost::hash_combine(res, boost::num_vertices(Graph));
  boost::hash_combine(res, boost::num_edges(Graph));
  return res;
}

bool check_iso(const Graph& small_graph, const Graph& large_graph) {
  // fast check at beginning
  if (boost::num_vertices(small_graph) != boost::num_vertices(large_graph) || boost::num_edges(small_graph) != boost::num_edges(large_graph)) {
    return false;
  }

  auto vertex_name_map1 = boost::get(boost::vertex_name, small_graph);
  auto vertex_name_map2 = boost::get(boost::vertex_name, large_graph);
  auto edge_name_map1 = boost::get(boost::edge_name, small_graph);
  auto edge_name_map2 = boost::get(boost::edge_name, large_graph);

  auto vertex_comp = boost::make_property_map_equivalent(vertex_name_map1, vertex_name_map2);
  auto edge_comp = boost::make_property_map_equivalent(edge_name_map1, edge_name_map2);
  // callback that do nothing
  auto cb = [&](auto &&f, auto &&) {
    // do nothing
    return true;
  };
  // boost::vf2_print_callback <Graph, Graph> callback(small_graph, large_graph);
  return boost::vf2_subgraph_iso(small_graph, large_graph, cb, boost::vertex_order_by_mult(small_graph),
                                 boost::edges_equivalent(edge_comp).vertices_equivalent(vertex_comp));
}

struct CmpGraph {
  inline bool operator()(const Graph &a, const Graph &b) const {
    return check_iso(a, b);
  }
};

struct CountKeyHash{
  std::size_t operator()(std::pair<unsigned, std::vector<unsigned>> const &pair) const {
      auto node_id = pair.first;
      auto& labels = pair.second;
      std::size_t res = 0;
      boost::hash_combine(res, boost::hash_range(labels.begin(), labels.end()));
      boost::hash_combine(res, node_id);
      return res;
  }
};

std::vector<Graph> get_id_graph_map_from_plan(Plan plan);

int main(int argc, char* argv[]) {
  Plan plan(argv[1]);
  std::ifstream count_file(argv[2]);
  std::vector<Graph> id_graph_map = get_id_graph_map_from_plan(plan);

  std::vector<unsigned> id_vertex_num_map = plan.get_id_vertex_num();
  std::unordered_map<Graph, unsigned, GraphHash, CmpGraph> labeled_query_count;
  std::unordered_map<std::pair<unsigned, std::vector<unsigned>>, unsigned, CountKeyHash> raw_count;

//deduplicate count record
  unsigned node_id;
  while (count_file >> node_id) {
    unsigned node_num = id_vertex_num_map[node_id]; 
    std::vector<unsigned> labels;
    labels.resize(node_num);
    for (unsigned i = 0; i < node_num; i++){
      unsigned label;
      count_file >> labels[i];
    }
    unsigned count;
    count_file >> count; 
    raw_count[std::make_pair(node_id,labels)] = count;
  }
  count_file.close();
  //combine isomorphic labeled queries
  
  for (auto iter = raw_count.begin(); iter != raw_count.end(); iter++) {
    auto& key = iter->first;
    auto node_id = key.first;
    auto& labels = key.second;
    auto count = iter->second;
    Graph g =  id_graph_map[node_id];
    auto labelling_vertex = boost::get(boost::vertex_name, g);
    for (unsigned i = 0; i < labels.size(); i++){
      labelling_vertex[i] = labels[i];
    }
    labeled_query_count[g] = labeled_query_count[g] + count;
  }
  for (auto iter = labeled_query_count.begin(); iter != labeled_query_count.end(); iter++) {
    std::cout << "Count:" << iter->second << std::endl;
    std::cout << iter->first << std::endl;
  }

  return 0;
}

std::vector<Graph> get_id_graph_map_from_plan(Plan plan){
  std::vector<Graph> ret;
  ret.resize(plan.nodes.size());
  unsigned root_node_id = plan.root_node_id;
  std::queue<unsigned> q; //proccessed node_id
  q.push(root_node_id);
  boost::add_vertex(ret[root_node_id]);
  boost::add_vertex(ret[root_node_id]);
  boost::add_edge(0, 1, ret[root_node_id]);
  while (!q.empty()) {
    auto cur = q.front();
    auto& cur_node = plan.nodes[cur];
    q.pop();
    unsigned start_edge = cur_node.edge_start_idx;
    unsigned end_edge = cur_node.edge_start_idx + cur_node.num_edges;
    for (unsigned i = start_edge; i < end_edge; i++) {
      auto& edge = plan.edges[i];
      auto child = edge.dst;
      boost::copy_graph(ret[cur], ret[child->idx]);
      if (cur_node.subgraph_num_vertices < child->subgraph_num_vertices) {
        boost::add_vertex(ret[child->idx]);
      }
      for (auto& opt: edge.operations) {
        if (opt.is_forward){
          boost::add_edge(opt.src_key, opt.dst_key, ret[child->idx]);
        }else{
          boost::add_edge(opt.dst_key, opt.src_key, ret[child->idx]);
        }
      }
      q.push(child->idx);
    }
  }
  return ret;
}
