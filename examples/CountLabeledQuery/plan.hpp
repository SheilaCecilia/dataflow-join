#include <fstream>
#include <iostream>

struct PlanNode {
	unsigned edge_start_idx;
	unsigned num_edges;
	unsigned subgraph_num_vertices;
	unsigned is_query;
	unsigned idx;
};

struct PlanOperation{
    unsigned src_key;
    unsigned dst_key;
    bool is_forward;
};

struct PlanEdge {
    unsigned id;
    const PlanNode* src;
    const PlanNode* dst;
    std::vector<PlanOperation> operations;
};

class Plan {
public:
    unsigned root_node_id;
    std::vector<PlanEdge> edges;
    std::vector<PlanNode> nodes;

	Plan(std::string filename){
	  std::ifstream file(filename);
	  unsigned nodes_num;
	  unsigned discard;
	  file >> discard >> discard >> discard;
	  file >> this->root_node_id;
	  file >> nodes_num; 
	  this->nodes.resize(nodes_num);
	  {// read nodes
	  	unsigned edge_start_idx, num_edges, subgraph_num_vertices, is_query;
	    for(unsigned i = 0; i < nodes_num; i++){
	      auto& node = this->nodes[i];
	      node.idx = i;
	      file >> node.edge_start_idx >> node.num_edges >> node.subgraph_num_vertices >> node.is_query;
	    }
	  }
		{ // read edges and operations
		    unsigned num_edges;
		    file >> num_edges;
		    this->edges.resize(num_edges);
		    for (unsigned i = 0; i < num_edges; i++) {
		        auto& edge = this->edges[i];
		        unsigned num_operations, src_idx, dst_idx;
		        file >> src_idx >> dst_idx >> num_operations;
		        edge.id = i;
		        edge.src = &this->nodes[src_idx];
		        edge.dst = &this->nodes[dst_idx];
		        edge.operations.resize(num_operations);
		        unsigned src_key, dst_key;
		        bool is_forward;
		        for (auto& o: edge.operations) {
		            file >> o.src_key >> o.dst_key >> o.is_forward;
		        }
		        // for (auto& o: edge.operations) {
		    }
		    file.close();
		}
	}

	std::vector<unsigned> get_id_vertex_num() {
		std::vector<unsigned> ret;
		unsigned size = this->nodes.size();

		for(auto& node: this->nodes) {
			ret.push_back(node.subgraph_num_vertices);
		}
		return ret;
	}
};