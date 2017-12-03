

#ifndef _BFS_H_
#define _BFS_H_

#include "graph/graph.h"
#include "ring.h"

using namespace Ring;

struct BFSData{
  int64_t parent;
  void init() { parent = -1;}
};

using V = Vertex<BFSData, Empty>;
using G = Graph<BFSData, Empty>;


double make_bfs_tree(GlobalAddress<G> g, GlobalAddress<int64_t> bfs_tree, int64_t root) {
  
  std::cout << "root is : " << root << std::endl;

  double t = walltime(); 

  each_vertex(g, [=](V* v){ (*v)->init(); });

  vertex_do(g, root, [=](V* v){ 
      (*v)->parent = root;
      v->set_imm_active();
  });
 
  /*---------- BFS ----------*/
  auto emit = [g](V* sv){ return g->id(*sv); };
  auto edgework = [](int64_t vdata, G::Edge* e){ return vdata; };
  auto slot = [](V* ev, int64_t edata){ 
    if((*ev)->parent == -1){
      (*ev)->parent = edata;
      return true;
    }
    return false;
  };
  auto cond = [](V* v){ return (*v)->parent == -1;};

  int64_t frontier_size = 1;
  while(frontier_size !=0){
    frontier_size = graph_do<Sparse>(g, frontier_size, emit, edgework, slot, cond);
  }
  /*-------------------------*/

  double bfs_time = walltime() - t;
  

  // storage bfs tree
  each_vertex(g, [=](int64_t i, V* v){
    int64_t parent = (*v)->parent;
    call<async>(bfs_tree+i, [parent](int64_t* bfst){
      *bfst = parent;
    });
  });
  
  return bfs_time;
}

#endif
