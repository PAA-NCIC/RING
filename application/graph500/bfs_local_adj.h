
#ifndef _BFS_LOCAL_ADJ_H_
#define _BFS_LOCAL_ADJ_H_

#include "common.h"
#include "graph.h"
#include "ring.h"

using namespace Ring;

thread_local GlobalAddress<Graph<VertexP>> g;
thread_local GlobalAddress<int64_t> bfs_tree;

thread_local int64_t * frontier;
thread_local int64_t frontier_sz;
thread_local int64_t frontier_head;
thread_local int64_t frontier_tail;
thread_local int64_t frontier_level_mark;

void frontier_push(int64_t v) { frontier[frontier_tail++] = v; ASSERT(frontier_tail < frontier_sz, "frontier push error"); }
int64_t frontier_pop() { return frontier[frontier_head++]; }
int64_t frontier_next_level_size() { return frontier_tail - frontier_level_mark; }



double make_bfs_tree(GlobalAddress<Graph<VertexP>> g_in, GlobalAddress<int64_t> _bfs_tree, int64_t root) {
  static_assert(sizeof(long) == sizeof(int64_t), "Can't use long as substitute for int64_t");
  
  all_do([g_in, _bfs_tree]{
    g = g_in;
    bfs_tree = _bfs_tree;
    
    frontier_sz = g->nv/cores()*2;
    frontier = (int64_t*)malloc(sizeof(int64_t)*(frontier_sz));
    frontier_head = 0;
    frontier_tail = 0;
    frontier_level_mark = 0;
  });
  
  double t = walltime(); 

  pfor(g->vs, g->nv, [root](int64_t i, VertexP* v){
    if (i == root) {
      v->parent(root);
      frontier_push(root);
    } else {
      v->parent(-1);
    }
  });

  // bfs expand frontier
  all_do([root]{
    
    int64_t next_level_total;
    int64_t tot_point = 1;
    int64_t yield_ct = 0;
    do {
      frontier_level_mark = frontier_tail;
      barrier();
      //double _tm;
      //if(my_id() == 0){
        //sync_printf("##########");
        //_tm = walltime();
      //}
      
      //int call_send_cnt = 0;
      while (frontier_head < frontier_level_mark) {
        int64_t sv = frontier_pop();
        auto& src_v = *((g->vs+sv).ptr());
        for (auto& ev : src_v.adj_iter()) {
          //call_send_cnt ++;
          call<async>( (g->vs+ev).get_id(), [sv,ev]{
            auto& end_v = *(g->vs+ev).ptr();
            if (end_v.parent() == -1) {
              end_v.parent(sv);  // set as parent
              frontier_push(ev); // visit child in next level 
            }
          });
        }
        maybe_yield();
      }

      barrier();
      //sync_printf(my_id(), call_send_cnt);
      //if(my_id() == 0){
        //std::cout << "_tm :" << walltime()-_tm << std::endl;
      //}
      next_level_total = allreduce<int64_t,collective_add>( frontier_tail - frontier_level_mark );      
      tot_point += next_level_total;
      //if(my_id() == 0) sync_printf(next_level_total, tot_point);
    } while (next_level_total > 0);
  });
 
  double bfs_time = walltime() - t;
  
  
  pfor(g->vs, g->nv, [](int64_t i, VertexP* v){
    int64_t parent = v->parent();
    call<async>(bfs_tree+i, [parent](int64_t* bfst){
        *bfst = parent;
    });
  });
  
  
  all_do([]{
    free(frontier);
  });
  
  return bfs_time;
}

#endif
