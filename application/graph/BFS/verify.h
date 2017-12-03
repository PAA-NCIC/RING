#ifndef _BFS_VERIFY_H_
#define _BFS_VERIFY_H_

#include "ring.h"
#include "graph/graph.h"

using namespace Ring;

void compute_levels(GlobalAddress<int64_t> level, int64_t nv, GlobalAddress<int64_t> bfs_tree, int64_t root) {
  
  memset(level, (int64_t)-1, nv);
  write(level+root, 0);
  
  pfor(level, nv, [bfs_tree, level, nv, root] (int64_t k, int64_t* level_k) {
    if (*level_k >= 0) return;
  
    int64_t tree_k = read(bfs_tree+k);
    if (tree_k >= 0 && k != root) {
      int64_t parent = k;
      int64_t nhop = 0;
      int64_t next_parent;
    
      /* Run up the three until we encounter an already-leveled vertex. */
      while (parent >= 0 && read(level+parent) < 0 && nhop < nv) {
        next_parent = read(bfs_tree+parent);
        ASSERT(parent != next_parent, "parent == next_parent");
        parent = next_parent;
        ++nhop;
      }
      ASSERT( nhop < nv, "Error: root had a cycle.");
      ASSERT( parent >= 0, "Ran off the end for root.");
    
      // Now assign levels until we meet an already-leveled vertex
      // NOTE: This permits benign races if parallelized.
      nhop += read(level+parent);
      parent = k;
      while (read(level+parent) < 0) {
        ASSERT(nhop > 0, "nhop > 0")
        write(level+parent, nhop);
        nhop--;
        parent = read(bfs_tree+parent);
      }
      ASSERT(nhop == read(level+parent), "nhop == read(level+parent)");
    }
  });  
}


static thread_local int64_t nedge_traversed;
int64_t verify_bfs_tree(GlobalAddress<int64_t> bfs_tree, int64_t max_bfsvtx, int64_t root, EdgeList * el) {
  double start = walltime(); 

  ASSERT(read(bfs_tree+root) == root, "bfs_tree + root == root");
  
  int64_t nv = max_bfsvtx+1;
  
  GlobalAddress<int64_t> seen_edge = gmalloc<int64_t>(nv);
  GlobalAddress<int64_t> level = gmalloc<int64_t>(nv);
  
  compute_levels(level, nv, bfs_tree, root);
  //sync_printf("compute_levels");
  
  Ring::memset(seen_edge, 0, nv);
  
  all_do([]{ nedge_traversed = 0; });
    
  pfor(el->edges, el->nedge, [seen_edge, bfs_tree, level, max_bfsvtx]
      (int64_t e, WEdge* cedge) {  
    const int64_t i = cedge->v0;
    const int64_t j = cedge->v1;
    int64_t lvldiff;

    if (i < 0 || j < 0) return;
    ASSERT(!(i > max_bfsvtx && j <= max_bfsvtx), "Error!");
    ASSERT(!(j > max_bfsvtx && i <= max_bfsvtx), "Error!");
    if (i > max_bfsvtx) // both i & j are on the same side of max_bfsvtx
      return;

    // All neighbors must be in the tree.
    int64_t ti = read(bfs_tree+i);
    int64_t tj = read(bfs_tree+j);

    ASSERT(!(ti >= 0 && tj < 0), "Error! ");
    ASSERT(!(tj >= 0 && ti < 0), "Error! ");
    if (ti < 0) // both i & j have the same sign
      return;

    /* Both i and j are in the tree, count as a traversed edge.
     NOTE: This counts self-edges and repeated edges.  They're
     part of the input data.
     */
    nedge_traversed++;

    // Mark seen tree edges.
    if (i != j) {
      if (ti == j)
        write(seen_edge+i, 1);
      if (tj == i)
        write(seen_edge+j, 1);
    }
    lvldiff = read(level+i) - read(level+j);
    /* Check that the levels differ by no more than one. */
    ASSERT(!(lvldiff > 1 || lvldiff < -1), "Error, levels differ by more than one! ");
  });
  //sync_printf("check edges");

  nedge_traversed = reduce( nedge_traversed , collective_add);
  //sync_printf("reduce nedge_traversed");
  

  /* Check that every BFS edge was seen and that there's only one root. */
  pfor(bfs_tree, nv, [root, seen_edge](int64_t k, int64_t* tk){
    if (k != root) {
      ASSERT( !(*tk >= 0 && !read(seen_edge+k)), "Error!" );
      ASSERT( *tk != k, "Error!");
    }
  });  
  //sync_printf("check only one root");
  
  gfree(seen_edge);
  gfree(level);
    
  std::cout << "   (verify time : " << walltime() - start << "s.)";
  return nedge_traversed;
}




#endif
