
#ifndef _GRAPH_H_
#define _GRAPH_H_


#include <algorithm>

#include "common.h"
#include "ring.h"

// #define USE_MPI3_COLLECTIVES
#undef USE_MPI3_COLLECTIVES

using namespace Ring;

struct Vertex {
  int64_t * local_adj; // adjacencies that are local
  int64_t nadj;        // number of adjacencies
  int64_t local_sz;    // size of local allocation (regardless of how full it is)
  // int64_t parent;
  void * vertex_data;
  
  Vertex(): local_adj(nullptr), nadj(0), local_sz(0) {}    
  ~Vertex() {}
  
  auto adj_iter() -> decltype(iterate(local_adj)) { return iterate(local_adj, nadj); }
};

// vertex with parent
struct VertexP : public Vertex {
  VertexP(): Vertex() { parent(-1); }
  int64_t parent() { return (int64_t)vertex_data; }
  void parent(int64_t parent) { vertex_data = (void*)parent; }
};


template< typename V=Vertex >
struct Graph {
  static_assert( BLOCK_SIZE % sizeof(V) == 0, "V size not evenly divisible into blocks!");
  
  // Fields
  GlobalAddress<V> vs;
  int64_t nv, nadj, nadj_local;
  
  // Internal fields
  int64_t * adj_buf;
  int64_t * scratch;
  
  GlobalAddress<Graph> self;
    
  Graph(GlobalAddress<Graph> self, GlobalAddress<V> vs, int64_t nv)
    : self(self)
    , vs(vs)
    , nv(nv)
    , nadj(0)
    , nadj_local(0)
    , adj_buf(nullptr)
    , scratch(nullptr)
  { }
  
  ~Graph() {
    for (V& v : iterate_local(vs, nv)) { v.~V(); }
    if (adj_buf) free(adj_buf);
  }
  
  void destroy() {
    auto self = this->self;
    gfree(this->vs);
    all_do([self]{ self->~Graph(); });
    gfree(self);
  }
  
  /// Cast graph to new type, and allow user to re-initialize each V by providing a 
  /// functor (the body of a forall() over the vertices)
  template< typename VNew, typename VOld, typename InitFunc = decltype(nullptr) >
  static GlobalAddress<Graph<VNew>> transform_vertices(GlobalAddress<Graph<VOld>> o, InitFunc init) {
    static_assert(sizeof(VNew) == sizeof(V), "transformed vertex size must be the unchanged.");
    auto g = static_cast<GlobalAddress<Graph<VNew>>>(o);
    pfor(g->vs, g->nv, init);
    return g;
  }
  
  // Constructor
  static GlobalAddress<Graph> create(const tuple_graph& tg, bool directed = false) {
    double t;
    auto g = symm_gmalloc<Graph<V>>();
    sync_printf("symm_gmalloc");
  
    // find nv
    pfor(tg.edges, tg.nedge, [g](packed_edge* e){
      if (e->v0 > g->nv) { g->nv = e->v0; }
      if (e->v1 > g->nv) { g->nv = e->v1; }
    });

    all_do([g]{
      g->nv = allreduce<int64_t, collective_max>(g->nv) + 1;
    });
    

    
    auto vs = gmalloc<V>(g->nv);
    auto self = g;
    all_do([g,vs]{
      new (g.ptr()) Graph(g, vs, g->nv);
      for (V& v : iterate_local(g->vs, g->nv)) {
        new (&v) V();
      }
    });
    sync_printf("find nv over, omit time");

    // count the outgoing/undirected edges per vertex
    pfor(tg.edges, tg.nedge, [g,directed](packed_edge* e){
      ASSERT( e->v0 < g->nv, "e.v0 < g->nv");
      ASSERT( e->v1 < g->nv, "e.v1 < g->nv");
      auto count = [](GlobalAddress<V> v){
        call<async>(v.get_id(), [v]{ v->local_sz++; });
      };
      count( g->vs + e->v0 );
      if (!directed) count( g->vs + e->v1 );
    });
    sync_printf("find outgoing edges over, omit time");
  
  
    // allocate space for each vertex's adjacencies (+ duplicates)
    pfor(g->vs, g->nv, [g](int64_t i, V* v) {
      v->nadj = 0;
      if (v->local_sz > 0) v->local_adj = new int64_t[v->local_sz];
    });
    sync_printf("allocate space for each vertex's adjancencies.");
  
    // scatter
    pfor(tg.edges, tg.nedge, [g,directed](packed_edge* e){
      auto scatter = [g](int64_t vi, int64_t adj) {
        auto vaddr = g->vs+vi;
        call<async>(vaddr.get_id(), [vaddr,adj]{
          auto& v = *vaddr.ptr();
          v.local_adj[v.nadj++] = adj;
        });
      };
      scatter(e->v0, e->v1);
      if (!directed) scatter(e->v1, e->v0);
    });
    sync_printf("fill each vertex's adjancencies.");
  
    // sort & de-dup
    pfor(g->vs, g->nv, [g](int64_t vi, V* v){
      ASSERT( v->nadj == v->local_sz, "Vertex's adjancencies not right.");
      std::sort(v->local_adj, v->local_adj+v->nadj);
         
      int64_t tail = 0;
      for (int64_t i=0; i < v->nadj; i++, tail++) {
        v->local_adj[tail] = v->local_adj[i];
        while (v->local_adj[tail] == v->local_adj[i+1]) i++;
      }

      v->nadj = tail;
      g->nadj_local += v->nadj;
    });
    sync_printf("sort vertex's adjancencies and remove duplicates");

    // compact
    all_do([g]{
      // allocate storage for local vertices' adjacencies
      g->adj_buf = (int64_t*)malloc(sizeof(int64_t)*g->nadj_local);
      // compute total nadj
      g->nadj = allreduce<int64_t,collective_add>(g->nadj_local);

      int64_t * adj = g->adj_buf;
      for (V& v : iterate_local(g->vs, g->nv)) {
       
        memcpy(adj, v.local_adj, v.nadj*sizeof(int64_t));
        
        if (v.local_sz > 0) delete[] v.local_adj;
        v.local_sz = v.nadj;
        v.local_adj = adj;
        adj += v.nadj;
      }
      ASSERT(adj - g->adj_buf == g->nadj_local, "compact error");
    });
    sync_printf("compact adjacencies to one long buf per core.");

  
    return g;
  }
  
} CACHE_ALIGNED;

#endif
