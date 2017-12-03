/* graph.h
 * 
 * Copyright (c) 2016 Institute of Computing Technology.
 * All rights reserved.
 * 
 * Author: Ke Meng <mengke@ncic.ac.cn>
 * 
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 * 
 *     * Redistributions of source code must retain the above copyright
 *       notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above copyright
 *       notice, this list of conditions and the following disclaimer in the
 *       documentation and/or other materials provided with the distribution.
 *     * Neither the name of the Institute of Computing Technology, Chinese 
 *       Academy of Sciences nor the names of its contributors may be used 
 *       to endorse or promote products derived from this software without 
 *       specific prior written permission.
 * 
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS
 * FOR A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE
 * INSTITUTE OF COMPUTING TECHNOLOGY AND CONTRIBUTORS BE LIABLE FOR ANY
 * DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL 
 * DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS
 * OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION)
 * HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, 
 * STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN 
 * ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY
 * OF SUCH DAMAGE.
 */

#ifndef _GRAPH_H_
#define _GRAPH_H_

#include <iostream>

#include "graph/edgelist.h"

namespace Ring{

using VertexID = int64_t;
using WEdge = Edge<double>;

struct VertexBase {
  union{
    struct{
      intptr_t local_adj : 48;
      bool active : 1;
      bool next_active : 1;
      bool solitude : 1;
      bool pivot : 1;
      //VertexID* local_adj;
    };
    intptr_t raw;
  };
  int64_t nadj;
  int64_t in_nadj;
  //bool active;
  //bool next_active;

  VertexBase(): local_adj(0), nadj(0), in_nadj(0) {}    
  ~VertexBase() {}

  inline VertexID* get_adj(){
    return reinterpret_cast<VertexID*>(local_adj);
    //return local_adj;
  }

  inline bool is_active() { return active; }
  inline void set_active(bool dft = true) { next_active=dft; }
  inline void set_imm_active(bool dft = true) { active=dft; }
  inline void update_active() { active = next_active; next_active = false; }
};


template<typename T, typename E>
struct Vertex : public VertexBase {
  T data;
  E* local_edge_state;

  T* operator->() { return &data; }
  const T* operator->() const { return &data; }

  Vertex(): VertexBase(), data(){}
  Vertex(const VertexBase& v): VertexBase(v), data(){}
} CACHE_ALIGNED;



template<typename E>
inline void load_weight(E& e, double val){
  e.data = val; 
}

template<>
inline void load_weight<Empty>(Empty& e, double val){
  // do nothing;
}

template< typename T=Empty, typename E=Empty >
struct Graph {
  using V=Vertex<T,E>;

  static_assert( BLOCK_SIZE % sizeof(V) == 0, "V size not evenly divisible into blocks!");

  struct Edge {
    VertexID id;
    GlobalAddress<V> ga;
    E& data;

    E* operator->() { return &data; }
    const E* operator->() const { return &data; }
  };
  
  // Fields
  GlobalAddress<V> vs;
  int64_t nv, nadj, nadj_local;
  bool directed, weighted;

  // Local fields
  VertexID * adj_buf;
  E * edge_storage;
  
  GlobalAddress<Graph> self;
    
  Graph(GlobalAddress<Graph> self, GlobalAddress<V> vs, int64_t nv)
    : vs(vs)
    , nv(nv)
    , nadj(0)
    , nadj_local(0)
    , adj_buf(nullptr)
    , edge_storage(nullptr)
    , self(self)
  {}

  inline int64_t get_nv() {return nv;}
  inline int64_t get_nedge() {return nadj;}
  inline bool is_directed() {return directed;}
  inline bool is_weighted() {return weighted;}
  
  ~Graph() {
    for (V& v : iterate_local(vs, nv)) { v.~V(); }
    if(edge_storage){
      for(int64_t i=0; i<nadj_local; ++i){
        edge_storage[i].~E();
      }
      free(edge_storage);
    }
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
  static GlobalAddress<Graph<VNew,E>> transform_vertices(GlobalAddress<Graph<VOld,E>> o, InitFunc init) {
    static_assert(sizeof(VNew) == sizeof(V), "transformed vertex size must be the unchanged.");
    auto g = static_cast<GlobalAddress<Graph<VNew,E>>>(o);
    pfor(g->vs, g->nv, init);
    return g;
  }

  VertexID id(V& v) {
    return make_linear(&v) - vs; // physics addr to virtual addr
  }

  VertexID id(V* v){
    return make_linear(v) - vs; // physics addr to virtual addr
  }
  
  Edge edge(V& v, size_t i) {
    auto j = v.get_adj()[i];
    return Edge{ j, vs+j, v.local_edge_state[i] };
  }

  static void find_nv(const EdgeList& el, GlobalAddress<Graph>& g){
    // find nv
    pfor(el.edges, el.nedge, [g](WEdge* e){
      if (e->v0 > g->nv) { g->nv = e->v0; }
      if (e->v1 > g->nv) { g->nv = e->v1; }
    });
  
    // TODO(mengke) : if it start from one, we should not increase it
    all_do([g]{
      g->nv = allreduce<int64_t, collective_max>(g->nv) + 1;
    });
  
    auto vs = gmalloc<V>(g->nv);
    all_do([g,vs]{
      new (g.ptr()) Graph(g, vs, g->nv);
      for (V& v : iterate_local(g->vs, g->nv)) {
        new (&v) V();
      }
    });
  }

  static void count_edge(const EdgeList& el, GlobalAddress<Graph>& g, bool directed){
    // count the outgoing/undirected edges per vertex
    pfor(el.edges, el.nedge, [g,directed](WEdge* e){
      ASSERT( e->v0 < g->nv, "e.v0 < g->nv");
      ASSERT( e->v1 < g->nv, "e.v1 < g->nv");
      auto count = [](GlobalAddress<V> v){
        call<async>(v.get_id(), [v]{ v->nadj++; });
      };
      count( g->vs + e->v0 );
      if (!directed) count( g->vs + e->v1 );
    });

    if(directed){
      // count the incoming/directed edges per vertex for pull mode
      pfor(el.edges, el.nedge, [g](WEdge* e){
        auto count = [](GlobalAddress<V> v){
          call<async>(v.get_id(), [v]{ v->in_nadj++; });
        };
        // only count the in edge
        count(g->vs + e->v1);
      });
    }

    
  }

  static void alloc_adj(GlobalAddress<Graph>& g, bool is_weight){
    // allocate space for each vertex's adjacencies (+ duplicates)
    pfor(g->vs, g->nv, [=](int64_t i, V* v) {
      if (v->nadj + v->in_nadj > 0){
        v->local_adj = reinterpret_cast<intptr_t>(new int64_t[v->nadj+v->in_nadj]);
        //v->local_adj = new int64_t[v->nadj+v->in_nadj];
      }
      // need not remove duplicate
      if(is_weight){
        v->local_edge_state = new E[v->nadj+v->in_nadj];
      }
      v->nadj = 0;
      v->in_nadj = 0;
    });
  }

  static void scatter_uwedge(const EdgeList& el, GlobalAddress<Graph>& g, bool directed){
    // scatter
    pfor(el.edges, el.nedge, [g,directed](WEdge* e){
      auto scatter = [g](int64_t vi, int64_t adj) {
        auto vaddr = g->vs+vi;
        call<async>(vaddr.get_id(), [vaddr,adj]{
          auto& v = *vaddr.ptr();
          v.get_adj()[v.nadj++] = adj;
        });
      };
      scatter(e->v0, e->v1);
      if (!directed) scatter(e->v1, e->v0);
    });

    if(directed){
      // scatter for reversed graph.
      pfor(el.edges, el.nedge, [g](WEdge* e){
        auto scatter = [g](int64_t vi, int64_t adj) {
          auto vaddr = g->vs+vi;
          call<async>(vaddr.get_id(), [vaddr,adj]{
            auto& v = *vaddr.ptr();
            v.get_adj()[v.nadj+v.in_nadj++] = adj;
          });
        };
        scatter(e->v1, e->v0);
      });
    }
  }

  static void scatter_wedge(const EdgeList& el, GlobalAddress<Graph>& g, bool directed){
    // scatter
    pfor(el.edges, el.nedge, [g,directed](WEdge* e){
      auto scatter = [g](int64_t vi, int64_t adj, double data) {
        auto vaddr = g->vs+vi;
        call<async>(vaddr.get_id(), [vaddr,adj,data]{
          auto& v = *vaddr.ptr();
          v.get_adj()[v.nadj] = adj;
          //v.local_edge_state[v.nadj].data = data;
          load_weight(v.local_edge_state[v.nadj], data);
          v.nadj ++;
        });
      };
      scatter(e->v0, e->v1, e->data);
      if (!directed) scatter(e->v1, e->v0, e->data);
    });

    if(directed){
      // scatter for reversed graph.
      pfor(el.edges, el.nedge, [g](WEdge* e){
        auto scatter = [g](int64_t vi, int64_t adj, double data) {
          auto vaddr = g->vs+vi;
          call<async>(vaddr.get_id(), [vaddr,adj,data]{
            auto& v = *vaddr.ptr();
            v.get_adj()[v.nadj+v.in_nadj] = adj;
            //v.local_edge_state[v.nadj+v.in_nadj].data = data;
            load_weight(v.local_edge_state[v.nadj+v.in_nadj], data);
            v.in_nadj++;
          });
        };
        scatter(e->v1, e->v0, e->data);
      });
    }
  }

  static void rm_duplicate(GlobalAddress<Graph>& g){
    // sort & remove duplication
    pfor(g->vs, g->nv, [g](int64_t vi, V* v){
      std::sort(v->get_adj(), v->get_adj()+v->nadj);
      std::sort(v->get_adj()+v->nadj, v->get_adj()+v->nadj+v->in_nadj);
         
      int64_t tail = 0;
      for (int64_t i=0; i < v->nadj; i++, tail++) {
        v->get_adj()[tail] = v->get_adj()[i];
        while (v->get_adj()[tail] == v->get_adj()[i+1]) i++;
      }
  
      int64_t st = v->nadj;
      v->nadj = tail;

      for (int64_t i=st; i < st+v->in_nadj; i++, tail++) {
        v->get_adj()[tail] = v->get_adj()[i];
        while (v->get_adj()[tail] == v->get_adj()[i+1]) i++;
      }

      v->in_nadj = tail - v->nadj;

      g->nadj_local += tail;

    });
  }

  static void compact(GlobalAddress<Graph>& g, bool directed, bool is_weight){
    all_do([=]{
      // allocate storage for local vertices' adjacencies
      g->adj_buf = (int64_t*)malloc(sizeof(int64_t)*g->nadj_local);
      g->edge_storage = (E*)malloc(sizeof(E)*g->nadj_local);
      for (int64_t i=0; i < g->nadj_local; i++) {
        new (g->edge_storage+i) E();
      }

      // compute total nadj
      g->nadj = allreduce<int64_t,collective_add>(g->nadj_local);

      //TODO(mengke):(should we do it ?)
      //if(directed) 
      //g->nadj >>= 1;

      int64_t * adj = g->adj_buf;
      E* eadj = g->edge_storage;
      //size_t offset=0;
      for (V& v : iterate_local(g->vs, g->nv)) {
        if(v.nadj == 0) v.solitude = true;

        memcpy(adj, v.get_adj(), (v.nadj+v.in_nadj)*sizeof(int64_t));
        if(is_weight) memcpy(eadj, v.local_edge_state , (v.nadj+v.in_nadj)*sizeof(E));
        
        if (v.nadj+v.in_nadj > 0){
          delete[] v.get_adj();
          if(is_weight) delete[] v.local_edge_state;
        }

        v.local_adj = reinterpret_cast<intptr_t>(adj);
        //v.local_adj = adj;
        if(is_weight) v.local_edge_state = eadj;
        adj += v.nadj+v.in_nadj;
        if(is_weight) eadj += v.nadj+v.in_nadj;
        //v.local_edge_state = g->edge_storage + offset;
        //offset += v.nadj;
      }
      ASSERT(adj - g->adj_buf == g->nadj_local, "compact error");
    });
  }

  static void output_size(GlobalAddress<Graph>& g, bool directed){
    auto lmd_graph_size = [](GlobalAddress<Graph> g) -> int64_t {
      return (sizeof(Vertex<T,E>) * g->nv + (sizeof(VertexID) + sizeof(E))*g->nadj );
    };

    double graph_size = lmd_graph_size(g);
    if(directed) graph_size += (sizeof(VertexID) + sizeof(E))*g->nadj;

    if(g->is_directed()) 
      std::cout << "[Directed] ";
    else 
      std::cout << "[Undirected] ";

    if(g->is_weighted())
      std::cout << "[Weighted] ";
    else
      std::cout << "[Unweighted] ";
    std::cout << std::endl;
  
    std::cout << " Graph size : " << graph_size / (1L<<30) << 
      " G. (" << g->nv << ", " << g->nadj << ")" << std::endl;
  }


  static GlobalAddress<Graph> create_weight(const EdgeList& el, bool directed=false){
    auto g = symm_gmalloc<Graph>();
    all_do([=]{
      g->weighted = true;
      g->directed = directed;
    });

    find_nv(el, g);
    count_edge(el, g, directed);
    alloc_adj(g, true);
    scatter_wedge(el, g, directed);
    pfor(g->vs, g->nv, [=](int64_t vi, V* v){
      g->nadj_local += v->nadj + v->in_nadj;
    });
    compact(g, directed, true);

    output_size(g, directed);
    return g;
  }

  // Constructor
  static GlobalAddress<Graph> create_unweight(const EdgeList& el, bool directed=false) {
    auto g = symm_gmalloc<Graph>();
    all_do([=]{
      g->weighted = false;
      g->directed = directed;
    });

    find_nv(el, g);
    count_edge(el, g, directed);
    alloc_adj(g, false);
    scatter_uwedge(el, g, directed);
    rm_duplicate(g);
    compact(g, directed, false);

    output_size(g, directed);
    return g;
  }

  static GlobalAddress<Graph> Undirected(const EdgeList& el, bool is_weight=false){
    if(is_weight) return create_weight(el, false);
    else return create_unweight(el, false);
  }

  static GlobalAddress<Graph> Directed(const EdgeList& el, bool is_weight=false){
    if(is_weight) return create_weight(el, true);
    else return create_unweight(el, true);
  }

} CACHE_ALIGNED;







//////////////////////////////////////////////////////////////////////////////////////////////





template< typename G >
struct AdjIterator {
  GlobalAddress<G> g;
  VertexID i;
  AdjIterator(GlobalAddress<G> g, VertexID i): g(g), i(i) {}
};

template< typename G >
AdjIterator<G> adj(GlobalAddress<G> g, VertexID i) { 
  return AdjIterator<G>(g, i); 
}  

template< typename G >
AdjIterator<G> adj(GlobalAddress<G> g, typename G::V* v) {
  return AdjIterator<G>(g, make_linear(v) - g->vs);
}

template< typename G >
AdjIterator<G> adj(GlobalAddress<G> g, GlobalAddress<typename G::V> v) {
  return AdjIterator<G>(g, v - g->vs);
}

template< SyncMode S,  typename G, typename F >
void pfor_out(AdjIterator<G> a, F body, void (F::*mf)(int64_t, typename G::Edge*) const){
  auto task = [a,body]{
    auto vs = a.g->vs;
    auto v = (vs+a.i).ptr();
    auto adjbuf = v->get_adj();
    //for(int i = 0; i < v->nadj; ++i){
      //auto j = v->get_adj()[i];
      //typename G::Edge e = {j, vs+j, v->local_edge_state[i]};
      //body(i, &e);
    //}
    pfor_local<SyncMode::async>(0, v->nadj, [=](int64_t i){
      auto j = adjbuf[i];
      typename G::Edge e = {j, vs+j, v->local_edge_state[i]};
      body(i, &e);
    });
  };
  auto v = a.g->vs + a.i;
  if(v.get_id() == my_id()){
    task();
  }else{
    ASSERT(false, "edge storeage must stay together with vertex.");
  }
}

template< SyncMode S,  typename G, typename F >
void pfor_in(AdjIterator<G> a, F body, void (F::*mf)(int64_t, typename G::Edge*) const){
  auto task = [a,body]{
    auto vs = a.g->vs;
    auto v = (vs+a.i).ptr();
    auto adjbuf = v->get_adj();
    //for(int i = v->nadj; i < v->nadj+v->in_nadj; ++i){
      //auto j = adjbuf[i];
      //typename G::Edge e = {j, vs+j, v->local_edge_state[i]};
      //body(i, &e);
    //}
    pfor_local<SyncMode::async>(v->nadj, v->in_nadj, [=](int64_t i){
      auto j = adjbuf[i];
      typename G::Edge e = {j, vs+j, v->local_edge_state[i]};
      body(i, &e);
    });
  };
  auto v = a.g->vs + a.i;
  if(v.get_id() == my_id()){
    task();
  }else{
    ASSERT(false, "edge storeage must stay together with vertex.");
  }
}

// traverse out edge
template< SyncMode S = SyncMode::blocking, typename G, typename F >
void pfor_out(AdjIterator<G> a, F body, void (F::*mf)(int64_t) const) {
  auto f = [body](int64_t i, typename G::Edge* e){ body(i); };
  pfor_out<S>(a, f, &decltype(f)::operator());
}

template< SyncMode S = SyncMode::blocking, typename G, typename F >
void pfor_out(AdjIterator<G> a, F body, void (F::*mf)(typename G::Edge*) const) {
  auto f = [body](int64_t i, typename G::Edge* e){ body(e); };
  pfor_out<S>(a, f, &decltype(f)::operator());
}

template< SyncMode S = SyncMode::blocking, typename G, typename F >
void pfor_out(AdjIterator<G> a, F body){
  pfor_out<S>(a, body, &F::operator());
}

// traverse in edge
template< SyncMode S = SyncMode::blocking, typename G, typename F >
void pfor_in(AdjIterator<G> a, F body, void (F::*mf)(int64_t) const) {
  auto f = [body](int64_t i, typename G::Edge* e){ body(i); };
  pfor_in<S>(a, f, &decltype(f)::operator());
}

template< SyncMode S = SyncMode::blocking, typename G, typename F >
void pfor_in(AdjIterator<G> a, F body, void (F::*mf)(typename G::Edge*) const) {
  auto f = [body](int64_t i, typename G::Edge* e){ body(e); };
  pfor_in<S>(a, f, &decltype(f)::operator());
}

template< SyncMode S = SyncMode::blocking, typename G, typename F >
void pfor_in(AdjIterator<G> a, F body){
  pfor_in<S>(a, body, &F::operator());
}




// for user use
template<SyncMode S = SyncMode::blocking, typename G, typename F>
void each_vertex(GlobalAddress<G> g, F task){
  pfor(g->vs, g->nv, task);
}

// this is special for Undirected Graph
template<SyncMode S = SyncMode::blocking, typename G, typename F>
void each_adj_edge(GlobalAddress<G> g, VertexID vid, F update){
  pfor_out(adj(g,vid), update);
  //maybe_yield();
}

template<SyncMode S = SyncMode::blocking, typename G, typename F>
void each_out_edge(GlobalAddress<G> g, VertexID vid, F update){
  pfor_out(adj(g,vid), update);
  //maybe_yield();
}

template<SyncMode S = SyncMode::blocking, typename G, typename F>
void each_in_edge(GlobalAddress<G> g, VertexID vid, F update){
  if(g->is_directed()) pfor_in(adj(g,vid), update);
  else pfor_out(adj(g,vid), update);
  //maybe_yield();
}

template<SyncMode S = SyncMode::blocking, typename G, typename F>
void vertex_do(GlobalAddress<G> g, VertexID vid, F task){
  call<S>((g->vs+vid), task);
}

template<SyncMode S = SyncMode::blocking, typename E, typename F>
void vertex_do(E* e, F task){
  call<S>(e->ga, task);
}

template<typename E, typename F>
inline void vertex_push(E* e, F task){
  call<async>(e->ga, task);
}

template<typename E, typename F>
inline auto vertex_pull(E* e, F task) -> decltype(call(e->ga, task)) {
  return call<blocking>(e->ga, task);
}










//////////////////////////////////////////////////////////////////////////////////////////////
//
//
//high level
enum struct GraphMode{
  Auto, Dense, Sparse
};
const GraphMode Auto = GraphMode::Auto;
const GraphMode Dense = GraphMode::Dense;
const GraphMode Sparse = GraphMode::Sparse;

extern thread_local int64_t local_active_set_size;
template<typename G, typename E, typename W, typename F, typename C>
void sparse_proxy(GlobalAddress<G> g, E emit, W edgework, F task, C cond){
  each_vertex(g, [=](VertexID vsid, typename G::V* sv){
    if(!sv->solitude && sv->is_active()){
      each_out_edge(g, vsid, [=](typename G::Edge* e){
        auto vdata = emit(sv);
        auto data = edgework(vdata, e);
        vertex_push(e, [task,cond,data](typename G::V* ev){
          if( cond(ev) && task(ev,data) )
            ev->set_active();
        });
      });
    }
  });
}

template<typename T>
struct Maybe{
  bool tag;
  T storage;
  Maybe(bool x, T y) : tag(x), storage(y){}
  Maybe(bool x) : tag(x){}
  Maybe() {}
};

template<typename G, typename E, typename W,typename F, typename C>
void dense_proxy(GlobalAddress<G> g, E emit, W edgework, F task, C cond){
  each_vertex(g, [=](VertexID veid, typename G::V* ev){
    each_in_edge(g, veid, [=](typename G::Edge* e){
      if( !ev->solitude && cond(ev)){
        auto vdata = vertex_pull(e, [emit](typename G::V* sv){
          return Maybe<decltype(emit(sv))>(sv->is_active(), emit(sv));
        });
        auto data = edgework(vdata.storage, e);
        if (vdata.tag && task(ev, data)) 
          ev->set_active();

        if( !cond(ev) ) return;
      }
    });
  });
}


template<typename G>
int64_t count_active_size(GlobalAddress<G> g){
  each_vertex(g, [=](int64_t vsid, typename G::V* sv){
    sv->update_active();
    if(sv->is_active()){
       local_active_set_size += sv->nadj + sv->in_nadj + 1;
    }
  });
  int64_t active_set_size = reduce_and_clean(local_active_set_size, collective_add);
  return active_set_size;
}

template<GraphMode GM = GraphMode::Auto, typename G, typename E, typename W, typename F, typename C>
int64_t graph_do(GlobalAddress<G> g, int64_t active_set_size, E emit, W edgework, F task, C cond) {

  // phase 1 : traverse the graph, realwork
  if(GM == Sparse){
    double start = walltime();
    sparse_proxy(g, emit, edgework, task, cond);
    std::cout << "sparse : " << active_set_size << " (" << walltime() - start << ")" << std::endl;
  }else if(GM == Dense){
    double start = walltime();
    dense_proxy(g, emit, edgework, task, cond);
    std::cout << "dense : " << active_set_size << " (" << walltime() - start << ")" << std::endl;
  }else if(GM == Auto){

    if(active_set_size*20 > g->nadj){
      double start = walltime();
      dense_proxy(g, emit, edgework, task ,cond);
      std::cout << "dense : " << active_set_size << " (" << walltime() - start << ")" << std::endl;
    }else{
      double start = walltime();
      sparse_proxy(g, emit, edgework, task, cond);
      std::cout << "sparse : " << active_set_size << " (" << walltime() - start << ")" << std::endl;
    }
  }else{
    ASSERT(false, "choose Mode : [Spase], [Dense], [Auto]");
  }

  // phase 2 : update the graph active state, overhead
  return count_active_size(g);
}



};//namespace Ring

#endif

