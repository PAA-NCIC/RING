/* pagerank.h
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

#ifndef _PAGERANK_H_
#define _PAGERANK_H_

#include <cmath>

#include "ring.h"
#include "graph/graph.h"

using namespace Ring;
const float D = 0.85; // damping factor
const float eps = 1e-9;
struct PageRankData{
  float pr;
  float old_pr;
  float delta;
  void init(int64_t nv){
    old_pr = 0;
    pr = (1-D)/nv; 
    delta = 0;
  }
  bool update(int64_t outd){
    if ( fabs(delta) > eps) {
      old_pr = pr;
      pr += delta*D/outd;
      delta = 0;
      return true;
    }
    return false;
  }
};
using V = Vertex<PageRankData, Empty>;
using G = Graph<PageRankData, Empty>;
double do_pagerank(GlobalAddress<G> &g){
  double start = walltime();
  each_vertex(g, [=](V* v){ 
    (*v)->init(g->nv); 
    v->set_imm_active();
  });
  /*---------- PR ----------*/
  auto emit = [](V* sv){ return ((*sv)->pr - (*sv)->old_pr)/sv->nadj; };
  auto edgework = [](float vdata, G::Edge* e) { return vdata; };
  auto slot = [](V* ev, float edata){
    (*ev)->delta += edata;
    return false;
  };
  auto cond = [](V* v){ return true; };
  /*------------------------*/
  int64_t active_size = 1;
  while(active_size !=0){
    // scatter
    graph_do<Sparse>(g, active_size, emit, edgework, slot, cond);
    // update
    each_vertex(g, [=](VertexID vid, V* v){
      if ((*v)->update(v->nadj))
        v->set_active();
    });
    active_size = count_active_size(g);
  }
  return walltime() - start;
}

#endif

