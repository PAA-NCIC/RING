/* sssp.h
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

#ifndef _SSSP_H_
#define _SSSP_H_


#include "ring.h"
#include "graph/graph.h"

using namespace Ring;
struct SSSPData {
  double dist;
  //double new_dist;
  int64_t parent;
  void init() {
    //new_dist = std::numeric_limits<double>::max();
    dist = std::numeric_limits<double>::max();
    parent = -1;
  }
};
struct SSSPEdgeData {
  double data;
};
struct msg{
  double dist;
  int64_t who;
};
using V = Vertex<SSSPData,SSSPEdgeData>;
using G = Graph<SSSPData,SSSPEdgeData>;
double do_sssp(GlobalAddress<G> &g, int64_t root) {
  double start = walltime();
  each_vertex(g, [](V* v){ (*v)->init(); });
  vertex_do(g, root, [=](V* v) { 
      (*v)->dist = 0.0;
      //(*v)->new_dist = 0.0;
      (*v)->parent = root; 
      v->set_imm_active();
  });
  /*---------- CC ----------*/
  auto emit = [g](V* sv){ return msg{(*sv)->dist, g->id(*sv) }; };
  auto edgework = [](msg vmsg, G::Edge* e){ vmsg.dist += (*e)->data;return vmsg; };
  auto slot = [](V* ev, msg emsg){
    if (emsg.dist < (*ev)->dist ){
      (*ev)->dist = emsg.dist;
      (*ev)->parent = emsg.who;
      return true;
    }
    return false;
  };
  auto cond = [](V* v){ return true; };
  /*------------------------*/
  int64_t active_size = 1;
  while (active_size != 0) {
    active_size = graph_do<Sparse>(g, active_size, emit, edgework, slot, cond);
  }//while
  return walltime() - start;
}

#endif
