/* verify.h
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

#ifndef _SSSP_VERIFY_H_
#define _SSSP_VERIFY_H_

#include "ring.h"
#include "graph/graph.h"
#include "sssp.h"

inline int64_t get_parent(GlobalAddress<G> g, int64_t j){
  return call(g->vs+j, [](V* v){ return (*v)->parent; });
}

inline double get_dist(GlobalAddress<G> g, int64_t j) {
  return call(g->vs+j, [](V* v){ return (*v)->dist; });
}

inline double get_min_weight(GlobalAddress<G> g, int64_t s, int64_t e){
  return call(g->vs+s, [=](V* v){
      double ret = std::numeric_limits<double>::max();
      for(int i = 0; i < v->nadj; i++){
        if(v->get_adj()[i] == e) {
          ret = std::min(ret, v->local_edge_state[i].data);
        }
      }
      return ret;
  });
}

inline double verify_sssp(EdgeList& el, GlobalAddress<G> g, int64_t root, bool directed = false) {
  
  double start = walltime();
  // SSSP distances verification
  pfor(el.edges, el.nedge, [=](WEdge* e){
    auto i = e->v0, j = e->v1;

    /* Eliminate self loops from verification */
    if ( i == j ) return;

    /* SSSP specific checks */
    auto ti = get_parent(g,i);
    auto tj = get_parent(g,j);
    auto di = get_dist(g,i), dj = get_dist(g,j);
    auto wij = e->data;

    /* maybe duplicate edge between two vertex */
    auto min_wij = get_min_weight(g, i, j);

    ASSERT( min_wij <= wij, "shorter path exists");
    ASSERT(!((di < dj) && ((di + wij) < dj)), "Error, distance of the nearest neighbor is too great");
    if(!directed) ASSERT(!((dj < di) && ((dj + wij) < di)), "Error, distance of the nearest neighbor is too great");
    ASSERT(!((i == tj) && ((di + min_wij) != dj)), "Error, distance of the child vertex is not equal to");
    if(!directed) ASSERT(!((j == ti) && ((dj + min_wij) != di)), "Error, distance of the child vertex is not equal to");

  });

  // everything checked out!
  return walltime() - start;
}




#endif
