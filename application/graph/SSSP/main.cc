/* main.cc
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

#include <stdlib.h>

#include "sssp.h"
#include "verify.h"

#include "ring.h"
#include "graph/graph.h"

using namespace Ring;

const int scale = 24;
const int edgefactor = 16;

template<typename T, typename E>
inline int64_t choose_root(GlobalAddress<Graph<T,E>> g){
  int64_t root;
  do{
    root = random() % g->nv;
  } while(call(g->vs+root, [](Vertex<T,E> * v){return v->nadj; }) == 0);
  return root;
}

void sssp(EdgeList& tg, GlobalAddress<G> g){

  bool verify = false;
  int64_t root = choose_root(g);

  double sssp_time = do_sssp(g, root);
  std::cout << "# sssp time : " << sssp_time << " s";

  if(verify){
    double time = verify_sssp(tg, g, root);
    std::cout << "  passed! (" << time << " s)" << std::endl;
  }else{
    std::cout << "  skip verfication... "<< std::endl;
  }

}

int main(int argc, char* argv[]){
  RING_Init(&argc, &argv);
  run([]{
    int64_t nvtx_scale = ((int64_t)1L) << scale;
    int64_t desired_nedge = nvtx_scale * edgefactor;
    EdgeList el;
    el = EdgeList::Kronecker(scale, desired_nedge, 111, 222);
    el.random_weight();

    auto g = G::Undirected(el, true);
    sssp(el, g);
    mlog_dump();
  });
  RING_Finalize();
  return 0;
}
