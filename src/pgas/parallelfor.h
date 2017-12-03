/* parallelfor.h
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

#ifndef _PARALLELFOR_H_
#define _PARALLELFOR_H_

#include <vector>
#include <memory>

#include "pgas/garray.h"
#include "pgas/collective.h"
#include "sched/scheduler.h"


namespace Ring{

extern Barrier locale_barrier;

// loop will be decomposed into small pieces in order
// to have sufficent coroutine task to hide latency.
template<bool is_decomp=true, typename F>
void loop_decomp(int64_t start, int64_t size, F loop_body){
  if( 0 == size ){ //return
    return;
  }else if(size <= LOOP_THRESHOLD || !is_decomp){ // do loop_body
    loop_body(start, size);
  }else{ // split
    int64_t rstart = start + (size+1)/2;
    int64_t rsize = size/2; 

    call_inflight[thread_rank()]->issue();
    delivery_private([rstart, rsize, loop_body]{
      loop_decomp(rstart, rsize, loop_body);
      call_inflight[thread_rank()]->complete();
    });
    loop_decomp(start, (size+1)/2, loop_body);
  }
}

// pfor helper.
// loop on one core without data exchange. GlobalAddress is captured by work.
template<SyncMode S, typename F>
void pfor_local(int64_t local_start, int64_t local_size, F loop_body, void (F::*mf)(int64_t,int64_t) const){
  // very important, all loop_decomp group share one lambda, 
  // thus dramatically alleviate heap pressure.
  struct HeapF {
    const F loop_body;
    int64_t refs; // loop_body will be execute refs times.
    HeapF(F loop_body, int64_t refs): loop_body(std::move(loop_body)), refs(refs){}
    void ref(int64_t dec){
      refs -= dec;
      if(refs == 0) delete this;
    }
  };

  // this will be shared by all the coroutine int this core
  auto hf = new HeapF(loop_body, local_size); 
  loop_decomp<true>(local_start, local_size, [hf](int64_t s, int64_t n){
    hf->loop_body(s, n);
    hf->ref(n);
  });

  if(S == blocking) call_inflight[thread_rank()]->wait();
}

template<SyncMode S, typename F>
void pfor_local(int64_t local_start, int64_t local_size, F loop_body, void (F::*mf)(int64_t) const){
  auto f = [loop_body](int64_t s, int64_t n){
    for (int64_t i=0; i < n; i++) {
      loop_body(s+i);
    }
  };
  pfor_local<S>(local_start, local_size, f, &decltype(f)::operator());
}
    


template<SyncMode S, typename F>
void pfor_local(int64_t local_start, int64_t local_size, F loop_body){
  pfor_local<S>(local_start, local_size, loop_body, &F::operator());
}

// figure out which cores have work to do, so we can avoid assigning 
// empty tasks to the core that does't hold global array data.
// warning: sizeof(T) must be 4 8 16 32 64.
template<typename T>
std::pair<size_t, size_t> cores_have_work(GlobalAddress<T> base, int64_t size){
  size_t start_core = base.get_id();
  size_t core_num = 0;
  size_t block_elems = BLOCK_SIZE / sizeof(T);
  if(size > 0){
    auto first_byte = base.block_first();
    int64_t n = size + (base.get_raw()-first_byte.get_raw())/sizeof(T);
    core_num = std::min<size_t>(cores(), (n/block_elems) + (n%block_elems == 0 ? 0 : 1));
  }
  return std::make_pair(start_core, core_num);
}


// make each core loop on their local data.
// task is void(int64_t start, int64_t local_size, T* local_base)
template<typename T, typename F>
void assign_task(GlobalAddress<T> base, int64_t size, F task){
  auto rge = cores_have_work(base, size);
  int64_t core_num = rge.second;
  size_t nc = cores();
  size_t from_id = my_id();

  locale_barrier.issue(core_num);

  struct{int64_t size:48, from_id:16;} packed = {size, static_cast<uint16_t>(from_id)};

  for(int64_t i = 0; i < core_num; ++i){
    send_request(from_id, (rge.first+i)%nc, [packed,base,task]{
      auto lmd = [packed,base,task]{
        auto id = thread_size()*node_rank() + thread_rank();
        range_t mld = base.get_my_local_data(id, packed.size);

        //sync_printf(id, mld.s_idx, mld.n);
        auto local_start = mld.s_idx;
        auto local_size = mld.n;
        T* local_base = reinterpret_cast<T*>(mld.paddr);
        task(local_start, local_size, local_base);

        send_request(packed.from_id, []{locale_barrier.complete();});
      };
      delivery_private(lmd);
    });
  }
}



// specializer, you can't call this directly.
// work is void(int64_t start, int64_t size, T* first)
template<SyncMode S, typename T, typename F>
void pfor(GlobalAddress<T> A, int64_t size, F loop_body, void(F::*pf)(int64_t,int64_t,T*) const){

  // split loop to each core
  auto task = [loop_body, A](int64_t local_start, int64_t local_size, T* local_base){
    pfor_local<S>(0, local_size, [loop_body, local_base, A](int64_t s, int64_t n){
      // index must be infered from s, while s is local index
      loop_body( make_linear(local_base+s)-A, n, local_base+s); 
    });
  };
  assign_task(A, size, task);
  locale_barrier.wait();
}



// without index
template<SyncMode S, typename T, typename F>
void pfor(GlobalAddress<T> A, int64_t size, F work, void(F::*pf)(T*) const){
  auto f = [work](int64_t start, int64_t n, T* first){
    for(int64_t i = 0; i < n; ++i) {
      work(first + i);
    }
  };
  pfor<S>(A, size, f, &decltype(f)::operator());
}

// with index
template<SyncMode S = SyncMode::blocking, typename T, typename F>
void pfor(GlobalAddress<T> A, int64_t size, F work, void(F::*pf)(int64_t,T*) const){
  auto f = [work](int64_t start, int64_t n, T* first){
    auto block_elems = BLOCK_SIZE / sizeof(T);
    auto index = start;
    auto a = make_linear(first);
    auto n_to_boundary = block_elems - (a - a.block_first());
    ASSERT(n_to_boundary > 0, "something wrong in pfor with idx");

    for(int64_t i = 0; i < n; i++,index++,n_to_boundary--){
      if(n_to_boundary == 0){ // should jump 
        index += block_elems * (cores()-1);
        n_to_boundary = block_elems;
      }
      work(index, first+i);
    }
  };
  pfor<S>(A, size, f, &decltype(f)::operator());
}

template<SyncMode S = SyncMode::blocking, typename T, typename F>
void pfor(GlobalAddress<T> A, int64_t size, F work){
  pfor<S>(A, size, work, &F::operator());
}


//utils
template<typename T, typename S>
void memset(GlobalAddress<T> base, S value, size_t count){
  pfor(base, count, [value](T* w){
    *w = value;
  });
}


}//namespace Ring

#endif
