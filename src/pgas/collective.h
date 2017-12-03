/* collective.h -- collective communication
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

#ifndef _COLLECTIVE_H_
#define _COLLECTIVE_H_

#include <memory>
#include <vector>


#include "pgas/rpc.h"
#include "pgas/addressing.h"

template< typename T >
T collective_add(const T& a, const T& b)  { return a+b; }
template< typename T >
T collective_sum(const T& a, const T& b)  { return a+b; }
template< typename T >
T collective_max(const T& a, const T& b)  { return (a>b) ? a : b; }
template< typename T >
T collective_min(const T& a, const T& b)  { return (a<b) ? a : b; }
template< typename T >
T collective_prod(const T& a, const T& b) { return a*b; }
template< typename T >
T collective_mult(const T& a, const T& b) { return a*b; }
template< typename T >
T collective_or(const T& a, const T& b)   { return a || b; }
template< typename T >
T collective_lor(const T& a, const T& b)  { return a || b; }
template< typename T >
T collective_bor(const T& a, const T& b)  { return a | b; }
template< typename T >
T collective_and(const T& a, const T& b)  { return a && b; }
template< typename T >
T collective_land(const T& a, const T& b) { return a && b; }
template< typename T >
T collective_band(const T& a, const T& b) { return a & b; }
template< typename T >
T collective_xor(const T& a, const T& b)  { return a ^ b; }
template< typename T >
T collective_lxor(const T& a, const T& b) { return (a && (!b)) || ((!a) && b); }
template< typename T >
T collective_bxor(const T& a, const T& b) { return a ^ b; }

namespace Ring{


inline size_t id(){
  return node_rank()*thread_size() + thread_rank();
}

inline void done(){
  global_scheduler->finish();
  global_exit_flag = true;
  running = false;
}

inline void tell_all_to_exit(){
  global_barrier->wait();
  const size_t size = mpi_env->size;
  for(size_t i = 1; i < size; ++i){
    send_request(0,0,i,0,[]{ done(); });
  }
  /*make sure that all done() are sent*/
  global_comm->wait_for_finish_msg();
  done();
}

template<typename F>
void run(F f){
  running = true;
  /* this will suspended all the hidden thread 
   * until main task finish */
  if( 0 == mpi_env->rank ){
    global_barrier->issue();
    global_scheduler->private_enqueue(0, [f]{ 
      f();
      call_inflight[0]->wait();
      global_barrier->complete();
      tell_all_to_exit();
    });
  }
}

template<typename F>
void all_do(F work){
  //size_t _node_size = node_size();
  //size_t _thread_size = thread_size();
  size_t nc = cores();

  // This Barrier must on 0-th node
  Barrier * barrier = new Barrier( nc ); 
  
  call_inflight[0]->wait();
  //work();
  delivery_private([work, barrier]{
    work();
    call_inflight[0]->wait();
    barrier->complete();
  });
  //barrier->complete();

  for(uint16_t i = 1; i < nc; ++i){
    auto lmd = [i,work,barrier]{
      work();
      /* this will guarantee that all the call 
       * will be finished before send complition back */
      call_inflight[i%THREAD_SIZE]->wait();

      send_request(i,0, [barrier]{
        barrier->complete();
      });
    };
    send_request(0, i, [lmd]{
      delivery_private(lmd);
    }); 
  }

  barrier->wait();
  free(barrier);
}

template<typename T, T (*ReduceOp)(const T&, const T&)>
void reduce_helper(const T& val, GlobalAddress<T> target){
  static T total;
  static int64_t cores_in = 0;
  if(cores_in == 0){
    total = val;
  }else{
    total = ReduceOp(total, val);
  }

  cores_in ++;
  if(cores_in == cores()){
    cores_in = 0;
    *target.ptr() = total;
  }
}

template< typename T, T (*ReduceOp)(const T&, const T&) >
void _reduce(T myval, GlobalAddress<T> target) {
  send_request(0, [myval, target]{
    reduce_helper<T,ReduceOp>(myval, target);
  });
}

#define reduce( what, op ) ({         \
  auto tmp = what;                    \
  auto root_buf = make_global(&tmp);  \
  all_do([root_buf]{ _reduce<decltype(what), op>(what, root_buf);}); \
  tmp;                                \
})

#define reduce_and_clean( what, op ) ({ \
  auto tmp = what;                      \
  auto root_buf = make_global(&tmp);    \
  all_do([root_buf]{ _reduce<decltype(what), op>(what, root_buf); what=0;}); \
  tmp;                                  \
})



template<typename T, T (*ReduceOp)(const T&, const T&)>
void allreduce_helper(const T& val, GlobalAddress<Future<T>> target){
  static T total;
  static int64_t cores_in = 0;
  static intptr_t* waiters = new intptr_t[cores()];
  if(cores_in == 0){
    total = val;
  }else{
    total = ReduceOp(total, val);
  }

  waiters[target.get_id()] = target.get_raw();

  cores_in ++;
  if(cores_in == cores()){
    cores_in = 0;
    T tmp_total = total;
    for(int i = 0 ; i < cores(); ++i){
      intptr_t t = waiters[i];
      auto lmd = [t, tmp_total]{
        GlobalAddress<Future<T>> gfu;
        gfu.raw(t);
        auto pfu = gfu.ptr();
        pfu->set_value(tmp_total);
      };
      if(i == 0){
        lmd();
      }else{
        send_request(i, lmd);
      }
    }
  }
}

/* @example
 * do_all([g]{
 *    int val = allreduce(g->val);
 * });
 */
template< typename T, T (*ReduceOp)(const T&, const T&) >
T allreduce(T myval) {
  Future<T> ret;
  auto gret = make_global(&ret);
  send_request(0, [myval, gret]{
    allreduce_helper<T,ReduceOp>(myval, gret);
  });
  return ret.get();
}

inline void barrier(){
  call_inflight[thread_rank()]->wait();
  int x = 1;
  allreduce<int,collective_sum>(x);
}

};//namespace Ring


#endif
