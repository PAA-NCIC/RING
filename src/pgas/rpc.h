/* rpc.h
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

#ifndef _RPC_H_
#define _RPC_H_

#include <algorithm>
#include <cmath>

#include "sched/scheduler.h"
#include "comm/communicator.h"
#include "comm/completion.h"
#include "pgas/future.h"
#include "pgas/addressing.h"
#include "pgas/rpc_helper.h"

namespace Ring{

enum class SyncMode{ 
  blocking, async
};

const SyncMode async = SyncMode::async;
const SyncMode blocking = SyncMode::blocking;


template<SyncMode S, typename F>
struct Specializer {};

// async call
template <typename F>
struct Specializer<SyncMode::async, F>{

  // This is a async void-return version.
  static void call(int id, F func, void (F::*mf)() const){
    async_call_cnt ++;
    size_t from_node = node_rank();
    size_t from_thread = thread_rank();
    int16_t from_id = (int16_t)(from_node * THREAD_SIZE + from_thread);
    int16_t to_id = (int16_t)id;
    /* short-cut */
    if(from_id == to_id){
      func();
    }else{
      call_inflight[from_thread]->issue();
      send_request(from_id, to_id, [from_id, func]{
        func();
        /* send complete msg*/
        send_completion(from_id);
      });//send_request to
    }
  }

  // This is a async T-return version.
  template<typename T>
  static std::unique_ptr<Future<T>> 
  call(int id, F func, T (F::*mf)() const){
    async_call_cnt ++;
    size_t from_id = my_id();
    size_t to_id = id;
  
    //TODO(mengke) : heap allocation is bad :(
    std::unique_ptr<Future<T>> fu(new Future<T>());
    auto pfu = fu.get();
  
    if(from_id == to_id){
      T ans = func();
      pfu->set_value(ans);
    }else{
      auto gpfu = make_global(pfu);
      send_request(to_id, [func, gpfu]{
        T ans = func();
        /* send complete msg */
        send_request(gpfu.get_id(), [ans, gpfu]{ 
          auto pfu = gpfu.ptr();
          pfu->set_value(ans);
        });
      });
    }
    return fu;
  }
};

// blocking call
template <typename F>
struct Specializer<SyncMode::blocking, F>{

  // This is a blocking void-return version.
  static void call(int id, F func, void (F::*mf)() const){
    blocking_call_cnt ++;
    size_t from_id = my_id();
    size_t to_id = id;
  
    if(from_id == to_id){
      func(); 
    }else{
      Future<bool> fu;
      auto gfu = make_global(&fu);
      auto lantency_start = get_tick();
      send_request(to_id, [func, gfu]{
        func();
        /* send complete msg */
        send_request(gfu.get_id(), [gfu]{ 
          auto pfu = gfu.ptr();
          pfu->set_value(true);
        });
      });
      // block
      fu.get();
      auto lantency = get_tick() - lantency_start;
      blocking_call_lantency += lantency;
      blocking_call_lantency_cnt ++;
      blocking_call_lantency_max = std::max(blocking_call_lantency_max, (int64_t)lantency);
    }
  }

  //This is a blocking T-return version."
  template <typename T> 
  static T call(int id, F func, T (F::*mf)() const){
    blocking_call_cnt ++;
    size_t from_id = my_id();
    size_t to_id = id;
  
    if(from_id == to_id){
      return func(); 
    }else{
      Future<T> fu;
      auto gfu = make_global(&fu);
      auto lantency_start = get_tick();
      send_request(to_id, [func, gfu]{
        T ans = func();
        /* send complete msg */
        send_request(gfu.get_id(), [ans, gfu]{ 
          auto pfu = gfu.ptr();
          pfu->set_value(ans);
        });
      });
      // block
      T ans = fu.get();
      auto lantency = get_tick() - lantency_start;
      blocking_call_lantency += get_tick() - lantency_start;
      blocking_call_lantency_cnt ++;
      blocking_call_lantency_max = std::max(blocking_call_lantency_max, (int64_t)lantency);
      return ans;
    }
  }
};

template<SyncMode S, typename T>
struct Fake{};

template<typename T>
struct Fake<SyncMode::blocking, T>{
  static T op() { T t; return t; }
};

template<typename T>
struct Fake<SyncMode::async, T>{
  static std::unique_ptr<Future<T>> op() { std::unique_ptr<Future<T>> fu(new T); return fu;}
};


template<>
struct Fake<SyncMode::blocking, void>{
  static void op() {}
};

template<>
struct Fake<SyncMode::async, void>{
  static void op() {}
};

// API that expose to users.
template<SyncMode S = SyncMode::blocking, typename F>
inline auto call(int id, F func) -> decltype(Specializer<S,F>::call(id, func, &F::operator())) {
  return Specializer<S,F>::call(id, func, &F::operator());
}

template<SyncMode S = SyncMode::blocking, typename T, typename F>
inline auto call(GlobalAddress<T> t, F func) -> decltype( Fake<S,decltype(func(t.ptr()))>::op() ){
  return call<S>(t.get_id(), [func,t]{ return func(t.ptr()); });
}

template<SyncMode S = SyncMode::blocking, typename T, typename R>
inline void write(GlobalAddress<T> t, R val){
  call<S>(t, [val](T* pt){
    *pt = val;
  });
}

template<typename T>
inline T read(GlobalAddress<T> t){
  return call(t, [](T* pt){
    return *pt;
  });
}


};//namespace Ring

#endif
