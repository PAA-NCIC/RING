/* coroutine.h
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

#ifndef _COROUTINE_H_
#define _COROUTINE_H_

#include <iostream>
#include <cstdio>
#include <cstdint>
#include <string>
#include <cstring>
#include <functional>

#include "sched/context.h"
#include "tasking/task_queue.h"
#include "comm/mpienv.h"
#include "utils/utils.h"
#include "utils/timestamp.h"

namespace Ring{

using coro_func_t=std::function<void()>;

/* some times will overbuf this stack */
#define DFT_STACK_PER_CORO (1<<15)
class Coro_Scheduler;

class Coroutine{
public:
  friend class Coro_Scheduler;
  friend class CoroutineQueue;
  friend class PrefetchCoroutineQueue;
  friend class Condition;

  inline void reset(coro_func_t func){
    sch_ = NULL;
    this->func_ = func;
    memset(stack_, 0, DFT_STACK_PER_CORO);
  }

  ~Coroutine(){
    free(stack_);
    free(ctx_);
  }

  Coroutine(int tag = 0) : 
    next_(NULL), sch_(NULL), ctx_(NULL),
    father_(NULL), func_(NULL), tag_(tag), status_(0) {
    ctx_ = new ucontext_t();
    stack_ = (char*) malloc( DFT_STACK_PER_CORO );
    memset(stack_, 0, DFT_STACK_PER_CORO);
  }


private:
  char* stack_;

  Coroutine *next_;
  Coro_Scheduler* sch_;
  ucontext_t* ctx_;
  ucontext_t* father_;
  coro_func_t func_;
  bool is_periodic_;
  int tag_;

  union{
    struct{
      unsigned int idle_ : 1;
      unsigned int ready_ : 1;
      unsigned int running_ : 1;
      unsigned int suspended_ : 1;
    };
    int8_t status_;
  };
};


class Condition{
public:
  Condition():victims_(0){}

  inline void add_victim(Coroutine* victim){
    victim->next_ = reinterpret_cast<Coroutine *>(victims_);
    victims_ = reinterpret_cast<intptr_t>(victim);
  }
  inline Coroutine* resume(){
    Coroutine* coro = reinterpret_cast<Coroutine *>(victims_);
    if(NULL == coro) return coro;
    victims_ = reinterpret_cast<intptr_t>(coro->next_);
    coro->next_ = NULL;
    return coro;
  }

  inline bool empty(){
    return NULL == reinterpret_cast<void *>(victims_);
  }
private:
  intptr_t victims_;
};



class CoroutineQueue{

public:

  CoroutineQueue()
    : head_(NULL)
    , tail_(NULL)
    , size_(0){}

  inline void enqueue(Coroutine * c){
    if(NULL == head_) head_ = c;
    else tail_->next_ = c;
    tail_ = c;
    c->next_ = NULL;
    size_++;
  }


  inline Coroutine* dequeue(){
    Coroutine* ret = head_;
    if(NULL != ret){
      head_ = head_->next_;
      ret->next_ = NULL;
      size_ -- ;
#ifdef PREFETCH
      if(NULL != ret->next_){
        __builtin_prefetch( ret->next_, 1, 3);
        __builtin_prefetch( (char*)(ret->next_->ctx_->rsp), 1, 3 );
        __builtin_prefetch( (char*)(ret->next_->ctx_->rsp)-64, 1, 3 );
        __builtin_prefetch( (char*)(ret->next_->ctx_->rsp)-128, 1, 3 );
      }
#endif
    }else{
      tail_ = NULL;
    }
    return ret;
  }

  inline Coroutine* dequeueLazy(){
    Coroutine * ret = head_;
    if(NULL != ret){
      head_ = ret->next_;
      size_ --;
    }else{
      tail_ = NULL;
    }
    return ret;
  }

  inline Coroutine* front(){
    return head_;
  }
  inline uint64_t size(){
    return size_;
  }

private:
  Coroutine* head_;
  Coroutine* tail_;
  uint64_t size_;
};

class PrefetchCoroutineQueue{
public:
  PrefetchCoroutineQueue() : 
    queues(NULL),
    eq_index(0),
    dq_index(0),
    num_queues(0),
    size_(0){}
  
  void init( int64_t prefetch_dist){
    queues = new CoroutineQueue[prefetch_dist*2];
    num_queues = prefetch_dist*2;
  }

  size_t size() const{
    return size_;
  }

  void enqueue(Coroutine * coro){
    queues[eq_index].enqueue( coro );
    eq_index = ((eq_index+1) == num_queues) ? 0 : eq_index+1;
    size_ ++;
  }

  Coroutine * dequeue() {
    Coroutine * ret = queues[dq_index].dequeueLazy();

    if(ret){
      size_t t = dq_index;
      dq_index = ((dq_index+1) == num_queues) ? 0 : dq_index+1;
      size_ --;
#ifdef PREFETCH
      __builtin_prefetch( ret->next_, 1, 3);
#endif
      ret->next_ = NULL;
#ifdef PREFETCH
      uint64_t tstack = (t+(num_queues/2))%num_queues;
      Coroutine * tstack_coro = queues[tstack].front();
      if(tstack_coro){
        //__builtin_prefetch( tstack_coro->ctx_->rsp, 1, 3);
        __builtin_prefetch( ((char*)(tstack_coro->ctx_->rsp))-64, 1, 3);
        __builtin_prefetch( ((char*)(tstack_coro->ctx_->rsp))-128, 1, 3);
      }
#endif
      return ret;
    }else{
      //for(size_t i = 0; i < num_queues; ++i){
        //ASSERT(queues[i].size() == 0, "all queues is truly empty.");
      //}
      return NULL;
    }
  }


private:
  CoroutineQueue * queues;
  size_t eq_index;
  size_t dq_index;
  size_t num_queues;
  int64_t size_;
};

class Coro_Scheduler{
public:
  Coro_Scheduler(int64_t owner_thread, TaskQueue * taskq) : 
    active_coro_num(0),
    executant_(NULL), 
    running_(true),
    owner_thread(owner_thread),
    taskQ (taskq) {
    //candidates_.init(2);
    last_timestamp_ = get_tick(); 
    last_lazy_timestamp_ = last_timestamp_;
    last_switchin_timestamp = 0;
  }

  inline Coroutine * get_cur_coro(){
    return executant_;
  }

  inline size_t depth(){
    return candidates_.size();
  }

  void await(); 

  Coroutine * _spawn_coro(coro_func_t func, int tag=0);
  void _delete_coro(Coroutine * coro);
  Coroutine * _next_coro(); 
  void debug();
  void update_tick_info(Coroutine* coro);

  void coroutine_create( coro_func_t func, int tag=0);
  void coroutine_create_periodic( coro_func_t func, int tag=0);
  void coroutine_yield_periodic();
  void coroutine_yield();
  void coroutine_maybe_yield();
  void coroutine_suspend();
  void coroutine_idle();
  void coroutine_wait( Condition *cond );
  void coroutine_signal( Condition *cond );
  static void tramp(void * arg);

  int64_t active_coro_num;

private:
  ucontext_t main_;
  Coroutine * executant_;

  /* just a flag to see if current coroutine is Coro_Scheduler */
  bool running_;
  uint64_t last_timestamp_;
  uint64_t last_lazy_timestamp_;
  int64_t owner_thread;
  TaskQueue* taskQ;

  uint64_t last_switchin_timestamp;
 
  //PrefetchCoroutineQueue candidates_;
  CoroutineQueue candidates_;
  CoroutineQueue periodics_;
  CoroutineQueue unassigns_;

};


}//namespace Ring

#endif
