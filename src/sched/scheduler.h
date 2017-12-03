/* scheduler.h
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

#ifndef _SCHEDULER_H_
#define _SCHEDULER_H_

#include <thread>
#include <functional>
#include <mutex>
#include <vector>
#include <queue>
#include <iostream>
#include <future>

#include "sched/coroutine.h"
#include "sched/thread_barrier.h"
#include "tasking/task_queue.h"
#include "utils/timestamp.h"
#include "utils/utils.h"
#include "utils/memlog.h"
#include "comm/mpienv.h"


namespace Ring{

extern bool global_exit_flag;
extern thread_local bool allow_yield;

class Worker{
public:

  Worker( size_t ith, 
          int worker_num,
          Thread_Barrier * tb );

  void task_worker();

  inline void yield(){
    csched->coroutine_yield();
  }

  inline void maybe_yield(){
    csched->coroutine_maybe_yield();
  }

  inline void idle(){
    csched->coroutine_idle();
  }

  inline void wait(Condition* c){
    csched->coroutine_wait(c); 
  }

  inline void signal(Condition* c){
    csched->coroutine_signal(c);
  }

  inline void spawn_coroutine(std::function<void()> f, int tag = 0){
    csched->coroutine_create(f, tag);
  }

  inline void spawn_coroutine_periodic(std::function<void()> f, int tag = 0){
    csched->coroutine_create_periodic(f, tag);
  }

  inline void await(){
    t.join();
  }

  inline void finish(){
    this->stop = true;
  }

  inline void add_task_worker(int num){
    while(num-- > 0){
      spawn_coroutine([this]{
        this->task_worker();
      });
    }
  }

  template<typename F>
  void private_enqueue(F f){
    if( sizeof(f) > sizeof(arg_pack_t) ){
      heap_task_created_cnt ++;
      F * tp = new F(f);
      taskQ.spawn_private(task_heapfunctor_proxy<F>, tp, tp, tp);
      //signal(this->have_tasks);
    }else{
      task_created_cnt ++;
      uint64_t args[3] = {0};
      char* copyf = reinterpret_cast<char*>(&f);
      char* fargs = reinterpret_cast<char*>(&args);
      memcpy(fargs, copyf, sizeof(f));
      //uint64_t * args = reinterpret_cast<uint64_t*>(&f);
      taskQ.spawn_private(task_functor_proxy<F>, args[0],args[1],args[2]); 
      //signal(this->have_tasks);
    }
  }

 
private:
  size_t rank;
  bool stop;
  std::thread t;
  Coro_Scheduler * csched;
  //Condition * have_tasks;
  TaskQueue taskQ;
  std::mutex l_mtx, r_mtx;
};


class Scheduler{
public:

  Scheduler(int worker_num);
  void await();
  void finish();
  size_t get_size();
  int get_id();
  inline Worker* get_cur_worker(){
    return workers[get_id()];
  }

  inline void start(){
    start_barrier->wait();
  }

  void spawn_coroutine(size_t ith, std::function<void()> f, int tag = 0);
  void spawn_coroutine_periodic(size_t ith, std::function<void()> f, int tag = 0);


  /* enqueue a private task */
  template<typename F>
  inline void private_enqueue(size_t ith, F f){
    workers[ith]->private_enqueue(f);
  }

 
private:
  size_t num_threads;
  std::vector<Worker*> workers;
  Thread_Barrier * start_barrier;
};

extern Scheduler * global_scheduler;

void yield();
void maybe_yield();

inline size_t thread_rank(){
  return global_scheduler->get_id();
}

inline size_t thread_size(){
  return global_scheduler->get_size();
}

inline void set_allow_yield(bool val){
  allow_yield = val;
}

inline size_t my_id(){
  return mpi_env->rank * THREAD_SIZE + thread_rank();
}

template <typename F>
inline void delivery_private(F f){
   global_scheduler->private_enqueue(thread_rank(), f);
}




}//namespace Ring
#endif
