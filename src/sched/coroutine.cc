/* coroutine.cc
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

#include "sched/coroutine.h"
#include "utils/memlog.h"

namespace Ring{
void Coro_Scheduler::update_tick_info(Coroutine* coro){
  if(last_switchin_timestamp == 0){
    last_switchin_timestamp = get_tick();
  }else{
    uint64_t tmp = get_tick();
    if(coro->is_periodic_){
      thread_comm_ticks += tmp - last_switchin_timestamp;
    }else{
      thread_work_ticks += tmp - last_switchin_timestamp;
    }
    last_switchin_timestamp = tmp;
  }
}

Coroutine * Coro_Scheduler::_spawn_coro(coro_func_t func, int tag){
  Coroutine * coro = new Coroutine(tag);
  coro->reset(func);
  return coro;
}

void Coro_Scheduler::_delete_coro(Coroutine * coro){
  free(coro);
  coro = NULL;
}


// return the next Coroutine to execute,
// since it can always return a peridoic task, this should never return null, 
// except exiting
Coroutine* 
Coro_Scheduler::_next_coro(){
  Coroutine* next_coro = NULL;

  //uint64_t cur_time = current_time();
  uint64_t cur_time = get_tick();
  if( cur_time - last_timestamp_ > TIME_THRESHOLD){
    last_timestamp_ = cur_time;
    next_coro = periodics_.dequeue();

    // don't flush to node too frequently
    if( next_coro && next_coro->tag_ == 233 ){
      if( cur_time - last_lazy_timestamp_ > TIME_LAZY_THRESHOLD ) {
        last_lazy_timestamp_ = cur_time;
      }else{
        periodics_.enqueue(next_coro);
        next_coro = candidates_.dequeue();
      }
    }

  }else{ 
    next_coro = candidates_.dequeue();
  }

  if( NULL == next_coro && active_coro_num < MAX_ALLOWED_CORO && !taskQ->private_is_empty()){
    next_coro = unassigns_.dequeue();
  }

  if( NULL == next_coro){
    next_coro = periodics_.dequeue();
  }
  //if(node_rank() == 0 && owner_thread == 1) sync_printf(next_coro->tag_);
  return next_coro;
}


void Coro_Scheduler::debug(){
  sync_printf(" candidates_: ", candidates_.size(), "\n periodics_: ", periodics_.size()); 
}

/* block until all coro finish */
void Coro_Scheduler::await(){
  //sync_printf(" candidates_: ", candidates_.size(), "\n periodics_: ", periodics_.size()); 
  for(;;){
    Coroutine * next_coro = _next_coro();
    if( next_coro == NULL ) break;
    next_coro->running_ = 1;
    next_coro->ready_ = 0;
    if(executant_!=NULL) update_tick_info(executant_);
    executant_ = next_coro;
    running_ = false;
    context_swap_cnt ++ ;
    swapcontext(&main_, next_coro->ctx_);
    running_ = true;
  }
}

void Coro_Scheduler::coroutine_create( coro_func_t func, int tag ){
  Coroutine * coro = _spawn_coro( func, tag );
#ifndef FASTCS
  getcontext(coro->ctx_);
#endif
  coro->ctx_->uc_stack.ss_sp = coro->stack_;
  coro->ctx_->uc_stack.ss_size = DFT_STACK_PER_CORO;
  coro->ctx_->uc_stack.ss_flags = 0;
  coro->ctx_->uc_link = &main_;
  //coro->ready_ = 1;
  coro->sch_ = this;
  coro->is_periodic_ = false;

  if( running_ ){
    coro->father_ = &main_;
  }else{
    coro->father_ = executant_->ctx_;
  }

#ifdef FASTCS
  makecontext(coro->ctx_, (void (*)(void*))(tramp), 1, reinterpret_cast<void*>(coro));
#else
  makecontext(coro->ctx_, (void (*)())(tramp), 1, reinterpret_cast<void*>(coro));
#endif

  context_swap_cnt ++ ;
  swapcontext(coro->father_, coro->ctx_);
  //candidates_.enqueue(coro);
  unassigns_.enqueue(coro);
}

void Coro_Scheduler::coroutine_create_periodic( coro_func_t func, int tag ){
  Coroutine * coro = _spawn_coro( func, tag );
#ifndef FASTCS
  getcontext(coro->ctx_);
#endif
  coro->ctx_->uc_stack.ss_sp = coro->stack_;
  coro->ctx_->uc_stack.ss_size = DFT_STACK_PER_CORO;
  coro->ctx_->uc_stack.ss_flags = 0;
  coro->ctx_->uc_link = &main_;
  coro->ready_ = 1;
  coro->sch_ = this;
  coro->is_periodic_ = true;

  if(running_){
    coro->father_ = &main_;
  }else{
    coro->father_ = executant_->ctx_;
  }

#ifdef FASTCS
  makecontext(coro->ctx_, (void (*)(void*))(tramp), 1, reinterpret_cast<void*>(coro));
#else
  makecontext(coro->ctx_, (void (*)())(tramp), 1, reinterpret_cast<void*>(coro));
#endif
  context_swap_cnt ++;
  swapcontext(coro->father_, coro->ctx_);
  periodics_.enqueue(coro);
}

void Coro_Scheduler::coroutine_maybe_yield(){
  if( get_tick() - last_timestamp_ > TIME_THRESHOLD ){
    coroutine_yield();
  }else{
    return;
  }
}

void Coro_Scheduler::coroutine_yield(){
  Coroutine* next_coro = _next_coro();

  if( NULL == next_coro ){
    return;
  }

  Coroutine * coro = executant_;
  /* put it back */
  if(coro->is_periodic_)
    periodics_.enqueue(coro);
  else
    candidates_.enqueue(coro);

  executant_ = next_coro;

  coro->running_ = 0;
  coro->ready_ = 1;
  next_coro->ready_ = 0;
  next_coro->running_ = 1;

  context_swap_cnt ++;
  update_tick_info(coro);
  swapcontext( coro->ctx_, next_coro->ctx_ );
}

/*we can only suspend task coro */
void Coro_Scheduler::coroutine_suspend(){
  Coroutine* coro = executant_;
  //if(coro->is_periodic_) sync_printf("you are", coro->tag_);
  ASSERT(coro->is_periodic_ == false, "suspend periodic coroutine is not allowed");

  Coroutine* next_coro = _next_coro();

  ASSERT( next_coro != NULL, "_next_coro should never return NULL." );
  ASSERT( next_coro != coro || coro->idle_ == 1, "wake idle coroutine instantly");

  executant_ = next_coro;

  coro->running_ = 0;
  coro->suspended_ = 1;
  next_coro->ready_ = 0;
  next_coro->running_ = 1;

  context_swap_cnt ++;
  update_tick_info(coro);
  swapcontext(coro->ctx_, next_coro->ctx_) ;
}

void Coro_Scheduler::coroutine_idle(){
  Coroutine* coro = executant_;

  coro->idle_ = 1;
  unassigns_.enqueue(coro);
  coroutine_suspend();
  // when it is awake
  coro->idle_ = 0;
}

void Coro_Scheduler::coroutine_wait(Condition *cond){
  Coroutine * coro = executant_;
  if(executant_->is_periodic_){
    coroutine_yield();
  }else{
    cond->add_victim(coro);  
    coroutine_suspend();
  }
}

void Coro_Scheduler::coroutine_signal( Condition *cond ){
  Coroutine * coro = cond->resume();  
  /* do nothing when there is no victim */
  if( NULL == coro ) return;
  coro->suspended_ = 0;
  coro->ready_ = 1;
  candidates_.enqueue(coro);
}

void Coro_Scheduler::tramp(void * arg){
  Coroutine * cur = reinterpret_cast<Coroutine*>(arg);
  context_swap_cnt ++;
  swapcontext( cur->ctx_, cur->father_ );

  // this time it is a real swapcontext
  cur->func_();
  cur->running_ = 0;
  cur->idle_ = 1;
#ifdef FASTCS
  context_swap_cnt ++;
  swapcontext( cur->ctx_, cur->father_);
#endif
}

}//namespace Ring
