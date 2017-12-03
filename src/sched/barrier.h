/* barrier.h -- coroutine barrier
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

#ifndef _BARRIER_H_
#define _BARRIER_H_

#include <mutex>
#include <atomic>

#include "sched/coroutine.h"
#include "sched/scheduler.h"

namespace Ring{

/* this Barrier can be reused */
class Barrier : public Condition{
public:
  Barrier() : cnt_(0){}
  Barrier(int64_t num) : cnt_(num) {}

  inline void issue(int inc = 1){
    cnt_ += inc;
  }

  inline void complete(int dec = 1){
    cnt_ -= dec;
    if( cnt_ == 0 ){
      auto w = global_scheduler->get_cur_worker();
      while( !this->empty() ){
        w->signal(this);
      }
    }
  }

  inline void wait(){
    auto w = global_scheduler->get_cur_worker();

    if( cnt_ == 0 ){
      while( !this->empty() ){
        w->signal(this);
      }
    }else{
      w->wait(this);
    }
  }

private:
  int64_t cnt_;
};

extern Barrier* global_barrier;
extern Barrier** call_inflight;
extern Barrier locale_barrier;

}//namespace Ring

#endif
