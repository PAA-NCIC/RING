
/* task_queue.h
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

#ifndef _TASK_QUEUE_H_
#define _TASK_QUEUE_H_

#include <deque>
#include <cstdint>

#include <tasking/task.h>


namespace Ring{

class TaskQueue{
public:
  inline TaskQueue(){};  

  inline bool private_is_empty(){
    return privateQ.empty();
  }

  template<typename A0, typename A1, typename A2>
  void spawn_private(void(*f)(A0,A1,A2), A0 arg0, A1 arg1, A2 arg2){
     Task newtask = createTask(f, arg0, arg1, arg2);
     privateQ.push_back(std::move(newtask));
  }
  
  bool try_private(Task* victim);

private:
  std::deque<Task> privateQ;
};

template<typename F>
static void task_functor_proxy(uint64_t a0, uint64_t a1, uint64_t a2){
  uint64_t functor_storage[3] = {a0,a1,a2};
  F* tp = reinterpret_cast<F*>(&functor_storage[0]);
  (*tp)();
  tp->~F();
}

template<typename F>
static void task_heapfunctor_proxy(F *tp, F* unused1, F*unused2){
  (*tp)();
  delete tp;
}

};//namespace Ring



#endif
