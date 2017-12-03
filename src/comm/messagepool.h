/* messagepool.h
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
#ifndef _MESSAGEPOOL_H_
#define _MESSAGEPOOL_H_

#include <mutex>
#include <thread>

#include "comm/pool_allocator.h"
#include "comm/message.h"
#include "sched/scheduler.h"

namespace Ring{

/* TODO(mengke): we can't reuse this MessagePool right now
 * since it will make the whole system more complicated */
const int MAX_MESSAGE_SIZE = 1<<10;
const int MAX_POOL_CNT = MAX_MESSAGE_SIZE / CACHE_LINE_SIZE;
const int MAX_POOL_CUTOFF = 1<<8;
const int MAX_POOL_CACHELINE = MAX_POOL_CUTOFF / CACHE_LINE_SIZE;
const int EACH_POOL_SIZE = 1<<15;

class MessagePool{
public:
  void init(int which_numa);
  void* alloc(size_t size);
  void free(Message* m, size_t size);
private:
  PoolAllocator pools[MAX_POOL_CNT];
} CACHE_ALIGNED;


}//namespace Ring



#endif
