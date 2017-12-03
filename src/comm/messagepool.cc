/* messagepool.cc
 * 
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

#include "comm/messagepool.h"

namespace Ring{


void MessagePool::init(int which_numa){
  for(int i = 0; i < MAX_POOL_CNT; ++i){
    size_t object_size = (i+1) * CACHE_LINE_SIZE;
    size_t preset_num = EACH_POOL_SIZE / object_size;

    // bigger than MAX_POOL_CUTOFF message will just allocate 1 chunk.
    if(i >= MAX_POOL_CACHELINE){
      preset_num = 1;
    }

    pools[i].init(object_size, preset_num, which_numa);
  }
}

void* MessagePool::alloc(size_t size){
  size_t sz = (size_t)ALIGN_UP(size, CACHE_LINE_SIZE);
  size_t cacheline_cnt = sz / CACHE_LINE_SIZE; // 53 / 64 == 0

  if( sz > MAX_MESSAGE_SIZE ){
    // use heap allocation
    return align_alloc<char>(CACHE_LINE_SIZE, sz);
    msg_on_heap ++ ;
  }else{
    switch(cacheline_cnt){
      case 1 : msg_1_cacheline ++;break;
      case 2 : msg_2_cacheline ++;break;
      case 3 : msg_3_cacheline ++;break;
      case 4 : msg_4_cacheline ++;break;
      default : msg_x_cacheline ++; break;
    }
    // use pool allocation
    return pools[cacheline_cnt].alloc();
  }
}

void MessagePool::free(Message* m, size_t size){
  size_t sz = (size_t)ALIGN_UP(size, CACHE_LINE_SIZE);
  size_t cacheline_cnt = sz / CACHE_LINE_SIZE; 

  if( sz > MAX_MESSAGE_SIZE){
    // use heap free (no reuse)
    ::free(m);
  }else{
    // use pool free (reuse)
    pools[cacheline_cnt].free(m);
  }
}

};//namespace Ring
