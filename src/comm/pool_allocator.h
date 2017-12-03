/* pool_allocator.h
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

#ifndef _POOL_ALLOCATOR_H_
#define _POOL_ALLOCATOR_H_

#include <cstdint>
#include <malloc.h>


#include "utils/utils.h"



namespace Ring{

// align ptr x to y boundary (rounding up)
#define ALIGN_UP(x, y) \
    ((((u_int64_t)(x) & (((u_int64_t)(y))-1)) != 0) ? \
        ((void *)(((u_int64_t)(x) & (~(u_int64_t)((y)-1)))+(y))) \
        : ((void *)(x)))

struct memory_chunk_t{
  struct memory_chunk_t* next;
  char* chunk;
  size_t chunk_size;
  size_t offset;
};

struct link_object_t{
  struct link_object_t* next;
};

class ChunkAllocator{
public:
  ChunkAllocator() : total_allocated(0){}; 
  void init(size_t chunk_size, int which_numa);
  void alloc_new_chunk(size_t size);
  void* alloc(size_t size);
  void free(void* p);

  size_t total_allocated;
private:
  int which_numa;
  size_t chunk_size;
  struct memory_chunk_t* first;
  struct memory_chunk_t* last;
};


class PoolAllocator{
public:
  PoolAllocator(){}
  void init(size_t object_size, size_t preset_num, int which_numa);
  void* alloc();
  void free(void* p);

private:
  ChunkAllocator chunk_allocator;
  size_t object_size;
  size_t preset_num;
  int index;
  struct link_object_t* prefetch[POOL_PREFETCH_DIST];
  //size_t allocated_cnt;
  //size_t fall_back_allocated_cnt;
};

#ifdef NUMA_AWARE
template<typename T>
T* numa_alloc(int which_numa, size_t num=1){
  void* tmp = numa_alloc_onnode(sizeof(T)*num, which_numa);
  return (T*)tmp;
}
#endif

template<typename T>
T* align_alloc(size_t align, size_t num=1){
  void * tmp;
  posix_memalign(&tmp, align, sizeof(T)*(num));
  return (T*)tmp;
}


};//namespace Ring

#endif
