/* vmemory.h
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



#ifndef _VMEMORY_H_
#define _VMEMORY_H_

#include <iostream> 
#include <stdio.h>
#include <stdlib.h>

#ifdef NUMA_AWARE
#include <numa.h>
#endif

#include "comm/mpienv.h"
#include "sched/scheduler.h"
#include "pgas/allocator.h"
#include "pgas/addressing.h"
#include "utils/utils.h"


namespace Ring{

inline size_t round_up_page_size( size_t s ) {
  const size_t page_size = 1 << 12; // USE 4K page size
  size_t new_s = s;

  if( s < page_size ) {
    new_s = page_size;
  } else {
    size_t diff = s % page_size;
    if( 0 != diff ) {
      new_s = s + page_size - diff;
    }
  }
  return new_s;
}

inline void config_available_memory(){
  long pages = sysconf(_SC_PHYS_PAGES);
  long page_size = sysconf(_SC_PAGE_SIZE);
  //ASSERT(MEMORY_FRACTION + PGAS_FRACTION < 1, "Desired memory out of range(more than 100% of the node memory)")
  uint64_t node_memory = pages*page_size;
  double used_node_memory = node_memory*MEMORY_FRACTION;
  double comm_memory = used_node_memory*COMM_FRACTION;
  double pool_memory = used_node_memory*POOL_FRACTION / THREAD_SIZE;
  COMM_MEMORY_SIZE = round_up_page_size((size_t)comm_memory);
  POOL_MAX_SIZE = round_up_page_size((size_t)pool_memory);
  ASSERT(COMM_MEMORY_SIZE > node_size()*(sizeof(size_t) + MSG_SIZE*MSG_NUM*2 ), "communication memory is not enough.");

  size_t per_node_pgas = PGAS_MEMORY_SIZE;
  SHARED_MEMORY_SIZE = node_size() * round_up_page_size((size_t)per_node_pgas);
  double per_node_mem = ( 0.0 + round_up_page_size( SHARED_MEMORY_SIZE/ (node_size()*THREAD_SIZE)) * THREAD_SIZE) / GIGA;
  if(node_rank() == 0){
    std::cout << "# Memory Usage :" << std::endl
      << " - node memory : " << (double)node_memory/GIGA << " G" << std::endl
      << " - communication buffer : " << (double)COMM_MEMORY_SIZE/GIGA << " G" << std::endl
      << " - message pool size in total: " << (double)POOL_MAX_SIZE*THREAD_SIZE/GIGA << " G" << std::endl
      << " - total shared memory size : " << (double)SHARED_MEMORY_SIZE/GIGA << " G (" << per_node_mem << " per node. )" << std::endl << std::endl;

  }
}

#ifdef NUMA_AWARE
inline bool is_same_socket(int t1, int t2){
  int sockets = numa_num_configured_nodes();
  int threads = THREAD_SIZE;
  int threads_per_node = threads / sockets;
  return (t1 % threads_per_node) == (t2 % threads_per_node);
}
#endif




class Pmemory{
public:
  inline explicit Pmemory( size_t size_per_core ){
#ifdef NUMA_AWARE
    int sockets = numa_num_configured_nodes();
    int threads = THREAD_SIZE;
    int threads_per_node = threads / sockets;
#endif
    void* mem = NULL;
    base_ = (intptr_t*)malloc( sizeof(intptr_t)*THREAD_SIZE );
    for(size_t i = 0; i < THREAD_SIZE; ++i){
#ifdef NUMA_AWARE
      int which_numa = i / threads_per_node;
      mem = (void*)numa_alloc_onnode(size_per_core, which_numa);
#else
      posix_memalign( &mem, 64, size_per_core); 
#endif
      ASSERT(mem != NULL, "aligned alloc failed!");
      base_[i] = reinterpret_cast<intptr_t>(mem);
    }
  }
  inline intptr_t get_chunk_base(int i) const{
    return base_[i];
  }
private:
  size_t size_;
  intptr_t* base_;
};


class Vmemory{

public:
  explicit Vmemory( size_t total_shared_size );

  inline GlobalAddress<void> vmalloc( size_t size ){
    void* ptr = allocator_.xmalloc(size); 
    GlobalAddress<void> ga;
    ga.create_Linear(ptr);
    return ga;
  }

  inline void vfree( GlobalAddress<void> vaddr ){
    void * va = reinterpret_cast<void*>(vaddr.get_raw() );
    allocator_.xfree(va);
  }

  inline intptr_t get_base(int i) const {
    return my_pm_base_.get_chunk_base(i);
  }

private:
  size_t size_per_core_;
  size_t size_per_node_;
  Pmemory my_pm_base_;
  Allocator allocator_;
};

extern Vmemory* global_mem_manager;

extern intptr_t* partition_mem_base;



}//namespace Ring

#endif
