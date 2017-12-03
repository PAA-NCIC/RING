/* allocator.h
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

#ifndef _ALLOCATOR_H_
#define _ALLOCATOR_H_

#include <iostream>
#include <map>
#include <list>

#include "utils/utils.h"

namespace Ring{

struct Chunk;

using ChunkMap = std::map<uintptr_t, Chunk>;
using Freelist = std::list<ChunkMap::iterator>;
using FreeListMap = std::map<size_t, Freelist>;

struct Chunk {
  bool used;
  uintptr_t addr;
  size_t size;
  Freelist::iterator free_list_ptr;

  Chunk( uintptr_t addr, size_t size)
    : used(false), addr(addr)
    , size(size), free_list_ptr(){};
};

class Allocator{
public:

  explicit Allocator(int which_numa, int64_t size, bool vmode);
  explicit Allocator(int64_t size, bool vmode);

  void* get_base();
  size_t get_size();
    
  void* xmalloc(size_t size);
  void xfree(void * addr);


  std::ostream& dump(std::ostream& o = std::cout);
  size_t debug_byte_used();
  size_t debug_byte_total();
  size_t debug_byte_free();
  size_t debug_chunk_nums();
  size_t debug_freelist_nums(size_t slot);

private:

  uintptr_t base;
  size_t size;
  
  ChunkMap chunks;
  FreeListMap freelists;

  inline int64_t next_largest_power_of_2( int64_t v );
  inline bool is_power_of_2(int64_t x);

  void remove_from_freelist(const ChunkMap::iterator & it);
  void add_to_freelist(const ChunkMap::iterator & it);

  ChunkMap::iterator add_to_chunkmap(const Chunk& chunk);
  void merge_buddy_recursive(ChunkMap::iterator it);


};

#ifdef NUMA_BUF
extern Allocator* global_allocators[SOCKETS];
#else
extern Allocator *global_allocator;
#endif

}//namespace Ring
#endif
