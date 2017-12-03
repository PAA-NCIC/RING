/* xmalloc.cc
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

#include <cstring>

#include "pgas/allocator.h"

namespace Ring{


#ifdef NUMA_BUF
Allocator* global_allocators[SOCKETS];
#else
Allocator* global_allocator;
#endif


inline int64_t
Allocator::next_largest_power_of_2( int64_t v ) {
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    v++;
    return v;
  }

inline bool 
Allocator::is_power_of_2(int64_t x){
  return (x & (x-1)) == 0;
}

void 
Allocator::remove_from_freelist(const ChunkMap::iterator & it){
  auto _size = it->second.size;
  freelists[_size].erase(it->second.free_list_ptr);
  if( freelists[_size].size() == 0 ){
    freelists.erase(_size);
  }
  it->second.used = true;
}
  
void 
Allocator::add_to_freelist(const ChunkMap::iterator & it){
  auto _size = it->second.size;
  freelists[_size].push_front( it );
  it->second.free_list_ptr = freelists[_size].begin();
  it->second.used = false;
}

ChunkMap::iterator 
Allocator::add_to_chunkmap(const Chunk& chunk){
  //std::pair<ChunkMap::iterator, bool>
  auto res = chunks.insert( std::make_pair(chunk.addr, chunk) );
  ASSERT( res.second, "add_to_chunkmap failed");
  return res.first;
}

void 
Allocator::merge_buddy_recursive(ChunkMap::iterator it){
  uintptr_t addr = it->second.addr;
  uintptr_t buddy_addr = (addr ^ it->second.size);

  auto buddy_it = chunks.find( buddy_addr );
  if( buddy_it != chunks.end() && buddy_it->second.size == it->second.size && false == buddy_it->second.used ){
    auto h_it = addr < buddy_addr ? buddy_it : it;
    auto l_it = addr < buddy_addr ? it : buddy_it;

    remove_from_freelist(h_it);
    chunks.erase(h_it);

    remove_from_freelist(l_it);
    l_it->second.size <<= 1;
    add_to_freelist( l_it );

    merge_buddy_recursive( l_it );
  }
}

Allocator::Allocator(int which_numa, int64_t size, bool vmode=false)
    : size(size)
    , chunks()
    , freelists() {

   ASSERT( size > 0, "size should be a positive integer" );

   if(vmode){
     base = 0;
   }else{
#ifdef NUMA_BUF
     char* tmp = (char*)numa_alloc_onnode(size* sizeof(char), which_numa);
#else
     char* tmp = (char*)malloc( size * sizeof(char) );
#endif
     ASSERT(tmp != NULL, "allocated memory failed");
     memset(tmp, 0, size*sizeof(char));
     base = reinterpret_cast<uintptr_t>(tmp);
   }

   uintptr_t offset = 0;

   while( size > 0 ){
     uintptr_t _offset = offset;
     size_t _size = size;

     if( !is_power_of_2(size) ){
       _size = next_largest_power_of_2( size>>1 );
     }

     auto it = add_to_chunkmap( Chunk(_offset, _size) );
     add_to_freelist( it );

     size -= _size;
     offset += _size;
   }
}

Allocator::Allocator(int64_t size, bool vmode=false)
    : size(size)
    , chunks()
    , freelists() {

   ASSERT( size > 0, "size should be a positive integer" );

   if(vmode){
     base = 0;
   }else{
     char * tmp = (char*)malloc( size * sizeof(char) );
     memset(tmp, 0, size*sizeof(char));
     base = reinterpret_cast<uintptr_t>(tmp);
   }

   uintptr_t offset = 0;

   while( size > 0 ){
     uintptr_t _offset = offset;
     size_t _size = size;

     if( !is_power_of_2(size) ){
       _size = next_largest_power_of_2( size>>1 );
     }

     auto it = add_to_chunkmap( Chunk(_offset, _size) );
     add_to_freelist( it );

     size -= _size;
     offset += _size;
   }
}

void*
Allocator::get_base(){
  return reinterpret_cast<void*>(this->base);
}

size_t
Allocator::get_size(){
  return this->size;
}

void* 
Allocator::xmalloc(size_t size){
  int64_t _size = next_largest_power_of_2(size);
  auto it = freelists.lower_bound( _size );
  ASSERT(it != freelists.end(), "Fatal error: OOM" );

  int64_t chunk_size = it->first;
  auto chunk_it = it->second.front();

  while( chunk_size > _size ){
    remove_from_freelist(chunk_it);

    chunk_size >>= 1;
    chunk_it->second.size = chunk_size;

    add_to_freelist(chunk_it);

    uintptr_t new_chunk_addr = chunk_it->second.addr + chunk_it->second.size;
    chunk_it = add_to_chunkmap( Chunk(new_chunk_addr, chunk_size) );
    add_to_freelist( chunk_it );
  }

  chunk_it->second.used = true;
  remove_from_freelist(chunk_it);
  return reinterpret_cast< void * >(chunk_it->second.addr + base);
}

void
Allocator::xfree(void * addr){
  uintptr_t _addr = reinterpret_cast<uintptr_t>(addr) - base;
  auto free_it = chunks.find(_addr);
  ASSERT( free_it != chunks.end() && free_it->second.used == true, "no way to free a invalid block");
  add_to_freelist(free_it);
  merge_buddy_recursive(free_it);
}


std::ostream& 
Allocator::dump(std::ostream &o){
  size_t tot_bytes = 0;
  size_t tot_bytes_in_use = 0;
  size_t tot_bytes_free = 0;
  size_t chunk_nums = chunks.size();

  for( auto it : chunks ){
    tot_bytes += it.second.size;
    if(it.second.used) tot_bytes_in_use += it.second.size;
    else tot_bytes_free += it.second.size;
  }

  o << " Total : " << std::endl;
  o << "  tot_bytes : " << tot_bytes << std::endl;
  o << "  tot_bytes_in_use : " << tot_bytes_in_use << std::endl;
  o << "  tot_bytes_free : " << tot_bytes_free << std::endl;
  o << "  chunk_nums : " << chunk_nums << std::endl;

  o << "all chunks :" << std::endl;
  for( auto it : chunks){
    char ch = it.second.used ? '+': '-';
    o << "  [" << it.second.addr << " : " << it.second.size << " "  << ch << " ]"<< std::endl;
  }
  o << "all freelists :" << std::endl;
  for( auto it : freelists){
    o<< "  " << it.first << " :";
    for(auto jt : it.second){
      o << " [" << (*jt).second.addr << ":"<< (*jt).second.size << "]";
    }
    o << std::endl;
  }
  return o;
}

size_t 
Allocator::debug_byte_total(){
  size_t tot_bytes = 0;
  for( auto it : chunks ){
    tot_bytes += it.second.size;
  }
  return tot_bytes;
}

size_t 
Allocator::debug_byte_used(){
  size_t tot_bytes_in_use = 0;
  for( auto it : chunks ){
    if( it.second.used ) tot_bytes_in_use += it.second.size;
  }
  return tot_bytes_in_use;
}

size_t
Allocator::debug_byte_free(){
  size_t tot_bytes_free = 0;
  for( auto it : chunks ){
    if( !it.second.used ) tot_bytes_free += it.second.size;
  }
  return tot_bytes_free;
}

size_t
Allocator::debug_chunk_nums(){
  return chunks.size();
}

size_t
Allocator::debug_freelist_nums(size_t slot){
  auto it = freelists.lower_bound( slot );
  return it->second.size();
}

}//namespace Ring
