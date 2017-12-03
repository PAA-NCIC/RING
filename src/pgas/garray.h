/* garray.h
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

#ifndef _GARRAY_H
#define _GARRAY_H


#include "pgas/collective.h"
#include "pgas/rpc.h"
#include "pgas/addressing.h"
#include "pgas/vmemory.h"

namespace Ring{

/* let node 0 to malloc the space virtualy */
inline GlobalAddress<void> rmalloc(size_t size){
  auto vaddr = call(0, [size]{
    GlobalAddress<void> ret;
    auto ga = global_mem_manager->vmalloc(size);
    return ga;
  });
  //return vaddr->get();
  return vaddr;
}

/* let node 0 to free the space virtualy */
inline void rfree(GlobalAddress<void> vaddr){
  //auto flag = 
  call(0, [vaddr]{
    global_mem_manager->vfree(vaddr);
    return true;
  });
  //flag->get();
}


/* don't make it compatible with BLOCK_SIZE,
 * because we want a easy addressing mode
 */
// be careful that the sizeof(T) must be less than one CACHE_LINE_SIZE,
// since we don't want to traverse the array in a mess
template <typename T=char>
GlobalAddress<T> gmalloc(size_t size){
  ASSERT( BLOCK_SIZE%sizeof(T) == 0, "please pack the data_type size to divisor of 64 bytes!");
  return static_cast<GlobalAddress<T>>(rmalloc( sizeof(T)*size ));
}

/* A crafty way to trick GlobalAddress ptr() to get local pointer with the same local_mem_offset */
// it is ok that the global object is bigger than one CACHE_LINE_SIZE
// since it preserve the memory first than call the allocation function.
template <typename T=char>
GlobalAddress<T> symm_gmalloc(){
  ASSERT(sizeof(T)%BLOCK_SIZE==0, "must pad global proxy to multiple of BLOCK_SIZE");
  auto symm_proxy = gmalloc<char>( cores() * (sizeof(T) + BLOCK_SIZE) );
  while(symm_proxy.get_id() != 0) ++symm_proxy;
  return static_cast<GlobalAddress<T>>(symm_proxy);
}

template <typename T>
void gfree(GlobalAddress<T> vaddr){
  rfree(static_cast<GlobalAddress<void>>(vaddr));
}



//Iterator
template<typename T>
struct SimpleIterator {
  T * base;
  size_t nelem;
  T * begin() { return base; }
  T * end()   { return base+nelem; }
  const T * begin() const { return base; }
  const T * end() const { return base+nelem; }
  size_t size() const { return nelem; }
};

/// Easier C++11 iteration over local array. Similar idea to Addressing::iterate_local().
///
/// @code
///   auto array = new long[N];
///   for (auto& v : iterate(array,N)) {
///     v++;
///   }
/// @endcode
template<typename T>
SimpleIterator<T> iterate(T* base = nullptr, size_t nelem = 0) {
  return SimpleIterator<T>{base, nelem}; 
}

template<typename T>
struct LocalIterator {
  GlobalAddress<T> base;
  size_t nelem;
  T* local_base;
  size_t local_size;
  LocalIterator(GlobalAddress<T> base, size_t nelem) : base(base), nelem(nelem){
    auto rge = base.get_my_local_data(my_id(), nelem);
    local_base = (T*)rge.paddr;
    local_size = rge.n;
    //sync_printf("(", my_id(), local_base, local_size, rge.s_idx, ")");
  }
  T * begin() { return local_base; }
  T * end()   { return local_base+local_size; }
  size_t size() { return local_size; }
};

/// Helper for iterating over local elements of a Linear address range.
///
/// @code
///   auto array = gmalloc<long>(N);
///   all_do([]{
///     for (auto& v : iterate_local(array,N)) {
///       v++;
///     }
///   });
/// @endcode
template<typename T>
LocalIterator<T> iterate_local(GlobalAddress<T> base, size_t nelem) { return LocalIterator<T>{base, nelem}; }




}//namespace Ring

#endif
