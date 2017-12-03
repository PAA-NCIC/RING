/* addressing.h -- PGAS 
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

#ifndef _ADDRESSING_H_
#define _ADDRESSING_H_

#include <iostream>


#include "utils/utils.h"
#include "comm/verbs.h"
#include "sched/scheduler.h"

namespace Ring{

extern intptr_t* partition_mem_base;

static const int tag_bits = 1;
static const int thread_bits = 15;
static const int pointer_bits = 48;

static const int tag_shift = 64 - tag_bits;
static const int thread_shift = 64 - tag_bits - thread_bits;
static const int pointer_shift = tag_bits + thread_bits;

static const intptr_t tag_mask = (1L << tag_shift);
static const intptr_t thread_mask = (1L << thread_bits) - 1;
static const intptr_t pointer_mask = (1L << pointer_bits) - 1;


struct range_t{
  intptr_t paddr;
  size_t n;
  size_t s_idx;
  size_t bs;
};


template <typename T>
class GlobalAddress{
public:
  explicit GlobalAddress() : pointer_(0){}
  explicit GlobalAddress(T* t) : pointer_(0){
    create_2D(my_id(), t); 
  }

  template<typename U>
  explicit GlobalAddress(GlobalAddress<U> n){
    raw(n.get_raw());
  }
 
  inline void raw( intptr_t rawdata){
    pointer_ = rawdata;
  }

  static GlobalAddress set(intptr_t rawdata){
    GlobalAddress g;
    g.raw(rawdata);
    return g;
  }

  inline intptr_t get_raw() const {
    return pointer_;
  }

  inline bool is_2D() const {
    return (pointer_ >> tag_shift) & tag_mask;
  }

  inline bool is_Linear() const {
    return !((pointer_ >> tag_shift) & tag_mask);
  }

  inline void create_2D(uint64_t id, T *t){
    ASSERT( id <= thread_mask, "too many threads, thread num should be less than 32768.");
    ASSERT( (int64_t)id < cores(), "out of range");
    pointer_ = ( 1L << tag_shift )
             | ( id << thread_shift )
             | ( pointer_mask & (reinterpret_cast<intptr_t>(t)) );
  }

  inline void create_Linear(T *t){
    pointer_ = ( 0L << tag_shift )
             | ( pointer_mask & (reinterpret_cast<intptr_t>(t)) );
  }

  // this should convert physical address to virtual address
  inline GlobalAddress<T> to_virtual(T *t){
    intptr_t tt = reinterpret_cast<intptr_t>(t) - reinterpret_cast<intptr_t>(partition_mem_base[thread_rank()]);
    intptr_t offset = tt % BLOCK_SIZE;
    intptr_t block = tt / BLOCK_SIZE;
    intptr_t bits = (block * cores() + my_id()) * BLOCK_SIZE + offset;
    
    GlobalAddress<T> ret;
    ret.create_Linear( reinterpret_cast<T*>(bits) );
    return ret;
  }

  inline size_t get_id() const {
    if(is_2D()){
      return (pointer_ >> thread_shift) & thread_mask;
    }else{
      //size_t threads = thread_size();
      //size_t nodes = node_size();
      size_t on_which_block = pointer_ / BLOCK_SIZE ;
      size_t on_which_core = on_which_block % ( cores() );
      return on_which_core;
    }
  }

  inline size_t get_node_rank() const {
    if(is_2D()){
      uint64_t id = (pointer_ >> thread_shift) & thread_mask;
      return id / thread_size();
    }else{
      size_t threads = thread_size();
      size_t nodes = node_size();
      size_t on_which_block = pointer_ / BLOCK_SIZE ;
      size_t on_which_core = on_which_block % ( threads * nodes );
      size_t on_which_node = on_which_core / threads;
      return on_which_node;
    }
  }

  inline size_t get_thread_rank() const {
    if(is_2D()){
      uint64_t id = (pointer_ >> thread_shift)  & thread_mask;
      return id % thread_size();
    }else{
      size_t threads = thread_size();
      size_t nodes = node_size();
      size_t on_which_block = pointer_ / BLOCK_SIZE ;
      size_t on_which_core = on_which_block % ( threads * nodes );
      size_t on_which_thread = on_which_core % threads;
      return on_which_thread;
    }
  }

  /* The only interface to Convert the virtual pointer to local pointer
   * don't guarantee that the pointer is still vaild after converting */
  inline T* ptr() const {
    if( is_2D() ){
      return reinterpret_cast<T*>( (pointer_ << pointer_shift ) >> pointer_shift );
    }else{
      size_t threads = thread_size();
      size_t nodes = node_size();
      uintptr_t on_which_block = pointer_ / BLOCK_SIZE ;
      //uintptr_t on_which_core = on_which_block % ( threads * nodes );
      uintptr_t block_depth = on_which_block / (threads * nodes);
      //uintptr_t on_which_node = on_which_core / threads;
      //uintptr_t on_which_thread = on_which_core % threads;
      uintptr_t block_offset = pointer_ & (BLOCK_SIZE-1); // equal to : pointer_ % BLOCK_SIZE
      uintptr_t local_mem_offset = block_depth * BLOCK_SIZE + block_offset;
      //return reinterpret_cast<T*>( local_mem_offset + partition_mem_base[on_which_thread] );
      return reinterpret_cast<T*>( local_mem_offset + partition_mem_base[thread_rank()] );
    }
  }

  T* operator->() const { return ptr(); }

  inline GlobalAddress<T> operator+(ptrdiff_t i) const {
    GlobalAddress<T> ret;
    ret.raw(pointer_ + i*sizeof(T));
    return ret;
  }

  inline GlobalAddress<T> operator-(ptrdiff_t i) const {
    GlobalAddress<T> ret;
    ret.raw(pointer_ - i*sizeof(T));
    return ret;
  }

  inline GlobalAddress<T>& operator+=(ptrdiff_t i) { 
    pointer_ += i * sizeof(T); 
    return *this; 
  }

  inline GlobalAddress<T>& operator-=(ptrdiff_t i) { 
    pointer_ -= i * sizeof(T); 
    return *this; 
  }

  inline GlobalAddress<T>& operator++() {
    pointer_ += sizeof(T); 
    return *this;
  }

  inline GlobalAddress<T> operator++(int){
    GlobalAddress<T>result = *this;
    pointer_ += sizeof(T);
    return result;
  }
  
  inline GlobalAddress<T>& operator--() { 
    pointer_ -= sizeof(T); 
    return *this; 
  } 


  bool operator<(const GlobalAddress<T>& t) const {
    return get_raw() < t.get_raw();
  }

  bool operator>(const GlobalAddress<T>& t) const {
    return get_raw() > t.get_raw();
  }

  bool operator==(const GlobalAddress<T>& t) const {
    return get_raw() == t.get_raw();
  }

  template< typename U >
  explicit operator GlobalAddress<U>( ) {
    GlobalAddress<U> u = GlobalAddress<U>::set(pointer_);
    return u;
  }


  inline range_t get_my_local_data(size_t id, int64_t tot_num) const{
    intptr_t bits = 0;
    size_t threads = thread_size();
    size_t nodes = node_size();
    size_t id_of_cur_addr = get_id(); 
    range_t ret;
    ret.bs = BLOCK_SIZE/sizeof(T);
    if(id_of_cur_addr < id){
      bits = pointer_ + (id-id_of_cur_addr) * BLOCK_SIZE;
      bits -= bits % BLOCK_SIZE;
    }else if(id_of_cur_addr > id){
      bits = pointer_ + (id-id_of_cur_addr)*BLOCK_SIZE;
      bits += BLOCK_SIZE * threads * nodes;
      bits -= bits % BLOCK_SIZE;
    }else{
      bits = pointer_;
    }
    ret.s_idx = (bits-pointer_)/(sizeof(T));

    // keep smile, please
    int64_t s = (ret.s_idx / ret.bs) * ret.bs;
    int64_t tmp1 = ret.bs - ret.s_idx % ret.bs;
    int64_t tmp2 = (tot_num - (s+ret.bs)) / (threads * nodes * ret.bs);
    if(tmp2 < 0) tmp2 = 0;
    size_t tmp3 = ((*this) + tot_num-1).get_id() == id ? tot_num%ret.bs : 0;
    if(tot_num < (int64_t)(threads*nodes*ret.bs)){
      if((int64_t)(s + ret.bs) <= tot_num) ret.n = tmp1;
      else ret.n = tot_num%ret.bs - ret.s_idx%ret.bs;
    }else{
      ret.n = tmp1 + tmp2*ret.bs + tmp3;
    }
    ret.paddr = reinterpret_cast<intptr_t>(((*this) + ret.s_idx).ptr());

    return ret;
  }

  /* used for loop decomposition */
  inline GlobalAddress<T> block_first() const{
    GlobalAddress<T> ret;
    intptr_t offset = pointer_ % BLOCK_SIZE;
    ret.raw(pointer_ - offset);
    return ret;
  }

private:
  intptr_t pointer_;
};

template<typename T>
inline ptrdiff_t operator-(const GlobalAddress<T>& t, const GlobalAddress<T>& u){
  return (t.get_raw() - u.get_raw())/ sizeof(T);
}


template <typename T>
GlobalAddress<T> make_global(T* t){
  return GlobalAddress<T>( t );
}

template <typename T>
GlobalAddress<T> make_global(uint64_t node_rank, uint64_t thread_rank, T* t){
  auto x = GlobalAddress<T>();
  x.create_2D(node_rank*thread_size() + thread_rank, t);
  return x; 
}

template <typename T>
GlobalAddress<T> make_linear(T* t){
  GlobalAddress<T> ret;
  return ret.to_virtual(t);
}

}//namespace Ring
#endif
