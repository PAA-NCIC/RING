/* communicator.h
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

#ifndef _COMMUNICATOR_H_
#define _COMMUNICATOR_H_

#include <atomic>
#include <vector>
#include <malloc.h>

#include "utils/utils.h"
#include "utils/memlog.h"
#include "comm/message.h"
#include "comm/messagepool.h"
#include "comm/verbs.h"
#include "sched/barrier.h"
#include "sched/scheduler.h"
#include "pgas/allocator.h"

namespace Ring{


struct mail_t {
  union {
    struct {
      unsigned size_ : 16;
      intptr_t pointer_ : 48;
    };
    intptr_t raw_;
  };
};

static inline void set_pointer(mail_t* mail, Message* msg){
  mail->pointer_ = reinterpret_cast<intptr_t>(msg);
}

static inline Message * get_pointer(mail_t* mail){
  return reinterpret_cast<Message*>(mail->pointer_);
}


/* eliminate false sharing */
struct mailbox_t{
  mail_t msg;
  mail_t prefetch_queue[PREFETCH_DIST];
  mailbox_t(){}
} CACHE_ALIGNED ;


struct recv_context_t{
  char* buf;
  size_t size;
  std::atomic<int> ref_cnt;
  inline bool is_ok(){
    uint32_t* header = reinterpret_cast<uint32_t*>(buf);
    if(header[0]){
      size_t s = header[1];
      if(buf[s]) return true;
    }
    return false;
  }
  inline uint32_t* get_header(){
    return reinterpret_cast<uint32_t*>(buf);
  }
  inline uint16_t* get_count(){
    return (reinterpret_cast<uint16_t*>(buf)+4);
  }
  inline char* get_payload(){
    int payload_offset = get_meta_size();
    return &buf[payload_offset];
  }
  inline int get_meta_size(){
#ifdef NUMA_BUF
    //return (THREAD_PER_NUMA+2) * sizeof(uint32_t);
    return (THREAD_PER_NUMA+4) * sizeof(uint16_t);
#else
    //return (THREAD_SIZE+2) * sizeof(uint32_t);
    return (THREAD_SIZE+4) * sizeof(uint16_t);
#endif
  }
  inline int get_payload_size(){
    int payload_offset = get_meta_size();
    return size - payload_offset;
  }
};

struct recv_ring_buf_t{
  recv_context_t slots[MSG_NUM];
  int head ,cur, cnt;
  recv_ring_buf_t():head(0),cur(0),cnt(0){}
  inline int add(int x){
    return (x+1 == MSG_NUM ? 0 : (x+1));
  }
  inline recv_context_t* get_empty_slot(){
    /* don't increase ref_cnt here, 
     * we will increase it in process_ring_buf_of() */
    if(slots[cur].is_ok()){
      auto ret = &slots[cur];
      cur = add(cur);
      return ret;
    }else{
      return NULL;
    }
  }
};

struct send_context_t{
  char* buf;
  size_t size;
  inline uint32_t* get_header(){
    return reinterpret_cast<uint32_t*>(buf);
  }
  inline uint16_t* get_count(){
    return (reinterpret_cast<uint16_t*>(buf)+4);
  }
  inline char* get_payload(){
    int payload_offset = get_meta_size();
    return &buf[payload_offset];
  }
  inline int get_meta_size(){
#ifdef NUMA_BUF
    //return (THREAD_PER_NUMA+2) * sizeof(uint32_t);
    return (THREAD_PER_NUMA+4) * sizeof(uint16_t);
#else
    //return (THREAD_SIZE+2) * sizeof(uint32_t);
    return (THREAD_SIZE+4) * sizeof(uint16_t);
#endif
  }
  inline int get_payload_size(){
    int payload_offset = get_meta_size();
    return size - payload_offset;
  }
};

struct send_ring_buf_t{
  send_context_t slots[MSG_NUM];
  int head, tail_;
  send_ring_buf_t():head(1),tail_(0){}
  inline int add(int x){
    return (x+1 == MSG_NUM ? 0 : (x+1));
  }

  inline send_context_t* get_empty_slot(){
    /* not full */
    if(head != tail_){
      int tmp = head;
      head = add(head);
      return &slots[tmp];
    }else{
      return NULL;
    }
  }
  inline void clean_one_slot(){
    memset(slots[tail_].buf, 0, slots[tail_].size*sizeof(char));
    tail_ = add(tail_);
  }

  inline void roll_back(){
    if(head > 0) head --;
    else head = MSG_NUM-1;
  }
};

class Communicator{
public:

  Communicator();
  ~Communicator();

  mailbox_t* MsgData(int64_t me, int64_t node_rank, int64_t thread_rank);

  void enqueue( Message * m);
  /* start sender recver poller for every thread in this node */
  void activate();
  /* CAS message for node i and use verbs send to target node */
  void flush_to_node(int me, size_t rank);
  /* process one ring buf and enqueue remote task */
  void process_ring_buf_of(int me, size_t rank);
  /* repost used buf */
  void polling(int me, size_t rank);
  /* used to communicate with peers */
  void peer_msg(int me);
  /* update remote head copy */
#ifdef NUMA_BUF
  void update_remote_head_copy_(size_t rank, int which_numa);
#else
  void update_remote_head_copy_(size_t rank);
#endif

#ifdef NUMA_BUF
  inline uint64_t** get_my_recv_buf(){
    return my_recv_buf_;
  }
#else
  inline uint64_t* get_my_recv_buf(){
    return my_recv_buf_;
  }
#endif

  inline void wait_for_finish_msg(){
    size_t node_size = mpi_env->size;
    size_t thread_size = THREAD_SIZE;
    for(size_t i = 1; i < node_size; ++i) {
      size_t idx = i*thread_size;
      //this is right because only thread_rank 0 will do this
      while(simple_msg_map_[idx].msg.raw_ != 0){
        yield();
      }
    }
  }

  inline void* allocate_msg(int which_thread, size_t size){
    return msg_pool_[which_thread]->alloc(size);
  }

  inline void debug_remote_recv_addr(){
#ifdef NUMA_BUF
    for(int s = 0; s < SOCKETS; ++s){
      for(int i = 0; i < node_size(); ++i){
        sync_printf("in node:",node_rank(),s,"for node :",i,"is",(uint64_t)(uintptr_t)(my_recv_buf_[s][i]));
      }
    }
#else
    for(int i = 0; i < node_size(); ++i){
      sync_printf("in node:",node_rank(),"for node :",i,"is",(uint64_t)(uintptr_t)(my_recv_buf_[i]));
    }
#endif
  }

private:
  /* size = N*T(N:node number, T:thread number) */
  mailbox_t* simple_msg_map_; 
  /* MessagePool */
  MessagePool** msg_pool_;
#ifdef NUMA_BUF
  recv_ring_buf_t** simple_recv_map_[SOCKETS];
  send_ring_buf_t** simple_send_map_[SOCKETS];
#else
  /* size = N (buf head must be monopolized by remote node) */
  recv_ring_buf_t** simple_recv_map_;
  /* size = N one for one node*/
  send_ring_buf_t** simple_send_map_;
#endif

#ifdef NUMA_BUF
  char *recv_heap_[SOCKETS];
  char *send_heap_[SOCKETS];
#else
  /* size = MSG_SIZE*MSG_NUM */
  char *recv_heap_, *send_heap_;
#endif

  /* sender' copy of head, size = N, one node should have N head copy of other queue */
#ifdef NUMA_BUF
  size_t* head_copy_[SOCKETS];
  size_t* tail_[SOCKETS];
#else
  size_t* head_copy_;
  size_t* tail_;
#endif

#ifdef NUMA_BUF
  uint64_t* remote_head_addr_[SOCKETS];
  uint64_t* my_recv_buf_[SOCKETS];
#else
  uint64_t* remote_head_addr_;
  uint64_t* my_recv_buf_;
#endif
};

extern Communicator* global_comm;

template<typename F>
inline void send_request( size_t from_node, size_t from_thread,
                          size_t to_node, size_t to_thread, F f ){
  void* buf = global_comm->allocate_msg(from_thread, sizeof(Request<F>));
  auto x = new(buf) Request<F>(f);
  x->delete_after_send();
  x->route( from_node, from_thread, to_node, to_thread );
  global_comm->enqueue(x);
}


template<typename F>
inline void send_request( size_t from_id, size_t to_id, F f ){
  void* buf = global_comm->allocate_msg(from_id%THREAD_SIZE, sizeof(Request<F>));
  auto x = new(buf) Request<F>(f);
  x->delete_after_send();
  x->route( from_id, to_id );
  global_comm->enqueue(x);
}

template<typename F>
inline void send_request( size_t to_id, F f){
  size_t from_id = my_id(); 
  send_request(from_id, to_id, f);
}




}//namespace Ring
#endif
