/* communicator.cc
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

#include <iostream>

#include "comm/communicator.h"
#include "comm/completion.h"
#include "pgas/vmemory.h"
#include "utils/memlog.h"

namespace Ring{



/* me: sender thread rank;
 * node_rank : target node_rank;
 * thread_rank : target thread_rank;
 */
inline mailbox_t* Communicator::MsgData(int64_t me, int64_t node_rank, int64_t thread_rank){
  return &simple_msg_map_[ me*mpi_env->cores + node_rank*THREAD_SIZE + thread_rank];
}

Communicator* global_comm;

Communicator::Communicator(){
  const size_t node_size = mpi_env->size;
  const size_t thread_size = THREAD_SIZE;
  const size_t core_size  = mpi_env->cores;

#ifdef NUMA_BUF
  for(int i = 0; i < SOCKETS; ++i){
    recv_heap_[i] = (char*)global_allocators[i]->xmalloc( node_size*MSG_SIZE*MSG_NUM );
    simple_recv_map_[i] = (recv_ring_buf_t**) malloc( node_size*sizeof(recv_ring_buf_t*) );
    my_recv_buf_[i] = (uint64_t *) malloc (node_size * sizeof(uint64_t) );

    send_heap_[i] = (char *)global_allocators[i]->xmalloc( node_size*MSG_SIZE*MSG_NUM );
    simple_send_map_[i] = (send_ring_buf_t**) malloc( node_size*sizeof(send_ring_buf_t*) );

    head_copy_[i] = (size_t *)global_allocators[i]->xmalloc( node_size*sizeof(size_t) );
    remote_head_addr_[i] = (uint64_t*) malloc( node_size*sizeof(uint64_t) );
  }
#else
  recv_heap_ = (char *)global_allocator->xmalloc( node_size*MSG_SIZE*MSG_NUM );  
  simple_recv_map_ = (recv_ring_buf_t**) malloc( node_size*sizeof(recv_ring_buf_t*) );
  my_recv_buf_ = (uint64_t *) malloc (node_size * sizeof(uint64_t) );

  send_heap_ = (char *)global_allocator->xmalloc( node_size*MSG_SIZE*MSG_NUM );
  simple_send_map_ = (send_ring_buf_t**) malloc( node_size*sizeof(send_ring_buf_t*) );

  head_copy_ = (size_t *)global_allocator->xmalloc( node_size*sizeof(size_t) );
  remote_head_addr_ = (uint64_t*) malloc( node_size*sizeof(uint64_t) );
#endif
  
#ifdef NUMA_BUF
  uint64_t* my_head_addr  = (uint64_t *) malloc ( node_size*sizeof(uint64_t) );
  for(int j = 0; j < SOCKETS; ++j){
    for(size_t i=0; i < node_size; ++i) my_head_addr[i] = (uint64_t)(uintptr_t)(&head_copy_[j][i]);
    MPI_Alltoall( &my_head_addr[0], 1, MPI_UINT64_T, &remote_head_addr_[j][0], 1, MPI_UINT64_T, mpi_env->get_comm() );
  }
  free(my_head_addr);
#else
  uint64_t* my_head_addr  = (uint64_t *) malloc ( node_size*sizeof(uint64_t) );
  for(size_t i=0; i < node_size; ++i) my_head_addr[i] = (uint64_t)(uintptr_t)(&head_copy_[i]);
  MPI_Alltoall( &my_head_addr[0], 1, MPI_UINT64_T, &remote_head_addr_[0], 1, MPI_UINT64_T, mpi_env->get_comm() );
  free(my_head_addr);
#endif


#ifdef NUMA_BUF
  for(int j = 0; j < SOCKETS; j++){
    tail_[j] = (size_t*)malloc( node_size*sizeof(size_t) );
    for(size_t i = 0; i < node_size; ++i){
      tail_[j][i] = 0;
      head_copy_[j][i] = 0;
    }
  }
#else
  tail_ = (size_t*)malloc( node_size*sizeof(size_t) );
  for(size_t i = 0; i < node_size; ++i){
    tail_[i] = 0;
    head_copy_[i] = 0;
  }
#endif

  void* tmp = NULL;
  posix_memalign( &tmp, 64, (thread_size*core_size)*sizeof(mailbox_t));
  simple_msg_map_ = (mailbox_t*)tmp;
  for(size_t i = 0; i < thread_size*core_size; ++i) simple_msg_map_[i].msg.raw_ = 0;

  tmp = NULL;
  posix_memalign( &tmp, 64, thread_size*sizeof(MessagePool*));
  msg_pool_ = (MessagePool**) tmp;
  for(size_t i=0; i< thread_size; ++i) {
    int which_numa = 0;
    msg_pool_[i] = new MessagePool();
    msg_pool_[i]->init(which_numa);
  }

#ifdef NUMA_BUF 
  for(int k = 0; k < SOCKETS; ++k){
    size_t offset = 0;
    for(size_t i = 0 ; i < node_size; ++i){
      simple_recv_map_[k][i] = new recv_ring_buf_t();
      my_recv_buf_[k][i] = (uint64_t)(uintptr_t)(recv_heap_[k] + offset);
      for(size_t j = 0; j < MSG_NUM ; ++j){
        simple_recv_map_[k][i]->slots[j].buf = recv_heap_[k] + offset;
        simple_recv_map_[k][i]->slots[j].size = MSG_SIZE;
        simple_recv_map_[k][i]->slots[j].ref_cnt = 0;
        offset += MSG_SIZE;
      }
    }
  }

  for(int k = 0; k < SOCKETS; ++k){
    size_t offset = 0;
    for(size_t i = 0 ; i < node_size; ++i){
      simple_send_map_[k][i] = new send_ring_buf_t();
      for(size_t j = 0; j < MSG_NUM ; ++j){
        simple_send_map_[k][i]->slots[j].buf = send_heap_[k] + offset;
        simple_send_map_[k][i]->slots[j].size = MSG_SIZE;
        offset += MSG_SIZE;
      }
    }
  }

#else
  size_t offset = 0;
  for(size_t i = 0 ; i < node_size; ++i){
    simple_recv_map_[i] = new recv_ring_buf_t();
    my_recv_buf_[i] = (uint64_t)(uintptr_t)(recv_heap_ + offset);
    for(size_t j = 0; j < MSG_NUM ; ++j){
      simple_recv_map_[i]->slots[j].buf = recv_heap_ + offset;
      simple_recv_map_[i]->slots[j].size = MSG_SIZE;
      simple_recv_map_[i]->slots[j].ref_cnt = 0;
      offset += MSG_SIZE;
    }
  }

  offset = 0;
  for(size_t i = 0 ; i < node_size; ++i){
    simple_send_map_[i] = new send_ring_buf_t();
    for(size_t j = 0; j < MSG_NUM ; ++j){
      simple_send_map_[i]->slots[j].buf = send_heap_ + offset;
      simple_send_map_[i]->slots[j].size = MSG_SIZE;
      offset += MSG_SIZE;
    }
  }
#endif

  //debug_remote_recv_addr();
}



void Communicator::activate(){
  size_t node_size = mpi_env->size;
  size_t my_rank = mpi_env->rank;
  int thread_size = global_scheduler->get_size();

  for( int i = 0 ; i < thread_size ; ++i ){

    global_scheduler->spawn_coroutine_periodic(i, [this, i]{
      while(!global_exit_flag){
        this->peer_msg(i);
        yield();
      }
    });

    size_t rank = i;
    while(rank < node_size){
      if(rank != my_rank){
        /* spawn senders for this thread
         * a sender is binded to a target node ( send to ) */
        global_scheduler->spawn_coroutine_periodic(i, [this, rank, i]{
          while(!global_exit_flag){
            this->flush_to_node(i, rank);
            yield();
          }
        }, 233);
        /* spawn a recver for this thread 
         * a recver is binded to a target node ( recv from ) */
        global_scheduler->spawn_coroutine_periodic(i, [this, rank, i]{
          while(!global_exit_flag){
            this->process_ring_buf_of(i, rank);
            yield();
          }
        },666);
        /* spawn a pooler for this thread 
         * a poller is binded to a target node ( recv from ) */
        global_scheduler->spawn_coroutine_periodic(i, [this, rank, i]{
          while(!global_exit_flag){
            this->polling(i, rank);
            yield();
          }
        },555);
      }
      rank += thread_size;
    }//while
  }//for
}

void Communicator::enqueue(Message * m){

  msg_enqueue_bytes += m->get_size();
  msg_enqueue_cnts ++;

  size_t from_thread = thread_rank();
  size_t to_node = m->get_destination_core() / THREAD_SIZE;
  size_t to_thread = m->get_destination_core() % THREAD_SIZE;

  m->set_enqueued();

  /* insert it in to message list */
  mailbox_t *dest = MsgData(from_thread, to_node, to_thread);
  mail_t old_mail, new_mail;
  new_mail.raw_ = 0;
  int count = 0;

  set_pointer(&new_mail, m);

  //int cas_inc = 0;
  do{
    old_mail = dest->msg;
    count = old_mail.size_ + 1;
    new_mail.size_ = count;
    m->next = get_pointer(&old_mail);
    m->prefetch = get_pointer(&(dest->prefetch_queue[ count % PREFETCH_DIST ]));
    enqueue_cas_cnt ++ ;
    //cas_inc ++;
  }while( !__sync_bool_compare_and_swap( &(dest->msg.raw_), old_mail.raw_, new_mail.raw_ ) );

  
  // warning : maybe overwrite
  set_pointer(&(dest->prefetch_queue[count % PREFETCH_DIST]), m);
}

void Communicator::peer_msg(int me){

  peer_msg_cnt ++;
  bool useful = false;

  poolling_completion();

  size_t node_rank = mpi_env->rank;
  size_t thread_size = THREAD_SIZE;
  int from_id = 0;
  mail_t msg_list;
  //size_t id = node_rank*thread_size + me;

  /* Cache aligned, no false sharing */
  for(size_t i = 0 ; i < thread_size; ++i){
    from_id = i + node_rank * THREAD_SIZE;
    msg_list.raw_ = 0;
    mailbox_t* dest = MsgData(i, node_rank, me);

    // Avoid CAS contention when cpu is idle
    msg_list = dest->msg;
    if(msg_list.raw_ == 0) continue;

    //int cas_inc = 0;
    do {
      msg_list = dest->msg;
      enqueue_cas_cnt ++;
      //cas_inc ++;
    } while( !__sync_bool_compare_and_swap( &(dest->msg.raw_), msg_list.raw_, 0) );

    // warning : maybe overwrite
#ifdef PREFETCH
    for(int j = 0; j < PREFETCH_DIST; ++j){
      char * pf = reinterpret_cast<char*>(get_pointer( &dest->prefetch_queue[j] ));
      __builtin_prefetch(pf + 0, 1, PREFETCH_TYPE);
      //__builtin_prefetch(pf + 64, 1, PREFETCH_TYPE);
      dest->prefetch_queue[j].raw_ = 0;
    }
#endif


    Message * msg_to_process = get_pointer(&msg_list);

    while(true){
      Message * m = msg_to_process;
      if(m == NULL) break;
      msg_to_process = msg_to_process->next;
#ifdef PREFETCH
      char* pf = reinterpret_cast<char*>( m->prefetch);
      __builtin_prefetch(pf + 0, 1, PREFETCH_TYPE);
      //__builtin_prefetch(pf + 64, 1, PREFETCH_TYPE);
#endif
      //m->delivery();
      msg_local_bytes += m->get_size();
      useful = true;

      if(m->is_reuse){
        msg_pool_[me]->free(m, m->size());
        continue;
      }else{
        set_allow_yield(false);
        m->call();
        set_allow_yield(true);
      }


      m->mark_as_sent();
      if(m->is_delete_after_send){
        if(from_id == my_id()) msg_pool_[me]->free(m, m->size());
        else{
          // re-send this message back to origin MessagePool
          m->set_reuse();
          m->next = NULL;
          m->prefetch = NULL;
          m->to_id = (int16_t)from_id;
          enqueue(m);
        }
      }
    }
  }

  if(useful) useful_peer_msg_cnt ++;
}

// pop mailbox_t in a coroutine way.
class MessageIterator{
public:
  MessageIterator() = delete;
  MessageIterator( const MessageIterator& ) = delete;
  MessageIterator( MessageIterator&& ) = delete;
#ifdef NUMA_BUF
  MessageIterator(int s_id, int64_t target_rank, Communicator* C) :
    src_start_(0),
    src_end_(THREAD_SIZE),
    src_cur_(0),
    dst_start_(s_id*THREAD_PER_NUMA),
    dst_end_(s_id*THREAD_PER_NUMA + THREAD_PER_NUMA),
    dst_cur_(s_id*THREAD_PER_NUMA),
    target_rank_(target_rank),
    done_(false),
    C(C){}
#else
  MessageIterator(int64_t target_rank, Communicator* C) :
    src_start_(0),
    src_end_(THREAD_SIZE),
    src_cur_(0),
    dst_start_(0),
    dst_end_(THREAD_SIZE),
    dst_cur_(0),
    target_rank_(target_rank),
    done_(false),
    C(C){}
#endif

  mailbox_t* next(int64_t* from , int64_t * to){
    mailbox_t* candidate = NULL; 
    if( !done_ ){
      do{
        candidate = C->MsgData(src_cur_, target_rank_, dst_cur_); 
        *to = dst_cur_;
        *from = src_cur_;

        src_cur_ ++;
        if(src_cur_ >= src_end_) {
          src_cur_ = src_start_;
          dst_cur_ ++;
          if(dst_cur_ >= dst_end_){
            done_ = true;
          }
        }
#ifdef PREFETCH
        if( !done_ ){
          mailbox_t * next_one = C->MsgData(src_cur_, target_rank_, dst_cur_);
          char* pf = reinterpret_cast<char*>(next_one);
          __builtin_prefetch(pf + 0, 1, PREFETCH_TYPE);
          //__builtin_prefetch(pf + 64, 1, PREFETCH_TYPE);
        }
#endif
        if( 0 != candidate->msg.raw_) break;
        else candidate = NULL;
      }while( !done_ );
    }
    return candidate;
  }
private:
  int64_t src_start_, src_end_, src_cur_;
  int64_t dst_start_, dst_end_, dst_cur_;
  int64_t target_rank_;
  bool done_;
  Communicator* C;
};

void Communicator::flush_to_node(int me, size_t rank){
  flush_to_node_cnt ++;
  bool useful = false;
#ifdef NUMA_BUF
  for(int s_id = 0; s_id < SOCKETS; ++s_id){
#endif

#ifdef NUMA_BUF
    MessageIterator mi(s_id, rank, this);
#else
    MessageIterator mi(rank, this);
#endif

    Message* msg_to_send = NULL;
    bool all_msgs_clear = false;
    int64_t to = 0;
    int64_t from = 0;

    std::vector<RDMA_pack_t> pack;
#ifdef NUMA_BUF
    send_ring_buf_t * ring_buf = simple_send_map_[s_id][rank];
#else
    send_ring_buf_t * ring_buf = simple_send_map_[rank];
#endif


    while( (!all_msgs_clear) || (msg_to_send != NULL) ){
      // get a buffer to serialize;
      send_context_t* ctx = ring_buf->get_empty_slot();
      //if(my_id() == 1) sync_printf(ring_buf->head, ring_buf->tail_);
      if(ctx == NULL){
#ifdef NUMA_BUF
        global_verbs->poll_cq( rank, s_id, 1 );
#else
        global_verbs->poll_cq( rank, 1 );
#endif
        ring_buf->clean_one_slot();
        ctx = ring_buf->get_empty_slot();
        ASSERT(ctx!=NULL, "send contex should not be NULL after poll_cq");
      }

      uint32_t * header = ctx->get_header();
      int64_t rest_bytes = ctx->get_payload_size() - 1;
      int64_t used_bytes = 0;
      char* send_buf = ctx->get_payload();
      //uint32_t* send_cnt = ctx->get_count();
      uint16_t* send_cnt = ctx->get_count();

      bool ready_to_send = false;
      //fill the buff
      while( (!ready_to_send) && ( (!all_msgs_clear) || (msg_to_send != NULL) ) ){
        // if we have no more to send now, get some
        mailbox_t* data;
        if(msg_to_send == NULL){

          data = mi.next(&from, &to);// pop a message iterator

          if(data != NULL){
            ASSERT( data->msg.raw_ != 0, "data should not contain no message." );
            // 1. CAS a message list
            mail_t old_msg;
            do{
              old_msg = data->msg;
              enqueue_cas_cnt ++;
            }while( !__sync_bool_compare_and_swap( &(data->msg.raw_), old_msg.raw_, 0) );

            // 2. prefetch message list
#ifdef PREFETCH
            for(int j = 0; j < PREFETCH_DIST; ++j){
              char * pf = reinterpret_cast<char*>(get_pointer( &data->prefetch_queue[j] ));
              __builtin_prefetch(pf + 0, 1, PREFETCH_TYPE);
              //__builtin_prefetch(pf + 64, 1, PREFETCH_TYPE);
              data->prefetch_queue[j].raw_ = 0;
            }
#endif

            // 3. now we got some message.
            msg_to_send = get_pointer(&old_msg);

          }else{
            all_msgs_clear = true;
            break;
          }
        }// now we have message to send


        //serialize to buffer
        while( (!ready_to_send) && (msg_to_send != NULL)){
          Message* m = msg_to_send;

#ifdef PREFETCH
          char* pf = reinterpret_cast<char*>( m->prefetch );
          __builtin_prefetch(pf + 0, 1, PREFETCH_TYPE);
          //__builtin_prefetch(pf + 64, 1, PREFETCH_TYPE);
#endif
          // try to put message in to buffer;
          if( (int64_t)(m->get_size()) > rest_bytes ){
            ready_to_send = true;
            break;
          }

          // now serialize real message to  this buf
          size_t tmp = m->serialize_to(send_buf);
          msg_remote_bytes += tmp;
          send_buf += tmp;
          rest_bytes -= tmp;
          used_bytes += tmp;
#ifdef NUMA_BUF
          send_cnt[to - s_id*THREAD_PER_NUMA] += tmp;
#else
          send_cnt[to] += tmp;
#endif

          msg_to_send = msg_to_send->next;
          m->mark_as_sent();
          if(m->is_delete_after_send){
            int from_id = from + node_rank()*THREAD_SIZE;
            if(from_id == my_id()) msg_pool_[me]->free(m, m->size());
            else{
              // re-send this message back to origin MessagePool
              m->set_reuse();
              m->next = NULL;
              m->prefetch = NULL;
              m->to_id = (int16_t)from_id;
              enqueue(m);
            }
          }
        }
      }//while

      if( used_bytes > 0 ){
        double cur_msg_usage = ((double)used_bytes)/ctx->get_payload_size();
        msg_usage += cur_msg_usage;
        msg_usage_max = std::max(msg_usage_max, cur_msg_usage);
        msg_usage_min = std::min(msg_usage_min, cur_msg_usage);
        msg_usage_cnt += 1;

        // done tag
        //ctx->buf[ctx->size-1] = 1;
        header[0] = 1;
        header[1] = (uint32_t) used_bytes + ctx->get_meta_size();
        ctx->buf[header[1]] = 1;
#ifdef NUMA_BUF
        pack.emplace_back((uint64_t)(uintptr_t)(ctx->buf), header[1] + 1, tail_[s_id][rank]);
#else
        pack.emplace_back((uint64_t)(uintptr_t)(ctx->buf), header[1] + 1, tail_[rank]);
#endif

#ifdef NUMA_BUF
        while( MADD(tail_[s_id][rank]) == head_copy_[s_id][rank] ){
#else
        while( MADD(tail_[rank]) == head_copy_[rank] ){
#endif
          // remote node has no palce for new msg. yield
          //sync_printf("tail_", MADD(tail_[rank]), "head_copy_", head_copy_[rank]);
          //TODO : something may go wrong
          if(global_exit_flag) return;
          //sp_cnt ++;
          yield();
        }
#ifdef NUMA_BUF
        tail_[s_id][rank] = (tail_[s_id][rank]+1 == MSG_NUM ? 0 : tail_[s_id][rank]+1);
#else
        tail_[rank] = (tail_[rank]+1 == MSG_NUM ? 0 : tail_[rank]+1);
#endif
        if(pack.size() == MAX_PACK_SIZE){
          useful = true;
          msg_invoke_send ++;
#ifdef NUMA_BUF
          global_verbs->post_RDMA_write_batch(rank, s_id, std::move(pack));
#else
          global_verbs->post_RDMA_write_batch(rank, std::move(pack));
#endif
          verbs_rdma_batch_cnt ++;
          verbs_rdma_cnt ++;
        }
      }else{
        // we got no message to send, return the buf
        ring_buf->roll_back();
      }
    }//outer while
    // if we still have some message left, send it
    if(pack.size() != 0){
      useful = true;
      msg_invoke_send ++;
#ifdef NUMA_BUF
      global_verbs->post_RDMA_write_batch(rank, s_id, std::move(pack));
      //sync_printf("hah");
#else
      global_verbs->post_RDMA_write_batch(rank, std::move(pack));
#endif
      verbs_rdma_cnt ++ ;
    }
#ifdef NUMA_BUF
  }
#endif
  if(useful) useful_flush_to_node_cnt ++ ;
}

void Communicator::process_ring_buf_of(int me, size_t rank) {
  process_ring_buf_cnt ++;

#ifdef NUMA_BUF
  for(int s_id = 0; s_id < SOCKETS; ++s_id){
#endif

#ifdef NUMA_BUF
    recv_ring_buf_t* ring_buf = simple_recv_map_[s_id][rank];
#else
    recv_ring_buf_t* ring_buf = simple_recv_map_[rank];
#endif

    using Des=char*(*)(char *);
    recv_context_t* ctx = NULL;

    struct am_t{
      char* buf;
      uint32_t size;
      recv_context_t* ctx;
      void operator()(){
        char * end = buf + size;
        while(buf < end){
          auto fp = reinterpret_cast<Des *>(buf);
          char* next = (*fp)( (char*)(fp+1) );
          buf = next;
        }
        ctx->ref_cnt --;
      }
    };
#ifdef NUMA_BUF
    struct am_t am[THREAD_PER_NUMA];
#else
    struct am_t am[THREAD_SIZE];
#endif

    do {
      ctx = ring_buf->get_empty_slot();
      if(ctx != NULL) {
        //sync_printf("got message");
        char* recv_buf = ctx->get_payload();
#ifdef PREFETCH
        __builtin_prefetch(ctx->buf, 1, 0);
        __builtin_prefetch(ctx->buf+64, 1, 0);
#endif
        //uint32_t* recv_cnt = ctx->get_count();
        uint16_t* recv_cnt = ctx->get_count();
#ifdef NUMA_BUF
        for(size_t _i = 0; _i < THREAD_PER_NUMA; ++_i){
          size_t i = _i + s_id*THREAD_PER_NUMA;
          if(recv_cnt[_i] > 0) {
            am[_i].buf = recv_buf;
            am[_i].size = recv_cnt[_i];
            am[_i].ctx = ctx;
            ctx->ref_cnt ++;
            if((size_t)me != i){
              send_request( node_rank(), me, node_rank(), i, am[_i] );
            }
            recv_buf += recv_cnt[_i];
          }
        }
#else
        for(size_t i = 0; i < THREAD_SIZE; ++i){
          if(recv_cnt[i] > 0) {
            am[i].buf = recv_buf;
            am[i].size = recv_cnt[i];
            am[i].ctx = ctx;
            ctx->ref_cnt ++;
            if((size_t)me != i){
              send_request( node_rank(), me, node_rank(), i, am[i] );
            }
            recv_buf += recv_cnt[i];
          }
        }
#endif
#ifdef NUMA_BUF
        if( me/THREAD_PER_NUMA == s_id && recv_cnt[me-s_id*THREAD_PER_NUMA] > 0){
#else
        if(recv_cnt[me] > 0){
#endif
          set_allow_yield(false);
#ifdef NUMA_BUF
          am[me-s_id*THREAD_PER_NUMA]();
#else
          am[me]();
#endif
          set_allow_yield(true);
        }
      }
    }while(ctx != NULL);
#ifdef NUMA_BUF
  }
#endif
}

void Communicator::polling(int me, size_t rank){
  polling_cnt ++;

#ifdef NUMA_BUF
  for(int s_id = 0; s_id < SOCKETS; ++s_id){
#endif
#ifdef NUMA_BUF
    recv_ring_buf_t* ring_buf = simple_recv_map_[s_id][rank];
#else
    recv_ring_buf_t* ring_buf = simple_recv_map_[rank];
#endif
    int& _head = ring_buf->head;
    int& _cur = ring_buf->cur;
    int& _cnt = ring_buf->cnt;
    while( _head != _cur ){
      if(ring_buf->slots[_head].ref_cnt == 0){
        memset(ring_buf->slots[_head].buf, 0, ring_buf->slots[_head].size);
        _head = MADD(_head);
        _cnt ++;
      }else{
        break;
      }
    }
    if(_cnt >= MSG_NUM/2){
      _cnt = 0;
#ifdef NUMA_BUF
      update_remote_head_copy_(rank, s_id);
#else
      update_remote_head_copy_(rank);
#endif
    }
#ifdef NUMA_BUF
  }
#endif
}

#ifdef NUMA_BUF
void Communicator::update_remote_head_copy_(size_t rank, int which_numa){
  recv_ring_buf_t* ring_buf = simple_recv_map_[which_numa][rank];
  uint64_t new_head = ring_buf->head;
  //sync_printf(mpi_env->get_rank(), "write", new_head, "to", rank);
  uint32_t* remote_addr = (uint32_t*)(uintptr_t)remote_head_addr_[which_numa][rank];
  global_verbs->post_RDMA_write_raw(rank, which_numa, new_head, remote_addr);
}
#else
void Communicator::update_remote_head_copy_(size_t rank){
  recv_ring_buf_t* ring_buf = simple_recv_map_[rank];
  uint64_t new_head = ring_buf->head;
  //sync_printf(mpi_env->get_rank(), "write", new_head, "to", rank);
  uint32_t* remote_addr = (uint32_t*)(uintptr_t)remote_head_addr_[rank];
  global_verbs->post_RDMA_write_raw(rank, new_head, remote_addr);
}
#endif

Communicator::~Communicator(){
#ifdef NUMA_BUF
  size_t node_size = mpi_env->size;
  size_t thread_size = global_scheduler->get_size();
  for(size_t i = 0; i < node_size*thread_size; ++i){
    ASSERT( simple_msg_map_[i].msg.raw_==0, "Fatal error: destruction with active message"  );
  }
  free(simple_msg_map_);
  for(int s_id = 0; s_id < SOCKETS; ++s_id){
    for(size_t i= 0 ; i < node_size; ++i){
      free(simple_recv_map_[s_id][i]);
      free(simple_send_map_[s_id][i]);
    }
  
    global_allocators[s_id]->xfree(recv_heap_);
    global_allocators[s_id]->xfree(send_heap_);
    global_allocators[s_id]->xfree(head_copy_);
    free(tail_[s_id]);
    free(remote_head_addr_[s_id]);
  }
#else
  size_t node_size = mpi_env->size;
  size_t thread_size = global_scheduler->get_size();

  for(size_t i = 0; i < node_size*thread_size; ++i){
    ASSERT( simple_msg_map_[i].msg.raw_==0, "Fatal error: destruction with active message"  );
  }
  free(simple_msg_map_);

  for(size_t i= 0 ; i < node_size; ++i){
    free(simple_recv_map_[i]);
    free(simple_send_map_[i]);
  }

  global_allocator->xfree(recv_heap_);
  global_allocator->xfree(send_heap_);
  global_allocator->xfree(head_copy_);
  free(tail_);
  free(remote_head_addr_);
#endif
}

}//namespace Ring
