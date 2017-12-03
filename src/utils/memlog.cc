/* memlog.cc
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
#include <fstream>

#include "memlog.h"
#include "pgas/collective.h"
#include "pgas/addressing.h"


namespace Ring{
thread_local double msg_usage=0;
thread_local double msg_usage_max=0;
thread_local double msg_usage_min=2;

thread_local int64_t msg_usage_cnt=0;
thread_local int64_t context_swap_cnt=0;

thread_local int64_t blocking_call_lantency=0;
thread_local int64_t blocking_call_lantency_max=0;
thread_local int64_t blocking_call_lantency_cnt=0;

thread_local int64_t thread_comm_ticks=0;
thread_local int64_t thread_work_ticks=0;

thread_local int64_t task_created_cnt=0;
thread_local int64_t heap_task_created_cnt=0;

thread_local int64_t msg_enqueue_cnts=0;
thread_local int64_t msg_coalesce_cnts=0;
thread_local int64_t msg_invoke_send=0;


thread_local int64_t msg_enqueue_bytes=0;
thread_local int64_t msg_local_bytes=0;
thread_local int64_t msg_remote_bytes=0;
thread_local int64_t enqueue_cas_cnt=0;

thread_local int64_t verbs_rdma_batch_cnt=0;
thread_local int64_t verbs_rdma_cnt=0;

thread_local int64_t msg_1_cacheline = 0;
thread_local int64_t msg_2_cacheline = 0;
thread_local int64_t msg_3_cacheline = 0;
thread_local int64_t msg_4_cacheline = 0;
thread_local int64_t msg_x_cacheline = 0;
thread_local int64_t msg_on_heap = 0;

thread_local int64_t peer_msg_cnt=0;
thread_local int64_t useful_peer_msg_cnt=0;
thread_local int64_t flush_to_node_cnt=0;
thread_local int64_t useful_flush_to_node_cnt=0;
thread_local int64_t process_ring_buf_cnt=0;
thread_local int64_t polling_cnt=0;

thread_local int64_t blocking_call_cnt=0;
thread_local int64_t async_call_cnt=0;

thread_local int64_t sp_cnt=0;

void mlog_dump(){
  double msg_usage_ = reduce( msg_usage, collective_add );
  double msg_usage_max_ = reduce( msg_usage_max, collective_max );
  double msg_usage_min_ = reduce( msg_usage_min, collective_min );
  if(msg_usage_min_ > 1) msg_usage_min_ = 0;
  int64_t msg_usage_cnt_ = reduce(msg_usage_cnt, collective_add);

  int64_t sp_cnt_ = reduce(sp_cnt, collective_add);
  int64_t context_swap_cnt_ = reduce(context_swap_cnt, collective_add);

  int64_t blocking_call_lantency_ = reduce(blocking_call_lantency, collective_add);
  int64_t blocking_call_lantency_cnt_ = reduce(blocking_call_lantency_cnt, collective_add);
  int64_t blocking_call_lantency_max_ =  reduce(blocking_call_lantency_max, collective_max);

  int64_t thread_comm_ticks_ = reduce(thread_comm_ticks, collective_add);
  int64_t thread_work_ticks_ = reduce(thread_work_ticks, collective_add);

  int64_t task_created_cnt_ = reduce(task_created_cnt, collective_add);
  int64_t heap_task_created_cnt_ = reduce(heap_task_created_cnt, collective_add);

  int64_t msg_enqueue_bytes_ = reduce(msg_enqueue_bytes, collective_add);
  int64_t msg_local_bytes_ = reduce(msg_local_bytes, collective_add);
  int64_t msg_remote_bytes_ = reduce(msg_remote_bytes, collective_add);

  int64_t enqueue_cas_cnt_ = reduce(enqueue_cas_cnt, collective_add);

  int64_t verbs_rdma_batch_cnt_ = reduce(verbs_rdma_batch_cnt, collective_add);
  int64_t verbs_rdma_cnt_ = reduce(verbs_rdma_cnt, collective_add);

  int64_t msg_1_cacheline_ = reduce(msg_1_cacheline, collective_add);
  int64_t msg_2_cacheline_ = reduce(msg_2_cacheline, collective_add);
  int64_t msg_3_cacheline_ = reduce(msg_3_cacheline, collective_add);
  int64_t msg_4_cacheline_ = reduce(msg_4_cacheline, collective_add);
  int64_t msg_x_cacheline_ = reduce(msg_x_cacheline, collective_add);
  int64_t msg_on_heap_ = reduce(msg_on_heap, collective_add);

  int64_t peer_msg_cnt_ = reduce(peer_msg_cnt, collective_add);
  int64_t useful_peer_msg_cnt_ = reduce(useful_peer_msg_cnt, collective_add);
  int64_t flush_to_node_cnt_ = reduce(flush_to_node_cnt, collective_add);
  int64_t useful_flush_to_node_cnt_ = reduce(useful_flush_to_node_cnt, collective_add);
  int64_t process_ring_buf_cnt_ = reduce(process_ring_buf_cnt, collective_add);
  int64_t polling_cnt_ = reduce(polling_cnt, collective_add);

  int64_t blocking_call_cnt_ = reduce(blocking_call_cnt, collective_add);
  int64_t async_call_cnt_ = reduce(async_call_cnt, collective_add);

  int64_t msg_enqueue_cnts_ = reduce(msg_enqueue_cnts, collective_add);
  int64_t msg_coalesce_cnts_ = reduce(msg_coalesce_cnts, collective_add);
  int64_t msg_invoke_send_ = reduce(msg_invoke_send, collective_add);


  std::ofstream fout("profiling.txt");
  fout << "## Overview " << std::endl;
  fout << "┌ total thread communication ticks : " << thread_comm_ticks_ << " ticks" << std::endl;
  fout << "└ total thread task worker ticks : " << thread_work_ticks_ << " ticks" << std::endl;
  fout << "## Communicator behavior" << std::endl;
  fout << "─ message buffer usage (max, avg, min) : ("
    << msg_usage_max_*100 << "%, "
    << msg_usage_*100/msg_usage_cnt_ << "%, "
    << msg_usage_min_*100 << "%)" << std::endl;
  fout << "─ message enqueue CAS count : " << enqueue_cas_cnt_ << std::endl;
  fout << "─ verbs rdma batching proportion : " << ((double)verbs_rdma_batch_cnt_*100.0 / verbs_rdma_cnt_) 
    << "% (" << verbs_rdma_batch_cnt_ << "/" << verbs_rdma_cnt_ << ")" << std::endl;
  fout << "┌ average blocking call lantency : " << ((double)blocking_call_lantency_) / blocking_call_lantency_cnt_ << " ticks" << std::endl;
  fout << "└ max blocking call lantency : " << blocking_call_lantency_max_ << " ticks" << std::endl;
  fout << "┌ total enqueue message bytes : " << msg_enqueue_bytes_ << " bytes" << std::endl;
  fout << "├ total send-to-local message bytes : " << msg_local_bytes_ << " bytes" << std::endl;
  fout << "└ total send-to-remote message bytes : " << msg_remote_bytes_ << " bytes" << std::endl;
  fout << "┌ total enqueue message counts : " << msg_enqueue_cnts_ << std::endl;
  fout << "├ total mssage coalesce counts : " << msg_coalesce_cnts_ << std::endl;
  fout << "└ total enqueue invoke post_send : " << msg_invoke_send_ << std::endl;
  fout << "┌ total peer_msg_cnt : " << peer_msg_cnt_ << " ( useful : " << 100.0*useful_peer_msg_cnt_/peer_msg_cnt_ << "% )" << std::endl;
  fout << "├ total flush_to_node_cnt : " << flush_to_node_cnt_ << " ( useful : " << 100.0*useful_flush_to_node_cnt_ / flush_to_node_cnt_ << "% )" << std::endl;
  fout << "├ total process_ring_buf : " << process_ring_buf_cnt_ << std::endl;
  fout << "└ total polling_cnt :" << polling_cnt_ << std::endl;
  fout << "┌ message in 1 cacheline :" << msg_1_cacheline_  << std::endl;
  fout << "├ message in 2 cacheline :" << msg_2_cacheline_ << std::endl;
  fout << "├ message in 3 cacheline :" << msg_3_cacheline_ << std::endl;
  fout << "├ message in 4 cacheline :" << msg_4_cacheline_ << std::endl;
  fout << "├ message in x cacheline :" << msg_x_cacheline_ << std::endl;
  fout << "└ message in heap :" << msg_on_heap_ << std::endl;
  fout << "## Scheduler behavior" << std::endl;
  fout << "─ context swap count : " << context_swap_cnt_ << std::endl;
  fout << "┌ total creadted task count : " << task_created_cnt_ << std::endl;
  fout << "└ total creadted heap task count : " << heap_task_created_cnt_ << std::endl;
  fout << "┌ total blocking call count : " << blocking_call_cnt_ << std::endl;
  fout << "└ total async call count : "  <<  async_call_cnt_ << std::endl;
  fout << "## Special count" << std::endl;
  fout << "sp cnt : " << sp_cnt_ << std::endl;

  fout.close();
}

}//namespace Ring
