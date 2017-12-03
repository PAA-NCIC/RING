/* memlog.h
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
#ifndef _MEM_LOG_H_
#define _MEM_LOG_H_

#include <iostream>
#include <atomic>

#include <mpi.h>

#include "utils/utils.h"

namespace Ring{

// 1. message usage
extern thread_local double msg_usage;
extern thread_local int64_t msg_usage_cnt;
extern thread_local double msg_usage_max;
extern thread_local double msg_usage_min;

// 2.context swap count
extern thread_local int64_t context_swap_cnt;

// 3. blocking call lantency
extern thread_local int64_t blocking_call_lantency;
extern thread_local int64_t blocking_call_lantency_max;
extern thread_local int64_t blocking_call_lantency_cnt;
extern thread_local int64_t blocking_call_cnt;
extern thread_local int64_t async_call_cnt;

// 4. thread cpu usage
extern thread_local int64_t thread_comm_ticks;
extern thread_local int64_t thread_work_ticks;

// 5. tasking
extern thread_local int64_t task_created_cnt;
extern thread_local int64_t heap_task_created_cnt;

// optimization
extern thread_local int64_t msg_enqueue_cnts;
extern thread_local int64_t msg_coalesce_cnts;
extern thread_local int64_t msg_invoke_send;

// 6. message proportion
extern thread_local int64_t msg_enqueue_bytes;
extern thread_local int64_t msg_local_bytes;
extern thread_local int64_t msg_remote_bytes;

extern thread_local int64_t verbs_rdma_batch_cnt;
extern thread_local int64_t verbs_rdma_cnt;

extern thread_local int64_t msg_1_cacheline;
extern thread_local int64_t msg_2_cacheline;
extern thread_local int64_t msg_3_cacheline;
extern thread_local int64_t msg_4_cacheline;
extern thread_local int64_t msg_x_cacheline;
extern thread_local int64_t msg_on_heap;

// 7. Communicator Behavior
extern thread_local int64_t peer_msg_cnt;
extern thread_local int64_t useful_peer_msg_cnt;
extern thread_local int64_t flush_to_node_cnt;
extern thread_local int64_t useful_flush_to_node_cnt;
extern thread_local int64_t process_ring_buf_cnt;
extern thread_local int64_t polling_cnt;

extern thread_local int64_t enqueue_cas_cnt;

// Special count
extern thread_local int64_t sp_cnt;

void mlog_dump();

};

#endif
