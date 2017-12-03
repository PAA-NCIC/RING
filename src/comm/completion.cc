/* completion.cc
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

#include "comm/completion.h"

namespace Ring{

Completion ** completion_msgs;

void init_completion_msgs(){
  size_t from_node = mpi_env->rank;
  size_t cores = mpi_env->cores;

  completion_msgs = (Completion**)malloc( cores * THREAD_SIZE * sizeof(Completion*) );

  for(size_t i = 0; i < THREAD_SIZE; ++i){
    for(size_t j = 0; j < cores; ++j){

      auto x = new Completion(j);
      // you know, this pointer on all the node must be the same.
      x->get_rpc()->barrier = &call_inflight;
      x->get_rpc()->target = j%THREAD_SIZE;
      x->route( from_node, i, j/THREAD_SIZE, j%THREAD_SIZE );

      completion_msgs[ i*cores + j ] = x;
    }
  }
}

Completion* get_completion_msg(size_t owner){
  return completion_msgs[ thread_rank() * mpi_env->cores + owner ];
}

#ifdef COALESCE
void send_completion(size_t owner, int64_t dec){
  if(owner == my_id()){
    call_inflight[thread_rank()]->complete(dec);
  }else{
    Completion * cm = get_completion_msg(owner);
    if(cm->is_too_late()){
      cm->completion_to_send +=dec;
      msg_coalesce_cnts++;
    }else{
      // no one has issued a Completion message
      // so we will do this
      cm->completion_to_send +=dec;
      cm->maybe_resend();
    }
  }
}
#else
void send_completion(size_t owner, int64_t dec){
  if(owner == my_id()){
    call_inflight[thread_rank()]->complete(dec);
  }else{
    int tr = owner%THREAD_SIZE;
    if(dec == 1){
      send_request(owner,[tr]{
        call_inflight[tr]->complete(1);
      });
    }else{
      send_request(owner, [tr,dec]{
        call_inflight[tr]->complete(dec);
      });
    }
  }
}
#endif

#ifdef COALESCE
void poolling_completion(){
  size_t cores = mpi_env->cores;
  for(size_t i = 0; i < cores; ++i){
    Completion * cm = get_completion_msg(i);
    if( !cm->is_too_late() ){
      cm->maybe_resend();
    }
  }
}
#else
void poolling_completion(){
  // do nothing
}
#endif

};//namespace Ring
