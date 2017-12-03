/* completion.h
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

#ifndef _COMPLETION_H_
#define _COMPLETION_H_

#include "utils/utils.h"
#include "comm/message.h"
#include "comm/communicator.h"
#include "sched/barrier.h"
#include "comm/mpienv.h"
#include "sched/scheduler.h"

namespace Ring{

struct DoComplete {
  // oh my god, why I have to write like this?
  Barrier *** barrier;
  int target;
  int64_t dec;
    
  DoComplete(): barrier(NULL),target(0),dec(0) {}
    
  void operator()() {
    (*barrier)[target]->complete(dec);
  }
};

class Completion: public Request<DoComplete>{
public:
  int target;
  int64_t completion_to_send;

  Completion(int target = -1)
    : Request(), target(target), completion_to_send(0){}
  

  // since message has enqueued,
  // put the completion in the waiting room and wait for next one;
  inline bool is_too_late(){
    return this->is_enqueued && (!this->is_sent);
  }

  //used in the pooling 
  inline void maybe_resend(){
    //ASSERT (my_id() == this->get_source_core(), "why resend is not on source core." );
    Message::flag_reset();
    // if we have some completions in the waiting room.
    // re-send a message right now
    if( completion_to_send > 0 ){
      // set DoComplete dec;
      this->get_rpc()->dec = completion_to_send;
      completion_to_send = 0;
      // enqueue this message again;
      global_comm->enqueue(this);
    }
  }

} ;//class Completion

extern Completion ** completion_msgs;

void init_completion_msgs();

Completion * get_completion_msg(size_t owner);
void send_completion(size_t owner, int64_t dec=1);

void poolling_completion();


};//namespace Ring
#endif
