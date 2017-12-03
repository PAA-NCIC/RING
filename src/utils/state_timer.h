/* state_timer.h
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

#ifndef _STATE_TIMER_H_
#define _STATE_TIMER_H_

#include <vector>

namespace Ring{

using TIME=double;

enum Stage{
  Undefine = -1,
  Task_worker = 0,
  Send_worker = 1,
  Recv_worker = 2,
  Polling_worker = 3,
  Peer_worker = 4
};

struct Record{
  TIME task;
  TIME send;
  TIME recv;
  TIME polling;
  TIME peer;
  inline void reset(){
    task = 0;
    send = 0;
    recv = 0;
    polling = 0;
    peer = 0;
  }
};

class Thread_Timer{
public:
  void stage_init(int thread_rank);
  void stage_start();
  void stage_stop();
  void stage_switch_to(enum Stage cur_stage);
  void stage_dump();
private:
  enum Stage last_stage_;
  TIME last_stamp_;
  
  int thread_rank_;
  int do_trace_;
  struct Record cur_rec_;
  std::vector<struct Record> history_;
};

extern Thread_Timer* ttimer;


}//namespace Ring
#endif
