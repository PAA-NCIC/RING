/* ring.cc
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

#include "ring.h"

namespace Ring{

void RING_Init(int* argc, char** argv[]){
  /* bring up MPI environment */
  mpi_env = new mpi_env_t();
  mpi_env->mpi_env_init(argc, argv);

#ifdef NUMA_AWARE
  ASSERT( numa_available() >= 0, "System does not support Numa API.");
#endif

  config_available_memory();

  /* PGAS */
  global_mem_manager = new Vmemory(SHARED_MEMORY_SIZE);


  /* open infiniband device */
  global_verbs = new Verbs();

  /* get a memory and pin it */
#ifdef NUMA_BUF
  int64_t per_socket_size = COMM_MEMORY_SIZE/SOCKETS;
  for(int i = 0; i < SOCKETS; ++i){
    global_allocators[i] = new Allocator(i, per_socket_size, false);
    global_verbs->pin_buffer(i, (char *)global_allocators[i]->get_base(), per_socket_size);
  }
#else
  global_allocator = new Allocator( COMM_MEMORY_SIZE, false);
  global_verbs->pin_buffer((char *)global_allocator->get_base(), COMM_MEMORY_SIZE);
#endif

  /* build scheduler */
  /* add task worker */
  global_scheduler = new Scheduler( INIT_CORO_NUM );

  /* build Communicator */
  global_comm = new Communicator();
  global_barrier = new Barrier();
  size_t num = global_scheduler->get_size();
  call_inflight = (Barrier**)malloc( num*sizeof(Barrier*) );
  for(size_t i = 0; i < num; ++i) call_inflight[i] = new Barrier();

  init_completion_msgs();

  /* activate verbs */
  /*TODO: the API is really ugly */
  auto my_recv_buf = global_comm->get_my_recv_buf();

  global_verbs->activate(2, &my_recv_buf);
  /* activate communicator */
  global_exit_flag = false;
  global_comm->activate();

  running = false;
  mpi_env->barrier();
  global_scheduler->start();
//#ifdef PROFILING
  //if(mpi_env->get_rank() == 0) ProfilerStart("ring.prof");
//#endif
  //std::cout << "--init--" << std::endl;
}

void RING_Finalize(){
  if(!running) done();
  global_scheduler->await();
  //if(mpi_env->rank == 0) log_dump();
//#ifdef PROFILING
  //if(mpi_env->get_rank() == 0) ProfilerStop();
//#endif
  mpi_env->mpi_env_finalize();
}

};//namespace Ring;
