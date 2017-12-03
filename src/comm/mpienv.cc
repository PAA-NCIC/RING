/* mpienv.cc
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

#include "comm/mpienv.h"

namespace Ring{

mpi_env_t* mpi_env;



/* build MPI env */
void mpi_env_t::mpi_env_init(int *argc, char** argv[]){
  int is_inited = 0;
  MPI_Initialized( &is_inited );
  if(is_inited) return;
  MPI_Init(argc, argv);
  comm = MPI_COMM_WORLD;
  MPI_Comm_rank( MPI_COMM_WORLD, &rank_ );
  MPI_Comm_size( MPI_COMM_WORLD, &size_ );
  cores_ = size * THREAD_SIZE;

  // check that every node should have only one MPI process.
  MPI_Comm _tmp_comm;
  int num = 0;
  MPI_Comm_split_type(MPI_COMM_WORLD, MPI_COMM_TYPE_SHARED, 0, MPI_INFO_NULL, &_tmp_comm);
  MPI_Comm_size(_tmp_comm, &num);
  ASSERT( num==1, "Every node should have only one MPI process" );
}

void mpi_env_t::mpi_env_finalize(){
  int is_finalized = 0;
  MPI_Finalized( &is_finalized );
  if( is_finalized ) return;

  MPI_Barrier(MPI_COMM_WORLD);
  MPI_Finalize();
}

void mpi_env_t::barrier(){
  MPI_Barrier(comm);
}

mpi_env_t * get_mpi_env(){
  return mpi_env;
}

}//namespace Ring

