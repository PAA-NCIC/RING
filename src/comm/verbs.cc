/* verbs.cc
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

#include <cstring>
#include <string>
#include <vector>

#include <mpi.h>

#include "comm/verbs.h"
#include "utils/utils.h"
#include "sched/scheduler.h"

namespace Ring{

Verbs* global_verbs;


#ifdef NUMA_BUF
void C_Ctl::init( int num_conn_qps, 
                  uint32_t* lkey, 
                  uint32_t* rkey, 
                  uint64_t** rm_recv_buf ){
  num_qps = num_conn_qps;
  for(int i = 0; i < SOCKETS; ++i){
    this->lkey[i] = lkey[i];
    this->rkey[i] = rkey[i];
  }
  _create_qps();
  _exchange_remote_qp_info(rm_recv_buf);
  _connect_qps();
  mpi_env->barrier();
}
#else
void C_Ctl::init( int num_conn_qps, 
                  uint32_t lkey, 
                  uint32_t rkey, 
                  uint64_t* rm_recv_buf ){
  num_qps = num_conn_qps;
  this->lkey = lkey;
  this->rkey = rkey;
  _create_qps();
  _exchange_remote_qp_info(rm_recv_buf);
  _connect_qps();
  mpi_env->barrier();
}
#endif

void C_Ctl::finalize(){
  /* clear remote qp attr info */
  if( remote_qps ){
    free( remote_qps );
    remote_qps = NULL;
  }
  /* clear conn qp and cq */
  if( cq || qp ){
    for(int i = 0; i < num_qps; ++i){
      int retval = ibv_destroy_qp( qp[i] );
      ASSERT( 0==retval, "Failed to destroy conn qp ");
    }
    free(qp);
    for(int i = 0; i < num_qps; ++i){
      int retval = ibv_destroy_cq( cq[i] );
      ASSERT( 0==retval, "Failed to destroy conn cq ");
    }
    free(cq);
  }
}

/* Create connected QPs and transit them to INIT */
void C_Ctl::_create_qps(){
  if( 0 == num_qps ) return;

  // malloc qps and cps
  qp = (struct ibv_qp **) malloc(num_qps * sizeof(struct ibv_qp *));
  cq = (struct ibv_cq **) malloc(num_qps * sizeof(struct ibv_cq *));
  ASSERT( qp!=NULL && cq!=NULL, "Failed to malloc memory for qp ans cq");

  for(int i = 0; i < num_qps; ++i){
    //create cq
    cq[i] = ibv_create_cq( context, DFT_Q_DEPTH, NULL, NULL, 0 );
    ASSERT( cq[i]!=NULL, "Failed to create cq");

#if USE_UC
    //create qp
    struct ibv_qp_init_attr init_attr;  
    memset(&init_attr, 0, sizeof(struct ibv_qp_init_attr));
    init_attr.send_cq = cq[i];
    init_attr.recv_cq = cq[i];
    init_attr.qp_type = IBV_QPT_UC;
    init_attr.cap.max_send_wr = DFT_Q_DEPTH;
    init_attr.cap.max_recv_wr = 1; // why?
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_inline_data = DFT_MAX_INLINE;

    qp[i] = ibv_create_qp(protection_domain, &init_attr);
    ASSERT( qp[i]!=NULL, "Faild to create conn qp");

    // move to INIT
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = port;
    attr.pkey_index = 0;
    attr.qp_access_flags = ( IBV_ACCESS_LOCAL_WRITE
              | IBV_ACCESS_REMOTE_WRITE);
    int retval = ibv_modify_qp( qp[i], &attr
        , IBV_QP_STATE
        | IBV_QP_PKEY_INDEX
        | IBV_QP_PORT
        | IBV_QP_ACCESS_FLAGS);
    ASSERT( 0==retval, "Failed to transit conn QP to INIT")
#endif

#if USE_RC
    //create qp
    struct ibv_qp_init_attr init_attr;  
    memset(&init_attr, 0, sizeof(struct ibv_qp_init_attr));
    //init_attr.pd = protection_domain;
    init_attr.send_cq = cq[i];
    init_attr.recv_cq = cq[i];
    init_attr.qp_type = IBV_QPT_RC;
    init_attr.cap.max_send_wr = DFT_Q_DEPTH;
    init_attr.cap.max_recv_wr = 1; 
    init_attr.cap.max_send_sge = 1;
    init_attr.cap.max_recv_sge = 1;
    init_attr.cap.max_inline_data = DFT_MAX_INLINE;

    qp[i] = ibv_create_qp(protection_domain, &init_attr);
    ASSERT( qp[i]!=NULL, "Faild to create qp");

    // move to INIT
    struct ibv_qp_attr attr;
    memset(&attr, 0, sizeof(struct ibv_qp_attr));
    attr.qp_state = IBV_QPS_INIT;
    attr.port_num = port;
    attr.pkey_index = 0;
    attr.qp_access_flags = ( IBV_ACCESS_LOCAL_WRITE
              | IBV_ACCESS_REMOTE_WRITE
              | IBV_ACCESS_REMOTE_READ
              | IBV_ACCESS_REMOTE_ATOMIC );
    int retval = ibv_modify_qp( qp[i], &attr
        , IBV_QP_STATE
        | IBV_QP_PKEY_INDEX
        | IBV_QP_PORT
        | IBV_QP_ACCESS_FLAGS);
    ASSERT( 0==retval, "Failed to transit conn QP to INIT")
#endif
  }
}

/* 
 * The following information needs to be exchanged before connect QPs
 *  - remote_rdma_buff ( UC && RC )
 *  - remote_rdma_buff_size ( UC && RC )
 *  - remote_rdma_buff_rkey ( UC && RC )
 *  - port lid
 *  - qpn
 *  - psn ( use default; UC && RC )
 *  - qkey ( use default; UD only )
 *  rm_recv_buf size should Equals mpi node size
 */
#ifdef NUMA_BUF
void C_Ctl::_exchange_remote_qp_info(uint64_t** rm_recv_buf){
#else
void C_Ctl::_exchange_remote_qp_info(uint64_t* rm_recv_buf){
#endif
  int is_inited = 0;
  MPI_Initialized( &is_inited );
  ASSERT( is_inited, "MPI env should be initialized first" );

  /* how many remote qp info should I keep */
  int _size = mpi_env->size;
  remote_qps = (struct remote_qp_attr_t *) malloc( _size * sizeof(struct remote_qp_attr_t) );

  mpi_env->barrier();
  /* exchange lid */
  uint16_t *lids = (uint16_t *) malloc( _size * sizeof(uint16_t) );
  memset( lids, 0, _size*sizeof(uint16_t) );
  MPI_Allgather( &port_attr.lid, 1, MPI_UINT16_T, &lids[0], 1, MPI_UINT16_T, mpi_env->get_comm() );
  for(int i = 0; i < _size; ++i) remote_qps[i].lid = lids[i];
  free(lids);

  /* exchange qpn */
  uint32_t *qpns = (uint32_t *) malloc( _size * sizeof(uint32_t) );
  uint32_t *my_qpns = (uint32_t *) malloc( _size * sizeof(uint32_t) );
  memset( qpns, 0, _size*sizeof(uint32_t) );
  for(int i = 0; i < _size; ++i) my_qpns[i] = qp[i]->qp_num;
  MPI_Alltoall( &my_qpns[0], 1, MPI_UINT32_T, &qpns[0], 1, MPI_UINT32_T, mpi_env->get_comm() );
  for(int i = 0; i < _size; ++i) remote_qps[i].qpn = qpns[i];
  free(qpns);
  free(my_qpns);

#ifdef NUMA_BUF
  uint64_t *rdma_buf = (uint64_t *) malloc( _size * sizeof(uint64_t) );
  uint32_t *rkeys = (uint32_t *) malloc( _size * sizeof(uint32_t) );
  for(int i = 0; i < SOCKETS; ++i){
    memset( rkeys, 0, _size*sizeof(uint32_t) );
    memset( rdma_buf, 0, _size*sizeof(uint64_t) );
    MPI_Alltoall( &(rm_recv_buf[i][0]), 1, MPI_UINT64_T, &rdma_buf[0], 1, MPI_UINT64_T, mpi_env->get_comm() );
    MPI_Allgather( &(this->rkey[i]), 1, MPI_UINT32_T, &rkeys[0], 1, MPI_UINT32_T, mpi_env->get_comm() );
    for(int j = 0; j < _size; ++j){
      remote_qps[j].rdma_buf[i] = reinterpret_cast<uintptr_t>(rdma_buf[j]);
      remote_qps[j].rkey[i] = rkeys[j];
      remote_qps[j].rdma_buf_size[i] = MSG_SIZE;
    }
  }
#else
  /* exchange rdma buffer info (conn_qp only) */
  uint64_t *rdma_buf = (uint64_t *) malloc( _size * sizeof(uint64_t) );
  uint32_t *rkeys = (uint32_t *) malloc( _size * sizeof(uint32_t) );
  memset( rdma_buf, 0, _size*sizeof(uint64_t) );
  memset( rkeys, 0, _size*sizeof(uint32_t) );
  MPI_Alltoall( &rm_recv_buf[0], 1, MPI_UINT64_T, &rdma_buf[0], 1, MPI_UINT64_T, mpi_env->get_comm() );
  MPI_Allgather( &this->rkey, 1, MPI_UINT32_T, &rkeys[0], 1, MPI_UINT32_T, mpi_env->get_comm() );
  //sync_printf("on node", mpi_env->get_rank(), "rkey is", this->rkey);
  for(int i = 0 ; i < _size; ++i){
    remote_qps[i].rdma_buf = reinterpret_cast<uintptr_t>(rdma_buf[i]);
    remote_qps[i].rdma_buf_size = MSG_SIZE;
    remote_qps[i].rkey = rkeys[i];
    //sync_printf("on node", mpi_env->get_rank(), "remote rkey of ", i, "is", remote_qps[i].rkey);
  }
#endif
  free(rdma_buf);
  free(rkeys);
  mpi_env->barrier();
}

void C_Ctl::__connect_qps(int i, struct remote_qp_attr_t *remote_qp_attr ){
  ASSERT( i>=0 && i<num_qps, "invalid arg i");
  ASSERT( qp[i] != NULL, "target qp is invalid" );

#if USE_UC
  struct ibv_qp_attr conn_attr;
  // move to RTR
  memset(&conn_attr, 0, sizeof(struct ibv_qp_attr));
  conn_attr.qp_state = IBV_QPS_RTR;
  //IBV_MTU_256 IBV_MTU_512 IBV_MTU_1024 IBV_MTU_2048
  conn_attr.path_mtu = IBV_MTU_2048;
  conn_attr.dest_qp_num = remote_qp_attr->qpn;
  conn_attr.rq_psn = DFT_PSN;

  conn_attr.ah_attr.is_global = 0;
  conn_attr.ah_attr.dlid = remote_qp_attr->lid;
  conn_attr.ah_attr.sl = 0;
  conn_attr.ah_attr.src_path_bits = 0;
  conn_attr.ah_attr.port_num = port;
  
  int retval = ibv_modify_qp( qp[i], &conn_attr
       , IBV_QP_STATE
       | IBV_QP_PATH_MTU
       | IBV_QP_DEST_QPN
       | IBV_QP_RQ_PSN
       | IBV_QP_AV );
  ASSERT(0==retval, "Failed to transit QP to RTR");

  // move to RTS
  memset(&conn_attr, 0, sizeof(struct ibv_qp_attr));
  conn_attr.qp_state = IBV_QPS_RTS;
  conn_attr.sq_psn = DFT_PSN;

  retval = ibv_modify_qp( qp[i], &conn_attr
      , IBV_QP_STATE
      | IBV_QP_SQ_PSN );
  ASSERT(0==retval, "Failed to transit QP to RTS");
#endif
#if USE_RC
  struct ibv_qp_attr conn_attr;
  // move to RTR
  memset(&conn_attr, 0, sizeof(struct ibv_qp_attr));
  conn_attr.qp_state = IBV_QPS_RTR;
  //IBV_MTU_256 IBV_MTU_512 IBV_MTU_1024 IBV_MTU_2048
  conn_attr.path_mtu = IBV_MTU_2048;
  conn_attr.dest_qp_num = remote_qp_attr->qpn;
  conn_attr.rq_psn = DFT_PSN;

  conn_attr.ah_attr.is_global = 0;
  conn_attr.ah_attr.dlid = remote_qp_attr->lid;
  conn_attr.ah_attr.sl = 0;
  conn_attr.ah_attr.src_path_bits = 0;
  conn_attr.ah_attr.port_num = port;

  conn_attr.max_dest_rd_atomic = DFT_MAX_DEST_RD_ATOMIC;
  conn_attr.min_rnr_timer = DFT_MIN_RNR_TIMER;
  
  int retval = ibv_modify_qp( qp[i], &conn_attr
       , IBV_QP_STATE
       | IBV_QP_PATH_MTU
       | IBV_QP_DEST_QPN
       | IBV_QP_RQ_PSN
       | IBV_QP_AV 
       | IBV_QP_MAX_DEST_RD_ATOMIC
       | IBV_QP_MIN_RNR_TIMER );
  ASSERT(0==retval, "Failed to transit QP to RTR");

  // move to RTS
  memset(&conn_attr, 0, sizeof(struct ibv_qp_attr));
  conn_attr.qp_state = IBV_QPS_RTS;
  conn_attr.sq_psn = DFT_PSN;
  conn_attr.timeout = DFT_TIMEOUT;
  conn_attr.retry_cnt = DFT_RETRY_CNT;
  conn_attr.rnr_retry = DFT_RNR_RETRY;
  conn_attr.max_rd_atomic = DFT_MAX_RD_ATOMIC;

  retval = ibv_modify_qp( qp[i], &conn_attr
      , IBV_QP_STATE
      | IBV_QP_SQ_PSN 
      | IBV_QP_TIMEOUT
      | IBV_QP_RETRY_CNT
      | IBV_QP_RNR_RETRY
      | IBV_QP_MAX_QP_RD_ATOMIC);
  ASSERT(0==retval, "Failed to transit QP to RTS");
#endif
}

void C_Ctl::_connect_qps(){
  for(int i = 0; i < mpi_env->size; ++i){
    if(i != mpi_env->rank) 
      __connect_qps(i, &remote_qps[i]);
  }
}
#ifdef NUMA_BUF
void C_Ctl::post_RDMA_write_single(int rank, int which_numa, uint8_t* msg, int size, int offset, uint32_t lkey){
  struct ibv_send_wr wr, *bad_send_wr;
  struct ibv_sge sgl;

  memset(&sgl, 0, sizeof(sgl));
  memset(&wr, 0, sizeof(wr));

  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &sgl;
  // use singnaled to flush send buf?
  wr.send_flags = IBV_SEND_SIGNALED;

  sgl.addr = reinterpret_cast<uint64_t>(msg);
  sgl.length = MSG_SIZE;
  sgl.lkey = lkey;

  /* remote rdma bufer should be monopolized by this node */
  wr.wr.rdma.remote_addr = remote_qps[rank].rdma_buf[which_numa] + offset*MSG_SIZE;
  wr.wr.rdma.rkey = remote_qps[rank].rkey[which_numa];

  int retval = ibv_post_send( qp[rank], &wr, &bad_send_wr );
  ASSERT( 0==retval, "Failed to post a single RDMA WRITE wr" );
}

bool C_Ctl::post_RDMA_write_batch(int rank, int which_numa, const std::vector<struct RDMA_pack_t>& pack, uint32_t lkey){
  struct ibv_send_wr wr[MAX_PACK_SIZE];
  struct ibv_sge sgl[MAX_PACK_SIZE];
  struct ibv_send_wr* bad_send_wr;

  for(size_t i = 0; i < pack.size(); ++i){
    wr[i].opcode = IBV_WR_RDMA_WRITE;
    wr[i].num_sge = 1;
    wr[i].next = (( i+1 == pack.size() ) ? NULL : &wr[i+1]);
    wr[i].sg_list = &sgl[i];

    // singaled to flush send buf
    wr[i].send_flags = IBV_SEND_SIGNALED;

    sgl[i].addr = pack[i].addr;
    sgl[i].length = pack[i].size;
    sgl[i].lkey = lkey;

    wr[i].wr.rdma.remote_addr = remote_qps[rank].rdma_buf[which_numa] + pack[i].offset*MSG_SIZE;
    //sync_printf("I am node", mpi_env->get_rank(), "I will write to ", remote_qps[rank].rdma_buf + pack[i].offset*MSG_SIZE);
    //sync_printf("I am node", mpi_env->get_rank(), "my pack size is", pack.size());
    wr[i].wr.rdma.rkey = remote_qps[rank].rkey[which_numa];
  }
  /* if DFT_QP_DEPTH is too small, it will throw a error */
  int retval = ibv_post_send( qp[rank], &wr[0], &bad_send_wr );
  //sync_printf("rdma post :",rank,which_numa,remote_qps[rank].rdma_buf[which_numa]);
  //ASSERT( 0==retval, "Failed to post a batch RDMA WRITE wr" );
  if(retval == 0) return true;
  else return false;
}

bool C_Ctl::post_RDMA_write_raw(int rank, int which_numa, uint64_t inline_data, 
                                uint32_t* target_addr, uint32_t lkey){
  struct ibv_send_wr wr, *bad_send_wr;
  struct ibv_sge sgl;

  memset(&sgl, 0, sizeof(sgl));
  memset(&wr, 0, sizeof(wr));

  sgl.addr = (uint64_t)&inline_data; // inline data need not to be registered
  sgl.length = 4;
  sgl.lkey = 666; // won't be checked in inlne data mode

  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &sgl;
  // don't singaled, no need to flush send buf
  wr.send_flags = IBV_SEND_INLINE;


  // remote rdma bufer should be monopolized by this node
  wr.wr.rdma.remote_addr = (uint64_t)(uintptr_t)target_addr;
  wr.wr.rdma.rkey = remote_qps[rank].rkey[which_numa];

  // inline can be reuse after this end
  int retval = ibv_post_send( qp[rank], &wr, &bad_send_wr );
  //ASSERT( 0==retval, "Failed to post a raw inline RDMA WRITE wr" );
  if(retval == 0 ) return true;
  else return false;
}


#else
void C_Ctl::post_RDMA_write_single(int rank, uint8_t* msg, int size, int offset, uint32_t lkey){
  struct ibv_send_wr wr, *bad_send_wr;
  struct ibv_sge sgl;

  memset(&sgl, 0, sizeof(sgl));
  memset(&wr, 0, sizeof(wr));

  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &sgl;
  // use singnaled to flush send buf?
  wr.send_flags = IBV_SEND_SIGNALED;

  sgl.addr = reinterpret_cast<uint64_t>(msg);
  sgl.length = MSG_SIZE;
  sgl.lkey = lkey;

  /* remote rdma bufer should be monopolized by this node */
  wr.wr.rdma.remote_addr = remote_qps[rank].rdma_buf + offset*MSG_SIZE;
  wr.wr.rdma.rkey = remote_qps[rank].rkey;

  int retval = ibv_post_send( qp[rank], &wr, &bad_send_wr );
  ASSERT( 0==retval, "Failed to post a single RDMA WRITE wr" );
}

bool C_Ctl::post_RDMA_write_batch(int rank, const std::vector<struct RDMA_pack_t>& pack, uint32_t lkey){
  //struct ibv_send_wr* wr = (struct ibv_send_wr *) malloc ( pack.size() * sizeof(struct ibv_send_wr) );
  struct ibv_send_wr wr[MAX_PACK_SIZE];
  //struct ibv_sge* sgl = (struct ibv_sge *) malloc ( pack.size() * sizeof(struct ibv_sge) );
  struct ibv_sge sgl[MAX_PACK_SIZE];
  struct ibv_send_wr* bad_send_wr;

  for(size_t i = 0; i < pack.size(); ++i){
    wr[i].opcode = IBV_WR_RDMA_WRITE;
    wr[i].num_sge = 1;
    wr[i].next = (( i+1 == pack.size() ) ? NULL : &wr[i+1]);
    wr[i].sg_list = &sgl[i];

    // singaled to flush send buf
    wr[i].send_flags = IBV_SEND_SIGNALED;

    sgl[i].addr = pack[i].addr;
    sgl[i].length = pack[i].size;
    sgl[i].lkey = lkey;

    wr[i].wr.rdma.remote_addr = remote_qps[rank].rdma_buf + pack[i].offset*MSG_SIZE;
    //sync_printf("I am node", mpi_env->get_rank(), "I will write to ", remote_qps[rank].rdma_buf + pack[i].offset*MSG_SIZE);
    //sync_printf("I am node", mpi_env->get_rank(), "my pack size is", pack.size());
    wr[i].wr.rdma.rkey = remote_qps[rank].rkey;
  }
  /* if DFT_QP_DEPTH is too small, it will throw a error */
  int retval = ibv_post_send( qp[rank], &wr[0], &bad_send_wr );
  //ASSERT( 0==retval, "Failed to post a batch RDMA WRITE wr" );
  //free(wr);
  //free(sgl);
  if(retval == 0) return true;
  else return false;
}

bool C_Ctl::post_RDMA_write_raw(int rank, uint64_t inline_data, 
                                uint32_t* target_addr, uint32_t lkey){
  struct ibv_send_wr wr, *bad_send_wr;
  struct ibv_sge sgl;

  memset(&sgl, 0, sizeof(sgl));
  memset(&wr, 0, sizeof(wr));

  sgl.addr = (uint64_t)&inline_data; // inline data need not to be registered
  sgl.length = 4;
  sgl.lkey = 666; // won't be checked in inlne data mode

  wr.opcode = IBV_WR_RDMA_WRITE;
  wr.num_sge = 1;
  wr.next = NULL;
  wr.sg_list = &sgl;
  // don't singaled, no need to flush send buf
  wr.send_flags = IBV_SEND_INLINE;


  // remote rdma bufer should be monopolized by this node
  wr.wr.rdma.remote_addr = (uint64_t)(uintptr_t)target_addr;
  wr.wr.rdma.rkey = remote_qps[rank].rkey;

  // inline can be reuse after this end
  int retval = ibv_post_send( qp[rank], &wr, &bad_send_wr );
  //ASSERT( 0==retval, "Failed to post a raw inline RDMA WRITE wr" );
  if(retval == 0 ) return true;
  else return false;
}

#endif

bool C_Ctl::poll_cq(int rank, int num){
  //int comps = 0;
  struct ibv_wc wc;
  struct ibv_cq *t_cq = cq[rank];
  //while(comps < num){
  int new_comps = ibv_poll_cq(t_cq, 1, &wc);
  //if(node_rank() == 0 && thread_rank() == 1) sync_printf("here");
  if( new_comps !=0 ){
    ASSERT( new_comps>0, "Fatal error, poll failed");
    //printf("%s\n", ibv_wc_status_str(wc.status));
    ASSERT( wc.status==IBV_WC_SUCCESS, "Fatal error, work not complete" );
    //printf("%d Yes", mpi_env->get_rank());
    //comps ++;
    return true;
  }else{
    return false;
  }
  //}
}


Verbs::Verbs(){
  init_device();
  ASSERT(context!=NULL, "please open device first");
  ASSERT(protection_domain!=NULL, "please open device first");
  c_ctl = NULL;
}

/* 
 * conn qp can only connect to one qp
 *  - if there are M nodes and T Thread, we will need M*T qps per node.
 * one dgram qp can communicate with many qp
 *  - to fufill the NIC, maybe we need more than one dgram qp per node.
 */
#ifdef NUMA_BUF
void Verbs::activate( int c_ctl_size, 
                      uint64_t*** rm_recv_buf ){
  int num_conn_qps = mpi_env->size;
  this->c_ctl_size = c_ctl_size;

  if( c_ctl_size ) c_ctl = (C_Ctl**)malloc( c_ctl_size * sizeof(C_Ctl*) );
  for( int i = 0; i < c_ctl_size; ++i) {
    c_ctl[i] = new C_Ctl( context, protection_domain, port, port_attr );
    uint32_t _lkey[SOCKETS], _rkey[SOCKETS];
    for(int j = 0; j < SOCKETS; ++j){
      _lkey[j] = this->pinned_buffer[j]->lkey;
      _rkey[j] = this->pinned_buffer[j]->rkey;
    }
    c_ctl[i]->init( num_conn_qps, _lkey, _rkey, *rm_recv_buf );
  }
}

#else
void Verbs::activate( int c_ctl_size, 
                      uint64_t** rm_recv_buf ){
  int num_conn_qps = mpi_env->size;
  this->c_ctl_size = c_ctl_size;
  if( c_ctl_size ) c_ctl = (C_Ctl**)malloc( c_ctl_size * sizeof(C_Ctl*) );
  for( int i = 0; i < c_ctl_size; ++i) {
    c_ctl[i] = new C_Ctl( context, protection_domain, port, port_attr );
    c_ctl[i]->init( num_conn_qps, this->pinned_buffer->lkey, this->pinned_buffer->rkey, *rm_recv_buf );
  }
}
#endif

/*
 * Get target device
 * Get device attribute
 * Open context
 * Allocate protection domain
 */
void Verbs::init_device( const char* target_device_name, uint8_t target_port){

  // 1. get device list
  int num_devices = 0;
  ibv_device ** devices = ibv_get_device_list( &num_devices );
  ASSERT( devices!=NULL, "no Verbs devices found!");

  // 2. get target device
  device = NULL;
  if(num_devices == 1){
    // find only one device.
    device = devices[0];
    device_name = ibv_get_device_name( device );
    device_guid = ibv_get_device_guid( device );
  }else{
    // find target device.
    for(int i = 0; i < num_devices; ++i) {
      if( 0 == strcmp(target_device_name, ibv_get_device_name(devices[i])) ) {
        device = devices[i];
        device_name = ibv_get_device_name( device );
        device_guid = ibv_get_device_guid( device );
      }
    }
  }

  ASSERT( device!=NULL, "Failed to find target device" );

  // 3. open context
  context = ibv_open_device( device );
  ASSERT( context!=NULL, "Failed to open target device context" );

  // 4. get device attr
  int retval = ibv_query_device( context, &device_attr );
  ASSERT( 0==retval, "Failed to get device attributes" );

  // 5. get port attr
  port = target_port;
  retval = ibv_query_port( context, port, &port_attr);
  ASSERT( 0==retval, "Failed to get port attributes" );

  // 6. create protection domain
  protection_domain = ibv_alloc_pd( context ); 
  ASSERT( protection_domain!=NULL, "Failed to alloc protection domain")
}

/* Destroy all resource */
void Verbs::finalize(){

  /* finalize qp control blocks */
  if( c_ctl ) for(int i = 0; i < c_ctl_size; ++i) {
    c_ctl[i]->finalize();
  }

  /* clean protection domain */
  if( protection_domain ){
    int retval = ibv_dealloc_pd( protection_domain );
    ASSERT( 0==retval, "Failed destroy protectiondomain ");
    protection_domain = NULL;
  }

  /* clean context */
  if( context ){
    int retval = ibv_close_device( context );
    ASSERT( 0==retval, "Failed to destroy context " );
    context = NULL;
  }
  /* clear device ptr */
  device = NULL;
}

#ifdef NUMA_BUF
void Verbs::pin_buffer(int which_numa, char * buf, int64_t size ){ 
  pinned_buffer[which_numa] = ibv_reg_mr( protection_domain, buf, size
      , IBV_ACCESS_LOCAL_WRITE
      | IBV_ACCESS_REMOTE_READ
      | IBV_ACCESS_REMOTE_WRITE
      | IBV_ACCESS_REMOTE_ATOMIC );
}
#else
void Verbs::pin_buffer( char * buf, int64_t size ){ 
  pinned_buffer = ibv_reg_mr( protection_domain, buf, size
      , IBV_ACCESS_LOCAL_WRITE
      | IBV_ACCESS_REMOTE_READ
      | IBV_ACCESS_REMOTE_WRITE
      | IBV_ACCESS_REMOTE_ATOMIC );
}
#endif

#ifdef NUMA_BUF
void Verbs::post_RDMA_write_single(int rank, int which_numa, uint8_t* msg, int size, int offset){
  c_ctl[which_numa]->post_RDMA_write_single(rank, which_numa, msg, size, offset, pinned_buffer[which_numa]->lkey);
}

void Verbs::post_RDMA_write_batch(int rank, int which_numa, std::vector<struct RDMA_pack_t> pack){
  bool retval = false;
  retval = c_ctl[which_numa]->post_RDMA_write_batch(rank, which_numa, pack, pinned_buffer[which_numa]->lkey);
  ASSERT( retval, "Failed to post a batch RDMA WRITE wr" );
}

void Verbs::post_RDMA_write_raw(int rank, int which_numa, uint32_t inline_data, uint32_t* target_addr){
  bool retval = false;
  retval = c_ctl[which_numa]->post_RDMA_write_raw(rank, which_numa, inline_data, target_addr, pinned_buffer[which_numa]->lkey);
  ASSERT( retval, "Failed to post a raw inline RDMA WRITE wr" );
}

void Verbs::poll_cq(int rank, int which_numa, int num){
  int comps = 0;
  while(comps < num){
    bool retval = c_ctl[which_numa]->poll_cq(rank, num);
    if(retval) comps++;
  }
}


#else

void Verbs::post_RDMA_write_single(int rank, uint8_t* msg, int size, int offset){
  c_ctl[0]->post_RDMA_write_single(rank, msg, size, offset, pinned_buffer->lkey);
}

void Verbs::post_RDMA_write_batch(int rank, std::vector<struct RDMA_pack_t> pack){
  bool retval = false;
  for(int j = 0; j < c_ctl_size; ++j){
    int i = (rank + j) & (c_ctl_size-1);
    retval = c_ctl[i]->post_RDMA_write_batch(rank, pack, pinned_buffer->lkey);
    if(retval) break;
  }
  ASSERT( retval, "Failed to post a batch RDMA WRITE wr" );
}
void Verbs::post_RDMA_write_raw(int rank, uint32_t inline_data, uint32_t* target_addr){
  bool retval = false;
  for(int j = 0; j < c_ctl_size; j++){
    int i = (rank + j) & (c_ctl_size-1);
    retval = c_ctl[i]->post_RDMA_write_raw(rank, inline_data, target_addr, pinned_buffer->lkey);
    if(retval) break;
  }
  ASSERT( retval, "Failed to post a raw inline RDMA WRITE wr" );
}

void Verbs::poll_cq(int rank, int num){
  int comps = 0;
  while(comps < num){
    for(int i = 0; i < c_ctl_size; ++i){
      bool retval = c_ctl[i]->poll_cq(rank, num);
      if(retval) comps++;
    }
  }
}


#endif



Verbs* get_verb(){
  return global_verbs;
}


}//namespace Ring
