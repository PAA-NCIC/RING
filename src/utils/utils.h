/* utils.h
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

#ifndef _UTILS_H_
#define _UTILS_H_

#include <iostream>
#include <cstdio>
#include <chrono>
#include <thread>
#include <mutex>
#include <atomic>

#include <mpi.h>
#include <unistd.h>

#ifdef NUMA_AWARE
#include <numa.h>
#endif

namespace Ring{

#define ONE                 (1ULL)
#define KILO                (1024ULL * ONE)
#define MEGA                (1024ULL * KILO)
#define GIGA                (1024ULL * MEGA)
#define TERA                (1024ULL * GIGA)
#define PETA                (1024ULL * TERA)
#define SIZE_OF_CACHE       (MEGA * 64ULL)
#define THOUSAND        (1000ULL * ONE)
#define MILLION         (1000ULL * THOUSAND)
#define BILLION         (1000ULL * MILLION)

// System Config
#define USE_RC true
//#define USE_UC true

const size_t THREAD_SIZE = std::thread::hardware_concurrency();
#ifdef NUMA_AWARE
//const size_t SOCKETS = numa_num_configured_nodes();
const size_t SOCKETS = 2;
const size_t THREAD_PER_NUMA = THREAD_SIZE / SOCKETS;
#endif

#ifdef AGGREGATION
const int MSG_SIZE = (1L<<13);
const int MAX_PACK_SIZE = 2;
#else
const int MSG_SIZE = (1L<<8);
const int MAX_PACK_SIZE = 1;
#endif
const int MSG_NUM = (1L<<12); // can't be larger than DFT_Q_DEPTH

const int PREFETCH_DIST = 64;
const int PREFETCH_TYPE = 3;
const int POOL_PREFETCH_DIST = 16;

//const double PGAS_FRACTION = 0.1; // every node use 10% memory for building PGAS
const uint64_t PGAS_MEMORY_SIZE = 5*1l<<30;
const double MEMORY_FRACTION = 0.5; // every node use 60% memory for system use.
const double COMM_FRACTION = 0.3; // used for Communicator buff, pinned by NIC for RDMA
const double POOL_FRACTION = 1-COMM_FRACTION; // used as message pool. 
extern uint64_t COMM_MEMORY_SIZE;
extern uint64_t POOL_MAX_SIZE;
extern uint64_t SHARED_MEMORY_SIZE;

const int64_t LOOP_THRESHOLD = 64;
const int64_t INIT_CORO_NUM = 256;
const int64_t MAX_ALLOWED_CORO = INIT_CORO_NUM;

#ifdef FASTCS
const int TIME_THRESHOLD = 480000;
const int TIME_LAZY_THRESHOLD = 1000000;
#else
const int TIME_THRESHOLD = 100;
const int TIME_LAZY_THRESHOLD = 200;
#endif


const int BLOCK_SIZE = 64;
const int CACHE_LINE_SIZE = 64;

struct Empty{};

#define CACHE_ALIGNED __attribute__((aligned(64)))

#define LOG(...) fprintf(stdout, __VA_ARGS__)

#define ASSERT( Predicate, Err_msg ) \
if(true){                            \
  if( !(Predicate) ){                \
    std::cerr << "CHECK failed :"    \
      << Err_msg  << " at ("         \
      << __FILE__ << ", "            \
      << __LINE__ << ")"             \
      << std::endl;                  \
		exit(1);						             \
  }                                  \
}

#define MADD(x) ((x+1) == MSG_NUM ? 0 : ((x)+1))

#define RDTSC(val) do {			                          \
    uint64_t __a,__d;					                        \
    asm volatile("rdtsc" : "=a" (__a), "=d" (__d));		\
    (val) = ((uint64_t)__a) | (((uint64_t)__d)<<32);	\
  } while(0)

static inline uint64_t rdtsc() {
  uint64_t val;
  RDTSC(val);
  return val;
}


extern std::mutex printf_mtx;
extern bool running;

template <typename T>
std::ostream &print(std::ostream& os, T t){
  return os << t << std::endl;
}

template <typename T, typename... Args>
std::ostream &print(std::ostream &os, T t, Args... rest){
  os << t << " ";
  return print(os, rest...);
}

template <typename T, typename... Args>
void sync_printf(T t, Args... rest){
  std::lock_guard<std::mutex> lk(printf_mtx);
  print(std::cout, t, rest...);
}


// from grappa, used for profiling
inline double walltime(void) {
	struct timespec tp;
	clock_gettime(CLOCK_MONOTONIC, &tp);
	return (double)tp.tv_sec + (double)tp.tv_nsec / BILLION;
}


/*used for profiling*/
inline auto time() -> decltype(std::chrono::high_resolution_clock::now()){
  return std::chrono::high_resolution_clock::now();
}

template<typename T>
inline double diff(T start, T end){
  return std::chrono::duration_cast<std::chrono::microseconds>(end-start).count();
}

}//namespace Ring
#endif
