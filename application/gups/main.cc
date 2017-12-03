/* main.cc
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
#include <vector>
#include <cstdio>
#include <random>

#ifdef PROFILING
#include <google/profiler.h>
#endif

#include "ring.h"

int x=0;

void GUPS(){
  using namespace Ring;
  const int64_t SZ=1<<30;
  run([]{

    auto A = gmalloc<int64_t>(SZ);
    auto B = gmalloc<int64_t>(SZ);

    pfor(B, SZ, [](int64_t* b){
      *b = random() % (SZ);
    });

    //sync_printf("hello");
    //ProfilerStart("ring.prof");
    //MLOG.on();
    auto start = time();
    pfor(B, SZ, [A](int64_t i, int64_t* b){
      int64_t x = *b;
      auto gp = A+x;
      //async
      call<async>(gp, [i](int64_t* w){
        *w ^= i; 
      });
    });//pfor
    auto end = time();
    //MLOG.off();
    //ProfilerStop();

    double _time = diff(start, end)/1000000.0;
    double gups = SZ/(_time);
    std::cout << "GUPS:" << gups << " in " <<  _time  <<  " sec." << std::endl;
    mlog_dump();
    //MLOG.dump();
  });//run
}

// run with arbitrary node
void GUPS1(){
  using namespace Ring;
  run([]{
    const int SZ=1<<20;
    auto A = (int*)malloc(SZ*sizeof(int));
    auto B = (int*)malloc(SZ*sizeof(int));
    for(int i = 0; i < SZ; ++i){
      B[i] = random() % (SZ);
    }

    auto start = time();
    for(int i = 0; i < SZ; ++i){
      A[B[i]] ^= i;
    }
    auto end = time();

    double _time = diff(start, end)/1000000.0;
    double gups = SZ/(_time);
    std::cout << "GUPS:" << gups << " in " <<  _time  <<  " sec." << std::endl;
  });//run
}


int main(int argc, char* argv[]){
  using namespace Ring;
  RING_Init(&argc, &argv);
  GUPS(); // shoule be 5e7
  RING_Finalize();
  return 0;
}
