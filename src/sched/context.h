/* context.h
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

#ifndef _CONTEXT_H_
#define _CONTEXT_H_

#ifndef FASTCS
#include <ucontext.h>
#endif

#ifdef FASTCS
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>

namespace Ring{


struct stack_t{
  void * ss_sp;
  size_t ss_size;
  int ss_flags;
  stack_t() : ss_sp(NULL), ss_size(0), ss_flags(0) {}
};

//40
struct ucontext_t{
  struct stack_t uc_stack;
  struct ucontext_t* uc_link;
  void* rsp;
  ucontext_t() : uc_stack(), uc_link(NULL), rsp(NULL) {}
};



static inline int setcontext(const ucontext_t *ucp){
  //TODO(mengke): not implemented
  return -1;
}

#define getcontext(ucp) \
  asm("mov %%rsp, %0" : "=r"( (ucp)->rsp )::);

void _swapstack(void** osp, void** sp)
  asm ("_swapstack_s");

static inline void swapcontext_inline(void **orsp, void** nrsp){
  asm volatile("":::"memory");
  _swapstack( orsp, nrsp );
}

inline void swapcontext(ucontext_t* oucp, ucontext_t* ucp){
  swapcontext_inline( &(oucp->rsp), &(ucp->rsp));
}

void _makestack(void ** father_sp, void** child_sp, void(*func)(void*), void* arg) 
  asm ("_makestack_s");

static inline void makecontext(ucontext_t* ucp, void(*func)(void*), int placeholder, void* arg){
  ucp->rsp = (void*)( (char*)ucp->uc_stack.ss_sp + ucp->uc_stack.ss_size);
  _makestack( &(ucp->uc_link->rsp), &(ucp->rsp), func, arg);
}

};//namespace Ring

#endif

#endif
