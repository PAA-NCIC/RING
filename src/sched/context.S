/* context.S
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

#ifdef FASTCS
#define SAVE_REGS H_SAVE_REGS
#define RESTORE_REGS H_RESTORE_REGS

#define H_SAVE_REGS \
  push %rdi; \
  push %rbp; \
  push %rbx; \
  push %r12; \
  push %r13; \
  push %r14; \
  push %r15; \
  sub $8, %rsp; \
  stmxcsr 4(%rsp); \
  fnstcw (%rsp);

#define H_RESTORE_REGS \
  fldcw (%rsp); \
  ldmxcsr 4(%rsp); \
  add $8, %rsp; \
  pop %r15; \
  pop %r14; \
  pop %r13; \
  pop %r12; \
  pop %rbx; \
  pop %rbp; \
  pop %rdi;

#define L_SAVE_REGS \
  sub $4, %rsp; \
  stmxcsr (%rsp);

#define L_RESTORE_REGS \
  ldmxcsr (%rsp); \
  add $4, %rsp;

.globl _swapstack_s
_swapstack_s:
  SAVE_REGS
  mov %rsp, (%rdi)
  mov (%rsi), %rsp
  RESTORE_REGS
  ret

.globl _makestack_s
_makestack_s: 
  //mov %rdx, %r12
  //mov %rcx, %r13

  SAVE_REGS
  mov %rsp, (%rdi)
  mov (%rsi), %rsp

  push %rdx
  push %rcx

  mov %rdi, %rax
  mov %rsi, %rdi
  mov %rax, %rsi
  call _swapstack_s
  
  //another world
  pop %rdi
  pop %r12
  call *%r12

  hlt
#endif 
