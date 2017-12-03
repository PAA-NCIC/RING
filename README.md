## Intro

RING is a **~6k lines** pure C++11 runtime system for scaling irregular applications in PGAS programming paradigm.


## Dependences

You must have a 64-bit Linux system with the following installed to build RING.

- Build
    - CMake >= 2.8.12
- Compiler
    - GCC > 4.8
- External:
    - Infiniband Verbs
    - MPI
    - pthread
- optional
    - libnuma
    - gperftool

## Quick Start


Like Grappa, Ring is primarly designed to be a "global view" model, whcih means that rather than coordinating where all parallel SPMD processes are at and how they devide up the data, the programmer is encourage to think of the system **as a large single shared memory**.


### Section 1 : Hello world

You can use `run()` to capture the whole process you want to run. Use `all_do()` to spawn the same task on all cores just like what you do in SPMD. Use `call()` to perform a remote process call.

`void run([]{ /*your codes*/ });`    
`void all_do([]{ /*your works*/ });`    

`void call(GlobalAddress<T> gt, [](T* t){} );`    
`auto f = call(GlobalAddress<T> gt, [](T* t){} );`   
`T result = f.get() // this will block explicitly just like C++11 future`  

`void call(int which_core, [](){} );`    
`auto f = call(int which_core, [](){} );`  
`T result = f.get() // this will block explicitly just like C++11 future`  


```c++
#include <iostream>
#include <vector>
#include <cstdio>
#include "ring.hpp"

int x;

int main(int argc, char* argv[]){
  RING_Init(&argc, &argv);
  run([]{
    all_do([]{
      x=0;
      if(thread_rank() == 0){
        auto gx = make_global(0, 1, &x);
        call(gx, [](int *x){
           sync_printf("hello world", (*x));
        });//call
      }
    });//all_do
  });//run
  RING_Finalize();
  return 0;
}
```

### Section 2 : Global memory

In Ring, all memory on all cores is addressable by any other core in spirit of **PGAS** programming paradigm.

You can allocate a global array by `gmalloc()`, free it by `gfree()`.

`auto A = gmalloc(size_t size);//only can be called inner run()`  
`free(A);`   

```c++
run([]{
  auto x = gmalloc<int>(1<<20);
  auto y = gmalloc<int>(1<<20);
  auto x1 = x + 230;
  auto y1 = y + 333;
  call(x1, [](int * w){
    sync_printf("hello pgas x", id());
  });

  call(y1, [](int * w){
    sync_printf("hello pgas y", id());
  });
  /* free request will be executed after rpc is executed. */
  gfree(x);
  gfree(y);
});

```

### Section 3 : parallel for

Instead of spawning tasks individually, it's almost always better to use a parallel loops of some sort. you can use `pfor()` to spwan loop iterations recursively untill hitting a threshold.

`void pfor(Array<T> A, int s, int e, [](T* t){})`  
`void pfor(Array<T> A, int s, int e, [](i, T* t){})`  

```c++
run([]{
  auto A = gmalloc<int>(1<<20);
  pfor(A, 0, 1024, [](int* t){
    auto a = id();
    call(1, [a]{
      sync_printf(id());
    });//call
  });//pfor
  gfree(std::move(A));
});//run
```
