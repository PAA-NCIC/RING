

set( SOURCES
    utils/IO.h
    utils/memlog.h
    utils/memlog.cc
    utils/state_timer.h
    utils/state_timer.cc
    utils/timestamp.cc
    utils/timestamp.h
    utils/utils.h
    utils/utils.cc
    comm/communicator.h
    comm/communicator.cc
    comm/completion.cc
    comm/completion.h
    comm/message.h
    comm/messagepool.h
    comm/messagepool.cc
    comm/pool_allocator.cc
    comm/pool_allocator.h
    comm/mpienv.cc
    comm/mpienv.h
    comm/verbs.h
    comm/verbs.cc
    sched/barrier.h
    sched/barrier.cc
    sched/context.h
    sched/context.S
    sched/coroutine.h
    sched/coroutine.cc
    sched/scheduler.h
    sched/scheduler.cc
    sched/thread_barrier.h
    tasking/task.h
    tasking/task_queue.h
    tasking/task_queue.cc
    pgas/addressing.h
    pgas/allocator.h
    pgas/allocator.cc
    pgas/garray.h
    pgas/vmemory.h
    pgas/vmemory.cc
    pgas/parallelfor.h
    pgas/future.h
    pgas/collective.h
    pgas/rpc.h
    graph/edgelist.h
    graph/edgelist.cc
    graph/graph.h
    graph/graph.cc
    ring.h
    ring.cc)



enable_language(ASM)
set_source_files_properties( sched/context.S PROPERTIES LANGUAGE ASM COMPILE_FLAGS " -g")


# add_executable(ring ${SOURCES})
add_library(Ring STATIC ${SOURCES})
add_dependencies(Ring tools)
target_link_libraries(Ring ${RING_LD_LIBS} ${RING_STATIC_LIBS} ${EXTRA_LIBS})
