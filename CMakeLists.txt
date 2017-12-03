cmake_minimum_required(VERSION 2.8.12)

# Ring
project(Ring)

#############################################################################
# Options
#############################################################################

option(LOGGING "enable memory logging." OFF)
option(PROFILING "enable Sample profile with GPerftools." OFF)
option(FAST_CONTEXT_SWITCH "enable fast context switch feature." ON)
option(PREFETCH "enable gcc builtin prefetch." ON)
option(NUMA_AWARE "enable numa aware pgas." ON)
option(NUMA_BUF "enable numa aware RDMA buf" ON)
option(COALESCE "enable coalesce" ON)
option(AGGREGATION "enable aggregation" ON)


#############################################################################
# Environment Check
#############################################################################

# Check GCC version
if("${CMAKE_CXX_COMPILER_ID}" STREQUAL "GNU")
  if (CMAKE_CXX_COMPILER_VERSION VERSION_LESS 4.8.0)
    message(FATAL_ERROR "When using GCC, the version must be at least 4.8.0. Found ${CMAKE_CXX_COMPILER_VERSION} instead.")
  endif()
else()
  message(WARNING "You are using an unsupported compiler! only tested with GCC.")
endif()

#set (_LIB_PATH $ENV{LD_LIBRARY_PATH})
set (_LIB_PATH "/home/mengke/runtime/gperf/lib/")
set (_INCLUDE_PATH "/home/mengke/runtime/gperf/include/")


#############################################################################
# Configure
#############################################################################

# configure c++ flags
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -std=c++11 -Wall -g -Wno-sign-compare -Wno-unused-variable")
set(EXTRA_LIBS "-libverbs")

# configure MPI
find_package(MPI REQUIRED)
message("MPI_CXX_INCLUDE_PATH: ${MPI_CXX_INCLUDE_PATH}")
message("MPI_CXX_COMPILE_FLAGS: ${MPI_CXX_COMPILE_FLAGS}")
message("MPI_CXX_LINK_FLAGS: ${MPI_CXX_LINK_FLAGS}")
message("MPI_CXX_LIBRARIES: ${MPI_CXX_LIBRARIES}")
include_directories(${MPI_INCLUDE_PATH})
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${MPI_CXX_COMPILE_FLAGS}")
set(LINK_FLAGS "${LINK_FLAGS} ${MPI_CXX_LINK_FLAGS}")
list(APPEND RING_LD_LIBS ${MPI_CXX_LIBRARIES})

# configure pthread
find_library(PTHREADS_FOUND pthread REQUIRED)
list(APPEND RING_LD_LIBS ${PTHREADS_FOUND})

# configure graph500_generator
list(APPEND RING_STATIC_LIBS ${CMAKE_BINARY_DIR}/tools/generator/libgraph500-generator.a)


if(LOGGING)
  message("-- enable logging ")
  add_definitions( -DLOGGING )
endif()

if(PREFETCH)
  message("-- enable prefetching ")
  add_definitions( -DPREFETCH )
endif()

if(NUMA_AWARE)
  message("-- enable numa aware PGAS.")
  find_library(NUMA_FOUND numa REQUIRED)
  list(APPEND RING_LD_LIBS ${NUMA_FOUND})
  add_definitions( -DNUMA_AWARE )
  if(NUMA_BUF)
    message("-- enable numa aware buf.")
    add_definitions( -DNUMA_BUF )
  endif()
endif()

if(COALESCE)
  message("-- enable coalesce.")
  add_definitions( -DCOALESCE )
endif()

if(AGGREGATION)
  message("-- enable aggregation.")
  add_definitions( -DAGGREGATION )
endif()


# configure fast_context_switch
if(FAST_CONTEXT_SWITCH)
  message("-- enable Fast context switch.")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS}  -mno-red-zone -fno-inline-functions  ")
  add_definitions( -DFASTCS )
endif()

# configure gperftools
if(PROFILING)
  message("-- Gperftools enabled.")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fno-omit-frame-pointer")
  add_definitions( -DPROFILING )
  find_library(PROFILER_LIB NAMES libprofiler.a PATHS ${_LIB_PATH} REQUIRED)
  include_directories(${_INCLUDE_PATH})
  message("-- ${PROFILER_LIB}")
  if(PROFILER_LIB)
    list(APPEND RING_LD_LIBS ${PROFILER_LIB})
  else()
    message(FATAL_ERROR "Gperftools not found!")
  endif()
endif()

message("-- RING_LD_LIBS: ${RING_LD_LIBS}")
message("-- RING_STATIC_LIBS: ${RING_STATIC_LIBS}")
message("-- CMAKE_CXX_FLAGS: ${CMAKE_CXX_FLAGS}")

#############################################################################
# subdirs
#############################################################################
include_directories(src)
include_directories(src/pgas)
include_directories(src/comm)
include_directories(src/utils)
include_directories(src/sched)
include_directories(tools)
include_directories(tools/generator)
add_subdirectory(src)
add_subdirectory(tools)
add_subdirectory(application)
