set(SRC
	generator/graph_generator.h
	generator/graph_generator.cc
	generator/make_graph.h
	generator/make_graph.cc
	generator/mod_arith.h
	generator/mod_arith_32bit.h
	generator/mod_arith_64bit.h
	generator/mod_arith_xmt.h
	generator/mrg_transitions.h
	generator/mrg_transitions.c
	generator/splittable_mrg.h
	generator/splittable_mrg.c
	generator/user_settings.h
	generator/utils.h
	generator/utils.cc
	bfs_local_adj.h
	graph.h
	compat.h
	common.h
	prng.h
	prng.c
	verify.h
	main.cc
)

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -ffast-math -w")
add_definitions( -DGRAPH_GENERATOR_RING -Drestrict=__restrict__)
add_executable(graph500 ${SRC})
target_link_libraries(graph500 Ring)
