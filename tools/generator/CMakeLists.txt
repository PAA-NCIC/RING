set(GENERATOR_SRC
	graph_generator.h
	graph_generator.cc
	make_graph.h
	make_graph.cc
	mod_arith.h
	mod_arith_32bit.h
	mod_arith_64bit.h
	mod_arith_xmt.h
	mrg_transitions.h
	mrg_transitions.c
	splittable_mrg.h
	splittable_mrg.c
	user_settings.h
	utils.h
	utils.cc
)

add_library(graph500-generator STATIC ${GENERATOR_SRC})
set_target_properties(graph500-generator PROPERTIES
  COMPILE_FLAGS " -Drestrict=__restrict__ -w -ffast-math ${CMAKE_CXX_FLAGS}"
  FOLDER "tools"
)



