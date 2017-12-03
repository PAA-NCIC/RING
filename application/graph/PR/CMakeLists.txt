set(SRC
  pagerank.h
	main.cc
)

add_executable(PR ${SRC})
target_link_libraries(PR Ring)
