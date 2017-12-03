set(SRC
	bfs.h
	verify.h
	main.cc
)

add_executable(BFS ${SRC})
target_link_libraries(BFS Ring)
