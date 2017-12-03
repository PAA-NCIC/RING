set(SRC
  sssp.h
	verify.h
	main.cc
)

add_executable(SSSP ${SRC})
target_link_libraries(SSSP Ring)
