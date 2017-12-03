set(SRC
  cc.h
	verify.h
	main.cc
)

add_executable(CC ${SRC})
target_link_libraries(CC Ring)
