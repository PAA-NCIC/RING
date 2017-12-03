add_subdirectory(generator)

list(APPEND tool_list graph500-generator)

add_custom_target(tools DEPENDS ${tool_list} )
