configure_file(mem_subsystems.txt mem_subsystems.txt COPYONLY)
configure_file(mem_mkstaticmspaces.awk mem_mkstaticmspaces.awk COPYONLY)
add_custom_command(
  OUTPUT mem_int.h
  COMMAND awk -f mem_mkstaticmspaces.awk mem_subsystems.txt > mem_int.h
)
add_library(mem mem.c mem_int.h)
include_directories(
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/dlmalloc
)
