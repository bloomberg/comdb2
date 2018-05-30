add_library(tclcdb2 SHARED
  tclcdb2.c
)

set(module tclcdb2)
set(MODULE TCLCDB2)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_tcl.h @ONLY)

add_definitions(
  -DUSE_TCL_STUBS
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/cdb2api
)
add_dependencies(tclcdb2 mem)
