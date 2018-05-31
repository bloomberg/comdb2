add_library(tclcdb2 SHARED
  tclcdb2.c
)

set(module tclcdb2)
set(MODULE TCLCDB2)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_tcl.h @ONLY)
configure_file(${PROJECT_SOURCE_DIR}/tcl/pkgIndex.tcl pkgIndex.tcl COPYONLY)

add_definitions(
  -DUSE_TCL_STUBS
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/cdb2api
)
add_dependencies(tclcdb2 mem cdb2api)
target_link_libraries(tclcdb2
  ${PROJECT_SOURCE_DIR}/build/cdb2api/libcdb2api.a
  ${OPENSSL_LIBRARIES}
  ${PROTOBUF_C_LIBRARY}
  ${UNWIND_LIBRARY}
)
