add_library(tclcdb2 SHARED
  tclcdb2.c
)

set(module tclcdb2)
set(MODULE TCLCDB2)
configure_file(${PROJECT_SOURCE_DIR}/tcl/helper.tcl helper.tcl COPYONLY)
configure_file(${PROJECT_SOURCE_DIR}/tcl/pkgIndex.tcl pkgIndex.tcl COPYONLY)

add_definitions(
  -DUSE_TCL_STUBS
)

find_path(TCL_INCLUDE_DIR
  NAMES tcl.h
  HINTS ${TCL_ROOT_DIR}
)
find_library(TCL_LIBRARY
  NAMES tcl8.6 tcl8.5
  HINTS ${TCL_ROOT_DIR}
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(TCL DEFAULT_MSG TCL_INCLUDE_DIR)
find_package_handle_standard_args(libtcl DEFAULT_MSG TCL_LIBRARY)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${TCL_INCLUDE_DIR}
)
add_dependencies(tclcdb2 cdb2api)
target_link_libraries(tclcdb2
  cdb2api
  ${TCL_LIBRARY}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${PROTOBUF-C_LIBRARY}
  ${UNWIND_LIBRARY}
)
