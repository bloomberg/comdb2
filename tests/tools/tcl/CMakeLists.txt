find_package(TclStub)

add_library(tclcdb2 SHARED tclcdb2.c)
add_definitions(-DUSE_TCL_STUBS)
add_dependencies(tclcdb2 cdb2api)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/helper.tcl helper.tcl COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/pkgIndex.tcl pkgIndex.tcl COPYONLY)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${TCL_INCLUDE_PATH}
)

target_link_libraries(tclcdb2
  cdb2api
  ${TCL_STUB_LIBRARY}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${PROTOBUF-C_LIBRARY}
  ${UNWIND_LIBRARY}
)
