find_package(TclStub)

add_library(tclcdb2 SHARED tclcdb2.c)
# macOS SDK Tcl ships no arm64 slice (only arm64e), so the stub library can't
# be linked; resolve Tcl symbols from the loading interpreter instead.
if(APPLE)
  target_link_options(tclcdb2 PRIVATE -undefined dynamic_lookup)
  set(TCL_STUB_LIBRARY "")
else()
  add_definitions(-DUSE_TCL_STUBS)
endif()
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
