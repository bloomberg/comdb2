set(src
  cdb2api.c
  ${PROJECT_SOURCE_DIR}/bb/sbuf2.c
  ${PROJECT_BINARY_DIR}/protobuf/sqlquery.pb-c.c
  ${PROJECT_BINARY_DIR}/protobuf/sqlresponse.pb-c.c
)

if(WITH_CDB2OPEN_STACK)
    list(APPEND src ${PROJECT_SOURCE_DIR}/bb/walkback.c)
    set_source_files_properties(${PROJECT_SOURCE_DIR}/bb/walkback.c PROPERTIES COMPILE_FLAGS -DUSE_UNWIND)
endif()
if(WITH_SSL)
  list(APPEND src ${PROJECT_SOURCE_DIR}/bb/ssl_support.c)
endif()
set_source_files_properties(
  ${PROJECT_BINARY_DIR}/protobuf/sqlquery.pb-c.c
  ${PROJECT_BINARY_DIR}/protobuf/sqlresponse.pb-c.c
  PROPERTIES GENERATED TRUE
)
include_directories(
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROTOBUF_C_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
)
# common obj files for .so/.dylib and .a
add_definitions(-DSBUF2_SERVER=0)
add_library(objlib OBJECT ${src})
set_property(TARGET objlib PROPERTY POSITION_INDEPENDENT_CODE 1)
add_dependencies(objlib protobuf)
# the two libraries
add_library(cdb2api STATIC $<TARGET_OBJECTS:objlib>)
add_library(cdb2api_shared SHARED $<TARGET_OBJECTS:objlib>)
set_target_properties(cdb2api_shared PROPERTIES OUTPUT_NAME cdb2api)
# required for generating .dylib on macOS
if(${CMAKE_C_COMPILER_ID} STREQUAL AppleClang)
  set(CMAKE_SHARED_LIBRARY_CREATE_C_FLAGS "${CMAKE_SHARED_LIBRARY_CREATE_C_FLAGS} -undefined dynamic_lookup")
endif()
configure_file(cdb2api.pc cdb2api.pc @ONLY)
install(TARGETS cdb2api ARCHIVE DESTINATION lib)
install(TARGETS cdb2api_shared LIBRARY DESTINATION lib)
install(FILES cdb2api.h DESTINATION include)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/cdb2api.pc
  DESTINATION lib/pkgconfig
)
