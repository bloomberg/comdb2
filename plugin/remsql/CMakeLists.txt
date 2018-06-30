include(${CMAKE_MODULE_PATH}/plugin.cmake)

set(LIBNAME "remsql")

include_directories(
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/crc32c
  ${PROJECT_SOURCE_DIR}/csc2
  ${PROJECT_SOURCE_DIR}/datetime
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/dfp/decNumber
  ${PROJECT_SOURCE_DIR}/dfp/dfpal
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/lua
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_SOURCE_DIR}/net
  ${PROJECT_SOURCE_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/sqlite
  ${PROJECT_BINARY_DIR}/db
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_BINARY_DIR}/sqlite
  ${CMAKE_CURRENT_BINARY_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF_C_INCLUDE_DIR}
)

set(SOURCES remsql.c fdb_comm.c)

add_plugin(${LIBNAME} "STATIC" "${SOURCES}")

# Wait for parse.h
add_dependencies(${LIBNAME} sqlite)
