include(${PROJECT_SOURCE_DIR}/cmake/plugin.cmake)
include(${PROJECT_SOURCE_DIR}/sqlite/definitions.cmake)
add_definitions(${SQLITE_FLAGS})

include_directories(
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
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
  ${PROJECT_SOURCE_DIR}/sqlite/src
  ${PROJECT_SOURCE_DIR}/berkdb
  ${PROJECT_BINARY_DIR}/db
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_BINARY_DIR}/sqlite
  ${PROJECT_BINARY_DIR}/plugins
  ${CMAKE_CURRENT_BINARY_DIR}
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF-C_INCLUDE_DIR}
)

set(SIMPLEAUTH_SRCS simpleauth.c simplecheck.c)
add_plugin(simpleauth STATIC "${SIMPLEAUTH_SRCS}")
add_dependencies(simpleauth sqlite) # Wait for parse.h
add_dependencies(simpleauth proto)
