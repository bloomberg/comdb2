add_library(net
  info.c
  net.c
  trace.c
)

set(module net)
set(MODULE NET)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_net.h @ONLY)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/berkdb
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF_C_INCLUDE_DIR}
)

add_dependencies(net mem)
