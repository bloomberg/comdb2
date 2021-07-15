add_library(net
  akbuf.c
  akq.c
  sqlwriter.c
  net_evbuffer.c
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
  ${PROJECT_SOURCE_DIR}/crc32c
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/berkdb
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_SOURCE_DIR}/db
  ${LIBEVENT_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
)

add_dependencies(net mem)
if (COMDB2_BBCMAKE)
  include_directories(${LIBEVENT_INCLUDE_DIRS})
endif()
