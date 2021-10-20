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
  ${PROJECT_SOURCE_DIR}/crc32c
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/berkdb
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
  ${OPENSSL_INCLUDE_DIR}
)

add_dependencies(net mem)
if (COMDB2_BBCMAKE)
  configure_bb_target(net V2 NO_BUILDID NO_PLINKSTRINGS HEADER_DIRS)
endif()
