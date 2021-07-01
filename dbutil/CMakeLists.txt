set(module dbutil)
set(MODULE DBUTIL)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_util.h @ONLY)
set(src
  db_checksum.c
  db_wrap.c
)
add_library(dbutil ${src})
include_directories(
  ${PROJECT_SOURCE_DIR}/crc32c
)
add_dependencies(dbutil mem)
