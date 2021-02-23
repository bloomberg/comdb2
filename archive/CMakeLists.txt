set(module cdb2archive)
set(MODULE CDB2ARCHIVE)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_util.h @ONLY)
set(src
  ar_checksum.c
  ar_wrap.c
)
add_library(cdb2archive ${src})
include_directories(
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/crc32c
)
add_dependencies(cdb2archive mem crc32c)
