set(module archive)
set(MODULE ARCHIVE)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_util.h @ONLY)
set(src
  ar_checksum.c
  ar_wrap.c
)
add_library(archive ${src})
include_directories(
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/crc32c
)
add_dependencies(archive mem crc32c)
