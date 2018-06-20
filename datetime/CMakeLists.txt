add_library(tz localtimedb.c asctime.c difftime.c)
set(module datetime)
set(MODULE DATETIME)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_datetime.h @ONLY)
add_definitions(-DSTD_INSPIRED)
include_directories(
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
)
add_dependencies(tz mem)
