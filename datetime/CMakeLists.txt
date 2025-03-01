add_library(tz localtimedb.c asctime.c difftime.c)
set(module datetime)
set(MODULE DATETIME)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_datetime.h @ONLY)
add_definitions(-DSTD_INSPIRED)
if (${CMAKE_C_COMPILER_ID} STREQUAL GNU)
  set_source_files_properties(localtimedb.c PROPERTIES COMPILE_FLAGS "-Wno-unused-variable -Wno-unused-but-set-variable -Wno-unused-label -Wno-strict-overflow")
elseif(${CMAKE_C_COMPILER_ID} STREQUAL Clang)
  set_source_files_properties(localtimedb.c PROPERTIES COMPILE_FLAGS "-Wno-unused-variable -Wno-unused-const-variable -Wno-unused-label -Wno-strict-overflow")
endif()
include_directories(
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
)
add_dependencies(tz mem)
if (COMDB2_BBCMAKE)
  comdb2_lib_target(tz)
endif()
