add_library(sockpool sockpool.c passfd.c)
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
)

if (COMDB2_BBCMAKE)
  comdb2_lib_target(sockpool)
endif()
