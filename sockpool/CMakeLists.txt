add_library(sockpool sockpool.c passfd.c)
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/bb
  ${PROJECT_SOURCE_DIR}/bbinc
)
