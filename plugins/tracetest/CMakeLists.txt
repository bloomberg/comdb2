include(${PROJECT_SOURCE_DIR}/cmake/plugin.cmake)

include_directories(
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_BINARY_DIR}/bbinc
  ${CMAKE_CURRENT_BINARY_DIR}
)

add_plugin(tracetest STATIC tracetest.c)
