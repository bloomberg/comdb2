add_library(crc32c crc32c.c)
include_directories(${PROJECT_SOURCE_DIR}/bb)
set(CMAKE_C_FLAGS ${CMAKE_C_FLAGS} "-msse4.2 -mpclmul")
