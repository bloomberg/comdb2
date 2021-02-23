add_library(crc32c crc32c.c sb8.c)
include_directories(${PROJECT_SOURCE_DIR}/util)

if(${CMAKE_SYSTEM_PROCESSOR} STREQUAL x86_64)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -msse4.2 -mpclmul")
elseif(${CMAKE_SYSTEM_PROCESSOR} STREQUAL armv7l OR ${CMAKE_SYSTEM_PROCESSOR} STREQUAL aarch64)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${CRC32_ARMV7_C_FLAGS}")
endif()
