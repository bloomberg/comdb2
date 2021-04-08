include(CheckCSourceCompiles)

set(SAV_CMAKE_C_FLAGS ${CMAKE_C_FLAGS})
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=armv8-a+crc+crypto")
CHECK_C_SOURCE_COMPILES(
"#include <arm_acle.h>
int main()
{
   __crc32cb(0,0); __crc32ch(0,0); __crc32cw(0,0); __crc32cd(0,0);
   return 0;
} " HAS_CRC32_ARM)
set(CMAKE_C_FLAGS ${SAV_CMAKE_C_FLAGS})

if(NOT HAS_CRC32_ARM)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -march=armv8-a+crc")
  CHECK_C_SOURCE_COMPILES(
  "#include <arm_acle.h>
  int main()
  {
     __crc32cb(0,0); __crc32ch(0,0); __crc32cw(0,0); __crc32cd(0,0);
     return 0;
  } " HAS_CRC32_ARM_CRC)
  set(CMAKE_C_FLAGS ${SAV_CMAKE_C_FLAGS})
endif()

if(HAS_CRC32_ARM_CRC OR HAS_CRC32_ARM)
  if(${CMAKE_SYSTEM_PROCESSOR} STREQUAL aarch64)
    add_definitions(-D_HAS_CRC32_ARMV8)
  else()
    add_definitions(-D_HAS_CRC32_ARMV7)
  endif()
endif()

if(HAS_CRC32_ARM_CRC)
  set(CRC32_ARMV7_C_FLAGS "-march=armv8-a+crc")
elseif(HAS_CRC32_ARM)
  set(CRC32_ARMV7_C_FLAGS "-march=armv8-a+crc+crypto")
endif()
