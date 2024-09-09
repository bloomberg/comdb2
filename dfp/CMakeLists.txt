add_library(dfpal
  decNumber/decContext.c
  decNumber/decDouble.c
  decNumber/decNumber.c
  decNumber/decPacked.c
  decNumber/decQuad.c
  decNumber/decSingle.c
  decNumber/decimal128.c
  decNumber/decimal32.c
  decNumber/decimal64.c
  dfpal/dfpal.c
)
set(module dfp_decNumber)
set(MODULE DFP_DECNUMBER)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_dfp_decNumber.h @ONLY)
add_definitions(-DDFPAL_NO_HW_DFP)
if (${CMAKE_C_COMPILER_ID} STREQUAL GNU)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-array-bounds")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-maybe-uninitialized")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-but-set-variable")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-value")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-variable")
elseif (${CMAKE_C_COMPILER_ID} STREQUAL Clang OR ${CMAKE_C_COMPILER_ID} STREQUAL AppleClang)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-variable")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-but-set-variable")
endif()
if(${CMAKE_SYSTEM_PROCESSOR} STREQUAL x86_64 OR ${CMAKE_SYSTEM_PROCESSOR} STREQUAL aarch64 OR ${CMAKE_SYSTEM_PROCESSOR} STREQUAL arm64)
    add_definitions(-DDECLITEND=1)
else()
    add_definitions(-DDECLITEND=0)
endif()
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/decNumber
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
)
add_dependencies(dfpal mem)
