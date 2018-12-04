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
if(${CMAKE_SYSTEM_NAME} STREQUAL "Linux")
    add_definitions(-DDECLITEND=1)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL "AIX")
    add_definitions(-DDECLITEND=0)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL "SunOS")
    add_definitions(-DDECLITEND=0)
endif()
set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-maybe-uninitialized -Wno-missing-braces -Wno-unused-variable -Wno-unused-but-set-variable")
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/decNumber
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
)
add_dependencies(dfpal mem)
