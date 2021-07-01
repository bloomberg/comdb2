if (${CMAKE_C_COMPILER_ID} STREQUAL GNU)
  set_source_files_properties(cson_amalgamation_core.c PROPERTIES COMPILE_FLAGS "-Wno-unused-variable -Wno-unused-but-set-variable -Wno-unused-label")
elseif(${CMAKE_C_COMPILER_ID} STREQUAL Clang)
  set_source_files_properties(cson_amalgamation_core.c PROPERTIES COMPILE_FLAGS "-Wno-unused-variable -Wno-unused-const-variable -Wno-unused-label")
endif()

set(module cson)
set(MODULE CSON)

configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_cson.h @ONLY)

add_library(cson cson_amalgamation_core.c)

include_directories(
    build
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CURRENT_BINARY_DIR}
    ${PROJECT_SOURCE_DIR}/mem
    ${PROJECT_BINARY_DIR}/mem
)

add_dependencies(cson mem)
