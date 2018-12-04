add_library(dlmalloc dlmalloc.c)
set_source_files_properties(dlmalloc.c PROPERTIES COMPILE_FLAGS "-Wno-unused-value -Wno-unused-variable -Wno-unused-but-set-variable -Wno-unused-label")
include_directories(${PROJECT_SOURCE_DIR}/util)
