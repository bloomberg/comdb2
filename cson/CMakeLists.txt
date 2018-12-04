set_source_files_properties(cson_amalgamation_core.c PROPERTIES COMPILE_FLAGS "-Wno-unused-variable -Wno-unused-but-set-variable -Wno-unused-label")
add_library(cson cson_amalgamation_core.c)
