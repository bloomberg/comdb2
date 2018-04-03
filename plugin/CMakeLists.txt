# Include all the sub-directories under ${PROJECT_SOURCE_DIR}/plugin.

# -DCOMDB2_PLUGIN_TYPE=(preferred|static|dynamic)
# preferred : Use the type specified in plugins's CMakeLists.txt
#             to build individual plugin libraries
# static    : Build all plugins into static libraries
# dynamic   : Build all plugins into dynamic libraries
set(COMDB2_PLUGIN_TYPE "preferred" CACHE STRING
  "Plugin type (preferred, static, dynamic)")

# Base directory for all plugin sources.
set(PLUGIN_BASE_DIR "${PROJECT_SOURCE_DIR}/plugin")

# Clear the list of static plugin libraries from cache.
unset(PLUGIN_LIBRARIES CACHE)

# List all plugin directories
file(GLOB files RELATIVE ${PLUGIN_BASE_DIR} ${PLUGIN_BASE_DIR}/*)

foreach(file ${files})
  if((IS_DIRECTORY ${PLUGIN_BASE_DIR}/${file}) AND
      (NOT (${file} STREQUAL "CMakeFiles")))
    add_subdirectory(${file})
  endif()
endforeach()

# Generate the all_static plugins.h header file. It contains the necessary
# code to list & install all the statically linked plugins.
set(PLUGIN_INCLUDE_FILE "${PROJECT_BINARY_DIR}/plugin/all_static_plugins.h")

file(WRITE ${PLUGIN_INCLUDE_FILE}
  "/* WARNING: auto-generated file, do not edit. */\n"
  "#ifndef _INCLUDED_ALL_STATIC_PLUGINS_H\n"
  "#define _INCLUDED_ALL_STATIC_PLUGINS_H\n")

foreach(PLUGIN ${PLUGIN_LIBRARIES})
  file(APPEND ${PLUGIN_INCLUDE_FILE}
    "extern struct comdb2_plugin comdb2_plugin_${PLUGIN};\n")
endforeach(PLUGIN)

file(APPEND ${PLUGIN_INCLUDE_FILE}
  "struct comdb2_plugin *all_static_plugins[] = {\n")

foreach(PLUGIN ${PLUGIN_LIBRARIES})
  file(APPEND ${PLUGIN_INCLUDE_FILE}
    "&comdb2_plugin_${PLUGIN},\n")
endforeach(PLUGIN)

file(APPEND ${PLUGIN_INCLUDE_FILE} "};\n")
file(APPEND ${PLUGIN_INCLUDE_FILE} "#endif\n")
