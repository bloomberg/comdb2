# Include all the sub-directories under ${PROJECT_SOURCE_DIR}/plugins.

# -DCOMDB2_PLUGIN_TYPE=(preferred|static|dynamic)
# preferred : Use the type specified in plugins's CMakeLists.txt
#             to build individual plugin libraries
# static    : Build all plugins into static libraries
# dynamic   : Build all plugins into dynamic libraries
set(COMDB2_PLUGIN_TYPE "preferred" CACHE STRING
  "Plugin type (preferred, static, dynamic)")

# Base directory for all plugin sources.
set(PLUGIN_BASE_DIR "${PROJECT_SOURCE_DIR}/plugins")

# Clear the list of static plugin libraries from cache.
unset(PLUGIN_LIBRARIES CACHE)

set(module plugins)
set(MODULE PLUGINS)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_plugins.h @ONLY)

add_subdirectory(dbqueuedb)
add_subdirectory(logdelete)
add_subdirectory(newsql)
add_subdirectory(remsql)
add_subdirectory(repopnewlrl)
add_subdirectory(reversesql)
add_subdirectory(simpleauth)
add_subdirectory(sockbplog)

set(COMDB2_EXTRA_PLUGINS ${EXTRA_PLUGINS} CACHE PATH "Path to additional plugins")
if (COMDB2_EXTRA_PLUGINS)
  include(${COMDB2_EXTRA_PLUGINS}/CMakeLists.txt)
endif()

# Generate the all_static plugins.h header file. It contains the necessary
# code to list & install all the statically linked plugins.
set(PLUGIN_INCLUDE_FILE "${PROJECT_BINARY_DIR}/plugins/all_static_plugins.h")

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
