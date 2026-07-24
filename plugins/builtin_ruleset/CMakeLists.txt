include(${PROJECT_SOURCE_DIR}/cmake/plugin.cmake)

include_directories(
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/crc32c
  ${PROJECT_SOURCE_DIR}/csc2
  ${PROJECT_SOURCE_DIR}/datetime
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/dfp/decNumber
  ${PROJECT_SOURCE_DIR}/dfp/dfpal
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_SOURCE_DIR}/net
  ${PROJECT_SOURCE_DIR}/sqlite/src
  ${PROJECT_SOURCE_DIR}/schemachange
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/db
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_BINARY_DIR}/protobuf
  ${CMAKE_CURRENT_BINARY_DIR}
  ${OPENSSL_INCLUDE_DIR}
)

file (READ "system.ruleset" RULESET)
string(REGEX REPLACE "(\")" "\\\\\\1" RULESET ${RULESET})
string(REPLACE "\n" "\\n" RULESET ${RULESET})
set(RULESET "\"${RULESET}\"")

configure_file(ruleset.h.in ruleset.h @ONLY)

set(SOURCES
    builtin_ruleset.c
)

add_plugin(builtin_ruleset STATIC "${SOURCES}")
if (COMDB2_BBCMAKE)
  comdb2_lib_target(builtin_ruleset)
endif()

add_dependencies(builtin_ruleset util)
