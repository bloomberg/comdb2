add_custom_command(OUTPUT syssp.c
                   COMMAND ${CMAKE_COMMAND} -DCURRENT_SOURCE_DIR=${CMAKE_CURRENT_SOURCE_DIR} -DCURRENT_BUILD_DIR=${CMAKE_CURRENT_BINARY_DIR} -P ${CMAKE_CURRENT_SOURCE_DIR}/configure_syssp_c.cmake.in
                   DEPENDS lib/
                   COMMENT "Configuring lua/syssp.c")

add_library(lua
  lapi.c
  lauxlib.c
  lbaselib.c
  lcode.c
  ldblib.c
  ldebug.c
  ldo.c
  ldump.c
  lfunc.c
  lgc.c
  linit.c
  liolib.c
  llex.c
  lmathlib.c
  lmem.c
  loadlib.c
  lobject.c
  lopcodes.c
  loslib.c
  lparser.c
  lstate.c
  lstring.c
  lstrlib.c
  ltable.c
  ltablib.c
  ltm.c
  ltypes.c
  luautil.c
  lundump.c
  lvm.c
  lzio.c
  sp.c
  ${CMAKE_BINARY_DIR}/lua/syssp.c
)

set(module lua)
set(MODULE LUA)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_lua.h @ONLY)

file(READ ${CMAKE_CURRENT_SOURCE_DIR}/trigger_main.lua trigger_main)
string(REPLACE "\n" "\\n\\\n" trigger_main ${trigger_main})
configure_file(trigger_main.h.in trigger_main.h @ONLY)

include(${PROJECT_SOURCE_DIR}/sqlite/definitions.cmake)
add_definitions(
  -DLUASP
  -DLUA_USE_POSIX
  -DLUA_USE_DLOPEN
  ${SQLITE_FLAGS}
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/csc2
  ${PROJECT_SOURCE_DIR}/cson
  ${PROJECT_SOURCE_DIR}/datetime
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_BINARY_DIR}/db
  ${PROJECT_SOURCE_DIR}/dfp/decNumber
  ${PROJECT_SOURCE_DIR}/dfp/dfpal
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_SOURCE_DIR}/net
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/sqlite/src
  ${PROJECT_SOURCE_DIR}/sqlite/ext/misc
  ${PROJECT_BINARY_DIR}/sqlite
  ${PROJECT_SOURCE_DIR}/berkdb
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF-C_INCLUDE_DIR}
if(WITH_RDKAFKA)
  ${RDKAFKA_INCLUDE_DIR}
endif()
)
if (COMDB2_BBCMAKE)
  target_link_libraries(lua PUBLIC protobuf-c openssl)
  comdb2_bb_target(lua)
endif()
add_dependencies(lua mem proto sqlite)
