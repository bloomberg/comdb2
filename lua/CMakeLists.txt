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
  syssp.c
)

set(module lua)
set(MODULE LUA)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_lua.h @ONLY)

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
  ${PROJECT_SOURCE_DIR}/sqlite
  ${PROJECT_BINARY_DIR}/sqlite
  ${PROJECT_SOURCE_DIR}/berkdb
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF_C_INCLUDE_DIR}
)
add_dependencies(lua mem protobuf sqlite)
