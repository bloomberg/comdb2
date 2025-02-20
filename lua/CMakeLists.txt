SET(SYSSP_LIST
  # Name -> lua script file path
  #
  # Hint: Make sure that the stored procedure name is not longer that 31
  # characters (MAX_SPNAME-1 (31)), i.e. not be longer that the below
  # line. If it is, you'll get a "bad procedure name" error.
  #-------------------------------|
  "sys.physrep.get_reverse_hosts   -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_get_reverse_hosts.lua"
  "sys.physrep.get_revhosts_v2     -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_get_revhosts_v2.lua"
  "sys.physrep.keepalive           -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_keepalive.lua"
  "sys.physrep.keepalive_v2        -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_keepalive_v2.lua"
  "sys.physrep.register_replicant  -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_register_replicant.lua"
  "sys.physrep.reset_nodes         -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_reset_nodes.lua"
  "sys.physrep.should_wait_for_con -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_should_wait_for_con.lua"
  "sys.physrep.shouldwait_v2       -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_shouldwait_v2.lua"
  "sys.physrep.topology            -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_topology.lua"
  "sys.physrep.update_registry     -> ${CMAKE_CURRENT_SOURCE_DIR}/lib/physrep_update_registry.lua"
)

foreach(SYSSP IN LISTS SYSSP_LIST)
  if (SYSSP MATCHES "^(.*)->(.*)$")
    string(STRIP ${CMAKE_MATCH_1} SYSSP_NAME)
    string(STRIP ${CMAKE_MATCH_2} SYSSP_PATH)

    file(READ ${SYSSP_PATH} FILE_CONTENT)

    string(REGEX REPLACE "(\")" "\\\\\\1" FILE_CONTENT ${FILE_CONTENT})
    string(REPLACE "\n" "\\n" FILE_CONTENT ${FILE_CONTENT})

    set(CMAKE_SYSSP_LIST ${CMAKE_SYSSP_LIST}{\"${SYSSP_NAME}\",\"${FILE_CONTENT}\",\"${SYSSP_NAME}\"},\n)
  else()
    message(FATAL_ERROR "Invalid system stored procedure entry: ${SYSSP}")
  endif()
endforeach()

configure_file(syssp.c.in syssp.c @ONLY)

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
  ${CMAKE_CURRENT_BINARY_DIR}/syssp.c
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
