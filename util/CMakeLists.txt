set(module util)
set(MODULE UTIL)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_util.h @ONLY)
set(src
  averager.c
  bb_asprintf.c
  bb_daemon.c
  bb_getopt_long.c
  bb_oscompat.c
  bbhrtime.c
  cheapstub.c
  comdb2_pthread_create.c
  comdb2file.c
  compat.c
  ctrace.c
  debug_switches.c
  flibc.c
  fsnapf.c
  hostname_support.c
  int_overflow.c
  intern_strings.c
  list.c
  logmsg.c
  memdup.c
  misc.c
  nodemap.c
  object_pool.c
  parse_lsn.c
  pb_alloc.c
  perf.c
  plhash.c
  pool.c
  pooltest.c
  portmuxusr.c
  quantize.c
  queue.c
  queuetest.c
  roll_file.c
  rtcpu.c
  sbuf2.c
  schema_lk.c
  segstring.c
  stackutil.c
  sltpck.c
  ssl_glue.c
  ssl_support.c
  str0.c
  strbuf.c
  string_ref.c
  switches.c
  tcputil.c
  thdpool.c
  thread_malloc.c
  thread_util.c
  time_accounting.c
  time_epoch.c
  tohex.c
  utilmisc.c
  walkback.c
)
if(COMDB2_LEGACY_DEFAULTS)
  add_definitions(-DLEGACY_DEFAULTS)
endif()

if (${CMAKE_C_COMPILER_ID} STREQUAL GNU)
  set_source_files_properties(sltpck.c PROPERTIES COMPILE_FLAGS "-Wno-return-local-addr")
endif()

if (${CMAKE_C_COMPILER_ID} STREQUAL GNU)
  set_source_files_properties(schema_lk.c PROPERTIES COMPILE_FLAGS "-O0")
endif()

add_library(util ${src})
include_directories(
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/berkdb
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_SOURCE_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/sockpool
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF-C_INCLUDE_DIR}
  ${UNWIND_INCLUDE_DIR}
)
add_dependencies(util mem)
if (COMDB2_BBCMAKE)
  if(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
    target_link_libraries(util PUBLIC libunwind)
  endif()
  target_link_libraries(util PUBLIC openssl)
  configure_bb_target(util V2 NO_BUILDID NO_PLINKSTRINGS HEADER_DIRS)
endif()
