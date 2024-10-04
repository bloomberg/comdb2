set(src
  alias.c
  api_history.c
  appsock_handler.c
  autoanalyze.c
  block_internal.c
  bpfunc.c
  clienthost.c
  comdb2.c
  comdb2uuid.c
  comdb2_ruleset.c
  config.c
  constraints.c
  db_access.c
  db_fingerprint.c
  db_metrics.c
  db_query_plan.c
  db_sample_queries.c
  db_tunables.c
  db_util.c
  dbglog_iface.c
  dbqueuedb.c
  debug.c
  disttxn.c
  endian.c
  envstubs.c
  errstat.c
  eventlog.c
  fdb_access.c
  fdb_bend.c
  fdb_bend_sql.c
  fdb_boots.c
  fdb_fend.c
  fdb_fend_cache.c
  fdb_push.c
  glue.c
  handle_buf.c
  history.c
  import.c
  indices.c
  localrep.c
  lrucache.c
  marshal.c
  memdebug.c
  osql_srs.c
  osqlblkseq.c
  osqlblockproc.c
  osqlcheckboard.c
  osqlcomm.c
  osqlrepository.c
  osqlsession.c
  osqlshadtbl.c
  osqlsqlthr.c
  osqlsqlnet.c
  osqlsqlsocket.c
  phys_rep.c
  plugin_handler.c
  prefault.c
  prefault_helper.c
  prefault_net.c
  prefault_readahead.c
  prefault_toblock.c
  printlog.c
  process_message.c
  pushlogs.c
  record.c
  repl_wait.c
  reqdebug.c
  reqlog.c
  request_stats.c
  resource.c
  reverse_conn.c
  rmtpolicy.c
  sigutil.c
  sltdbt.c
  socket_interfaces.c
  sqlanalyze.c
  sqlexplain.c
  sqlglue.c
  sqlinterfaces.c
  sqllog.c
  sqlmaster.c
  sqloffload.c
  sqlpool.c
  sqlstat1.c
  sql_stmt_cache.c
  ssl_bend.c
  tag.c
  testcompr.c
  thrman.c
  timers.c
  toblob.c
  toblock.c
  toproxy.c
  trans.c
  translistener.c
  trigger.c
  truncate_log.c
  typessql.c
  utf8.c
  verify.c
  views.c
  cron.c
  logical_cron.c
  views_persist.c
  watchdog.c
  shard_range.c
  dohsql.c
  dohast.c
  machclass.c
  osqluprec.c
  macc_glue.c
  ${PROJECT_BINARY_DIR}/protobuf/bpfunc.pb-c.c
  ${PROJECT_BINARY_DIR}/protobuf/importdata.pb-c.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_dump/cdb2_dump.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_load/cdb2_load.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_printlog/cdb2_printlog.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_printlog/comdb2_dbprintlog.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_stat/cdb2_stat.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_verify/cdb2_verify.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_pgdump/cdb2_pgdump.c
)

option(DEBUG_TYPES "Build types.c independent of sqlglue.c" OFF)
if(DEBUG_TYPES)
  list(APPEND src types.c)
  add_definitions(-DDEBUG_TYPES)
endif()

if(COMDB2_LEGACY_DEFAULTS)
  add_definitions(-DLEGACY_DEFAULTS)
endif()

IF (NOT DEFINED COMDB2_GIT_VERSION_SHA AND EXISTS ${PROJECT_SOURCE_DIR}/.git)
  FIND_PACKAGE(Git)
  IF(GIT_FOUND)
    EXECUTE_PROCESS(
      COMMAND ${GIT_EXECUTABLE} rev-parse --short HEAD
      WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
      OUTPUT_VARIABLE "COMDB2_GIT_VERSION_SHA_NEW"
      ERROR_QUIET
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    EXECUTE_PROCESS(
      COMMAND ${GIT_EXECUTABLE} status --untracked-files=no --short --no-column
      WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}"
      OUTPUT_VARIABLE "COMDB2_GIT_LOCAL_CHANGES"
      ERROR_QUIET
      OUTPUT_STRIP_TRAILING_WHITESPACE)
    IF(NOT ${COMDB2_GIT_LOCAL_CHANGES} STREQUAL "")
      SET(COMDB2_GIT_VERSION_SHA_NEW "${COMDB2_GIT_VERSION_SHA_NEW} with local changes")
    ENDIF()
  ELSE()
    SET(COMDB2_GIT_VERSION_SHA_NEW 0)
  ENDIF()
  IF(NOT DEFINED COMDB2_GIT_VERSION_SHA OR NOT COMDB2_GIT_VERSION_SHA STREQUAL COMDB2_GIT_VERSION_SHA_NEW )
    SET(COMDB2_GIT_VERSION_SHA "${COMDB2_GIT_VERSION_SHA_NEW}")
  ENDIF()
ENDIF()

set_property(SOURCE comdb2.c PROPERTY COMPILE_DEFINITIONS
  COMDB2_GIT_VERSION_SHA=${COMDB2_GIT_VERSION_SHA}
  COMDB2_BUILD_VERSION=${COMDB2_BUILD_VERSION}
  COMDB2_CODENAME=${COMDB2_CODENAME}
  COMDB2_SEMVER=${COMDB2_SEMVER}
  COMDB2_BUILD_TYPE=${CMAKE_BUILD_TYPE}
)
if(${CMAKE_C_COMPILER_ID} STREQUAL Clang OR ${CMAKE_C_COMPILER_ID} STREQUAL AppleClang)
  set_source_files_properties(
    ${PROJECT_SOURCE_DIR}/tools/cdb2_dump/cdb2_dump.c
    ${PROJECT_SOURCE_DIR}/tools/cdb2_load/cdb2_load.c
    ${PROJECT_SOURCE_DIR}/tools/cdb2_stat/cdb2_stat.c
    ${PROJECT_SOURCE_DIR}/tools/cdb2_pgdump/cdb2_pgdump.c
    ${PROJECT_SOURCE_DIR}/tools/cdb2_printlog/cdb2_printlog.c
    ${PROJECT_SOURCE_DIR}/tools/cdb2_verify/cdb2_verify.c
    PROPERTIES COMPILE_FLAGS "-Wno-deprecated-non-prototype"
  )
endif()
set_source_files_properties(${PROJECT_BINARY_DIR}/protobuf/bpfunc.pb-c.c PROPERTIES GENERATED TRUE)

set(module uncategorized)
set(MODULE UNCATEGORIZED)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_uncategorized.h @ONLY)
set_source_files_properties(
  ${PROJECT_BINARY_DIR}/protobuf/importdata.pb-c.c
  ${PROJECT_BINARY_DIR}/protobuf/bpfunc.pb-c.c
  PROPERTIES GENERATED TRUE
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_BINARY_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/berkdb
  ${PROJECT_BINARY_DIR}/berkdb
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/comdb2rle
  ${PROJECT_SOURCE_DIR}/crc32c
  ${PROJECT_SOURCE_DIR}/csc2
  ${PROJECT_SOURCE_DIR}/cson
  ${PROJECT_SOURCE_DIR}/datetime
  ${PROJECT_SOURCE_DIR}/dfp/decNumber
  ${PROJECT_SOURCE_DIR}/dfp/dfpal
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/lua
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_SOURCE_DIR}/net
  ${PROJECT_BINARY_DIR}/plugins
  ${PROJECT_SOURCE_DIR}/protobuf
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/schemachange
  ${PROJECT_SOURCE_DIR}/sockpool
  ${PROJECT_SOURCE_DIR}/sqlite/ext/expert
  ${PROJECT_SOURCE_DIR}/sqlite/ext/misc
  ${PROJECT_SOURCE_DIR}/sqlite/src
  ${PROJECT_BINARY_DIR}/sqlite
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF-C_INCLUDE_DIR}
  ${PROJECT_SOURCE_DIR}/plugins/reversesql
)
include(${PROJECT_SOURCE_DIR}/sqlite/definitions.cmake)
add_definitions(
  -DMSPACES
  ${SQLITE_FLAGS}
)
add_executable(comdb2 ${src})
add_dependencies(comdb2 mem proto sqlite db bdb)
target_link_libraries(comdb2 PRIVATE
  ${PLUGIN_LIBRARIES}
  schemachange
  bdb
  util
  crc32c
  comdb2rle
  csc2
  cson
  db
  dfpal
  lua
  mem
  dlmalloc
  net
  sockpool
  sqlite
  util
  tz
  ${LibArchive_LIBRARIES}
  ${LZ4_LIBRARY}
  ${RDKAFKA_LIBRARY}
  ${OPENSSL_LIBRARIES}
  ${PROTOBUF-C_LIBRARY}
  ${UNWIND_LIBRARY}
  ${UUID_LIBRARY}
  ${ZLIB_LIBRARIES}
  ${LIBEVENT_LIBRARIES}
)

if (COMDB2_BBCMAKE)
  target_link_libraries(comdb2 PRIVATE opencdb2api uuid event_core_static event_pthreads_static)
  comdb2_bb_target(comdb2)
else()
  target_link_libraries(comdb2 PRIVATE cdb2api m ${CMAKE_DL_LIBS})
endif()

configure_file(copycomdb2 copycomdb2 @ONLY)

install(TARGETS comdb2 RUNTIME DESTINATION bin)
foreach(tool dump load printlog stat verify pgdump)
  add_custom_command(
    TARGET comdb2 POST_BUILD
    COMMAND ln -f comdb2 cdb2_${tool}
  )
  install(CODE
    "execute_process(COMMAND ln -f
      \$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/comdb2
      \$ENV{DESTDIR}${CMAKE_INSTALL_PREFIX}/bin/cdb2_${tool})"
    COMPONENT comdb2
  )
endforeach()

install(PROGRAMS
  ${CMAKE_CURRENT_BINARY_DIR}/copycomdb2
  ${CMAKE_CURRENT_SOURCE_DIR}/comdb2dumpcsc
  DESTINATION bin
)

install(SCRIPT ${PROJECT_SOURCE_DIR}/pkg/post_install.cmake)
