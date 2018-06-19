set(src
  appsock_handler.c
  autoanalyze.c
  block_internal.c
  bpfunc.c
  comdb2.c
  comdb2uuid.c
  config.c
  constraints.c
  db_tunables.c
  db_util.c
  dbglog_iface.c
  dbqueue.c
  debug.c
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
  fdb_util.c
  glue.c
  handle_buf.c
  history.c
  llops.c
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
  plugin_handler.c
  prefault.c
  prefault_helper.c
  prefault_net.c
  prefault_readahead.c
  prefault_toblock.c
  printlog.c
  process_message.c
  pushlogs.c
  quantize.c
  record.c
  repl_wait.c
  reqdebug.c
  reqlog.c
  request_stats.c
  resource.c
  rmtpolicy.c
  rowlocks_bench.c
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
  sqlstat1.c
  sqlsupport.c
  tag.c
  testcompr.c
  thrman.c
  toblob.c
  toblock.c
  toproxy.c
  trans.c
  translistener.c
  trigger.c
  twophase.c
  utf8.c
  verify.c
  views.c
  views_cron.c
  views_persist.c
  watchdog.c
  ${PROJECT_BINARY_DIR}/protobuf/bpfunc.pb-c.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_dump/cdb2_dump.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_printlog/cdb2_printlog.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_printlog/comdb2_dbprintlog.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_stat/cdb2_stat.c
  ${PROJECT_SOURCE_DIR}/tools/cdb2_verify/cdb2_verify.c
)
if(WITH_SSL)
  list(APPEND src ssl_bend.c)
endif()

if(WITH_PLINK_TIMESTAMP)
  list(APPEND src ${CMAKE_CURRENT_BINARY_DIR}/plink_timestamp.c)
  set_source_files_properties(
    ${CMAKE_CURRENT_BINARY_DIR}/plink_timestamp.c
    PROPERTIES GENERATED TRUE
  )
endif()

add_executable(comdb2 ${src})
set(module uncategorized)
set(MODULE UNCATEGORIZED)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_uncategorized.h @ONLY)
set_source_files_properties(
  ${PROJECT_BINARY_DIR}/protobuf/bpfunc.pb-c.c
  PROPERTIES GENERATED TRUE
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
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
  ${PROJECT_BINARY_DIR}/plugin
  ${PROJECT_SOURCE_DIR}/protobuf
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/schemachange
  ${PROJECT_SOURCE_DIR}/sockpool
  ${PROJECT_SOURCE_DIR}/sqlite
  ${PROJECT_BINARY_DIR}/sqlite
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF_C_INCLUDE_DIR}
)
include(${PROJECT_SOURCE_DIR}/sqlite/definitions.cmake)
add_definitions(
  -DMSPACES
  ${SQLITE_FLAGS}
)
add_dependencies(comdb2 mem protobuf sqlite db bdb)

target_link_libraries(comdb2
  schemachange
  bdb
  util
  cdb2api
  comdb2rle
  crc32c
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
  tz
  m
  ${CMAKE_DL_LIBS}
  ${LZ4_LIBRARY}
  ${OPENSSL_LIBRARIES}
  ${PROTOBUF_C_LIBRARY}
  ${UNWIND_LIBRARY}
  ${UUID_LIBRARY}
  ${ZLIB_LIBRARIES}
  ${PLUGIN_LIBRARIES}
  ${COMDB2_ROBO_LINK_FLAGS}
)
configure_file(copycomdb2 copycomdb2 @ONLY)
add_custom_command(
  TARGET comdb2 POST_BUILD
  COMMAND ln -f comdb2 cdb2_dump
  COMMAND ln -f comdb2 cdb2_printlog
  COMMAND ln -f comdb2 cdb2_stat
  COMMAND ln -f comdb2 cdb2_verify
)
install(TARGETS comdb2 RUNTIME DESTINATION bin)
install(PROGRAMS
  ${CMAKE_CURRENT_BINARY_DIR}/cdb2_dump
  ${CMAKE_CURRENT_BINARY_DIR}/cdb2_printlog
  ${CMAKE_CURRENT_BINARY_DIR}/cdb2_stat
  ${CMAKE_CURRENT_BINARY_DIR}/cdb2_verify
  ${CMAKE_CURRENT_BINARY_DIR}/copycomdb2
  ${CMAKE_CURRENT_SOURCE_DIR}/comdb2dumpcsc
  DESTINATION bin
)
install(SCRIPT ${PROJECT_SOURCE_DIR}/pkg/post_install.cmake)
