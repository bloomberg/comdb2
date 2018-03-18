add_library(bdb
  add.c
  attr.c
  bdb.c
  bdb_access.c
  bdb_blkseq.c
  bdb_llops.c
  bdb_net.c
  bdb_osqlbkfill.c
  bdb_osqlcur.c
  bdb_osqllog.c
  bdb_osqltrn.c
  bdb_schemachange.c
  bdb_thd_io.c
  bdb_verify.c
  bdblock.c
  berktest.c
  callback.c
  compact.c
  compress.c
  count.c
  cursor.c
  cursor_ll.c
  cursor_rowlocks.c
  custom_recover.c
  del.c
  error.c
  fetch.c
  file.c
  fstdump.c
  genid.c
  info.c
  lite.c
  ll.c
  llmeta.c
  llog_auto.c
  locks.c
  locktest.c
  odh.c
  os_namemangle.c
  phys.c
  queue.c
  queuedb.c
  read.c
  rep.c
  rep_qstat.c
  rowlocks.c
  rowlocks_util.c
  serializable.c
  summarize.c
  temphash.c
  temptable.c
  threads.c
  tran.c
  tranread.c
  upd.c
  util.c
)

set(module bdb)
set(MODULE BDB)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_bdb.h @ONLY)
set(BERK_DIST ${PROJECT_SOURCE_DIR}/berkdb/dist)
configure_file(${BERK_DIST}/RELEASE dist/RELEASE COPYONLY)
configure_file(${BERK_DIST}/genrec.sh dist/genrec.sh COPYONLY)
configure_file(${BERK_DIST}/geninc.sh dist/geninc.sh COPYONLY)
configure_file(${BERK_DIST}/gen_rec.awk dist/gen_rec.awk COPYONLY)
configure_file(${BERK_DIST}/gen_inc.awk dist/gen_inc.awk COPYONLY)
configure_file(${BERK_DIST}/template/rec_ctemp dist/template/rec_ctemp COPYONLY)
add_custom_command(
  OUTPUT llog_auto.c llog_auto.h llog_ext.h
  DEPENDS llog.src
  COMMAND dist/genrec.sh ${CMAKE_CURRENT_SOURCE_DIR}/llog.src llog_auto.c llog_auto.h rec_llog
  COMMAND dist/geninc.sh llog llog_ext.h llog_auto.c
)
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/sqlite
  ${PROJECT_SOURCE_DIR}/berkdb
  ${PROJECT_SOURCE_DIR}/berkdb/build
  ${PROJECT_BINARY_DIR}/berkdb
  ${PROJECT_SOURCE_DIR}/comdb2rle
  ${PROJECT_SOURCE_DIR}/crc32c
  ${PROJECT_SOURCE_DIR}/cson
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_SOURCE_DIR}/net
  ${PROJECT_BINARY_DIR}/protobuf
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF_C_INCLUDE_DIR}
)
add_definitions(-DBERKDB_4_2)
add_dependencies(bdb db mem protobuf)
