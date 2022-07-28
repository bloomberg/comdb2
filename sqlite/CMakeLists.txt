add_library(sqlite
  keywordhash.h
  memcompare.c
  opcodes.c
  parse.c
  serialget.c
  vdbeaux.c
  vdbecompare.c
  ext/comdb2/activelocks.c
  ext/comdb2/activeosqls.c
  ext/comdb2/appsock_handlers.c
  ext/comdb2/blkseq.c
  ext/comdb2/clientstats.c
  ext/comdb2/cluster.c
  ext/comdb2/columns.c
  ext/comdb2/connections.c 
  ext/comdb2/constraints.c
  ext/comdb2/crons.c
  ext/comdb2/ezsystables.c
  ext/comdb2/fingerprints.c
  ext/comdb2/functions.c
  ext/comdb2/indexuse.c
  ext/comdb2/keycomponents.c
  ext/comdb2/keys.c
  ext/comdb2/keywords.c
  ext/comdb2/limits.c
  ext/comdb2/logicalops.c
  ext/comdb2/memstats.c
  ext/comdb2/metrics.c
  ext/comdb2/netuserfunc.c
  ext/comdb2/opcode_handlers.c
  ext/comdb2/partial_datacopies.c
  ext/comdb2/permissions.c
  ext/comdb2/plugins.c
  ext/comdb2/procedures.c
  ext/comdb2/query_plans.c
  ext/comdb2/queues.c
  ext/comdb2/repl_stats.c
  ext/comdb2/repnetqueue.c
  ext/comdb2/schistory.c
  ext/comdb2/scstatus.c
  ext/comdb2/sqlclientstats.c
  ext/comdb2/sqlpoolqueue.c
  ext/comdb2/stacks.c
  ext/comdb2/prepared.c
  ext/comdb2/systables.c
  ext/comdb2/table_properties.c
  ext/comdb2/tables.c
  ext/comdb2/tablesizes.c
  ext/comdb2/temporary_file_sizes.c
  ext/comdb2/threadpools.c
  ext/comdb2/timepartitions.c
  ext/comdb2/timeseries.c
  ext/comdb2/trancommit.c
  ext/comdb2/tranlog.c
  ext/comdb2/transactionstate.c
  ext/comdb2/triggers.c
  ext/comdb2/tunables.c
  ext/comdb2/typesamples.c
  ext/comdb2/users.c
  ext/comdb2/views.c
  ext/comdb2/fdb.c
  ext/misc/carray.c
  ext/misc/completion.c
  ext/misc/regexp.c
  ext/misc/json1.c
  ext/expert/sqlite3expert.c
  ext/misc/series.c
  src/alter.c
  src/analyze.c
  src/attach.c
  src/auth.c
  src/bitvec.c
  src/btmutex.c
  src/build.c
  src/callback.c
  src/comdb2build.c
  src/comdb2lua.c
  src/comdb2vdbe.c
  src/complete.c
  src/ctime.c
  src/dbstat.c
  src/decimal.c
  src/delete.c
  src/dttz.c
  src/expr.c
  src/fault.c
  src/func.c
  src/global.c
  src/hash.c
  src/insert.c
  src/legacy.c
  src/loadext.c
  src/main.c
  src/malloc.c
  src/md5.c
  src/mem1.c
  src/mem2.c
  src/memjournal.c
  src/mutex.c
  src/mutex_noop.c
  src/mutex_unix.c
  src/os.c
  src/os_unix.c
  src/pragma.c
  src/prepare.c
  src/printf.c
  src/random.c
  src/resolve.c
  src/rowset.c
  src/select.c
  src/sqlite_tunables.c
  src/status.c
  src/table.c
  src/threads.c
  src/tokenize.c
  src/treeview.c
  src/trigger.c
  src/update.c
  src/upsert.c
  src/utf.c
  src/util.c
  src/vdbe.c
  src/vdbeapi.c
  src/vdbeblob.c
  src/vdbemem.c
  src/vdbesort.c
  src/vdbetrace.c
  src/vtab.c
  src/walker.c
  src/where.c
  src/wherecode.c
  src/whereexpr.c
  src/window.c
)
include(${PROJECT_SOURCE_DIR}/sqlite/definitions.cmake)
add_definitions(${SQLITE_FLAGS})
target_compile_definitions(sqlite PRIVATE ${SQLITE_FLAGS})

if (${CMAKE_C_COMPILER_ID} STREQUAL GNU OR ${CMAKE_C_COMPILER_ID} STREQUAL Clang)
  set_source_files_properties(src/hash.c PROPERTIES COMPILE_FLAGS "-Wno-char-subscripts")
  set_source_files_properties(src/select.c PROPERTIES COMPILE_FLAGS "-Wno-unused-value")
  set_source_files_properties(src/build.c PROPERTIES COMPILE_FLAGS "-Wno-misleading-indentation")
  set_source_files_properties(src/where.c PROPERTIES COMPILE_FLAGS "-Wno-misleading-indentation")
endif()
if (${CMAKE_C_COMPILER_ID} STREQUAL GNU)
  set_source_files_properties(src/trigger.c src/status.c PROPERTIES COMPILE_FLAGS "-Wno-unused-but-set-variable -Wno-unused-const-variable")
elseif(${CMAKE_C_COMPILER_ID} STREQUAL Clang OR ${CMAKE_C_COMPILER_ID} STREQUAL AppleClang)
  set_source_files_properties(src/trigger.c src/status.c PROPERTIES COMPILE_FLAGS "-Wno-unused-const-variable")
endif()


set(module sqlite)
set(MODULE SQLITE)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_sqlite.h @ONLY)
configure_file(src/parse.y parse.y COPYONLY)
configure_file(tool/lempar.c lempar.c COPYONLY)
configure_file(tool/mkopcodec.tcl mkopcodec.tcl COPYONLY)
configure_file(tool/mkopcodeh.tcl mkopcodeh.tcl COPYONLY)
configure_file(tool/mkvdbeauxinlines.tcl mkvdbeauxinlines.tcl COPYONLY)

file(READ ${PROJECT_SOURCE_DIR}/lua/default_consumer_v1.lua default_consumer_v1)
string(REPLACE "\n" "\\n\\\n" default_consumer_v1 ${default_consumer_v1})
configure_file(src/default_consumer_v1.h.in default_consumer_v1.h @ONLY)

add_executable(mkkeywordhash tool/mkkeywordhash.c)
target_compile_definitions(mkkeywordhash PRIVATE ${SQLITE_FLAGS})
add_custom_command(
  OUTPUT keywordhash.h
  DEPENDS mkkeywordhash
  COMMAND mkkeywordhash > keywordhash.h
)

find_program(tclsh NAMES tclsh8.6 tclsh8.5 tclsh)
if(NOT tclsh)
  message(FATAL_ERROR "tclsh not found!")
endif()
add_executable(lemon tool/lemon.c)
add_custom_command(
  OUTPUT parse.c parse.h
  DEPENDS lemon tool/lempar.c src/parse.y
  COMMAND lemon ${SQLITE_FLAGS} parse.y
)

add_custom_command(
  OUTPUT opcodes.c opcodes.h
  DEPENDS parse.h src/vdbe.c
  COMMAND cat parse.h ${CMAKE_CURRENT_SOURCE_DIR}/src/vdbe.c | ${tclsh} mkopcodeh.tcl > opcodes.h
  COMMAND sort -n -b -k 3 opcodes.h | ${tclsh} mkopcodec.tcl opcodes.h > opcodes.c
)
add_custom_command(
  OUTPUT serialget.c memcompare.c vdbecompare.c vdbeaux.c
  DEPENDS parse.c src/vdbeaux.c
  COMMAND ${tclsh} mkvdbeauxinlines.tcl ${CMAKE_CURRENT_SOURCE_DIR}/src/vdbeaux.c .
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}/src
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/ext/comdb2
  ${CMAKE_CURRENT_SOURCE_DIR}/ext/misc
  ${CMAKE_CURRENT_SOURCE_DIR}/ext/expert
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/csc2
  ${PROJECT_SOURCE_DIR}/datetime
  ${PROJECT_SOURCE_DIR}/db
  ${PROJECT_BINARY_DIR}/db
  ${PROJECT_SOURCE_DIR}/dfp/decNumber
  ${PROJECT_SOURCE_DIR}/dfp/dfpal
  ${PROJECT_SOURCE_DIR}/dlmalloc
  ${PROJECT_SOURCE_DIR}/lua
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
  ${PROJECT_SOURCE_DIR}/net
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/schemachange
  ${PROJECT_BINARY_DIR}/berkdb
  ${PROJECT_BINARY_DIR}/bdb
  ${PROJECT_SOURCE_DIR}/build/berkdb
  ${PROJECT_SOURCE_DIR}/build/bdb
  ${PROJECT_SOURCE_DIR}/berkdb
  ${OPENSSL_INCLUDE_DIR}
  ${PROTOBUF-C_INCLUDE_DIR}
)
add_dependencies(sqlite mem proto bdb)
if (COMDB2_BBCMAKE)
  target_link_libraries(sqlite PUBLIC protobuf-c uuid)
  configure_bb_target(sqlite V2 NO_BUILDID NO_PLINKSTRINGS HEADER_DIRS)
endif()
