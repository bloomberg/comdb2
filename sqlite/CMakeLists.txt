add_library(sqlite
  analyze.c
  attach.c
  auth.c
  bitvec.c
  btmutex.c
  build.c
  callback.c
  comdb2build.c
  comdb2lua.c
  comdb2vdbe.c
  complete.c
  ctime.c
  dbstat.c
  decimal.c
  delete.c
  dttz.c
  expr.c
  ext/comdb2/appsock_handlers.c
  ext/comdb2/columns.c
  ext/comdb2/constraints.c
  ext/comdb2/keycomponents.c
  ext/comdb2/keys.c
  ext/comdb2/keywords.c
  ext/comdb2/limits.c
  ext/comdb2/opcode_handlers.c
  ext/comdb2/plugins.c
  ext/comdb2/procedures.c
  ext/comdb2/tablepermissions.c
  ext/comdb2/tables.c
  ext/comdb2/timepartitions.c
  ext/comdb2/tablesizes.c
  ext/comdb2/threadpools.c
  ext/comdb2/triggers.c
  ext/comdb2/tunables.c
  ext/comdb2/users.c
  ext/comdb2/tranlog.c
  ext/comdb2/clientstats.c
  ext/comdb2/ezsystables.c
  ext/comdb2/typesamples.c 
  ext/comdb2/repnetqueue.c
  ext/comdb2/netuserfunc.c
  ext/misc/completion.c
  ext/misc/json1.c
  ext/expert/sqlite3expert.c
  ext/misc/series.c
  fault.c
  func.c
  global.c
  hash.c
  insert.c
  keywordhash.h
  legacy.c
  loadext.c
  main.c
  malloc.c
  md5.c
  mem1.c
  memcompare.c
  memjournal.c
  mutex.c
  mutex_noop.c
  mutex_unix.c
  opcodes.c
  os.c
  os_unix.c
  parse.c
  pragma.c
  prepare.c
  printf.c
  random.c
  resolve.c
  rowset.c
  select.c
  serialget.c
  sqlite_tunables.c
  status.c
  table.c
  tokenize.c
  treeview.c
  trigger.c
  update.c
  utf.c
  util.c
  vdbe.c
  vdbeapi.c
  vdbeaux.c
  vdbeblob.c
  vdbecompare.c
  vdbemem.c
  vdbesort.c
  vdbetrace.c
  vtab.c
  walker.c
  where.c
  wherecode.c
  whereexpr.c
)
include(${PROJECT_SOURCE_DIR}/sqlite/definitions.cmake)
add_definitions(${SQLITE_FLAGS})
target_compile_definitions(sqlite PRIVATE ${SQLITE_FLAGS})

set(module sqlite)
set(MODULE SQLITE)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_sqlite.h @ONLY)
configure_file(lempar.c lempar.c COPYONLY)
configure_file(parse.y parse.y COPYONLY)
configure_file(addopcodes.tcl addopcodes.tcl COPYONLY)
configure_file(mkopcodec.tcl mkopcodec.tcl COPYONLY)
configure_file(mkopcodeh.tcl mkopcodeh.tcl COPYONLY)

add_executable(mkkeywordhash mkkeywordhash.c)
target_compile_definitions(mkkeywordhash PRIVATE ${SQLITE_FLAGS})
add_custom_command(
  OUTPUT keywordhash.h
  DEPENDS mkkeywordhash
  COMMAND mkkeywordhash > keywordhash.h
)

find_program(tclsh NAMES tclsh tclsh8.5)
add_executable(lemon lemon.c)
add_custom_command(
  OUTPUT parse.c parse.h
  DEPENDS lemon lempar.c parse.y
  COMMAND lemon ${SQLITE_FLAGS} parse.y
  COMMAND ${CMAKE_COMMAND} -E copy parse.h parse.h.temp
  COMMAND ${tclsh} addopcodes.tcl parse.h.temp > parse.h
)

add_custom_command(
  OUTPUT opcodes.c opcodes.h
  DEPENDS parse.h vdbe.c
  COMMAND cat parse.h ${CMAKE_CURRENT_SOURCE_DIR}/vdbe.c | ${tclsh} mkopcodeh.tcl > opcodes.h
  COMMAND sort -n -b -k 3 opcodes.h | ${tclsh} mkopcodec.tcl opcodes.h > opcodes.c
)
add_custom_command(
  OUTPUT serialget.c
  DEPENDS parse.c vdbeaux.c
  COMMAND sed -n "/START_INLINE_SERIALGET/,/END_INLINE_SERIALGET/p" ${CMAKE_CURRENT_SOURCE_DIR}/vdbeaux.c > serialget.c
)
add_custom_command(
  OUTPUT memcompare.c
  DEPENDS parse.c vdbeaux.c serialget.c
  COMMAND ${CMAKE_COMMAND} -E echo '\#include <serialget.c>' > memcompare.c
  COMMAND sed -n "/START_INLINE_MEMCOMPARE/,/END_INLINE_MEMCOMPARE/p" ${CMAKE_CURRENT_SOURCE_DIR}/vdbeaux.c >> memcompare.c
)
add_custom_command(
  OUTPUT vdbecompare.c
  DEPENDS parse.c memcompare.c
  COMMAND echo '\#include <memcompare.c>' > vdbecompare.c
  COMMAND sed -n "/START_INLINE_VDBECOMPARE/,/END_INLINE_VDBECOMPARE/p" ${CMAKE_CURRENT_SOURCE_DIR}/vdbeaux.c >> vdbecompare.c
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${CMAKE_CURRENT_SOURCE_DIR}/ext/comdb2
  ${CMAKE_CURRENT_SOURCE_DIR}/ext/misc
  ${CMAKE_CURRENT_SOURCE_DIR}/ext/expert
  ${PROJECT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
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
  ${PROTOBUF_C_INCLUDE_DIR}
)
add_dependencies(sqlite mem protobuf)
