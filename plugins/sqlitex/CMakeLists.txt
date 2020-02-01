list(APPEND SQLITEX_FLAGS
  -DHAVE_USLEEP=1
  -DOS_UNIX=1
  -DSQLITE_BUILDING_FOR_COMDB2
  -DSQLITE_CASE_SENSITIVE_LIKE
  -DSQLITE_DEFAULT_FILE_FORMAT=4
  -DSQLITE_DEFAULT_MEMSTATUS=0
  -DSQLITE_ENABLE_COLUMN_METADATA
  -DSQLITE_ENABLE_NORMALIZE
  -DSQLITE_ENABLE_UPDATE_DELETE_LIMIT
  -DSQLITE_OMIT_AUTOINIT
  -DSQLITE_OMIT_ALTERTABLE
  -DSQLITE_OMIT_AUTHORIZATION
  -DSQLITE_OMIT_AUTOINCREMENT
  -DSQLITE_OMIT_AUTOVACUM
  -DSQLITE_OMIT_BUILTIN_TEST
  -DSQLITE_OMIT_FOREIGN_KEY
  -DSQLITE_OMIT_INCRBLOB
  -DSQLITE_OMIT_LOOKASIDE
  -DSQLITE_OMIT_PAGER_PRAGMAS
  -DSQLITE_OMIT_TCL_VARIABLE
  -DSQLITE_OMIT_SHARED_CACHE
  -DSQLITE_OMIT_VACUUM
  -DSQLITE_OMIT_WAL
  -DSQLITE_OMIT_XFER_OPT
  -DSQLITE_THREADSAFE=0
  -DSQLITE_SOUNDEX
  -DSQLITE_OMIT_DATETIME_FUNCS
  -DSQLITE_ENABLE_STAT4
  -DSQLITE_ENABLE_EXPLAIN_COMMENTS
  -DSQLITE_ENABLE_CURSOR_HINTS
  -DSQLITE_OMIT_UTF16
  -DSQLITE_ENABLE_STMT_SCANSTATUS
  -DSQLITE_EXPLAIN_ESTIMATED_ROWS
  -DSQLITE_ENABLE_COLUMN_USED_MASK
  -DSQLITE_ENABLE_HIDDEN_COLUMNS
  -DSQLITE_COUNTOFVIEW_OPTIMIZATION
  -DSQLITE_ENABLE_SERIES
)

set(SOURCES 
  keywordhash.h
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
  decimal.c
  delete.c
  dttz.c
  expr.c
  ext/misc/series.c
  ext/comdb2/tables.c
  fault.c
  fingerprint.c
  func.c
  global.c
  hash.c
  insert.c
  journal.c
  legacy.c
  loadext.c
  main.c
  malloc.c
  md5.c
  mem1.c
  memcompare.c
  mutex.c
  mutex_noop.c
  mutex_unix.c
  opcodes.c
  os.c
  os_unix.c
  parse.c
  plugin.c
  pragma.c
  prepare.c
  printf.c
  random.c
  resolve.c
  rowset.c
  select.c
  serialget.c
  sqlglue.c
  sqlite_tunables.c
  status.c
  table.c
  tokenize.c
  trigger.c
  update.c
  utf.c
  util.c
  vdbeapi.c
  vdbeaux.c
  vdbeblob.c
  vdbe.c
  vdbecompare.c
  vdbemem.c
  vdbesort.c
  vdbetrace.c
  vtab.c
  walker.c
  where.c
)

add_plugin(sqlitex STATIC "${SOURCES}")

add_definitions(${SQLITEX_FLAGS})
target_compile_definitions(sqlitex PRIVATE ${SQLITEX_FLAGS})

if (${CMAKE_C_COMPILER_ID} STREQUAL GNU OR ${CMAKE_C_COMPILER_ID} STREQUAL Clang)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-variable")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-but-set-variable")
  set_source_files_properties(func.c PROPERTIES COMPILE_FLAGS "-Wno-pointer-sign")
  set_source_files_properties(insert.c PROPERTIES COMPILE_FLAGS "-Wno-unused-label")
  set_source_files_properties(select.c PROPERTIES COMPILE_FLAGS "-Wno-unused-value")
  set_source_files_properties(vdbeapi.c PROPERTIES COMPILE_FLAGS "-Wno-unused-value -Wno-sizeof-array-argument -Wno-missing-braces")
  set_source_files_properties(vdbeaux.c PROPERTIES COMPILE_FLAGS "-Wno-unused-value -Wno-maybe-uninitialized")
  set_source_files_properties(vdbemem.c PROPERTIES COMPILE_FLAGS "-Wno-unused-value")
  set_source_files_properties(vdbe.c PROPERTIES COMPILE_FLAGS
      "-Wno-unused-value -Wno-parentheses -Wno-format")
endif()
if (${CMAKE_C_COMPILER_ID} STREQUAL GNU)
  set_source_files_properties(trigger.c status.c PROPERTIES COMPILE_FLAGS "-Wno-unused-but-set-variable -Wno-unused-const-variable")
elseif(${CMAKE_C_COMPILER_ID} STREQUAL Clang)
  set_source_files_properties(trigger.c status.c PROPERTIES COMPILE_FLAGS "-Wno-unused-const-variable")
endif()

set(module sqlite)
set(MODULE SQLITE)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_sqlite.h @ONLY)
configure_file(parse.y parse.y COPYONLY)
configure_file(lempar.c lempar.c COPYONLY)
configure_file(mkopcodec.awk mkopcodec.awk COPYONLY)
configure_file(mkopcodeh.awk mkopcodeh.awk COPYONLY)
configure_file(addopcodes.awk addopcodes.awk COPYONLY)
configure_file(vdbeaux.c vdbeaux.c COPYONLY)
configure_file(generate_src generate_src COPYONLY)

add_executable(mkkeywordhashx mkkeywordhash.c)
target_compile_definitions(mkkeywordhashx PRIVATE ${SQLITE_FLAGS})
add_custom_command(
  OUTPUT keywordhash.h
  DEPENDS mkkeywordhashx
  COMMAND mkkeywordhashx > keywordhash.h
)

add_executable(lemonx lemon.c)
add_custom_command(
  OUTPUT parse.c parse.h
  DEPENDS lemonx lempar.c parse.y
  COMMAND lemonx ${SQLITEX_FLAGS} parse.y
  COMMAND gawk -f addopcodes.awk parse.h > parse.h.new
  COMMAND mv parse.h.new parse.h
)

add_custom_command(
  OUTPUT opcodes.c opcodes.h
  DEPENDS parse.h vdbe.c
  COMMAND cat parse.h ${CMAKE_CURRENT_SOURCE_DIR}/vdbe.c | gawk -f mkopcodeh.awk > opcodes.h
  COMMAND sort -n -b -k 4 opcodes.h | gawk -f mkopcodec.awk > opcodes.c
)

add_custom_command(
  OUTPUT serialget.c memcompare.c vdbecompare.c
  DEPENDS vdbeaux.c
  COMMAND ${CMAKE_CURRENT_BINARY_DIR}/generate_src
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
  ${PROTOBUF-C_INCLUDE_DIR}
)
add_dependencies(sqlitex mem protobuf bdb)

set_target_properties(sqlitex PROPERTIES LINKER_LANGUAGE C)

