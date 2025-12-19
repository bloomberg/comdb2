find_package(Readline REQUIRED)
find_package(SQLite3 REQUIRED)
include_directories(
  ${PROJECT_SOURCE_DIR}/bb
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/comdb2rle
  ${PROJECT_SOURCE_DIR}/cson
  ${PROJECT_SOURCE_DIR}/datetime
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROTOBUF-C_INCLUDE_DIR}
  ${SQLite3_INCLUDE_DIRS}
  ${OPENSSL_INCLUDE_DIR}
  ${READLINE_INCLUDE_DIR}
)

add_definitions(-DSBUF2_SERVER=0)

macro(add_exe exe)
    add_executable(${exe} ${ARGN})
    list(APPEND test-tools ${exe})
endmacro()

add_exe(api_events api_events.c)
add_exe(appsock appsock.c)
add_exe(blob blob.c)
add_exe(bound bound.cpp)
add_exe(breakloop breakloop.c nemesis.c testutil.c)
add_exe(carray_insert carray_insert.c)
add_exe(cdb2_close_early cdb2_close_early.c)
add_exe(cdb2_open cdb2_open.c)
add_exe(cdb2api_admin cdb2api_admin.cpp)
add_exe(cdb2api_caller cdb2api_caller.cpp)
add_exe(cdb2api_cfg cdb2api_cfg.cpp)
add_exe(cdb2api_chunk cdb2api_chunk.cpp)
add_exe(cdb2api_drain cdb2api_drain.cpp)
add_exe(cdb2api_effects_on_chunk_error cdb2api_effects_on_chunk_error.c)
add_exe(cdb2api_enforce_timeout cdb2api_enforce_timeout.cpp)
add_exe(cdb2api_hasql cdb2api_hasql.cpp)
add_exe(cdb2api_localcache_systable cdb2api_localcache_systable.cpp)
add_exe(cdb2api_read_intrans_results cdb2api_read_intrans_results.c)
add_exe(cdb2api_rte cdb2api_rte.cpp)
add_exe(cdb2api_setoptions cdb2api_setoptions.cpp)
add_exe(cdb2api_sptrace cdb2api_sptrace.cpp)
add_exe(cdb2api_stmt_return_types cdb2api_stmt_return_types.cpp)
add_exe(cdb2api_string_escape cdb2api_string_escape.cpp)
add_exe(cdb2api_txn cdb2api_txn.cpp)
add_exe(cdb2api_unread_data cdb2api_unread_data.cpp)
add_exe(cdb2bind cdb2bind.c)
add_exe(cldeadlock cldeadlock.c)
add_exe(close_old_connections close_old_connections.c)
add_exe(comdb2_blobtest comdb2_blobtest.c)
add_exe(comdb2_sqltest client_datetime.c endian_core.c md5.c slt_comdb2.c slt_sqlite.c sqllogictest.c)
add_exe(conn conn.c)
add_exe(copy_db_files copy_db_files.cpp)
add_exe(crle crle.c)
add_exe(cson_test cson_test.c)
add_exe(deadlock_load deadlock_load.c)
add_exe(default_consumer default_consumer.c)
add_exe(emit_timeout emit_timeout.c)
add_exe(foreigndbconfig foreigndbconfig.c)
add_exe(hatest hatest.c)
add_exe(ins_upd_del ins_upd_del.cpp)
add_exe(insblob insblob.c)
add_exe(updblob updblob.c)
add_exe(inscntdel inscntdel.cpp)
add_exe(insert insert.c nemesis.c testutil.c)
add_exe(insert_lots_mt insert_lots_mt.cpp)
add_exe(leakcheck leakcheck.c)
add_exe(localrep localrep.c strbuf.c)
add_exe(makerecord_timer makerecord_timer.c)
add_exe(malloc_resize_test malloc_resize_test.c)
add_exe(multithd multithd.c)
add_exe(nowritetimeout nowritetimeout.c)
add_exe(overflow_blobtest overflow_blobtest.c)
add_exe(pmux_queries pmux_queries.cpp)
add_exe(ptrantest ptrantest.c)
add_exe(recom recom.c)
add_exe(reco-ddlk-sql reco-ddlk-sql.c)
add_exe(register register.c nemesis.c testutil.c)
add_exe(selectv selectv.c)
add_exe(selectv_deadlock selectv_deadlock.c)
add_exe(selectv_rcode selectv_rcode.c)
add_exe(serial serial.c)
add_exe(sicountbug sicountbug.c)
add_exe(simple_ssl simple_ssl.c)
add_exe(sirace sirace.c)
add_exe(slowreaders slowreaders.c)
add_exe(smartbeats smartbeats.c)
add_exe(sqlite_clnt sqlite_clnt.c)
add_exe(ssl_multi_certs_one_process ssl_multi_certs_one_process.c)
add_exe(stepper stepper.c stepper_client.c)
add_exe(test_threadpool test_threadpool.c)
add_exe(test_compare_semver test_compare_semver.c)
add_exe(test_str_util test_str_util.c)
add_exe(test_consistent_hash test_consistent_hash.c)
add_exe(test_consistent_hash_bench test_consistent_hash_bench.c)
add_exe(updater updater.c testutil.c)
add_exe(utf8 utf8.c)
add_exe(verify_atomics_work verify_atomics_work.c)
add_exe(upsert_replay upsert_replay.c)

target_link_libraries(cson_test cson)
target_link_libraries(stepper util mem dlmalloc)
target_link_libraries(test_threadpool util mem dlmalloc)
target_link_libraries(test_consistent_hash util mem dlmalloc crc32c)
target_link_libraries(test_consistent_hash_bench util mem dlmalloc crc32c)
target_link_libraries(test_compare_semver util)
target_link_libraries(test_str_util util)

# Build the crc32c library with UBSAN. Compiling the test driver alone with
# UBSAN and linking in a non-instrumented copy of the library is insufficient.
# The instrumentation needs to exist in the library object code.
add_crc32c_library(crc32c_ubsan WITH_UBSAN)

# Build the crc32c test driver.
add_executable(crc32c_test crc32c_test.c)
target_link_libraries(crc32c_test crc32c_ubsan)
list(APPEND test-tools crc32c_test)

list(APPEND common-deps
  ${READLINE_LIBRARIES}
  ${SQLite3_LIBRARIES}
  ${OPENSSL_LIBRARIES}
  ${PROTOBUF-C_LIBRARY}
  ${ZLIB_LIBRARIES}
  ${UNWIND_LIBRARY}
  ${CMAKE_DL_LIBS}
  m
)

if (NOT COMDB2_SKIP_CDB2API_SO)
  add_library(api_hello_world_lib SHARED api_hello_world_lib.c)
  list(APPEND test-tools api_hello_world_lib)
endif()

foreach(executable ${test-tools})
  if (COMDB2_BBCMAKE)
    target_link_libraries(${executable} opencdb2api ${common-deps})
  else()
    target_link_libraries(${executable} cdb2api ${common-deps})
  endif()
endforeach()

if(NOT COMDB2_SKIP_CDB2API_SO)
  add_exe(api_libs api_libs.c)
  target_link_libraries(api_libs cdb2api_shared ${common-deps})

  add_exe(cdb2_client cdb2_client.c)
  target_link_libraries(cdb2_client cdb2api_shared ${common-deps})

  add_exe(cdb2api_unit cdb2api_unit.c)
  target_link_libraries(cdb2api_unit cdb2api_shared ${common-deps})

  add_exe(test_get_comdb2db_hosts test_get_comdb2db_hosts.c)
  target_link_libraries(test_get_comdb2db_hosts cdb2api_shared ${common-deps})
endif()

add_subdirectory(tcl)
list(APPEND test-tools tclcdb2)

add_custom_target(test-tools DEPENDS ${test-tools})
