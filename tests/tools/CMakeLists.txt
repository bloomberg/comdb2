include_directories(
  ${PROJECT_SOURCE_DIR}/bb
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/comdb2rle
  ${PROJECT_SOURCE_DIR}/datetime
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/protobuf
  ${SQLITE3_INCLUDE_DIR}
)

add_definitions(-DSBUF2_SERVER=0)

macro(add_exe exe)
    add_executable(${exe} ${ARGN})
    list(APPEND test-tools ${exe})
endmacro()

add_exe(blob blob.c)
add_exe(bound bound.cpp)
add_exe(cdb2api_caller cdb2api_caller.cpp)
add_exe(cdb2bind cdb2bind.c)
add_exe(comdb2_blobtest comdb2_blobtest.c)
add_exe(comdb2_sqltest client_datetime.c endian_core.c md5.c slt_comdb2.c slt_sqlite.c sqllogictest.c)
add_exe(crle crle.c)
add_exe(hatest hatest.c)
add_exe(cldeadlock cldeadlock.c)
add_exe(insert_lots_mt insert_lots_mt.cpp)
add_exe(leakcheck leakcheck.c)
add_exe(localrep localrep.c strbuf.c)
add_exe(overflow_blobtest overflow_blobtest.c)
add_exe(ptrantest ptrantest.c)
add_exe(recom recom.c)
add_exe(selectv selectv.c)
add_exe(serial serial.c)
add_exe(sicountbug sicountbug.c)
add_exe(sirace sirace.c)
add_exe(multithd multithd.c)
add_exe(simple_ssl simple_ssl.c)
add_exe(ssl_multi_certs_one_process ssl_multi_certs_one_process.c)
add_exe(stepper stepper.c stepper_client.c)
add_exe(utf8 utf8.c)
add_exe(insert insert.c nemesis.c testutil.c)
add_exe(register register.c nemesis.c testutil.c)
add_exe(breakloop breakloop.c nemesis.c testutil.c)
add_exe(cdb2_open cdb2_open.c)
add_exe(verify_atomics_work verify_atomics_work.c)
add_exe(malloc_resize_test malloc_resize_test.c)
add_exe(cdb2_close_early cdb2_close_early.c)
add_exe(cdb2api_read_intrans_results cdb2api_read_intrans_results.c)
add_exe(nowritetimeout nowritetimeout.c)
add_exe(api_events api_events.c)
add_exe(deadlock_load deadlock_load.c)
add_exe(selectv_deadlock selectv_deadlock.c)
add_exe(selectv_rcode selectv_rcode.c)
add_exe(api_libs api_libs.c)
add_exe(test_threadpool test_threadpool.c)
add_library(api_hello_world_lib SHARED api_hello_world_lib.c)
list(APPEND test-tools api_hello_world_lib)

add_custom_target(test-tools DEPENDS ${test-tools})

foreach(executable blob bound cdb2api_caller cdb2bind comdb2_blobtest insert_lots_mt leakcheck localrep overflow_blobtest selectv serial sicountbug sirace simple_ssl utf8 insert register breakloop cdb2_open multithd verify_atomics_work malloc_resize_test cdb2_close_early cdb2api_read_intrans_results ssl_multi_certs_one_process nowritetimeout api_events deadlock_load selectv_deadlock selectv_rcode api_libs test_threadpool)
  target_link_libraries(${executable} cdb2api ${OPENSSL_LIBRARIES} ${PROTOBUF_C_LIBRARY} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS})
endforeach()

if(NOT COMDB2_SKIP_CDB2API_SO)
  add_exe(cdb2_client cdb2_client.c)
  target_link_libraries(cdb2_client cdb2api_shared ${OPENSSL_LIBRARIES} ${PROTOBUF_C_LIBRARY} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS} ${UNWIND_LIBRARY})

  add_exe(cdb2api_unit cdb2api_unit.c)
  target_link_libraries(cdb2api_unit cdb2api_shared ${OPENSSL_LIBRARIES} ${PROTOBUF_C_LIBRARY} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS} ${UNWIND_LIBRARY})
endif()

# needs readline!
target_link_libraries(hatest cdb2api ${READLINE_LIBRARY} ${CURSES_LIBRARIES} ${OPENSSL_LIBRARIES} ${PROTOBUF_C_LIBRARY} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS})
target_link_libraries(cldeadlock cdb2api ${OPENSSL_LIBRARIES} ${PROTOBUF_C_LIBRARY} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS})

# sqlite3_snprintf
target_link_libraries(comdb2_sqltest cdb2api ${OPENSSL_LIBRARIES} ${PROTOBUF_C_LIBRARY} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS} -lm)
target_link_libraries(ptrantest cdb2api ${OPENSSL_LIBRARIES} ${PROTOBUF_C_LIBRARY} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS} -lm)
target_link_libraries(recom cdb2api ${OPENSSL_LIBRARIES} ${PROTOBUF_C_LIBRARY} ${SQLITE3_LIBRARY} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS} -lm)

target_link_libraries(test_threadpool mem dlmalloc util -Ibbinc ${CMAKE_DL_LIBS})

# everything!
target_link_libraries(stepper cdb2api mem dlmalloc util ${OPENSSL_LIBRARIES} ${PROTOBUF_C_LIBRARY} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS})

foreach(executable blob bound cdb2api_caller cdb2bind comdb2_blobtest insert_lots_mt leakcheck localrep overflow_blobtest selectv serial sicountbug sirace simple_ssl utf8 insert register breakloop hatest cldeadlock comdb2_sqltest ptrantest recom stepper multithd cdb2_open verify_atomics_work malloc_resize_test cdb2_close_early cdb2api_read_intrans_results ssl_multi_certs_one_process nowritetimeout api_events deadlock_load selectv_deadlock selectv_rcode api_libs test_threadpool)
    target_link_libraries(${executable} ${UNWIND_LIBRARY})
endforeach()
