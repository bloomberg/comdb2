add_executable(cdb2sql
  cdb2sql.c
  ${PROJECT_SOURCE_DIR}/bb/bb_getopt_long.c
)
add_dependencies(cdb2sql cdb2api)
include_directories(
  ${PROJECT_SOURCE_DIR}/bb
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${OPENSSL_INCLUDE_DIR}
  ${READLINE_INCLUDE_DIR}
)
set(libs
  cdb2api
  ${PROTOBUF_C_LIBRARY}
  ${READLINE_LIBRARY}
  ${CURSES_LIBRARIES}
)
if(WITH_SSL)
  list(APPEND libs ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS})
endif()

list(APPEND libs ${UNWIND_LIBRARY})

target_link_libraries(cdb2sql ${libs})
install(TARGETS cdb2sql RUNTIME DESTINATION bin)
