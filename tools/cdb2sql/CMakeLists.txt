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
target_link_libraries(cdb2sql
  cdb2api
  ${OPENSSL_LIBRARIES}
  ${PROTOBUF_C_LIBRARY}
  ${READLINE_LIBRARY}
  Threads::Threads
)
if(${CMAKE_SYSTEM_NAME} STREQUAL "SunOS")
  target_link_libraries(cdb2sql -lsocket -lnsl)
endif()
install(TARGETS cdb2sql RUNTIME DESTINATION bin)
