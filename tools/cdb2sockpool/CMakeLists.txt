add_executable(cdb2sockpool
  cdb2sockpool.c
  settings.c
  ${PROJECT_SOURCE_DIR}/bb/bb_daemon.c
  ${PROJECT_SOURCE_DIR}/bb/list.c
  ${PROJECT_SOURCE_DIR}/bb/lockassert.c
  ${PROJECT_SOURCE_DIR}/bb/logmsg.c
  ${PROJECT_SOURCE_DIR}/bb/plhash.c
  ${PROJECT_SOURCE_DIR}/bb/pool.c
  ${PROJECT_SOURCE_DIR}/bb/safestrerror.c
  ${PROJECT_SOURCE_DIR}/bb/segstring.c
  ${PROJECT_SOURCE_DIR}/bb/strbuf.c
)
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/bb
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/sockpool
)
add_definitions(-DBUILDING_TOOLS)
target_link_libraries(cdb2sockpool sockpool)
configure_file(cdb2sockpool.service cdb2sockpool.service @ONLY)
install(TARGETS cdb2sockpool RUNTIME DESTINATION bin)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/cdb2sockpool.service
  DESTINATION lib/systemd/system
)
