add_executable(cdb2sockpool
  cdb2sockpool.c
  settings.c
  ${PROJECT_SOURCE_DIR}/util/bb_daemon.c
  ${PROJECT_SOURCE_DIR}/util/list.c
  ${PROJECT_SOURCE_DIR}/util/logmsg.c
  ${PROJECT_SOURCE_DIR}/util/plhash.c
  ${PROJECT_SOURCE_DIR}/util/pool.c
  ${PROJECT_SOURCE_DIR}/util/segstring.c
  ${PROJECT_SOURCE_DIR}/util/strbuf.c
  ${PROJECT_SOURCE_DIR}/util/tohex.c
)
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_BINARY_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/berkdb
  ${PROJECT_SOURCE_DIR}/sockpool
)
target_link_libraries(cdb2sockpool sockpool)
configure_file(cdb2sockpool.service cdb2sockpool.service @ONLY)
install(TARGETS cdb2sockpool RUNTIME DESTINATION bin)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/cdb2sockpool.service
  DESTINATION lib/systemd/system
)
