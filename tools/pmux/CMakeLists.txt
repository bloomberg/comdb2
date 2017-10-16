add_executable(pmux
  pmux.cpp
  ${PROJECT_SOURCE_DIR}/bb/bb_daemon.c
  ${PROJECT_SOURCE_DIR}/bb/logmsg.c
  ${PROJECT_SOURCE_DIR}/bb/segstring.c
)
add_definitions(-DBUILDING_TOOLS)
include_directories(
  ${PROJECT_SOURCE_DIR}/bb
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/sockpool
)
target_link_libraries(pmux
  cdb2api
  sockpool
  ${OPENSSL_LIBRARIES}
  ${PROTOBUF_C_LIBRARY}
  ${SQLITE3_LIBRARY}
  Threads::Threads
)
if(${CMAKE_SYSTEM_NAME} STREQUAL "SunOS")
  target_link_libraries(pmux -lsocket -lnsl)
endif()
configure_file(pmux.service pmux.service @ONLY)
install(TARGETS pmux RUNTIME DESTINATION bin)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/pmux.service
  DESTINATION lib/systemd/system
)
