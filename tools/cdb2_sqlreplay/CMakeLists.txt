add_executable(cdb2_sqlreplay cdb2_sqlreplay)
include_directories(
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/cson
)
target_link_libraries(cdb2_sqlreplay
  cdb2api
  cson
  ${OPENSSL_LIBRARIES}
  ${PROTOBUF_C_LIBRARY}
  Threads::Threads
)
install(TARGETS cdb2_sqlreplay RUNTIME DESTINATION bin)
