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
  ${SQLITE3_INCLUDE_DIR}
)
set(libs
  cdb2api
  sockpool
  ${PROTOBUF_C_LIBRARY}
  ${SQLITE3_LIBRARY}
)
if(WITH_SSL)
  list(APPEND libs ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS})
endif()

list(APPEND libs ${UNWIND_LIBRARY})

target_link_libraries(pmux ${libs})
if(COMDB2_BUILD_STATIC)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()
configure_file(pmux.service pmux.service @ONLY)
install(TARGETS pmux RUNTIME DESTINATION bin)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/pmux.service
  DESTINATION lib/systemd/system
)
