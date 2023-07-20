find_package(SQLite3 REQUIRED)
add_executable(pmux
  pmux.cpp
  ${PROJECT_SOURCE_DIR}/util/bb_daemon.c
)
include_directories(
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${SQLite3_INCLUDE_DIRS}
  ${LIBEVENT_INCLUDE_DIR}
)
set(libs
  cdb2api
  ${PROTOBUF-C_LIBRARY}
  ${SQLite3_LIBRARIES}
  ${LIBEVENT_LIBRARIES}
  ${CMAKE_DL_LIBS}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
)

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
