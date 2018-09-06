add_executable(cdb2sql
  cdb2sql.cpp
  ${PROJECT_SOURCE_DIR}/util/bb_getopt_long.c
)
add_dependencies(cdb2sql cdb2api)
include_directories(
  ${PROJECT_SOURCE_DIR}/util
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

if(COMDB2_BUILD_STATIC)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()

install(TARGETS cdb2sql RUNTIME DESTINATION bin)
