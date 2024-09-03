find_package(Readline REQUIRED)

get_property(lclENABLE_COSTS GLOBAL PROPERTY ENABLE_COSTS)
if(lclENABLE_COSTS)
  add_definitions("-DENABLE_COSTS=${lclENABLE_COSTS}")
endif()
get_property(lclREPORT_COSTS GLOBAL PROPERTY REPORT_COSTS)
if(lclREPORT_COSTS)
  add_definitions("-DREPORT_COSTS=${lclREPORT_COSTS}")
endif()

add_executable(cdb2sql
  cdb2sql.cpp
  ${PROJECT_SOURCE_DIR}/util/bb_getopt_long.c
  completer_buf.c
)

include_directories(
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${OPENSSL_INCLUDE_DIR}
  ${READLINE_INCLUDE_DIR}
)

set(libs
  ${PROTOBUF-C_LIBRARY}
  ${UNWIND_LIBRARY}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${CMAKE_DL_LIBS}
)

if(COMDB2_BBCMAKE)
  target_link_libraries(cdb2sql opencdb2api ${READLINE_LIBRARY} tinfo ${libs})
else()
  target_link_libraries(cdb2sql cdb2api ${READLINE_LIBRARIES} ${libs})
endif()

if(COMDB2_BUILD_STATIC)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()

install(TARGETS cdb2sql RUNTIME DESTINATION bin)
