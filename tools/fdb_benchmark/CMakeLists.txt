add_executable(fdb_benchmark
  fdb_benchmark.c
)

add_dependencies(fdb_benchmark cdb2api)

include_directories(
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${OPENSSL_INCLUDE_DIR}
  ${READLINE_INCLUDE_DIR}
)

set(libs
  cdb2api
  ${PROTOBUF-C_LIBRARY}
  ${READLINE_LIBRARIES}
  ${UNWIND_LIBRARY}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${CMAKE_DL_LIBS}
)

target_link_libraries(fdb_benchmark ${libs})

if(COMDB2_BUILD_STATIC)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()

install(TARGETS fdb_benchmark RUNTIME DESTINATION bin)
