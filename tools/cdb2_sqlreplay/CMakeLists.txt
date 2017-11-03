add_executable(cdb2_sqlreplay cdb2_sqlreplay.cpp)
include_directories(
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/cson
)
target_link_libraries(cdb2_sqlreplay
  cdb2api
  cson
  ${OPENSSL_LIBRARIES}
  ${PROTOBUF_C_LIBRARY}
  ${ZLIB_LIBRARIES}
  ${CMAKE_DL_LIBS}
)
if(COMDB2_BUILD_STATIC)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()
install(TARGETS cdb2_sqlreplay RUNTIME DESTINATION bin)
