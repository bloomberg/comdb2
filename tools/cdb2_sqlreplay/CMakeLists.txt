add_executable(cdb2_sqlreplay cdb2_sqlreplay.cpp)
include_directories(
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${PROJECT_SOURCE_DIR}/cson
)
set(libs
  cdb2api
  cson
  ${PROTOBUF-C_LIBRARY}
)
if(WITH_SSL)
  list(APPEND libs ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS})
endif()

list(APPEND libs ${UNWIND_LIBRARY})

target_link_libraries(cdb2_sqlreplay ${libs})
if(COMDB2_BUILD_STATIC)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()
install(TARGETS cdb2_sqlreplay RUNTIME DESTINATION bin)
