add_executable(cdb2_copylogs_start cdb2_copylogs_start.c)

include_directories(
  ${PROJECT_SOURCE_DIR}/cdb2api
)
set(libs
  cdb2api
  ${PROTOBUF-C_LIBRARY}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${CMAKE_DL_LIBS}
)

list(APPEND libs ${UNWIND_LIBRARY})

target_link_libraries(cdb2_copylogs_start ${libs})
if(COMDB2_BUILD_STATIC)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()

install(TARGETS cdb2_copylogs_start RUNTIME DESTINATION bin)
