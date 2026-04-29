set(src
  cdb2api.c
  cdb2api_ssl_test.c
  ${PROJECT_BINARY_DIR}/protobuf/sqlquery.pb-c.c
  ${PROJECT_BINARY_DIR}/protobuf/sqlresponse.pb-c.c
  ${PROJECT_SOURCE_DIR}/util/hostname_support.c
  ${PROJECT_SOURCE_DIR}/util/comdb2buf.c
  ${PROJECT_SOURCE_DIR}/util/ssl_glue.c
  ${PROJECT_SOURCE_DIR}/util/ssl_support.c
)

set_source_files_properties(
  ${PROJECT_BINARY_DIR}/protobuf/sqlquery.pb-c.c
  ${PROJECT_BINARY_DIR}/protobuf/sqlresponse.pb-c.c
  PROPERTIES GENERATED TRUE
)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_BINARY_DIR}/protobuf
  ${PROTOBUF-C_INCLUDE_DIR}
  ${OPENSSL_INCLUDE_DIR}
)

include(${CMAKE_CURRENT_SOURCE_DIR}/cdb2api_shared_definitions.cmake)

if (DEFINED COMDB2_SEMVER AND NOT COMDB2_SEMVER STREQUAL "")
  add_definitions(-DAPI_DRIVER_VERSION=${COMDB2_SEMVER})
endif()

if (COMDB2_BBCMAKE)
  include(${EXTRA_PLUGINS}/cdb2api/cdb2api.cmake)
  add_library(opencdb2api STATIC ${src})
  add_dependencies(opencdb2api proto)
  target_link_libraries(opencdb2api PUBLIC cdb2api)
  comdb2_lib_target(opencdb2api)
else()

add_library(cdb2api STATIC ${src})
add_dependencies(cdb2api proto)
target_compile_options(cdb2api PRIVATE -Wformat-security)
target_link_libraries(cdb2api PUBLIC resolv)

configure_file(cdb2api.pc cdb2api.pc @ONLY)
install(TARGETS cdb2api ARCHIVE DESTINATION lib)
install(FILES cdb2api.h DESTINATION include)
install(FILES
  ${CMAKE_CURRENT_BINARY_DIR}/cdb2api.pc
  DESTINATION lib/pkgconfig
)

endif(COMDB2_BBCMAKE)
