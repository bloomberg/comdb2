add_executable(cdb2ctl
    cdb2ctl.cpp
    ${PROJECT_SOURCE_DIR}/util/bb_getopt_long.c)

add_dependencies(cdb2ctl cdb2api)

include_directories(
    ${PROJECT_SOURCE_DIR}/cdb2api
    ${PROJECT_SOURCE_DIR}/util
    ${OPENSSL_INCLUDE_DIR}
)

set(libs
    cdb2api
    ${PROTOBUF-C_LIBRARY}
    ${UNWIND_LIBRARY}
)

if(WITH_SSL)
    list(APPEND libs ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARIES} ${CMAKE_DL_LIBS})
endif()

target_link_libraries(cdb2ctl ${libs})

if(COMDB2_BUILD_STATIC)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -static-libgcc -static-libstdc++")
endif()

install(TARGETS cdb2ctl RUNTIME DESTINATION bin)
