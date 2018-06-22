cmake_minimum_required (VERSION 3.1)
project (Comdb2)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED TRUE)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "RelWithDebInfo" "Release")
if(NOT CMAKE_BUILD_TYPE)
  get_property(helpstring CACHE CMAKE_BUILD_TYPE PROPERTY HELPSTRING)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING ${helpstring} FORCE)
endif()

if(COMDB2_BUILD_STATIC)
  SET(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
endif()

option(WITH_SSL "Turn OFF to compile without SSL" ON)
if(WITH_SSL)
  add_definitions(-DWITH_SSL=1)
else()
  add_definitions(-DWITH_SSL=0)
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  # Homebrew will install here by default
  list(APPEND CMAKE_PREFIX_PATH
    ${CMAKE_PREFIX_PATH}
    "/usr/local"
    "/usr/local/opt/openssl"
    "/usr/local/opt/readline")
endif()
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)
set(CURSES_NEED_NCURSES TRUE)
find_package(Curses REQUIRED)
find_package(LZ4 REQUIRED)
# Comdb2 uses libcrpyto. Hence OpenSSL is required even if WITH_SSL is off.
find_package(OpenSSL REQUIRED)
find_package(Protobuf_C REQUIRED)
find_package(Readline REQUIRED)
find_package(SQLite3 REQUIRED)
if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  find_package(UUID REQUIRED)
endif()
find_package(ZLIB REQUIRED)
if(${CMAKE_SYSTEM_NAME} STREQUAL Linux OR ${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  find_package(Unwind REQUIRED)
endif()

option(WITH_TCL "Turn ON to compile with Tcl bindings" OFF)
if(WITH_TCL)
  find_package(Tcl REQUIRED)
endif()

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(prefix /opt/bb)
  if(DEFINED ENV{COMDB2_ROOT})
    set(prefix $ENV{COMDB2_ROOT})
  endif()
  get_property(helpstring CACHE CMAKE_INSTALL_PREFIX PROPERTY HELPSTRING)
  set(CMAKE_INSTALL_PREFIX ${prefix} CACHE PATH ${helpstring} FORCE)
endif()

add_definitions(
  -DCOMDB2_ROOT=${CMAKE_INSTALL_PREFIX}
  -DCOMDB2_VERSION="2"
  -D_FILE_OFFSET_BITS=64
)

option(COMDB2_PER_THREAD_MALLOC "Turn OFF to run under Valgrind" ON)
if(COMDB2_PER_THREAD_MALLOC)
  add_definitions(-DPER_THREAD_MALLOC)
else()
  add_definitions(-DUSE_SYS_ALLOC)
endif()

add_definitions(-DNEWSI_ASOF_USE_TEMPTABLE)
add_definitions(-DNEWSI_MEMPOOL)

if(${CMAKE_SYSTEM_PROCESSOR} STREQUAL x86_64 OR ${CMAKE_SYSTEM_PROCESSOR} STREQUAL armv7l)
  # This is mostly misused as little-endian
  add_definitions(-D_LINUX_SOURCE)
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
  add_definitions(-D_GNU_SOURCE)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -pthread")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -fno-strict-aliasing")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror=implicit-function-declaration")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror=return-type")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -pthread -lrt")
  if (${CMAKE_C_COMPILER_ID} STREQUAL Clang)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -latomic")
  endif()
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  add_definitions(-D_DARWIN_C_SOURCE -D_XOPEN_SOURCE=600)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL AIX)
  add_definitions(-D_IBM_SOURCE)
  #-qxflag=dircache:71,256 ??  #-bmaxdata:0xb0000000/dsa ??
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -qlanglvl=extc99 -qcpluscmt -qtls -qdfp -Wl,-bmaxdata:0xb0000000/dsa")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lthread -lpthread")
elseif(${CMAKE_SYSTEM_NAME} STREQUAL SunOS)
  add_definitions(-D_SUN_SOURCE -D_POSIX_PTHREAD_SEMANTICS)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -errfmt=error -mt=yes -fma=fused -xmemalign=8i -xc99=all")
  set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -pthread")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lsocket -lnsl -lresolv -lpthread")
endif()

add_subdirectory(util)
add_subdirectory(bdb)
add_subdirectory(berkdb)
add_subdirectory(cdb2api)
add_subdirectory(comdb2rle)
add_subdirectory(crc32c)
add_subdirectory(csc2)
add_subdirectory(cson)
add_subdirectory(datetime)
add_subdirectory(dfp)
add_subdirectory(dlmalloc)
add_subdirectory(lua)
add_subdirectory(mem)
add_subdirectory(net)
add_subdirectory(protobuf)
add_subdirectory(schemachange)
add_subdirectory(sockpool)
add_subdirectory(sqlite)
if(WITH_TCL)
  add_subdirectory(tcl)
endif()
add_subdirectory(tools)
add_subdirectory(plugin)
add_subdirectory(db)
add_subdirectory(tests/tools EXCLUDE_FROM_ALL)

enable_testing()
file(GLOB TESTS LIST_DIRECTORIES true "${PROJECT_SOURCE_DIR}/tests/*.test")
foreach(TEST IN ITEMS ${TESTS}) 
     get_filename_component(TEST ${TEST} NAME_WE)
     add_test(NAME ${TEST} COMMAND make ${TEST} WORKING_DIRECTORY "${PROJECT_SOURCE_DIR}/tests/")
endforeach()

########################
######## TODO ##########
########################
# comdb2admin
# supervisor
########################

set(CPACK_PACKAGE_NAME Comdb2)
set(CPACK_PACKAGE_FILE_NAME comdb2)
set(CPACK_PACKAGE_VERSION "7.0.0pre")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Comdb2 is a distributed RDBMS")
set(CPACK_PACKAGE_CONTACT "Comdb2 <opencomdb2@bloomberg.com>")
set(URL "http://github.com/bloomberg/comdb2")
set(CPACK_PACKAGE_VENDOR Bloomberg)

if(EXISTS /etc/debian_version)
  set(CPACK_GENERATOR "DEB")
  set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${URL})
  set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON) # auto detect dependencies
  set(CPACK_DEBIAN_PACKAGE_DEPENDS "tzdata, liblz4-tool") # additionally, depend on these
  file(MAKE_DIRECTORY pkg)
  configure_file(pkg/deb_post_install pkg/postinst @ONLY)
  configure_file(pkg/deb_pre_uninstall pkg/prerm COPYONLY)
  set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${PROJECT_BINARY_DIR}/pkg/postinst;${PROJECT_BINARY_DIR}/pkg/prerm")
  set(CPACK_DEBIAN_PACKAGE_SUGGESTS supervisor)
elseif(EXISTS /etc/redhat-release)
  set(CPACK_GENERATOR "RPM")
  set(CPACK_RPM_PACKAGE_REQUIRES lz4)
  file(MAKE_DIRECTORY pkg)
  configure_file(pkg/rpm_post_install pkg/rpm_post_install @ONLY)
  set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${PROJECT_BINARY_DIR}/pkg/rpm_post_install)
  set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE ${PROJECT_SOURCE_DIR}/pkg/rpm_pre_uninstall)
  set(CPACK_RPM_PACKAGE_URL ${URL})
  set(CPACK_RPM_PACKAGE_LICENSE Various) #Apache?
else()
  set(CPACK_GENERATOR "TGZ")
endif()

include(CPack)
