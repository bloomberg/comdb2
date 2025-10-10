cmake_minimum_required (VERSION 3.16)
project (Comdb2 ${COMDB2_BBPROJECT})

set(CMAKE_C_STANDARD 99)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "RelWithDebInfo" "Release")
if(NOT CMAKE_BUILD_TYPE)
  get_property(helpstring CACHE CMAKE_BUILD_TYPE PROPERTY HELPSTRING)
  set(CMAKE_BUILD_TYPE RelWithDebInfo CACHE STRING ${helpstring} FORCE)
endif()

if(COMDB2_BUILD_STATIC)
  SET(CMAKE_FIND_LIBRARY_SUFFIXES .a ${CMAKE_FIND_LIBRARY_SUFFIXES})
endif()

option(COMDB2_TEST "Turn ON to compile with testing features" ON)
if(COMDB2_TEST)
	add_definitions(-DCOMDB2_TEST=1)
else()
	add_definitions(-DCOMDB2_TEST=0)
endif()

option(WITH_TINFO "Turn ON on platforms where tinfo library is required for cdb2sql" OFF)
if(WITH_TINFO)
  add_definitions(-DWITH_TINFO=1)
else()
  add_definitions(-DWITH_TINFO=0)
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  if(EXISTS /opt/homebrew)
    list(APPEND CMAKE_PREFIX_PATH
      ${CMAKE_PREFIX_PATH}
      "/opt/homebrew"
      "/opt/homebrew/opt/openssl"
      "/opt/homebrew/opt/readline")
  else()
    list(APPEND CMAKE_PREFIX_PATH
      ${CMAKE_PREFIX_PATH}
      "/usr/local"
      "/usr/local/opt/openssl"
      "/usr/local/opt/readline")
  endif()
endif()

list(APPEND CMAKE_MODULE_PATH ${PROJECT_SOURCE_DIR}/cmake)

if(COMDB2_BBCMAKE)
  include(${COMDB2_BBCMAKE})
else()
  configure_file(bbinc/plhash.in bbinc/plhash.h COPYONLY)
endif()

find_package(LibEvent REQUIRED)
find_package(LZ4 REQUIRED)
find_package(OpenSSL REQUIRED)
set(PROTOBUF_C_MIN_VERSION 1.1.0)
find_package(Protobuf_C ${PROTOBUF_C_MIN_VERSION} REQUIRED)
if (NOT ${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  find_package(UUID REQUIRED)
endif()
if(${CMAKE_SYSTEM_NAME} STREQUAL Linux OR ${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  find_package(Unwind REQUIRED)
endif()
find_package(ZLIB REQUIRED)

option(COMDB2_LEGACY_DEFAULTS "Legacy defaults without lrl override" OFF)

if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  set(prefix /opt/bb)
  if(DEFINED ENV{COMDB2_ROOT})
    set(prefix $ENV{COMDB2_ROOT})
  endif()
  get_property(helpstring CACHE CMAKE_INSTALL_PREFIX PROPERTY HELPSTRING)
  set(CMAKE_INSTALL_PREFIX ${prefix} CACHE PATH ${helpstring} FORCE)
endif()

if(NOT DEFINED COMDB2_ROOT)
    set(COMDB2_ROOT ${CMAKE_INSTALL_PREFIX})
endif()
set(COMDB2_ROOT ${COMDB2_ROOT} CACHE PATH "Directory for runtime files" FORCE)

if(DEFINED CDB2_DNS_SUFFIX)
  add_definitions(-DCDB2_DNS_SUFFIX=${CDB2_DNS_SUFFIX})
endif()

add_definitions(
  -DCOMDB2_ROOT=${COMDB2_ROOT}
  -DCOMDB2_VERSION="2"
  -D_FILE_OFFSET_BITS=64
  -Dlint
)

option(DEBUG_TXNLIST_PLHASH "Debug plhash txnlist implementation" OFF)
mark_as_advanced(DEBUG_TXNLIST_PLHASH)
if(DEBUG_TXNLIST_PLHASH)
    add_definitions(-DDEBUG_TXNLIST_PLHASH)
endif()

option(DEBUG_STACK_TAG_SCHEMA "Stack at add or remove a tag-schema" OFF)
mark_as_advanced(DEBUG_STACK_TAG_SCHEMA)
if(DEBUG_STACK_TAG_SCHEMA)
    add_definitions(-DDEBUG_STACK_TAG_SCHEMA)
endif()

option(DEBUG_STACK_AT_TXN_LOG "Stack when writing a commit record" OFF)
mark_as_advanced(DEBUG_STACK_AT_TXN_LOG)
if(DEBUG_STACK_AT_TXN_LOG)
    add_definitions(-DDEBUG_STACK_AT_TXN_LOG)
endif()

option(DEBUG_STACK_AT_DB_OPEN_CLOSE "Save a cheap-stack at every db_open" OFF)
mark_as_advanced(DEBUG_STACK_AT_DB_OPEN_CLOSE)
if(DEBUG_STACK_AT_DB_OPEN_CLOSE)
    add_definitions(-DDEBUG_STACK_AT_DB_OPEN_CLOSE)
endif()

option(DEBUG_SCHEMA_LK "Assert a thread isn't holding berkley locks when acquiring schemalk" OFF)
mark_as_advanced(DEBUG_SCHEMA_LK)
if(DEBUG_SCHEMA_LK)
    add_definitions(-DDEBUG_SCHEMA_LK)
endif()

option(DEBUG_STACK_AT_DBREG_RECOVER "Print a cheapstack at dbreg recover operations" OFF)
mark_as_advanced(DEBUG_STACK_AT_DBREG_RECOVER)
if(DEBUG_STACK_AT_DBREG_RECOVER)
    add_definitions(-DDEBUG_STACK_AT_DBREG_RECOVER)
endif()

option(DEBUG_STACK_AT_DBREG_LOG "Print a cheapstack at dbreg log operations" OFF)
mark_as_advanced(DEBUG_STACK_AT_DBREG_LOG)
if(DEBUG_STACK_AT_DBREG_LOG)
    add_definitions(-DDEBUG_STACK_AT_DBREG_LOG)
endif()

option(DEBUG_ABORT_ON_RECOVERY_FAILURE "Abort recovery failure" OFF)
mark_as_advanced(DEBUG_ABORT_ON_RECOVERY_FAILURE)
if(DEBUG_ABORT_ON_RECOVERY_FAILURE)
    add_definitions(-DDEBUG_ABORT_ON_RECOVERY_FAILURE)
endif()

option(DEBUG_SC "Enable schema-change debug trace" OFF)
mark_as_advanced(DEBUG_SC)
if(DEBUG_SC)
    add_definitions(-DDEBUG_SC)
endif()

option(DEBUG_PGFREE_ZERO "Enable pgfree-0 asserts" OFF)
mark_as_advanced(DEBUG_PGFREE_ZERO)
if(DEBUG_PGFREE_ZERO)
    add_definitions(-DDEBUG_PGFREE_ZERO)
endif()

option(DEBUG_PREPARE "Enable prepare debug trace" OFF)
mark_as_advanced(DEBUG_PREPARE)
if(DEBUG_PREPARE)
    add_definitions(-DDEBUG_PREPARE)
endif()

option(DEBUG_RECOVERY_LOCK "Enable recovery-lock debug trace" OFF)
mark_as_advanced(DEBUG_RECOVERY_LOCK)
if(DEBUG_RECOVERY_LOCK)
    add_definitions(-DDEBUG_RECOVERY_LOCK)
endif()

option(UFID_HASH_DEBUG "Enable ufid-hash debug trace" OFF)
mark_as_advanced(UFID_HASH_DEBUG)
if(UFID_HASH_DEBUG)
  add_definitions(-DUFID_HASH_DEBUG)
endif()

option(COMDB2_PER_THREAD_MALLOC "Turn OFF to run under Valgrind" ON)
if(COMDB2_PER_THREAD_MALLOC)
  add_definitions(-DPER_THREAD_MALLOC)
else()
  add_definitions(-DUSE_SYS_ALLOC)
endif()

message("CMAKE_SYSTEM_PROCESSOR ${CMAKE_SYSTEM_PROCESSOR}")
message("CMAKE_SYSTEM_NAME ${CMAKE_SYSTEM_NAME}")
message("CMAKE_C_COMPILER_ID ${CMAKE_C_COMPILER_ID}")

# _LINUX_SOURCE is mostly misused as little-endian
if(${CMAKE_SYSTEM_PROCESSOR} STREQUAL x86_64 OR ${CMAKE_SYSTEM_PROCESSOR} STREQUAL aarch64 OR ${CMAKE_SYSTEM_PROCESSOR} STREQUAL arm64)
  add_definitions(-D_LINUX_SOURCE)
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
  add_definitions(-D_GNU_SOURCE)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lrt")
elseif(${CMAKE_SYSTEM_NAME} STREQUAL SunOS)
  add_definitions(
    -D_SUN_SOURCE
    -D_POSIX_PTHREAD_SEMANTICS
    -D__USE_SUNOS_SOCKETS__
  )
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  add_definitions(
    -D_DARWIN_C_SOURCE
    -D_XOPEN_SOURCE=600
  )
  set(CMAKE_CXX_STANDARD 17)
endif()

option(GENERATE_COVERAGE "Enable gcov/lcov coverage" OFF)
function(configure_coverage)
  message(STATUS "Configuring gcov/lcov coverage flags")
  add_link_options(--coverage)
  add_compile_definitions(GENERATE_COVERAGE)
  # Ensure debug info, disable optimizations, and disable inlining for accurate coverage
  add_compile_options(-g -O0 --coverage -fno-inline)
endfunction()

if (${CMAKE_C_COMPILER_ID} STREQUAL GNU OR ${CMAKE_C_COMPILER_ID} STREQUAL Clang OR ${CMAKE_C_COMPILER_ID} STREQUAL AppleClang)
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Werror")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wignored-qualifiers")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-switch")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-unused-function")
  if (${CMAKE_C_COMPILER_ID} STREQUAL GNU)
    set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wno-stringop-truncation")
  endif()
  set(COMMON_FLAGS "${COMMON_FLAGS} -pthread")
  set(COMMON_FLAGS "${COMMON_FLAGS} -fno-strict-aliasing")
  if(GENERATE_COVERAGE)
    configure_coverage()
  endif()
elseif(${CMAKE_C_COMPILER_ID} STREQUAL SunPro)
  set(COMMON_FLAGS "${COMMON_FLAGS} -mt=yes")
  set(COMMON_FLAGS "${COMMON_FLAGS} -fma=fused")
  set(COMMON_FLAGS "${COMMON_FLAGS} -xmemalign=8i")
  set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -errfmt=error")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lsocket")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lnsl")
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lresolv")
endif()

set(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} ${COMMON_FLAGS}")
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${COMMON_FLAGS}")

add_subdirectory(archive)
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
add_subdirectory(util)
if(NOT COMDB2_SKIP_TOOLS)
  add_subdirectory(tools)
endif()
add_subdirectory(plugins)
add_subdirectory(db)
if(NOT COMDB2_SKIP_TESTS)
  add_subdirectory(tests/tools EXCLUDE_FROM_ALL)
endif()

set(CPACK_PACKAGE_NAME Comdb2)
set(CPACK_PACKAGE_FILE_NAME comdb2)
set(CPACK_PACKAGE_VERSION "7.0.0pre")
set(CPACK_PACKAGING_INSTALL_PREFIX ${CMAKE_INSTALL_PREFIX})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Comdb2 is a distributed RDBMS")
set(CPACK_PACKAGE_CONTACT "Comdb2 <opencomdb2@bloomberg.com>")
set(URL "http://github.com/bloomberg/comdb2")
set(CPACK_PACKAGE_VENDOR Bloomberg)

set(helpstring "Choose the type of package, options are: deb, rpm, tgz")
if(NOT COMDB2_PKG_TYPE)
  if(EXISTS /etc/debian_version)
    set(COMDB2_PKG_TYPE deb CACHE STRING "${helpstring}" FORCE)
  elseif(EXISTS /etc/redhat-release)
    set(COMDB2_PKG_TYPE rpm CACHE STRING "${helpstring}" FORCE)
  else()
    set(COMDB2_PKG_TYPE tgz CACHE STRING "${helpstring}" FORCE)
  endif()
endif()
set(COMDB2_PKG_TYPE ${COMDB2_PKG_TYPE} CACHE STRING "${helpstring}" FORCE)
set_property(CACHE COMDB2_PKG_TYPE PROPERTY STRINGS "deb" "rpm" "tgz")

if(${COMDB2_PKG_TYPE} STREQUAL deb)
  set(CPACK_GENERATOR "DEB")
  set(CPACK_DEBIAN_PACKAGE_HOMEPAGE ${URL})
  set(CPACK_DEBIAN_PACKAGE_SHLIBDEPS ON) # auto detect dependencies
  set(CPACK_DEBIAN_PACKAGE_DEPENDS "tzdata, liblz4-tool") # additionally, depend on these
  file(MAKE_DIRECTORY pkg)
  configure_file(pkg/deb_post_install pkg/postinst @ONLY)
  configure_file(pkg/deb_pre_uninstall pkg/prerm COPYONLY)
  set(CPACK_DEBIAN_PACKAGE_CONTROL_EXTRA "${PROJECT_BINARY_DIR}/pkg/postinst;${PROJECT_BINARY_DIR}/pkg/prerm")
  set(CPACK_DEBIAN_PACKAGE_SUGGESTS supervisor)
elseif(${COMDB2_PKG_TYPE} STREQUAL rpm)
  set(CPACK_GENERATOR "RPM")
  set(CPACK_RPM_PACKAGE_REQUIRES lz4)
  file(MAKE_DIRECTORY pkg)
  configure_file(pkg/rpm_post_install pkg/rpm_post_install @ONLY)
  set(CPACK_RPM_POST_INSTALL_SCRIPT_FILE ${PROJECT_BINARY_DIR}/pkg/rpm_post_install)
  set(CPACK_RPM_PRE_UNINSTALL_SCRIPT_FILE ${PROJECT_SOURCE_DIR}/pkg/rpm_pre_uninstall)
  set(CPACK_RPM_PACKAGE_URL ${URL})
  set(CPACK_RPM_PACKAGE_LICENSE Various) #Apache?
elseif(${COMDB2_PKG_TYPE} STREQUAL tgz)
  set(CPACK_GENERATOR "TGZ")
endif()

include(CPack)
