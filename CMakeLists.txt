cmake_minimum_required (VERSION 3.1)
project (Comdb2)

if(NOT CMAKE_BUILD_TYPE)
  get_property(helpstring CACHE CMAKE_BUILD_TYPE PROPERTY HELPSTRING)
  set(CMAKE_BUILD_TYPE Debug CACHE STRING ${helpstring} FORCE)
  set_property(CACHE CMAKE_BUILD_TYPE PROPERTY STRINGS "Debug" "RelWithDebInfo" "Release")
endif()
#if(${CMAKE_BUILD_TYPE} STREQUAL Release)
#-g
#endif()

# Find required libraries
set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} ${PROJECT_SOURCE_DIR}/cmake)
if(${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  # Homebrew will install here by default
  set(COMDB2_FIND_PKG_ROOT_DIR /usr/local)
  # Homebrew will not shadow macOS native libs; however, we need them
  set(OPENSSL_ROOT_DIR /usr/local/opt/openssl)
  set(READLINE_ROOT_DIR /usr/local/opt/readline)
endif()
set(CMAKE_THREAD_PREFER_PTHREAD TRUE)
set(THREADS_PREFER_PTHREAD_FLAG TRUE)
find_package(LZ4 REQUIRED)
find_package(OpenSSL REQUIRED)
find_package(Protobuf_C REQUIRED)
find_package(Readline REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(Threads REQUIRED)
find_package(UUID REQUIRED)
find_package(Unwind REQUIRED)
find_package(ZLIB REQUIRED)

set(CMAKE_C_STANDARD 99)
set(CMAKE_C_STANDARD_REQUIRED TRUE)

set(CMAKE_CXX_STANDARD 11)
set(CMAKE_CXX_STANDARD_REQUIRED TRUE)

if(${CMAKE_SYSTEM_PROCESSOR} STREQUAL x86_64)
  # This is mostly misused as little-endian
  add_definitions(-D_LINUX_SOURCE)
endif()

if(${CMAKE_SYSTEM_NAME} STREQUAL Linux)
  #-Wall -Wno-unused-variable -Wno-unused-but-set-variable -Wno-unused-function -Wno-unused-label
  #-Werror -Wno-parentheses
  # probably don't need this after logmsg
  #-fno-builtin
  # Make this conditional if Release (O3)
  #-fno-strict-aliasing
  add_definitions(-D_GNU_SOURCE)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
  add_definitions(-D_DARWIN_C_SOURCE)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL AIX)
  #CFLAGS_DEFS=-D__VACPP_MULTI__ -DMAXHOSTNAMELEN=64 -D_H_SYSMACROS
  #CFLAGS_MISC=-qlanglvl=extc99 -qtls -qcpluscmt -qthreaded  -qdfp -qthreaded -qchars=signed -qro -qroconst -qkeyword=inline -qhalt=e -qxflag=dircache:71,256 -qxflag=new_pragma_comment_user -qxflag=NoKeepDebugMetaTemplateType -qfuncsect
  #CFLAGS_DEBUGGING=-g
  #CFLAGS_64+=-q64
  #LDFLAGS+=-q64 -bmaxdata:0xb0000000/dsa -bbigtoc  -brtl -qtwolink
  add_definitions(-D_IBM_SOURCE)
elseif(${CMAKE_SYSTEM_NAME} STREQUAL SunOS)
  #OPT_CFLAGS=-xO3 -xprefetch=auto,explicit
  #CFLAGS_MISC=-xtarget=generic -fma=fused -xinline=%auto -xmemalign=8s
  #CFLAGS=-mt -xtarget=generic -xc99=all -errfmt=error -K PIC
  #add_definitions(-D__EXTENSIONS__)
  add_definitions(-D_SUN_SOURCE)
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
  -DBB64BIT
  -DBB_THREADED
  -DCOMDB2_VERSION="2"
  -DMONITOR_STACK
  -D_REENTRANT
  -D_THREAD_SAFE
  -D_XOPEN_SOURCE=600
)

option(COMDB2_PER_THREAD_MALLOC "Turn OFF to run under Valgrind" ON)
if(COMDB2_PER_THREAD_MALLOC)
  add_definitions(-DPER_THREAD_MALLOC)
else()
  add_definitions(-DUSE_SYS_ALLOC)
endif()

add_subdirectory(bb)
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
add_subdirectory(tools)
add_subdirectory(db)
add_subdirectory(tests/tools EXCLUDE_FROM_ALL)

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
