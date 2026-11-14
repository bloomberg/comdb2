# The Tcl in the macOS SDK only has arm64e/x86_64 slices, which an arm64 build
# cannot link against.  Prefer Homebrew's tcl-tk@8 (or tcl-tk) when present,
# and forget an SDK Tcl that an earlier configure cached.
if(APPLE)
  if(TCL_STUB_LIBRARY MATCHES "\\.sdk/")
    foreach(var TCL_STUB_LIBRARY TCL_INCLUDE_PATH TCL_LIBRARY TCL_TCLSH TK_STUB_LIBRARY TK_INCLUDE_PATH TK_LIBRARY TK_WISH)
      unset(${var} CACHE)
    endforeach()
  endif()
  find_program(BREW_EXECUTABLE brew)
  if(BREW_EXECUTABLE AND NOT TCL_STUB_LIBRARY)
    foreach(formula tcl-tk@8 tcl-tk)
      execute_process(COMMAND ${BREW_EXECUTABLE} --prefix --installed ${formula}
                      OUTPUT_VARIABLE brew_tcl_prefix OUTPUT_STRIP_TRAILING_WHITESPACE
                      RESULT_VARIABLE brew_rc ERROR_QUIET)
      if(brew_rc EQUAL 0 AND EXISTS "${brew_tcl_prefix}/lib")
        list(PREPEND CMAKE_PREFIX_PATH "${brew_tcl_prefix}")
        set(CMAKE_FIND_FRAMEWORK LAST)
        find_program(TCL_TCLSH NAMES tclsh8.6 tclsh PATHS "${brew_tcl_prefix}/bin" NO_DEFAULT_PATH)
        find_path(TCL_INCLUDE_PATH tcl.h PATHS "${brew_tcl_prefix}/include/tcl-tk" "${brew_tcl_prefix}/include"
                  NO_DEFAULT_PATH)
        find_library(TCL_LIBRARY NAMES tcl8.6 tcl PATHS "${brew_tcl_prefix}/lib" NO_DEFAULT_PATH)
        find_library(TCL_STUB_LIBRARY NAMES tclstub8.6 tclstub PATHS "${brew_tcl_prefix}/lib" NO_DEFAULT_PATH)
        break()
      endif()
    endforeach()
  endif()
endif()

find_package(TclStub)

add_library(tclcdb2 SHARED tclcdb2.c)
add_definitions(-DUSE_TCL_STUBS)
add_dependencies(tclcdb2 cdb2api)

configure_file(${CMAKE_CURRENT_SOURCE_DIR}/helper.tcl helper.tcl COPYONLY)
configure_file(${CMAKE_CURRENT_SOURCE_DIR}/pkgIndex.tcl pkgIndex.tcl COPYONLY)

include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${PROJECT_SOURCE_DIR}/cdb2api
  ${TCL_INCLUDE_PATH}
)

target_link_libraries(tclcdb2
  cdb2api
  ${TCL_STUB_LIBRARY}
  ${OPENSSL_LIBRARIES}
  ${ZLIB_LIBRARIES}
  ${PROTOBUF-C_LIBRARY}
  ${UNWIND_LIBRARY}
)
