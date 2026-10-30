# Whole-build sanitizer instrumentation.
#
# This file is the only place that maps COMDB2_SANITIZER to compiler and
# linker flags. Callers (tests/memory_tools, CI, nightly jobs) select a mode by
# value and must not repeat the flags.
#
#   none               no instrumentation (default)
#   address-undefined  AddressSanitizer + UndefinedBehaviorSanitizer; LeakSanitizer
#                      is part of the ASan runtime on Linux and is enabled at run
#                      time via ASAN_OPTIONS=detect_leaks=1. UBSan is built
#                      recoverable: whether a report stops the program is the
#                      runtime UBSAN_OPTIONS=halt_on_error setting (config.sh)
#   thread             ThreadSanitizer
#
# Allocator selection is orthogonal and stays with COMDB2_PER_THREAD_MALLOC.
#
# Flags are appended to CMAKE_C_FLAGS/CMAKE_CXX_FLAGS (not add_compile_options)
# so that other enabled languages are not affected, and to the executable,
# shared and module linker flags so every C/C++ link pulls in the runtime.

set(COMDB2_SANITIZER_VALUES none address-undefined thread)

set(COMDB2_SANITIZER none CACHE STRING
    "Whole-build sanitizer: none, address-undefined or thread")
set_property(CACHE COMDB2_SANITIZER PROPERTY STRINGS ${COMDB2_SANITIZER_VALUES})

function(comdb2_configure_sanitizer)
  string(STRIP "${COMDB2_SANITIZER}" mode)
  string(TOLOWER "${mode}" mode)
  if(mode STREQUAL "")
    set(mode none)
  endif()
  set(COMDB2_SANITIZER ${mode} CACHE STRING
      "Whole-build sanitizer: none, address-undefined or thread" FORCE)

  list(FIND COMDB2_SANITIZER_VALUES "${mode}" idx)
  if(idx EQUAL -1)
    string(REPLACE ";" ", " allowed "${COMDB2_SANITIZER_VALUES}")
    message(FATAL_ERROR
      "Unknown COMDB2_SANITIZER value '${COMDB2_SANITIZER}'. Allowed values: ${allowed}")
  endif()

  message(STATUS "COMDB2_SANITIZER: ${mode}")
  message(STATUS "COMDB2_PER_THREAD_MALLOC: ${COMDB2_PER_THREAD_MALLOC}")

  if(mode STREQUAL none)
    return()
  endif()

  foreach(lang C CXX)
    if(CMAKE_${lang}_COMPILER_ID AND
       NOT CMAKE_${lang}_COMPILER_ID STREQUAL GNU AND
       NOT CMAKE_${lang}_COMPILER_ID STREQUAL Clang)
      message(FATAL_ERROR
        "COMDB2_SANITIZER=${mode} requires GNU or Clang; "
        "CMAKE_${lang}_COMPILER_ID is ${CMAKE_${lang}_COMPILER_ID}")
    endif()
  endforeach()

  if(mode STREQUAL address-undefined)
    set(sanitize -fsanitize=address,undefined)
    # Recoverable, so a whole-server run can report every UBSan finding and
    # keep going; berkdb hits several (e.g. misaligned log writes) on startup.
    set(compile_flags ${sanitize} -fno-omit-frame-pointer -fno-optimize-sibling-calls
                      -fsanitize-recover=undefined)
  else()
    set(sanitize -fsanitize=thread)
    set(compile_flags ${sanitize} -fno-omit-frame-pointer -fno-optimize-sibling-calls)
  endif()

  # Fail at configure time, not deep into the build, if the runtime is missing.
  include(CheckCCompilerFlag)
  string(MAKE_C_IDENTIFIER "COMDB2_SANITIZER_WORKS_${mode}" works)
  set(CMAKE_REQUIRED_LINK_OPTIONS ${sanitize})
  check_c_compiler_flag("${compile_flags}" ${works})
  unset(CMAKE_REQUIRED_LINK_OPTIONS)
  if(NOT ${works})
    message(FATAL_ERROR
      "COMDB2_SANITIZER=${mode}: ${CMAKE_C_COMPILER} cannot compile and link with "
      "'${compile_flags}'. Is the sanitizer runtime installed?")
  endif()

  string(REPLACE ";" " " compile_flags "${compile_flags}")

  # The build runs instrumented generators (lemon, mkkeywordhash, ...), so the
  # runtime must also be able to start on this host. ASan and TSan reserve
  # terabytes of address space, which fails under ulimit -v or with strict
  # overcommit (vm.overcommit_memory=2). Re-checked on every configure.
  if(NOT CMAKE_CROSSCOMPILING)
    include(CheckCSourceRuns)
    string(MAKE_C_IDENTIFIER "COMDB2_SANITIZER_STARTS_${mode}" starts)
    unset(${starts} CACHE)
    set(CMAKE_REQUIRED_FLAGS "${compile_flags}")
    set(CMAKE_REQUIRED_LINK_OPTIONS ${sanitize})
    check_c_source_runs("int main(void) { return 0; }" ${starts})
    unset(CMAKE_REQUIRED_FLAGS)
    unset(CMAKE_REQUIRED_LINK_OPTIONS)
    if(NOT ${starts})
      set(overcommit "unknown")
      if(EXISTS /proc/sys/vm/overcommit_memory)
        file(STRINGS /proc/sys/vm/overcommit_memory overcommit)
      endif()
      message(FATAL_ERROR
        "COMDB2_SANITIZER=${mode}: an instrumented program cannot start on this host. "
        "The sanitizer runtime reserves terabytes of virtual address space; check "
        "'ulimit -v' and vm.overcommit_memory (currently ${overcommit}; 2 = strict "
        "overcommit, which prevents it).")
    endif()
  endif()

  # Instrumentation changes GCC's flow analysis. Only warnings shown to fail a
  # sanitized build are made non-fatal, and only for that mode; they are still
  # reported. Add an entry only with evidence.
  if(CMAKE_C_COMPILER_ID STREQUAL GNU)
    if(mode STREQUAL address-undefined)
      # GCC 14/15: false positive in berkdb's P_ENTRY/P_INP, whose (pg) ? .. : 0
      # gives GCC a NULL-page path ("source object is likely at address zero").
      string(APPEND compile_flags " -Wno-error=array-bounds")
      # GCC 13/14: recoverable UBSan non-null checks leave a NULL path past the
      # check, reported as "null format string" (csc2/macc_so.c vsnprintf) and
      # "null destination pointer" (bdb/file.c, util/roll_file.c sprintf).
      string(APPEND compile_flags " -Wno-error=format-truncation -Wno-error=format-overflow")
    elseif(mode STREQUAL thread)
      # GCC 13/14: db/fdb_fend.c snprintf()s an int into a 3-byte buffer. Real
      # truncation for values >= 100, to be fixed separately; remove this then.
      string(APPEND compile_flags " -Wno-error=format-truncation")
    endif()
  endif()

  foreach(var CMAKE_C_FLAGS CMAKE_CXX_FLAGS)
    set(${var} "${${var}} ${compile_flags}" PARENT_SCOPE)
  endforeach()
  foreach(var CMAKE_EXE_LINKER_FLAGS CMAKE_SHARED_LINKER_FLAGS CMAKE_MODULE_LINKER_FLAGS)
    set(${var} "${${var}} ${sanitize}" PARENT_SCOPE)
  endforeach()
  message(STATUS "COMDB2_SANITIZER compile flags: ${compile_flags}")
  message(STATUS "COMDB2_SANITIZER link flags: ${sanitize}")
endfunction()
