# Canonical memory-tool build variants.
#
# COMDB2_MEMORY_VARIANT names a whole memory-tool build, so a caller (builddb,
# CI, a nightly job) passes only the name and never the settings behind it.
# This file is the only place that maps a variant to build settings:
#
#   variant          COMDB2_SANITIZER   COMDB2_PER_THREAD_MALLOC
#   asan-system      address-undefined  OFF
#   asan-native      address-undefined  ON
#   tsan-system      thread             OFF
#   valgrind-system  none               OFF
#
# Every variant also turns on COMDB2_MEMORY_TEST_HOOKS (the lifecycle markers
# tests/memory_tools needs from a server) and nothing else: no COMDB2_TEST, no
# probes, no fault injection. tests/memory_tools/config.sh holds the runtime
# settings for the same names; tests/memory_tools/test_variant_mapping.sh
# checks that the two agree.
#
# A variant owns its settings. Giving one of them as well is accepted only if
# it has the variant's value; anything else is a configure error rather than a
# guess about which was meant. That includes a build directory configured
# before for a different variant: use a fresh one.
#
# Include before COMDB2_PER_THREAD_MALLOC and COMDB2_SANITIZER are declared.

set(COMDB2_MEMORY_VARIANTS asan-system asan-native tsan-system valgrind-system)

set(COMDB2_MEMORY_VARIANT "" CACHE STRING
    "Canonical memory-tool build: asan-system, asan-native, tsan-system or valgrind-system (empty: none)")
set_property(CACHE COMDB2_MEMORY_VARIANT PROPERTY STRINGS "" ${COMDB2_MEMORY_VARIANTS})

# comdb2_memory_variant_settings(<variant> <sanitizer-var> <per-thread-malloc-var>)
# Sets the two variables to the variant's values, or to "" for an unknown name.
function(comdb2_memory_variant_settings variant sanitizer_var ptm_var)
  set(sanitizer "")
  set(ptm "")
  if(variant STREQUAL "asan-system")
    set(sanitizer address-undefined)
    set(ptm OFF)
  elseif(variant STREQUAL "asan-native")
    set(sanitizer address-undefined)
    set(ptm ON)
  elseif(variant STREQUAL "tsan-system")
    set(sanitizer thread)
    set(ptm OFF)
  elseif(variant STREQUAL "valgrind-system")
    set(sanitizer none)
    set(ptm OFF)
  endif()
  set(${sanitizer_var} "${sanitizer}" PARENT_SCOPE)
  set(${ptm_var} "${ptm}" PARENT_SCOPE)
endfunction()

function(comdb2_configure_memory_variant)
  set(variant "${COMDB2_MEMORY_VARIANT}")
  if("${variant}" STREQUAL "")
    return()
  endif()

  comdb2_memory_variant_settings("${variant}" sanitizer ptm)
  if("${sanitizer}" STREQUAL "")
    string(REPLACE ";" ", " allowed "${COMDB2_MEMORY_VARIANTS}")
    message(FATAL_ERROR
      "Unknown COMDB2_MEMORY_VARIANT value '${variant}'. Allowed values: ${allowed}")
  endif()

  # Values already in the cache: given with -D, or left by an earlier configure.
  set(conflicts "")
  if(DEFINED CACHE{COMDB2_SANITIZER})
    string(STRIP "${COMDB2_SANITIZER}" have)
    string(TOLOWER "${have}" have)
    if("${have}" STREQUAL "")
      set(have none)
    endif()
    if(NOT "${have}" STREQUAL "${sanitizer}")
      list(APPEND conflicts "COMDB2_SANITIZER=${COMDB2_SANITIZER} (${variant} needs ${sanitizer})")
    endif()
  endif()
  foreach(pair "COMDB2_PER_THREAD_MALLOC;${ptm}" "COMDB2_MEMORY_TEST_HOOKS;ON")
    list(GET pair 0 var)
    list(GET pair 1 want)
    if(DEFINED CACHE{${var}})
      if((${var} AND NOT want) OR (NOT ${var} AND want))
        list(APPEND conflicts "${var}=${${var}} (${variant} needs ${want})")
      endif()
    endif()
  endforeach()
  if(conflicts)
    string(REPLACE ";" "\n  " conflicts "${conflicts}")
    message(FATAL_ERROR
      "COMDB2_MEMORY_VARIANT=${variant} sets these itself, and they were given "
      "with other values:\n  ${conflicts}\n"
      "Drop them, or use a fresh build directory.")
  endif()

  set(COMDB2_SANITIZER ${sanitizer} CACHE STRING
      "Whole-build sanitizer: none, address-undefined or thread" FORCE)
  set(COMDB2_PER_THREAD_MALLOC ${ptm} CACHE BOOL "Turn OFF to run under Valgrind" FORCE)
  set(COMDB2_MEMORY_TEST_HOOKS ON CACHE BOOL
      "Server hooks for memory-tool test runs (clean-exit marker, leak checkpoint)" FORCE)
  message(STATUS "COMDB2_MEMORY_VARIANT: ${variant}")
endfunction()

comdb2_configure_memory_variant()

option(COMDB2_MEMORY_TEST_HOOKS
       "Server hooks for memory-tool test runs (clean-exit marker, leak checkpoint)" OFF)
option(COMDB2_MEMORY_FAULT_INJECTION
       "memdebug testleak: leak on request, to test the memory-tool harness (requires COMDB2_MEMORY_TEST_HOOKS)" OFF)
if(COMDB2_MEMORY_FAULT_INJECTION AND NOT COMDB2_MEMORY_TEST_HOOKS)
  message(FATAL_ERROR "COMDB2_MEMORY_FAULT_INJECTION requires COMDB2_MEMORY_TEST_HOOKS=ON")
endif()
if(COMDB2_MEMORY_TEST_HOOKS)
  add_definitions(-DCOMDB2_MEMORY_TEST_HOOKS)
endif()
if(COMDB2_MEMORY_FAULT_INJECTION)
  add_definitions(-DCOMDB2_MEMORY_FAULT_INJECTION)
endif()
