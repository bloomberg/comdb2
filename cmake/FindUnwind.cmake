include(${CMAKE_MODULE_PATH}/pkg_helper.cmake)
set(lib unwind)
if(${CMAKE_SYSTEM_NAME} STREQUAL "Darwin")
  set(lib System)
endif()
find_pkg_for_comdb2(Unwind
  "libunwind.h"
  "${lib}"
  "${UNWIND_ROOT_DIR}"
  ""
  UNWIND_INCLUDE_DIR
  UNWIND_LIBRARY
)
mark_as_advanced(UNWIND_INCLUDE_DIR UNWIND_LIBRARY)
