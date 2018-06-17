include(${CMAKE_MODULE_PATH}/pkg_helper.cmake)
find_pkg_for_comdb2(LZ4
  "lz4.h"
  "lz4"
  "${LZ4_ROOT_DIR}"
  ""
  LZ4_INCLUDE_DIR
  LZ4_LIBRARY
)
mark_as_advanced(LZ4_INCLUDE_DIR LZ4_LIBRARY)
