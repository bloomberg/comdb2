include(${CMAKE_MODULE_PATH}/pkg_helper.cmake)
find_pkg_for_comdb2(Protobuf_C
  "protobuf-c/protobuf-c.h"
  "protobuf-c"
  "${PROTOBUF_C_ROOT_DIR}"
  ""
  PROTOBUF_C_INCLUDE_DIR
  PROTOBUF_C_LIBRARY
)
mark_as_advanced(PROTOBUF_C_INCLUDE_DIR PROTOBUF_C_LIBRARY)
