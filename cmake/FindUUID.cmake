include(${CMAKE_MODULE_PATH}/pkg_helper.cmake)
find_pkg_for_comdb2(UUID
  "uuid/uuid.h"
  "uuid"
  "${UUID_ROOT_DIR}"
  ""
  UUID_INCLUDE_DIR
  UUID_LIBRARY
)
mark_as_advanced(UUID_INCLUDE_DIR UUID_LIBRARY)
