include(${CMAKE_MODULE_PATH}/pkg_helper.cmake)
find_pkg_for_comdb2(SQLite3
  "sqlite3.h"
  "sqlite3"
  "${SQLITE3_ROOT_DIR}"
  ""
  SQLITE3_INCLUDE_DIR
  SQLITE3_LIBRARY 
)
mark_as_advanced(SQLITE3_INCLUDE_DIR SQLITE3_LIBRARY)
