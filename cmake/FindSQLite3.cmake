find_path(SQLITE3_INCLUDE_DIR
  NAMES sqlite3.h
  HINTS ${SQLITE3_ROOT_DIR}
)
find_library(SQLITE3_LIBRARY
  NAMES sqlite3
  HINTS ${SQLITE3_ROOT_DIR}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(SQLite3 DEFAULT_MSG SQLITE3_INCLUDE_DIR SQLITE3_LIBRARY)
