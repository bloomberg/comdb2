find_path(UUID_INCLUDE_DIR
  NAMES uuid/uuid.h
  HINTS ${UUID_ROOT_DIR}
)
find_library(UUID_LIBRARY
  NAMES uuid
  HINTS ${UUID_ROOT_DIR}
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(UUID DEFAULT_MSG UUID_INCLUDE_DIR)
find_package_handle_standard_args(libuuid DEFAULT_MSG UUID_LIBRARY)
