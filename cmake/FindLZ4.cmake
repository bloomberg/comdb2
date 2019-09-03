find_path(LZ4_INCLUDE_DIR
  NAMES lz4.h
  HINTS ${LZ4_ROOT_DIR}
)
find_library(LZ4_LIBRARY
  NAMES lz4
  HINTS ${LZ4_ROOT_DIR}
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(LZ4 DEFAULT_MSG LZ4_INCLUDE_DIR)
find_package_handle_standard_args(liblz4 DEFAULT_MSG LZ4_LIBRARY)
