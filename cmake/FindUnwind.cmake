find_path(UNWIND_INCLUDE_DIR
  NAMES libunwind.h
  HINTS ${UNWIND_ROOT_DIR}
)
find_library(UNWIND_LIBRARY
  NAMES unwind System
  HINTS ${UNWIND_ROOT_DIR}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Unwind DEFAULT_MSG UNWIND_INCLUDE_DIR UNWIND_LIBRARY)
