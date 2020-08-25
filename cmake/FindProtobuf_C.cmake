find_path(PROTOBUF-C_INCLUDE_DIR
  NAMES protobuf-c/protobuf-c.h
  HINTS ${PROTOBUF-C_ROOT_DIR}
)
find_library(PROTOBUF-C_LIBRARY
  NAMES protobuf-c
  HINTS ${PROTOBUF-C_ROOT_DIR}
)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Protobuf_C DEFAULT_MSG PROTOBUF-C_INCLUDE_DIR PROTOBUF-C_LIBRARY)
