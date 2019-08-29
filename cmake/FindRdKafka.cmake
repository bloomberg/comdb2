find_path(RDKAFKA_INCLUDE_DIR
  NAMES librdkafka/rdkafka.h
  HINTS ${RDKAFKA_ROOT_DIR}
)
find_library(RDKAFKA_LIBRARY
  NAMES rdkafka
  HINTS ${RDKAFKA_ROOT_DIR}
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(RDKAFKA DEFAULT_MSG RDKAFKA_INCLUDE_DIR)
find_package_handle_standard_args(librdkafka DEFAULT_MSG RDKAFKA_LIBRARY)
