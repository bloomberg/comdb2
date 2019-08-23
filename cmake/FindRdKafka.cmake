include(${CMAKE_MODULE_PATH}/pkg_helper.cmake)
find_pkg_for_comdb2(RdKafka
  "librdkafka/rdkafka.h"
  "rdkafka"
  "${RDKAFKA_ROOT_DIR}"
  ""
  RDKAFKA_INCLUDE_DIR
  RDKAFKA_LIBRARY
)
mark_as_advanced(RDKAFKA_INCLUDE_DIR RDKAFKA_LIBRARY)
