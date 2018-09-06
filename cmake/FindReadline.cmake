include(${CMAKE_MODULE_PATH}/pkg_helper.cmake)
find_pkg_for_comdb2(Readline
  "readline/readline.h"
  "readline"
  "${READLINE_ROOT_DIR}"
  ""
  READLINE_INCLUDE_DIR
  READLINE_LIBRARY
)
mark_as_advanced(READLINE_INCLUDE_DIR READLINE_LIBRARY)
