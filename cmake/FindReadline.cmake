find_path(READLINE_INCLUDE_DIR
  NAMES readline/readline.h
  HINTS ${READLINE_ROOT_DIR}
)
find_library(READLINE_LIBRARY
  NAMES readline
  HINTS ${READLINE_ROOT_DIR}
)
find_library(NCURSES_LIBRARY
  NAMES ncurses
  HINTS ${NCURSES_ROOT_DIR}
)
find_library(TINFO_LIBRARY
  NAMES tinfo
  HINTS ${TINFO_ROOT_DIR}
)
include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(Readline DEFAULT_MSG READLINE_INCLUDE_DIR)
find_package_handle_standard_args(libreadline DEFAULT_MSG READLINE_LIBRARY)
find_package_handle_standard_args(libncurses  DEFAULT_MSG NCURSES_LIBRARY)
find_package_handle_standard_args(libtinfo DEFAULT_MSG TINFO_LIBRARY)
list(APPEND READLINE_LIBRARIES ${READLINE_LIBRARY} ${NCURSES_LIBRARY} ${TINFO_LIBRARY})
