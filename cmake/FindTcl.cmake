include(${CMAKE_MODULE_PATH}/pkg_helper.cmake)
find_pkg_for_comdb2(Tcl
  "tcl.h"
  "tclstub8.6;tclstub8.5"
  "${TCL_ROOT_DIR}"
  "tcl8.6;tcl8.5"
  TCL_INCLUDE_DIR
  TCL_LIBRARY_FILE
)
mark_as_advanced(TCL_INCLUDE_DIR TCL_LIBRARY_FILE)
