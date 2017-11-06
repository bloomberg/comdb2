macro(find_pkg_for_comdb2 PKG HDR LIB ROOT_DIR INCLUDE_DIR LIBRARY)
  find_path(${INCLUDE_DIR}
    NAMES ${HDR}
    HINTS ${ROOT_DIR} ${COMDB2_FIND_PKG_ROOT_DIR}
    PATH_SUFFIXES include
  )
  find_library(${LIBRARY}
    NAMES ${LIB}
    HINTS ${ROOT_DIR} ${COMDB2_FIND_PKG_ROOT_DIR}
    PATH_SUFFIXES lib lib64
  )
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(${PKG} DEFAULT_MSG
    ${INCLUDE_DIR} ${LIBRARY})
endmacro()
