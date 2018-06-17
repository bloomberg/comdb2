macro(find_pkg_for_comdb2 PKG HDR LIB ROOT_DIR SUFFIX_LIST INCLUDE_DIR LIBRARY)
  find_path(${INCLUDE_DIR}
    NAMES ${HDR}
    HINTS ${ROOT_DIR}
    PATH_SUFFIXES ${SUFFIX_LIST}
  )
  find_library(${LIBRARY}
    NAMES ${LIB}
    HINTS ${ROOT_DIR}
    PATH_SUFFIXES ${SUFFIX_LIST}
  )
  include(FindPackageHandleStandardArgs)
  find_package_handle_standard_args(${PKG} DEFAULT_MSG
    ${INCLUDE_DIR} ${LIBRARY})
endmacro()
