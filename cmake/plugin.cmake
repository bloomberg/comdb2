macro(add_static_plugin PLUGIN SOURCES)
  set(PLUGIN_SYM "comdb2_plugin_${PLUGIN}")
  configure_file(plugin.h.in plugin.h @ONLY)
  add_library(${PLUGIN} STATIC ${SOURCES})
  set(PLUGINS "${PLUGINS};${PLUGIN}"
      CACHE STRING
      "List of static plugins"
      FORCE)
  set(PLUGIN_LIBRARIES "${PLUGIN_LIBRARIES};${PLUGIN}"
      CACHE STRING
      "List of static plugin library paths"
      FORCE)
endmacro()

macro(add_dynamic_plugin PLUGIN SOURCES)
  set(PLUGIN_SYM "comdb2_plugin")
  configure_file(plugin.h.in plugin.h @ONLY)
  add_library(${PLUGIN} SHARED ${SOURCES})
  install(TARGETS ${PLUGIN} LIBRARY DESTINATION lib/plugin)
endmacro()

macro(add_plugin PLUGIN TYPE SOURCES)
  string(TOLOWER ${TYPE} _TYPE)
  string(TOLOWER ${COMDB2_PLUGIN_TYPE} _COMDB2_PLUGIN_TYPE)

  if (${_COMDB2_PLUGIN_TYPE} STREQUAL "preferred")
    if(${_TYPE} STREQUAL "static")
      add_static_plugin(${PLUGIN} "${SOURCES}")
    elseif(${_TYPE} STREQUAL "dynamic")
      add_dynamic_plugin(${PLUGIN} "${SOURCES}")
    else()
      message(FATAL_ERROR "Unsupported plugin type: ${TYPE}")
    endif()
  elseif(${_COMDB2_PLUGIN_TYPE} STREQUAL "static")
    add_static_plugin(${PLUGIN} "${SOURCES}")
  elseif(${_COMDB2_PLUGIN_TYPE} STREQUAL "dynamic")
    add_dynamic_plugin(${PLUGIN} "${SOURCES}")
  else()
    message(FATAL_ERROR "Unsupported COMDB2_PLUGIN_TYPE : ${COMDB2_PLUGIN_TYPE}")
  endif()
endmacro()
