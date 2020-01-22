include(FindPackageHandleStandardArgs)

find_path(LIBEVENT_INCLUDE_DIR NAMES event2/event.h)
find_package_handle_standard_args(LIBEVENT DEFAULT_MSG LIBEVENT_INCLUDE_DIR)

find_library(LIBEVENT_CORE_LIBRARY NAMES event_core)
find_package_handle_standard_args(libevent_core DEFAULT_MSG LIBEVENT_CORE_LIBRARY)

if(WITH_SSL)
  find_library(LIBEVENT_OPENSSL_LIBRARY NAMES event_openssl)
  find_package_handle_standard_args(libevent_openssl DEFAULT_MSG LIBEVENT_OPENSSL_LIBRARY)
endif()

find_library(LIBEVENT_PTHREADS_LIBRARY NAMES event_pthreads)
find_package_handle_standard_args(libevent_pthreads DEFAULT_MSG LIBEVENT_PTHREADS_LIBRARY)

list(APPEND LIBEVENT_LIBRARIES ${LIBEVENT_CORE_LIBRARY} ${LIBEVENT_OPENSSL_LIBRARY} ${LIBEVENT_PTHREADS_LIBRARY})
