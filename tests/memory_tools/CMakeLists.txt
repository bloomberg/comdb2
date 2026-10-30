# Deliberately faulty programs that prove a memory tool is active. Built only
# with -DCOMDB2_TEST=ON -DCOMDB2_MEMORY_PROBES=ON, never installed, never part
# of "all" or "test-tools". Instrumentation comes from the whole-build
# COMDB2_SANITIZER setting; do not add sanitizer flags here.
#
# Binaries land in <build-dir>/tests/memory_tools/memprobe_<name>, which is
# where selftest.sh looks for them.

set(memory-tool-probes)

macro(add_probe name)
  add_executable(memprobe_${name} probes/${name}.c)
  list(APPEND memory-tool-probes memprobe_${name})
endmacro()

add_probe(clean)
add_probe(heap_use_after_free)
add_probe(leak)
add_probe(signed_overflow)
add_probe(data_race)
add_probe(invalid_read)
add_probe(uninitialized_read)

# Only meaningful when Comdb2 subsystem allocators are plain malloc
# (COMDB2_PER_THREAD_MALLOC=OFF, i.e. USE_SYS_ALLOC); not expected otherwise.
if(NOT COMDB2_PER_THREAD_MALLOC)
  add_probe(comdb2_allocator_leak)
  target_include_directories(memprobe_comdb2_allocator_leak PRIVATE
    ${PROJECT_SOURCE_DIR}/mem
    ${PROJECT_BINARY_DIR}/berkdb # generated mem_berkdb.h
  )
endif()

add_custom_target(memory-tool-probes DEPENDS ${memory-tool-probes})
