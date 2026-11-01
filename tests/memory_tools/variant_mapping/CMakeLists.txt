# Configures only what decides a memory-tool build: the real
# cmake/Comdb2MemoryVariant.cmake, then the two settings it feeds, declared
# as the top-level CMakeLists.txt and Comdb2Sanitizers.cmake declare them.
# No compiler is needed. Used by test_variant_mapping.sh.
cmake_minimum_required(VERSION 3.16)
project(comdb2_memory_variant_mapping NONE)

list(APPEND CMAKE_MODULE_PATH ${CMAKE_CURRENT_LIST_DIR}/../../../cmake)
include(Comdb2MemoryVariant)
option(COMDB2_PER_THREAD_MALLOC "Turn OFF to run under Valgrind" ON)
include(Comdb2Sanitizers)
