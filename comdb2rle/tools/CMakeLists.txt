cmake_minimum_required(VERSION 2.8)
project(Comdb2RLE)

add_definitions("-DCRLE_TOOLS")

add_subdirectory(crle)
add_subdirectory(cunrle)
add_subdirectory(unittests)
add_subdirectory(bench)

enable_testing()

add_test(NAME UNITTESTS
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/unittests
    COMMAND unittest)

add_test(NAME RLETESTS
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/tests
    COMMAND run ${PROJECT_BINARY_DIR})

add_test(NAME BENCHMARKS
    WORKING_DIRECTORY ${PROJECT_BINARY_DIR}/bench
    COMMAND bench)
