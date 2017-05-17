add_definitions(-DCRLE_VERBOSE)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g")

set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++11")

if (${CMAKE_SYSTEM_NAME} MATCHES "AIX")
	add_definitions("-D_IBM_SOURCE")
elseif (${CMAKE_SYSTEM_NAME} MATCHES "SunOS")
	add_definitions("-D_SUN_SOURCE")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -static-libgcc -static-libstdc++")
elseif (${CMAKE_SYSTEM_NAME} MATCHES "Linux")
	add_definitions("-D_LINUX_SOURCE")
	set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -m64")
endif (${CMAKE_SYSTEM_NAME} MATCHES "AIX")

include_directories(${PROJECT_SOURCE_DIR}/..)

add_executable(cunrle cunrle.cpp)
