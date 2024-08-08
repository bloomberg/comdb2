if(${CMAKE_SYSTEM_NAME} STREQUAL Darwin)
    find_program(protobuf-generator NAMES protoc)
else()
    find_program(protobuf-generator NAMES protoc-c)
endif()
if(NOT protobuf-generator)
    message(FATAL_ERROR "protobuf generator not found!")
endif()
list(APPEND proto bpfunc.proto  connectmsg.proto  sqlquery.proto  sqlresponse.proto importdata.proto blobfiles.proto) 
foreach(p ${proto})
  string(REPLACE ".proto" ".pb-c.h" h ${p})
  string(REPLACE ".proto" ".pb-c.c" c ${p})
  list(APPEND dot_h ${h})
  list(APPEND dot_c ${c})
  add_custom_command(
    OUTPUT ${h} ${c}
    DEPENDS ${p}
    COMMAND ${protobuf-generator} -I${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/${p} --c_out=${CMAKE_CURRENT_BINARY_DIR}
  )
endforeach()
add_custom_target(proto DEPENDS ${dot_c} ${dot_h})

set(module protobuf)
set(MODULE PROTOBUF)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_protobuf.h @ONLY)
