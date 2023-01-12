find_program(protoc-c NAMES protoc-c)
if(NOT protoc-c)
  message(FATAL_ERROR "protoc-c not found!")
endif()
list(APPEND proto bpfunc.proto  connectmsg.proto  sqlquery.proto  sqlresponse.proto) 
foreach(p ${proto})
  string(REPLACE ".proto" ".pb-c.h" h ${p})
  string(REPLACE ".proto" ".pb-c.c" c ${p})
  list(APPEND dot_h ${h})
  list(APPEND dot_c ${c})
  add_custom_command(
    OUTPUT ${h} ${c}
    DEPENDS ${p}
    COMMAND ${protoc-c} -I${CMAKE_CURRENT_SOURCE_DIR} ${CMAKE_CURRENT_SOURCE_DIR}/${p} --c_out=${CMAKE_CURRENT_BINARY_DIR}
  )
endforeach()
add_custom_target(proto DEPENDS ${dot_c} ${dot_h})

set(module protobuf)
set(MODULE PROTOBUF)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_protobuf.h @ONLY)
