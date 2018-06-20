add_library(csc2
  macc_so.c
  maccglobals.c
  macclex.c
  maccparse.c
  maccparse.h
  maccprocarr.c
  maccwrite2.c
  mem_csc2.h
)
add_definitions(-DYYMAXDEPTH=1100)
set(module csc2)
set(MODULE CSC2)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_csc2.h @ONLY)
find_program(yacc NAMES bison yacc)
add_custom_command(
  OUTPUT maccparse.h maccparse.c
  DEPENDS maccparse.y
  COMMAND ${yacc} -d -o maccparse.c ${CMAKE_CURRENT_SOURCE_DIR}/maccparse.y
)
find_program(lex NAMES flex lex)
add_custom_command(
  OUTPUT macclex.c
  DEPENDS macclex.l
  COMMAND ${lex} -o macclex.c ${CMAKE_CURRENT_SOURCE_DIR}/macclex.l
)
include_directories(
  ${CMAKE_CURRENT_SOURCE_DIR}
  ${CMAKE_CURRENT_BINARY_DIR}
  ${PROJECT_SOURCE_DIR}/util
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/mem
  ${PROJECT_BINARY_DIR}/mem
)
add_dependencies(csc2 mem)
