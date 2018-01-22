configure_file(comdb2makecluster comdb2makecluster @ONLY)
install(PROGRAMS
  ${CMAKE_CURRENT_BINARY_DIR}/copycomdb2
  DESTINATION bin
)

