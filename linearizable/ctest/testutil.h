#ifndef _TESTUTIL_H
#define _TESTUTIL_H

#include <stdio.h>
#include <unistd.h> 
#include <stdlib.h>
#include <ctype.h>
#include <cdb2api.h>

void tdprintf(FILE *f, cdb2_hndl_tp *db, const char *func, int line, const char *format, ...);

#endif
