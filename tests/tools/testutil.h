#ifndef _TESTUTIL_H
#define _TESTUTIL_H

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <ctype.h>
#include <stdint.h>
#include <cdb2api.h>

void tdprintf(FILE *f, cdb2_hndl_tp *db, const char *func, int line,
              const char *format, ...);
char *master(const char *dbname, const char *cltype);
char *read_node(cdb2_hndl_tp *db);
void myexit(const char *func, int line, int status);
uint64_t timems(void);
uint64_t timeus(void);

#endif
