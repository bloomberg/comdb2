#ifndef SQLANALYZE_H
#define SQLANALYZE_H
#include "cdb2_constants.h"

struct table_descriptor;
typedef struct table_descriptor table_descriptor_t;

int analyze_regular_table(const char *tablename, table_descriptor_t *td,
                          struct sqlclntstate *clnt, struct errstat *err);
#endif
