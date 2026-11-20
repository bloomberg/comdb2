#ifndef SQLANALYZE_H
#define SQLANALYZE_H
#include "cdb2_constants.h"

struct table_descriptor;
typedef struct table_descriptor table_descriptor_t;

int analyze_regular_table(const char *tablename, table_descriptor_t *td,
                          struct sqlclntstate *clnt, struct errstat *err);

/* Like analyze_regular_table, but the caller supplies the dbtable rather than
 * having it looked up by name -- needed for a pre-commit schema-change table,
 * which is not yet registered in thedb->db_hash. */
int analyze_table_dbtable(struct dbtable *table, const char *tablename, table_descriptor_t *td,
                          struct sqlclntstate *clnt, struct errstat *err);
#endif
