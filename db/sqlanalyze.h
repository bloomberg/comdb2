#ifndef SQLANALYZE_H
#define SQLANALYZE_H
#include "cdb2_constants.h"

/* index-descriptor */
typedef struct index_descriptor {
    pthread_t thread_id;
    int comp_state;
    sampled_idx_t *s_ix;
    struct dbtable *tbl;
    int ix;
    int sampling_pct;
} index_descriptor_t;

/* table-descriptor */
typedef struct table_descriptor {
    pthread_t thread_id;
    int table_state;
    char table[MAXTABLELEN];
    SBUF2 *sb;
    int scale;
    int override_llmeta;
    index_descriptor_t index[MAXINDEX];
    struct user current_user;
    void *appdata;
    void *get_authdata;
} table_descriptor_t;

int analyze_regular_table(table_descriptor_t *td, struct sqlclntstate *clnt, char *zErrTab, size_t nErrTab);
#endif
