/*
   Copyright 2021 Bloomberg Finance L.P.

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */

#ifndef __INCLUDED_SQL_STMT_CACHE_H
#define __INCLUDED_SQL_STMT_CACHE_H

/*
  Statement caching in Comdb2
*/

#include <list.h>
#include <plhash.h>

#define MAX_HASH_SQL_LENGTH 8192
#define HINT_LEN 127

enum STMT_CACHE_FLAGS {
    STMT_CACHE_NONE = 0,  /* disable statement caching */
    STMT_CACHE_PARAM = 1, /* only cache queries with bound parameters */
    STMT_CACHE_ALL = 2    /* cache all the queries */
};

enum cache_status {
    CACHE_DISABLED = 0,
    CACHE_HAS_HINT = 1,
    CACHE_FOUND_STMT = 2,
    CACHE_FOUND_STR = 4,
};

enum query_data_ops {
    QUERY_DATA_SET = 1,
    QUERY_DATA_GET = 2,
    QUERY_DATA_DELETE = 3
};

enum query_data_type { QUERY_STMT_DATA = 1, QUERY_HINT_DATA = 2 };

/* Forward declaration */
struct sqlclntstate;

typedef int(plugin_query_data_func)(struct sqlclntstate *, void **, int *, int,
                                    int);

typedef struct stmt_cache_entry {
    char sql[MAX_HASH_SQL_LENGTH];
    char *query;
    sqlite3_stmt *stmt;

    void *stmt_data;
    int stmt_data_sz;

    plugin_query_data_func *qd_func; /* Pointer to the current client info */

    LINKC_T(struct stmt_cache_entry) lnk;
} stmt_cache_entry_t;

typedef struct stmt_cache {
    hash_t *hash;
    /* During overflow, the last entry from one of the following
      lists is freed. */
    LISTC_T(stmt_cache_entry_t) param_stmt_list;
    LISTC_T(stmt_cache_entry_t) noparam_stmt_list;
} stmt_cache_t;

struct sql_state {
    enum cache_status status;       /* populated by get_prepared_stmt */
    sqlite3_stmt *stmt;             /* cached engine, if any */
    char cache_hint[HINT_LEN];      /* hint copy, if any */
    const char *sql;                /* the actual string used */
    void *query_data;               /* data associated with sql */
    stmt_cache_entry_t *stmt_entry; /* fast pointer to hashed record */
    int prepFlags;                  /* flags to get_prepared_stmt_int */
};

stmt_cache_t *stmt_cache_new(stmt_cache_t *);
int stmt_cache_delete(stmt_cache_t *);
int stmt_cache_reset(stmt_cache_t *);
int stmt_cache_get(struct sqlthdstate *, struct sqlclntstate *,
                   struct sql_state *, int);
int stmt_cache_put(struct sqlthdstate *, struct sqlclntstate *,
                   struct sql_state *, int);
int stmt_cache_put_distributed(struct sqlthdstate *, struct sqlclntstate *,
                               struct sql_state *, int, int);
int stmt_cache_find_and_remove_entry(stmt_cache_t *stmt_cache, const char *sql, stmt_cache_entry_t **entry);
int stmt_cache_add_new_entry(stmt_cache_t *stmt_cache, const char *sql, const char *actual_sql, sqlite3_stmt *stmt,
                             struct sqlclntstate *clnt);
int stmt_cache_requeue_old_entry(stmt_cache_t *, stmt_cache_entry_t *);
#endif /* !__INCLUDED_SQL_STMT_CACHE_H */
