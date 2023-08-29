/*
   Copyright 2018 Bloomberg Finance L.P.

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

#ifndef _DOHSQL_H_
#define _DOHSQL_H_

#include "ast.h"

struct params_info {
    struct sqlclntstate *clnt;
    int nparams;
    struct param_data *params;
};

struct dohsql_node {
    enum ast_type type;
    char *sql;
    int ncols;
    struct dohsql_node **nodes;
    int nnodes;
    int order_size;
    int *order_dir;
    int nparams;
    int remotedb;
    struct params_info *params;
};
typedef struct dohsql_node dohsql_node_t;

typedef struct dohsql dohsql_t;

/**
 * Launch parallel sql engines
 *
 */
int dohsql_distribute(dohsql_node_t *node);

/**
 * Terminate a parallel execution
 *
 */
int dohsql_end_distribute(struct sqlclntstate *clnt, struct reqlogger *logger);

/**
 * Get sql for a certain parallel engine
 *
 */
const char *dohsql_get_sql(struct sqlclntstate *clnt, int index);

/**
 * Synchronize parallel execution
 *
 */
void dohsql_wait_for_master(sqlite3_stmt *stmt, struct sqlclntstate *clnt);

#define GET_CLNT                                                               \
    struct sql_thread *thd = pthread_getspecific(query_info_key);              \
    struct sqlclntstate *clnt = thd->clnt;

/**
 * Return 1 if this sql thread servers a parallel statement
 *
 */
int dohsql_is_parallel_shard(void);

/**
 * Retrieve error from a distributed execution plan, if any
 *
 */
int dohsql_error(struct sqlclntstate *clnt, const char **errstr);

/**
 * End distribution of the original sql query had a delayed syntax error
 *
 */
void dohsql_handle_delayed_syntax_error(struct sqlclntstate *clnt);

/**
 * Return global stats
 *
 */
void dohsql_stats(void);

/**
 * Return explain distribution information
 *
 */
void explain_distribution(dohsql_node_t *node);

/**
 * Notify worker threads master is done
 *
 */
void dohsql_signal_done(struct sqlclntstate *clnt);

/**
 * Bound parameters support
 * Callback function that registers a parameters with the proper
 * sqlite worker engine
 *
 */
struct params_info *dohsql_params_append(struct params_info **pparams,
                                         const char *name, int index);

/**
 * Clone bound parameters; deep-copy pointers
 *
 */
int dohsql_clone_params(int nparams, struct param_data * params,
                        int *pnparams, struct param_data **pparams);

/**
 * Free bound parameters
 *
 */
void dohsql_free_params(int *pnparams, struct param_data **pparams, int index);

#endif
