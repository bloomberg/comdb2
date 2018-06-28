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

struct dohsql_node
{
    enum ast_type type;
    char* sql;
    int ncols;
    struct dohsql_node **nodes;
    int nnodes;
};
typedef struct dohsql_node dohsql_node_t;

/** 
 * Launch parallel sql engines
 *
 */
int dohsql_distribute(dohsql_node_t *node);

/**
 * Get sql for a certain parallel engine
 *
 */
const char* dohsql_get_sql(struct sqlclntstate *clnt, int index);
#endif
