/*
   Copyright 2015 Bloomberg Finance L.P.

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

#ifndef __FDB_BOOTS_H_
#define __FDB_BOOTS_H_

#include "comdb2.h"

enum fdb_location_op {
    FDB_LOCATION_REFRESH = 1, /* forces a recreation of location information */
    FDB_LOCATION_INITIAL = 2, /* get an initial node, random */
    FDB_LOCATION_NEXT = 4,    /* get the next node, providing a crt node */
    FDB_LOCATION_IGNORE_LCL = 8,
};

typedef struct fdb_location fdb_location_t;
typedef struct fdb_affinity fdb_affitnity_t;

/**
 * Locate a certain database
 *
 * NOTE: fdb is dbcon_mtx locked here
 */
int fdb_locate(const char *dbname, enum mach_class class, int refresh,
               fdb_location_t **ploc);

/**
 * Routing algo
 *
 * Actions:
 * INITIAL = an intial node
 * NEXT = next node after "arg" initial
 *
 *  Modifiers
 * REFRESH will query comdb2db again
 * IGNORE_LCL will not prefer datacenter colocated nodes
 *
 *
 */
char *fdb_select_node(fdb_location_t **ploc, enum fdb_location_op op, char *arg,
                      int *avail_nodes, int *p_lcl_nodes);

/**
 * Get the number of available/rescpu nodes
 * and how many are collocated
 *
 */
int fdb_get_rescpu_nodes(fdb_location_t *loc, int *locals);

#endif

