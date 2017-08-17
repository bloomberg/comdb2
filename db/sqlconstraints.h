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

#ifndef INCLUDED_SQLCONSTRAINTS_H
#define INCLUDED_SQLCONSTRAINTS_H

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include "comdb2.h"

#include <sqliteInt.h>
#include <sqlite3.h>
#include <vdbe.h>
#include <vdbeInt.h>

typedef struct constraint_table constraint_table;

constraint_table *new_constraint_table(struct dbenv *env, int *bdberr);
int constraint_insert_add_op(constraint_table *tbl, struct dbtable *db, void *record,
                             unsigned long long genid);
int constraint_insert_delete_op(constraint_table *tbl, struct dbtable *db,
                                void *data, unsigned long long genid);
int close_constraint_table(constraint_table *tbl);
int constraint_insert_update_op(constraint_table *tbl, struct dbtable *db,
                                unsigned long long oldgenid,
                                unsigned long long newgenid, void *oldrecord,
                                void *newrecord);
int constraint_genid_added(constraint_table *tbl, struct dbtable *db,
                           unsigned long long genid);
int resolve_constraints(constraint_table *tbl, void *tran, Vdbe *vdbe);

#endif
