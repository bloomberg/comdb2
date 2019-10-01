/*
   Copyright 2019 Bloomberg Finance L.P.

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

#ifndef INCLUDED_CONSTRAINTS_H
#define INCLUDED_CONSTRAINTS_H

#include "cdb2_constants.h"
#define MAXREF 64

typedef struct {
    short dbnum;
    short ixnum;
} fk_ref_type;

typedef struct {
    short num;
    fk_ref_type ref[MAXREF];
} fk_ref_array_type;

typedef struct {
    struct dbtable *lcltable;
    char *consname;
    char *lclkeyname;
    int nrules;
    int flags;
    char *table[MAXCONSTRAINTS];
    char *keynm[MAXCONSTRAINTS];
} constraint_t;

typedef struct {
    char *consname;
    char *expr;
} check_constraint_t;

struct ireq;

int should_skip_constraint_for_index(struct dbtable *db, int ixnum, int nulls);
int check_single_key_constraint(struct ireq *ruleiq, constraint_t *ct,
        char *lcl_tag, char *lcl_key, char *tblname, void *trans, int *remote_ri);
constraint_t *get_ix_constraint(struct dbtable *db_table, int ix);
#endif
