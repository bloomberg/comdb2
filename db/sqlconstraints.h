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

#endif
