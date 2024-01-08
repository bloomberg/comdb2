/*
   Copyright 2015, 2021, Bloomberg Finance L.P.

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

#ifndef INCLUDED_MACC_GLUE_H
#define INCLUDED_MACC_GLUE_H

#include "comdb2.h"

/* create a dbtable with the provided schema "csc2" */
struct dbtable *create_new_dbtable(struct dbenv *dbenv, char *tablename,
                                   char *csc2, int dbnum, int sc_alt_tablename,
                                   int allow_ull, int no_sideeffects,
                                   struct errstat *err);

/* populate an existing db with .NEW tags for provided schema "csc2" */
int populate_db_with_alt_schema(struct dbenv *dbenv, struct dbtable *db,
                                char *csc2, struct errstat *err);

#endif /* !INCLUDED_MACC_GLUE_H */
