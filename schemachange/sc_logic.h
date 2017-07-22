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

#ifndef INCLUDE_SC_LOGIC_H
#define INCLUDE_SC_LOGIC_H

#include "schemachange.h"

int dryrun_int(struct schema_change_type *, struct dbtable *db, struct dbtable *newdb,
               struct scinfo *);
int dryrun(struct schema_change_type *s);
int finalize_schema_change_thd(struct ireq *, tran_type *);
int do_setcompr(struct ireq *iq, const char *rec, const char *blob);
int delete_temp_table(struct ireq *iq, struct dbtable *newdb);

int verify_new_temp_sc_db(struct dbtable *p_db, struct dbtable *p_newdb, tran_type *tran);
/****** This function is inside constraints.c and it was used by schemachange.c,
 * before it was declared extern in the c files I am putting the definition here
 * to move a step towards a proper header file. */
int verify_constraints_exist(struct dbtable *from_db, struct dbtable *to_db,
                             struct dbtable *new_db, struct schema_change_type *s);

int do_schema_change_tran(sc_arg_t *);
int do_schema_change(struct schema_change_type *);
#endif
