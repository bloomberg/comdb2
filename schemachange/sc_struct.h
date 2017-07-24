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

#ifndef INCLUDE_SC_STRUCT_H
#define INCLUDE_SC_STRUCT_H

struct dbtable *create_db_from_schema(struct dbenv *thedb,
                                 struct schema_change_type *s, int dbnum,
                                 int foundix, int version);

void print_schemachange_info(struct schema_change_type *s, struct dbtable *db,
                             struct dbtable *newdb);

void set_schemachange_options(struct schema_change_type *s, struct dbtable *db,
                              struct scinfo *scinfo);
void set_schemachange_options_tran(struct schema_change_type *s, struct dbtable *db,
                                   struct scinfo *scinfo, tran_type *tran);

int print_status(struct schema_change_type *s);

int reload_schema(char *table, const char *csc2, tran_type *tran);

void set_sc_flgs(struct schema_change_type *s);

int schema_change_headers(struct schema_change_type *s);

#endif
