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

#ifndef INCLUDE_SC_UTIL_H
#define INCLUDE_SC_UTIL_H

int close_all_dbs(void);
int open_all_dbs(void);
int llmeta_get_dbnum_tran(void *tran, char *tablename, int *bdberr);
int llmeta_get_dbnum(char *tablename, int *bdberr);
char *get_temp_db_name(struct dbtable *db, char *prefix, char tmpname[]);

// get offset of key name without .NEW. added to the name
int get_offset_of_keyname(const char *idx_name);

// check attr whether sc is allowed to be done via ddl only
int sc_via_ddl_only();

// check table for index name length
int validate_ix_names(struct dbtable *db);

int sc_via_ddl_only();

#endif
