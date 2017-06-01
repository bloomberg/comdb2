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

#ifndef INCLUDE_SC_SCHEMA_H
#define INCLUDE_SC_SCHEMA_H
int set_header_and_properties(void *tran, struct db *newdb,
                              struct schema_change_type *s, int inplace_upd,
                              int bthash);

int mark_schemachange_over(void *tran, const char *table);

int prepare_table_version_one(void *tran, struct db *db,
                              struct schema **version);

int fetch_schema_change_seed(struct schema_change_type *s, struct dbenv *thedb,
                             unsigned long long *stored_sc_genid);

int check_option_coherency(struct schema_change_type *s, struct db *db,
                           struct scinfo *scinfo);

int sc_request_disallowed(SBUF2 *sb);

int sc_cmp_fileids(unsigned long long a, unsigned long long b);

int verify_record_constraint(struct ireq *iq, struct db *db, void *trans,
                             void *old_dta, unsigned long long ins_keys,
                             blob_buffer_t *blobs, int maxblobs,
                             const char *from, int rebuild, int convert);
int verify_partial_rev_constraint(struct db *to_db, struct db *newdb,
                                  void *trans, void *od_dta,
                                  unsigned long long ins_keys,
                                  const char *from);

void verify_schema_change_constraint(struct ireq *iq, struct db *currdb,
                                     void *trans, void *od_dta,
                                     unsigned long long ins_keys);

int ondisk_schema_changed(const char *table, struct db *newdb, FILE *out,
                          struct schema_change_type *s);

int create_schema_change_plan(struct schema_change_type *s, struct db *olddb,
                              struct db *newdb, struct scplan *plan);

void transfer_db_settings(struct db *olddb, struct db *newdb);

int set_odh_options_tran(struct db *db, void *trans, int *bdber);
void set_odh_options(struct db *db);

int compare_constraints(const char *table, struct db *newdb);

int restore_constraint_pointers_main(struct db *db, struct db *newdb,
                                     int copyof);

int restore_constraint_pointers(struct db *db, struct db *newdb);

int backout_constraint_pointers(struct db *db, struct db *newdb);

int fk_source_change(struct db *newdb, FILE *out, struct schema_change_type *s);

int check_sc_headroom(struct schema_change_type *s, struct db *olddb,
                      struct db *newdb);

int compat_chg(struct db *olddb, struct schema *s2, const char *ixname);

int compatible_constraint_source(struct db *olddb, struct db *newdb,
                                 struct schema *newsc, const char *key,
                                 FILE *out, struct schema_change_type *s);

int remove_constraint_pointers(struct db *db);

void fix_constraint_pointers(struct db *db, struct db *newdb);

void change_schemas_recover(char *table);

#endif
