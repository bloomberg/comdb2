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

#ifndef INCLUDE_SC_CSC2_H
#define INCLUDE_SC_CSC2_H

int load_db_from_schema(struct schema_change_type *s, struct dbenv *thedb,
                        int *foundix, struct ireq *iq);

/* Given a table name, this makes sure that what we know about the schema
 * matches what is written in our meta table.  If we have nothing in our table
 * we populate it from the given file. */
int check_table_schema(struct dbenv *dbenv, const char *table,
                       const char *csc2file);

int schema_cmp(struct dbenv *dbenv, struct dbtable *db, const char *csc2cmp);

/* adds a new version of a schema (loaded from a file) to the meta table */
int load_new_table_schema_file_tran(struct dbenv *dbenv, tran_type *tran,
                                    const char *table, const char *csc2file);

/* call load_new_table_schema_tran without a transaction */
int load_new_table_schema_file(struct dbenv *dbenv, const char *table,
                               const char *csc2file);

/* adds a new version of a schema to the meta table */
int load_new_table_schema_tran(struct dbenv *dbenv, tran_type *tran,
                               const char *table, const char *csc2_text);

/* calls load_new_table_schema without a trasaction */
int load_new_table_schema(struct dbenv *dbenv, const char *table,
                          const char *csc2_text);

int write_csc2_file_fname(const char *fname, const char *csc2text);

int write_csc2_file(struct dbtable *db, const char *csc2text);

int get_generic_csc2_fname(const struct dbtable *db, char *fname, size_t fname_len);

/* write out all the schemas from meta for all open tables to disk */
int dump_all_csc2_to_disk();

/* TODO clean up all these csc2 dump functions, unify them */
/* write out all the schemas from meta for all open tables to disk */
int dump_table_csc2_to_disk_fname(struct dbtable *db, const char *csc2_fname);

/* write out all the schemas from meta for all open tables to disk */
int dump_table_csc2_to_disk(const char *table);

#endif
