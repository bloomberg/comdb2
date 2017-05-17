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

#ifndef _BDB_ACCESS_H_
#define _BDB_ACCESS_H_

struct bdb_access_tag;
typedef struct bdb_access_tag bdb_access_t;

/**
 * Returns 1 if the node "hostnum" is allowed to WRITE into table "table"
 * Returns 0 if not allowed
 * Returns -1 if error
 *
 */
int bdb_access_tbl_write_by_mach_get(bdb_state_type *bdb_state, tran_type *tran,
                                     char *table, int hostnum, int *bdberr);

/**
 * Returns 1 if the node "hostnum" is allowed to READ into table "table"
 * Returns 0 if not allowed
 * Returns -1 if error
 *
 */
int bdb_access_tbl_read_by_mach_get(bdb_state_type *bdb_state, tran_type *tran,
                                    char *table, int hostnum, int *bdberr);

/**
 * Sets the right to write for (table, hostname)
 *
 */
int bdb_access_tbl_write_by_mach_set(bdb_state_type *bdb_state, tran_type *tran,
                                     char *table, int hostnum, int *bdberr);

/**
 * Sets the right to read for (table, hostname)
 *
 */
int bdb_access_tbl_read_by_mach_set(bdb_state_type *bdb_state, tran_type *tran,
                                    char *table, int hostnum, int *bdberr);

/**
 * Enable access control for tableXnode
 *
 */
int bdb_accesscontrol_tableXnode_set(bdb_state_type *bdb_state,
                                     tran_type *input_trans, int *bdberr);

/**
 * Init/Destroy functions per table
 *
 */
bdb_access_t *bdb_access_create(bdb_state_type *bdb_state, int *bdberr);
void bdb_access_destroy(bdb_state_type *bdb_state);

/**
 * Remove the "write" right for table "tblname: and user/machine "username"
 *
 */
int bdb_tbl_access_write_delete(bdb_state_type *bdb_state,
                                tran_type *input_trans, const char *tblname,
                                const char *username, int *bdberr);

/**
 * Remove the "read" right for table "tblname: and user/machine "username"
 *
 */
int bdb_tbl_access_read_delete(bdb_state_type *bdb_state,
                               tran_type *input_trans, const char *tblname,
                               const char *username, int *bdberr);

/**
 * Remove the userschema right for user "username"
 *
 */
int bdb_tbl_access_userschema_delete(bdb_state_type *bdb_state,
                                     tran_type *input_trans, const char *userschema,
                                     const char *username, int *bdberr);

/**
 * Invalidate the cache when deleting
 *
 */
void bdb_access_tbl_invalidate(bdb_state_type *bdb_state);

#endif
