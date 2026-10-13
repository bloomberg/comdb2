/*
   Copyright 2018-2020 Bloomberg Finance L.P.

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

#include <stddef.h>
#include <stdlib.h>
#include <plhash_glue.h>
#include <bdb/bdb_int.h>

#include "comdb2.h"
#include "sql.h"
#include "build/db.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"
#include "types.h"

extern int gbl_utxnid_log;

sqlite3_module systblTransactionCommitModule =
{
    .access_flag = CDB2_ALLOW_USER,
};

extern int64_t __txn_commit_map_nentries_nolock(DB_ENV *);
extern int __txn_commit_map_foreach_nolock(DB_ENV *, int (*)(UTXNID_TRACK *, void *), void *);

typedef struct txn_commit_info {
    u_int64_t utxnid;
    u_int64_t commit_lsn_file;
    u_int64_t commit_lsn_offset;
} txn_commit_info;

typedef struct add_tran_commit_args {
    txn_commit_info **data;
    int *npoints;
} add_tran_commit_args;

static int add_tran_commit(UTXNID_TRACK *ritem, void *arg) {
    add_tran_commit_args* info = (add_tran_commit_args *) arg;
    txn_commit_info** data = info->data;
    txn_commit_info* litem = (txn_commit_info *) (((txn_commit_info *) *data)+(*info->npoints));

    litem->utxnid = ritem->utxnid;
    litem->commit_lsn_file = ritem->commit_lsn.file;
    litem->commit_lsn_offset = ritem->commit_lsn.offset;
    (*(info->npoints))++;

    return 0;
}

int get_tran_commits(void **data, int *npoints) {
    int ret = 0;

    if (!gbl_utxnid_log) {
        *data = NULL;
        *npoints = 0;
        return ret;
    }

    bdb_state_type *bdb_state = thedb->bdb_env;

    Pthread_mutex_lock(&bdb_state->dbenv->txmap->txmap_mutexp);
    *npoints = __txn_commit_map_nentries_nolock(bdb_state->dbenv);
    *data = malloc((*npoints)*sizeof(txn_commit_info));
    if (*data == NULL && *npoints > 0) {
        *npoints = 0;
        Pthread_mutex_unlock(&bdb_state->dbenv->txmap->txmap_mutexp);
        return ENOMEM;
    }
    add_tran_commit_args args = {.data = (txn_commit_info **) data, .npoints = npoints};
    *npoints = 0;

    ret = __txn_commit_map_foreach_nolock(bdb_state->dbenv, add_tran_commit, &args);
    Pthread_mutex_unlock(&bdb_state->dbenv->txmap->txmap_mutexp);

    return ret;
}

void free_tran_commits(void *data, int npoints) {
    txn_commit_info * info = (txn_commit_info *) data;
    if (info != NULL) {
            free(info);
        }
}

int systblTranCommitInit(sqlite3 *db) {
    return create_system_table(
        db, "comdb2_transaction_commit", &systblTransactionCommitModule,
        get_tran_commits, free_tran_commits, sizeof(txn_commit_info),
        CDB2_INTEGER, "utxnid", -1, offsetof(txn_commit_info, utxnid),
        CDB2_INTEGER, "commitlsnfile", -1, offsetof(txn_commit_info, commit_lsn_file),
        CDB2_INTEGER, "commitlsnoffset", -1, offsetof(txn_commit_info, commit_lsn_offset),
        SYSTABLE_END_OF_FIELDS
    );
}
