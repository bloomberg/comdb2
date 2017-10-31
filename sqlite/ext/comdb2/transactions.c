/*
   Copyright 2023 Bloomberg Finance L.P.

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

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"

#include <bdb_api.h>

int get_transactions(void **data, int *npoints) {
    int rc = bdb_txn_stats_collect(thedb->bdb_env, (struct bdb_active_berk_transaction**) data, npoints);
    if (rc) return -1;
    return 0;
}

void free_transactions(void *p, int n) {
    struct bdb_active_berk_transaction *t = (struct bdb_active_berk_transaction*) p;
    for (int i = 0; i < n; i++) {
        free(t[i].lsn);
    }
    free(t);
}

sqlite3_module systblTransactionsModule = {
    .access_flag = CDB2_ALLOW_ALL,
};

int systblActiveTransactionsInit(sqlite3 *db) {
    return create_system_table(db, "comdb2_active_transactions",
            &systblTransactionsModule, get_transactions, free_transactions,
            sizeof(struct bdb_active_berk_transaction),
            CDB2_INTEGER, "tid", -1, offsetof(struct bdb_active_berk_transaction, tid),
            CDB2_INTEGER, "age", -1, offsetof(struct bdb_active_berk_transaction, age),
            CDB2_CSTRING, "lsn", -1, offsetof(struct bdb_active_berk_transaction, lsn),
            SYSTABLE_END_OF_FIELDS);
}
