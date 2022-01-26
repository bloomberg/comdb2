/*
   Copyright 2022 Bloomberg Finance L.P.

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
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "cdb2api.h"
#include <bdb_api.h>

struct fill_estimate {
    char *tablename;
    int dtafile;
    int dtastripe_null;
    int stripe;
    int dtafile_null;
    int ix;
    int index_null;
    double percent_free_pages;
    double page_fill_factor;
};


static void free_fill_estimates(void *p, int n)
{
    return ;
    int nestimates = n;
    struct fill_estimate *estimates = (struct fill_estimate*) p;
    for (int i = 0; i < nestimates; i++) {
        free(estimates[i].tablename);
    }
    free(estimates);

}

static int get_fill_estimates(void **data, int *npoints)
{
    *npoints = 0;
    struct fill_estimate *estimates;
    int rc;
    int nestimates = 0;
    tran_type *trans = curtran_gettran();
    int bdberr;

    for (int tblnum = 0; tblnum < thedb->num_dbs; tblnum++) {
        struct dbtable *tbl = thedb->dbs[tblnum];
        nestimates += gbl_dtastripe;
        nestimates += (tbl->numblobs * (gbl_blobstripe ? gbl_dtastripe : 1));
        nestimates += tbl->nix;
    }
    estimates = calloc(nestimates, sizeof(struct fill_estimate));
    if (estimates == NULL)
        return -1;

    int estimate_num;
again:
    estimate_num = 0;
    for (int tblnum = 0; tblnum < thedb->num_dbs; tblnum++) {
        struct dbtable *tbl = thedb->dbs[tblnum];
        if (tbl->dbtype != DBTYPE_TAGGED_TABLE)
            continue;
        for (int dtanum = 0; dtanum < tbl->numblobs + 1; dtanum++) {
            for (int dtastripe = 0; dtastripe < (gbl_blobstripe ? gbl_dtastripe : 1); dtastripe++) {
                struct fill_estimate *est = &estimates[estimate_num];
                est->tablename = strdup(tbl->tablename);
                est->stripe = dtastripe;
                est->index_null = 1;
                est->dtastripe_null = 0;
                est->dtafile = 0;
                est->dtafile = dtanum;
                rc = bdb_estimate_table_fill(tbl->handle, trans, dtanum, dtastripe, -1, &est->percent_free_pages, &est->page_fill_factor, &bdberr);
                if (rc) {
                    if (bdberr == BDBERR_DEADLOCK)
                        goto again;
                    goto cleanup;
                }
                estimate_num++;
            }
        }
        for (int ix = 0; ix < tbl->nix; ix++) {
            struct fill_estimate *est = &estimates[estimate_num];
            est->tablename = strdup(tbl->tablename);
            est->index_null = 0;
            est->dtastripe_null = 1;
            est->dtafile_null = 1;
            est->ix = ix;
            rc = bdb_estimate_table_fill(tbl->handle, trans, 0, 0, ix, &est->percent_free_pages, &est->page_fill_factor, &bdberr);
            if (rc) {
                if (bdberr == BDBERR_DEADLOCK)
                    goto again;
                goto cleanup;
            }
            estimate_num++;
        }
    }
    *data = estimates;
    *npoints = nestimates;
    curtran_puttran(trans);
    trans = NULL;

    return 0;

cleanup:
    if (trans) {
        curtran_puttran(trans);
        trans = NULL;
    }
    free_fill_estimates(estimates, nestimates);
    return -1;
}

sqlite3_module systblFillEstimatesModule = {
        .access_flag = CDB2_ALLOW_ALL,
        .systable_lock = "comdb2_tables"
};

int systblPageFillEstimate(sqlite3 *db) {
    return create_system_table(
            db, "comdb2_space_estimate", &systblFillEstimatesModule,
            get_fill_estimates, free_fill_estimates, sizeof(struct fill_estimate),
            CDB2_CSTRING, "name", -1, offsetof(struct fill_estimate, tablename),
            CDB2_INTEGER, "datafile", offsetof(struct fill_estimate, dtastripe_null), offsetof(struct fill_estimate, dtafile),
            CDB2_INTEGER, "stripe", offsetof(struct fill_estimate, dtafile_null), offsetof(struct fill_estimate, stripe),
            CDB2_INTEGER, "indexnum", offsetof(struct fill_estimate, index_null), offsetof(struct fill_estimate, ix),
            CDB2_REAL, "percent_free_pages", -1, offsetof(struct fill_estimate, percent_free_pages),
            CDB2_REAL, "page_fill_factor", -1, offsetof(struct fill_estimate, page_fill_factor),
            SYSTABLE_END_OF_FIELDS);
    return 0;
}
