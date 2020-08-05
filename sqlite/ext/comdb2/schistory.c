/*
   Copyright 2020 Bloomberg Finance L.P.

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
#include "schemachange.h"
#include "sc_schema.h"
#include "tohex.h"

struct sc_hist_ent {
    char *name;
    char *seed;
    char *status;
    char *error;
    char *type;    // not used at the moment
    char *newcsc2; // not used at the moment
    cdb2_client_datetime_t start;
    cdb2_client_datetime_t lastupdated;
    int64_t converted;
};

static char *status_num2str(int s)
{
    switch (s) {
    case BDB_SC_RUNNING:
        return "RUNNING";
    case BDB_SC_PAUSED:
        return "PAUSED";
    case BDB_SC_COMMITTED:
        return "COMMITTED";
    case BDB_SC_ABORTED:
        return "ABORTED";
    case BDB_SC_COMMIT_PENDING:
        return "COMMIT PENDING";
    default:
        return "UNKNOWN";
    }
    return "UNKNOWN";
}

static int get_status(void **data, int *npoints)
{
    int rc, bdberr, nkeys;
    sc_hist_row *hist = NULL;
    struct sc_hist_ent *sc_hist_ents = NULL;
    tran_type *trans = curtran_gettran();
    if (trans == NULL) {
        logmsg(LOGMSG_ERROR, "%s: cannot create transaction object\n", __func__);
        return SQLITE_INTERNAL;
    }

    rc = bdb_llmeta_get_sc_history(trans, &hist, &nkeys, &bdberr, NULL);
    if (rc || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to get all schema change hist\n",
               __func__);
        return SQLITE_INTERNAL;
    }

    sc_hist_ents = calloc(nkeys, sizeof(struct sc_hist_ent));
    if (sc_hist_ents == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc\n", __func__);
        rc = SQLITE_NOMEM;
        goto cleanup;
    }

    for (int i = 0; i < nkeys; i++) {
        dttz_t d;

        d = (dttz_t){.dttz_sec = hist[i].start / 1000, .dttz_frac = hist[i].start - (hist[i].start / 1000 * 1000), .dttz_prec = DTTZ_PREC_MSEC};
        dttz_to_client_datetime(&d, "UTC", (cdb2_client_datetime_t *)&(sc_hist_ents[i].start));
        d = (dttz_t){.dttz_sec = hist[i].last / 1000, .dttz_frac = hist[i].last - (hist[i].last / 1000 * 1000), .dttz_prec = DTTZ_PREC_MSEC};
        dttz_to_client_datetime(&d, "UTC", (cdb2_client_datetime_t *)&(sc_hist_ents[i].lastupdated));

        sc_hist_ents[i].name = strdup(hist[i].tablename);
        sc_hist_ents[i].status = strdup(status_num2str(hist[i].status));

        char str[22];
        sprintf(str, "%0#16"PRIx64, hist[i].seed);
        sc_hist_ents[i].seed = strdup(str);

        if (hist[i].errstr)
            sc_hist_ents[i].error = strdup(hist[i].errstr);
        sc_hist_ents[i].converted = hist[i].converted;
    }

    *npoints = nkeys;
    *data = sc_hist_ents;

cleanup:
    curtran_puttran(trans);
    free(hist);

    return rc;
}

static void free_status(void *p, int n)
{
    struct sc_hist_ent *sc_hist_ents = p;
    for (int i = 0; i < n; i++) {
            free(sc_hist_ents[i].name);
            free(sc_hist_ents[i].status);
            free(sc_hist_ents[i].error);
            free(sc_hist_ents[i].seed);
    }
    free(sc_hist_ents);
}

sqlite3_module systblScHistoryModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblScHistoryInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_sc_history", &systblScHistoryModule,
        get_status, free_status, sizeof(struct sc_hist_ent),
        CDB2_CSTRING, "name", -1, offsetof(struct sc_hist_ent, name),
        CDB2_DATETIME, "start", -1, offsetof(struct sc_hist_ent, start),
        CDB2_CSTRING, "status", -1, offsetof(struct sc_hist_ent, status),
        CDB2_CSTRING, "seed", -1, offsetof(struct sc_hist_ent, seed),
        CDB2_DATETIME, "last_updated", -1, offsetof(struct sc_hist_ent,
                                                    lastupdated),
        CDB2_INTEGER, "converted", -1, offsetof(struct sc_hist_ent, converted),
        CDB2_CSTRING, "error", -1, offsetof(struct sc_hist_ent, error),
        SYSTABLE_END_OF_FIELDS);
}
