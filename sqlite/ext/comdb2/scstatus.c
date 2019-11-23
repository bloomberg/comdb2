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

struct sc_status_ent {
    char *name;
    char *type;
    char *newcsc2;
    char *seed;
    cdb2_client_datetime_t start;
    char *status;
    cdb2_client_datetime_t lastupdated;
    int64_t converted;
    char *error;
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

int get_status(void **data, int *npoints)
{
    int rc, bdberr, nkeys;
    llmeta_sc_status_data *status = NULL;
    void **sc_data = NULL;
    struct sc_status_ent *sc_status_ents = NULL;

    rc = bdb_llmeta_get_all_sc_status(&status, &sc_data, &nkeys, &bdberr);
    if (rc || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to get all schema change status\n",
               __func__);
        return SQLITE_INTERNAL;
    }

    sc_status_ents = calloc(nkeys, sizeof(struct sc_status_ent));
    if (sc_status_ents == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to malloc\n", __func__);
        rc = SQLITE_NOMEM;
        goto cleanup;
    }

    for (int i = 0; i < nkeys; i++) {
        dttz_t d;
        struct schema_change_type sc = {0}; // used for upacking

        rc = unpack_schema_change_type(&sc, sc_data[i], status[i].sc_data_len);
        if (rc) {
            free(sc_status_ents);
            logmsg(LOGMSG_ERROR, "%s: failed to unpack schema change\n",
                   __func__);
            rc = SQLITE_INTERNAL;
            goto cleanup;
        }
        sc_status_ents[i].name = strdup(sc.tablename);
        sc_status_ents[i].type = strdup(get_ddl_type_str(&sc));
        sc_status_ents[i].newcsc2 = strdup(get_ddl_csc2(&sc));

        d = (dttz_t){.dttz_sec = status[i].start / 1000,
                     .dttz_frac =
                         status[i].start - (status[i].start / 1000 * 1000),
                     .dttz_prec = DTTZ_PREC_MSEC};
        dttz_to_client_datetime(
            &d, "UTC", (cdb2_client_datetime_t *)&(sc_status_ents[i].start));
        d = (dttz_t){.dttz_sec = status[i].last / 1000,
                     .dttz_frac =
                         status[i].last - (status[i].last / 1000 * 1000),
                     .dttz_prec = DTTZ_PREC_MSEC};
        dttz_to_client_datetime(
            &d, "UTC",
            (cdb2_client_datetime_t *)&(sc_status_ents[i].lastupdated));
        sc_status_ents[i].status = strdup(status_num2str(status[i].status));

        struct dbtable *db = get_dbtable_by_name(sc.tablename);
        if (db && db->doing_conversion)
            sc_status_ents[i].converted = db->sc_nrecs;
        else
            sc_status_ents[i].converted = -1;

        sc_status_ents[i].error = strdup(status[i].errstr);
        if (status[i].status == BDB_SC_RUNNING || 
            status[i].status == BDB_SC_PAUSED || 
            status[i].status == BDB_SC_COMMIT_PENDING) {
            unsigned long long seed = 0;
            unsigned int host = 0;
            if ((rc = fetch_sc_seed(sc.tablename, thedb, &seed, &host)) == SC_OK) {
                char str[22];
                sprintf(str, "0x%llx", seed);
                sc_status_ents[i].seed = strdup(str);
            }
        }
    }

    *npoints = nkeys;
    *data = sc_status_ents;

cleanup:
    for (int i = 0; i < nkeys; i++) {
        free(sc_data[i]);
    }
    free(status);
    free(sc_data);

    return rc;
}

void free_status(void *p, int n)
{
    struct sc_status_ent *sc_status_ents = p;
    for (int i = 0; i < n; i++) {
        if (sc_status_ents[i].name)
            free(sc_status_ents[i].name);
        if (sc_status_ents[i].type)
            free(sc_status_ents[i].type);
        if (sc_status_ents[i].newcsc2)
            free(sc_status_ents[i].newcsc2);
        if (sc_status_ents[i].status)
            free(sc_status_ents[i].status);
        if (sc_status_ents[i].error)
            free(sc_status_ents[i].error);
    }
    free(sc_status_ents);
}

sqlite3_module systblScStatusModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int systblScStatusInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_sc_status", &systblScStatusModule,
        get_status, free_status, sizeof(struct sc_status_ent),
        CDB2_CSTRING, "name", -1, offsetof(struct sc_status_ent, name),
        CDB2_CSTRING, "type", -1, offsetof(struct sc_status_ent, type),
        CDB2_CSTRING, "newcsc2", -1, offsetof(struct sc_status_ent, newcsc2),
        CDB2_DATETIME, "start", -1, offsetof(struct sc_status_ent, start),
        CDB2_CSTRING, "status", -1, offsetof(struct sc_status_ent, status),
        CDB2_CSTRING, "seed", -1, offsetof(struct sc_status_ent, seed),
        CDB2_DATETIME, "last_updated", -1, offsetof(struct sc_status_ent,
                                                    lastupdated),
        CDB2_INTEGER, "converted", -1, offsetof(struct sc_status_ent,
                                                converted),
        CDB2_CSTRING, "error", -1, offsetof(struct sc_status_ent, error),
        SYSTABLE_END_OF_FIELDS);
}
