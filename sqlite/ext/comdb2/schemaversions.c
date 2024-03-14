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

static int get_versions(void **data, int *npoints)
{
    int rc, bdberr, nvers;

    nvers = 0;
    schema_version_row *vers = NULL;
    tran_type *trans = curtran_gettran();

    if (trans == NULL) {
        logmsg(LOGMSG_ERROR, "%s: cannot create transaction object\n", __func__);
        rc = SQLITE_INTERNAL;
        goto cleanup;
    }

    rc = bdb_llmeta_get_schema_versions(trans, &vers, &nvers, &bdberr);
    if (rc || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to get all schema versions\n",
               __func__);
        rc = SQLITE_INTERNAL;
        goto cleanup;
    }

    *npoints = nvers;
    *data = vers;

cleanup:
    curtran_puttran(trans);

    return rc;
}

static void free_versions(void *p, int n)
{
    bdb_llmeta_free_schema_versions((schema_version_row *) p, n);
}

sqlite3_module systblSchemaVersionsModule = {
    .access_flag = CDB2_ALLOW_USER,
    .systable_lock = "comdb2_tables",
};

int systblSchemaVersionsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_schemaversions", &systblSchemaVersionsModule,
        get_versions, free_versions, sizeof(struct schema_version_row),
        CDB2_CSTRING, "tablename", -1, offsetof(struct schema_version_row, db_name),
        CDB2_CSTRING, "csc2", -1, offsetof(struct schema_version_row, csc2),
        CDB2_INTEGER, "version", -1, offsetof(struct schema_version_row, vers),
        SYSTABLE_END_OF_FIELDS);
}
