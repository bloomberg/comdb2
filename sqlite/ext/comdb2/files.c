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

#include "bb_oscompat.h"
#include "bdb_int.h"
#include "comdb2.h"
#include "comdb2systblInt.h"
#include "ezsystables.h"

sqlite3_module systblFilesModule = {
    .access_flag = CDB2_ALLOW_USER,
};

sqlite3_module systblLogfilesModule = {
    .access_flag = CDB2_ALLOW_USER,
};

sqlite3_module systblTmpfilesModule = {
    .access_flag = CDB2_ALLOW_USER,
};

static int ls_dir(char *dir, void **datap, int *np)
{
    char **data;
    int n;
    const char *fname;
    struct dirent *buf, *ent;
    DIR *dirp;

    data = NULL;
    n = 0;
    dirp = opendir(dir);
    if (dirp == NULL) {
        logmsgperror(__func__);
        return -1;
    }

    buf = alloca(bb_dirent_size(dir));
    while (bb_readdir(dirp, buf, &ent) == 0 && ent != NULL) {
        fname = ent->d_name;
        if (strcmp(fname, ".") != 0 && strcmp(fname, "..") != 0) {
            data = realloc(data, (++n) * sizeof(char *));
            data[n - 1] = strdup(fname);
        }
    }
    closedir(dirp);

    *datap = data;
    *np = n;
    return 0;
}

int ls(void **datap, int *np)
{
    return ls_dir(thedb->bdb_env->dir, datap, np);
}

int ls_log(void **datap, int *np)
{
    return ls_dir(thedb->bdb_env->txndir, datap, np);
}

int ls_tmp(void **datap, int *np)
{
    return ls_dir(thedb->bdb_env->tmpdir, datap, np);
}

void free_ls(void *data, int n)
{
    while (--n >= 0)
        free(data[n]);
    free(data);
}

int systblFilesInit(sqlite3 *db)
{
    int rc;

    rc = create_system_table(db, "comdb2_files", &systblFilesModule, ls, free_ls, sizeof(char *),
                             CDB2_CSTRING, "file", -1, 0, SYSTABLE_END_OF_FIELDS);
    if (rc != 0)
        return rc;

    rc = create_system_table(db, "comdb2_log_files", &systblLogfilesModule, ls_log, free_ls, sizeof(char *),
                             CDB2_CSTRING, "file", -1, 0, SYSTABLE_END_OF_FIELDS);
    if (rc != 0)
        return rc;

    rc = create_system_table(db, "comdb2_tmp_files", &systblTmpfilesModule, ls_tmp, free_ls, sizeof(char *),
                             CDB2_CSTRING, "file", -1, 0, SYSTABLE_END_OF_FIELDS);
    return rc;
}
