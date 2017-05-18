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

/* db block request */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <plbitlib.h>
#include <alloca.h>

#include "comdb2.h"
#include "tag.h"
#include "block_internal.h"

#include "logmsg.h"

static char *get_temp_trnname(long long *ctid)

    static char *get_temp_trnname(long long *ctid)
{
    char *s;
    s = malloc(strlen(thedb->basedir) +
               43); /* 40 (38 really) is max length of string below */
    if (s == NULL)
        return s;
    sprintf(s, "%s/%s.tmpdbs/temp_trn_%d_%lld.db", thedb->basedir,
            thedb->envname, (int)pthread_self(), *ctid);
    *ctid = *ctid + 1LL;
    return s;
}

int delete_longtrn_table(void *table)
{
    int bdberr = 0;
    int rc = 0;
    if (table == NULL)
        return rc;
    rc = bdb_temp_table_close(thedb->bdb_env, table, &bdberr);
    return rc;
}

void *create_longtrn_table(long long *ctid)
{
    struct temp_table *newtbl = NULL;
    int bdberr = 0;
    char *tblname = NULL;
    tblname = get_temp_trnname(ctid);
    if (tblname == NULL) {
        logmsg(LOGMSG_ERROR, "failed to create temp table %s name.\n", tblname);
        return NULL;
    }
    newtbl = (struct temp_table *)bdb_temp_table_create(thedb->bdb_env, tblname,
                                                        &bdberr);
    if (newtbl == NULL || bdberr != 0) {
        logmsg(LOGMSG_ERROR, "failed to create temp table %s err %d\n", tblname,
                bdberr);
        free(tblname);
        return NULL;
    }
    free(tblname);
    return newtbl;
}

static void *get_trn_table_cursor(void *table)
{
    struct temp_cursor *cur = NULL;
    int err = 0;
    cur = (struct temp_cursor *)bdb_temp_table_cursor(thedb->bdb_env, table,
                                                      NULL, &err);
    if (cur == NULL)
        return NULL;
    return (void *)cur;
}

static int close_trn_table_cursor(void *cursor)
{
    int err = 0, rc = 0;
    rc = bdb_temp_table_close_cursor(thedb->bdb_env, cursor, &err);
    if (rc != 0)
        return -1;
    return 0;
}
