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

#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

#include "comdb2.h"

union list_tables {
    struct req {
        short prccom[3];
        short opcode;
        int option;
        int reserved[16];
    } req;

    struct rsp {
        short prccom[3];
        int ntables;
        int tlen;
        int reserved[16];
        char names[1];
    } rsp;
};

enum { TABLE_NAMES = 0, TABLE_CSC_FILES = 1 };

int tolisttables(struct ireq *iq)
{
    char *s;
    union list_tables *t;
    int i;
    int len;
    int option;

    t = (union list_tables *)iq->rq;
    s = t->rsp.names;
    t->rsp.tlen = 0;
    t->rsp.ntables = thedb->num_dbs;
    option = t->req.option;
    for (i = 0; i < thedb->num_dbs; i++) {
        if (option == TABLE_NAMES) {
            len = strlen(thedb->dbs[i]->dbname);
            strcpy(s, thedb->dbs[i]->dbname);
        } else {
            len = strlen(thedb->dbs[i]->csc2fname);
            strcpy(s, thedb->dbs[i]->csc2fname);
        }
        s += len + 1;
        t->rsp.tlen += len + 1;
    }
    iq->reply_len = offsetof(struct rsp, names) + t->rsp.tlen;
    return 0;
}

static int get_another_db_csc2_files_int(char *dbname, int *ntables,
                                         char **tblnames[], int option)
{
    intptr_t dbptr;
    union list_tables *bf;
    int rc;
    char *rp;
    int tblnum;
    int len;
    int dbnum;
    char **tbl;

    dbptr = gtlcldbpooled();
    if (dbptr == 0)
        return -1;

    rc = opendb(dbname, &dbnum);
    if (rc != 0)
        return -2;

    bf = (union list_tables *)resolve_dbptr(dbptr);
    bf->req.opcode = 112;    /* OP_LISTTABLES */
    bf->req.option = option; /* list of table names */
    setrmtlen(sizeof(bf->req), dbptr);
    rc = fstsnd(dbptr, dbnum);
    if (rc != 0) {
        rtnlcldbpooled(dbptr);
        return -3;
    }

    *ntables = bf->rsp.ntables;
    *tblnames = tbl = malloc(sizeof(char *) * bf->rsp.ntables);

    rp = bf->rsp.names;
    for (tblnum = 0; tblnum < bf->rsp.ntables; tblnum++) {
        len = strlen(rp);
        tbl[tblnum] = strdup(rp);
        rp += len + 1;
    }
    rtnlcldbpooled(dbptr);
    return 0;
}

int get_another_db_csc2_files(char *dbname, int *ntables, char **tblnames[])
{
    return get_another_db_csc2_files_int(dbname, ntables, tblnames, 1);
}

int get_another_db_names(char *dbname, int *ntables, char **tblnames[])
{
    return get_another_db_csc2_files_int(dbname, ntables, tblnames, 0);
}
