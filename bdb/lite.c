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

/*
 * "Lite" tables with less fat and lower calories.
 *
 * A lite table is a single Berkeley btree.  These are used for the fstblk
 * table and the meta tables.
 */

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stddef.h>

#include <db.h>
#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

#include <plbitlib.h> /* for bset/btst */
#include <logmsg.h>

static int bdb_lite_exact_fetch_int(bdb_state_type *bdb_state, tran_type *tran,
                                    void *key, void *fnddta, int maxlen,
                                    int *fndlen, int *bdberr)
{
    int rc, outrc = 0, ixlen;
    DBT dbt_key, dbt_data;
    DB_TXN *tid = NULL;

    *bdberr = BDBERR_NOERROR;

    if (tran) {
        tid = tran->tid;
    }

    memset(&dbt_key, 0, sizeof(dbt_key));
    memset(&dbt_data, 0, sizeof(dbt_data));

    ixlen = bdb_state->ixlen[0];
    dbt_key.data = key;
    dbt_key.size = ixlen;
    dbt_key.ulen = ixlen;
    dbt_key.flags |= DB_DBT_USERMEM;

    dbt_data.data = fnddta;
    dbt_data.ulen = maxlen;
    dbt_data.flags |= DB_DBT_USERMEM;

    rc = bdb_state->dbp_data[0][0]->get(bdb_state->dbp_data[0][0], tid,
                                        &dbt_key, &dbt_data, 0);

    if (rc == 0) {
        *fndlen = dbt_data.size;

        /* check that the key returned matches the key we expected */
        if ((dbt_key.size != ixlen) ||
            (memcmp(dbt_key.data, key, ixlen) != 0)) {
            *bdberr = BDBERR_FETCH_DTA;
            outrc = -1;
        }
    } else {
        bdb_get_error(bdb_state, tid, rc, BDBERR_FETCH_DTA, bdberr,
                      "bdb_lite_exact_fetch_int");
        outrc = -1;
    }

    return outrc;
}

/*allows you to pass a transaction if you want to (it can still be NULL)*/
int bdb_lite_exact_fetch_tran(bdb_state_type *bdb_state, tran_type *tran,
                              void *key, void *fnddta, int maxlen, int *fndlen,
                              int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_exact_fetch_tran");
    rc = bdb_lite_exact_fetch_int(bdb_state, tran, key, fnddta, maxlen, fndlen,
                                  bdberr);
    BDB_RELLOCK();

    return rc;
}

int bdb_lite_exact_fetch(bdb_state_type *bdb_state, void *key, void *fnddta,
                         int maxlen, int *fndlen, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_exact_fetch");
    rc = bdb_lite_exact_fetch_int(bdb_state, NULL /*tran*/, key, fnddta, maxlen,
                                  fndlen, bdberr);
    BDB_RELLOCK();

    return rc;
}

int bdb_lite_exact_fetch_alloc_int(bdb_state_type *bdb_state, tran_type *tran,
                                   void *key, void **fnddta, int *fndlen,
                                   int *bdberr)
{
    int rc;
    DBT dbt_key = {0}, dbt_data = {0};
    int ixlen = bdb_state->ixlen[0];
    DB_TXN *tid = NULL;

    dbt_key.flags = DB_DBT_USERMEM;
    dbt_key.data = key;
    dbt_key.size = ixlen;

    dbt_data.flags = DB_DBT_MALLOC;

    if (tran) {
        tid = tran->tid;
    }
    rc = bdb_state->dbp_data[0][0]->get(bdb_state->dbp_data[0][0], tid,
                                        &dbt_key, &dbt_data, 0);
    if (rc == 0) {
        *fndlen = dbt_data.size;
        *fnddta = dbt_data.data;
    } else {
        bdb_get_error(bdb_state, tid, rc, BDBERR_FETCH_DTA, bdberr,
                      "bdb_lite_exact_fetch_int");
        rc = -1;
    }

    return rc;
}

int bdb_lite_exact_fetch_alloc(bdb_state_type *bdb_state, void *key,
                               void **fnddta, int *fndlen, int *bdberr)
{
    return bdb_lite_exact_fetch_alloc_tran(bdb_state, NULL /*tran*/, key,
                                           fnddta, fndlen, bdberr);
}

int bdb_lite_exact_fetch_alloc_tran(bdb_state_type *bdb_state, tran_type *tran,
                                    void *key, void **fnddta, int *fndlen,
                                    int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_exact_fetch");
    rc = bdb_lite_exact_fetch_alloc_int(bdb_state, tran, key, fnddta, fndlen,
                                        bdberr);
    BDB_RELLOCK();

    return rc;
}

static int bdb_lite_exact_var_fetch_int(bdb_state_type *bdb_state,
                                        tran_type *tran, void *key,
                                        void **fnddta, int *fndlen, int *bdberr)
{
    int rc, outrc = 0, ixlen;
    DBT dbt_key, dbt_data;
    DB_TXN *tid = NULL;

    *bdberr = BDBERR_NOERROR;
    *fndlen = 0;
    *fnddta = NULL;

    if (tran) {
        tid = tran->tid;
    }

    memset(&dbt_key, 0, sizeof(dbt_key));
    memset(&dbt_data, 0, sizeof(dbt_data));

    ixlen = bdb_state->ixlen[0];
    dbt_key.data = key;
    dbt_key.size = ixlen;
    dbt_key.ulen = ixlen;
    dbt_key.flags = DB_DBT_USERMEM;

    dbt_data.flags = DB_DBT_MALLOC;

    rc = bdb_state->dbp_data[0][0]->get(bdb_state->dbp_data[0][0], tid,
                                        &dbt_key, &dbt_data, 0);

    if (rc == 0) {
        *fndlen = dbt_data.size;

        /* check that the key returned matches the key we expected */
        if ((dbt_key.size != ixlen) ||
            (memcmp(dbt_key.data, key, ixlen) != 0)) {
            *bdberr = BDBERR_FETCH_DTA;
            free(dbt_data.data);
            outrc = -1;
        } else {
            *fnddta = dbt_data.data;
        }
    } else {
        bdb_get_error(bdb_state, tid, rc, BDBERR_FETCH_DTA, bdberr,
                      "bdb_lite_exact_fetch_int");
        outrc = -1;
    }

    return outrc;
}

int bdb_lite_exact_var_fetch(bdb_state_type *bdb_state, void *key,
                             void **fnddta, int *fndlen, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_exact_var_fetch");
    rc = bdb_lite_exact_var_fetch_int(bdb_state, NULL /*tran*/, key, fnddta,
                                      fndlen, bdberr);
    BDB_RELLOCK();

    return rc;
}

int bdb_lite_exact_var_fetch_tran(bdb_state_type *bdb_state, tran_type *tran,
                                  void *key, void **fnddta, int *fndlen,
                                  int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_exact_var_fetch");
    rc = bdb_lite_exact_var_fetch_int(bdb_state, tran, key, fnddta, fndlen,
                                      bdberr);
    BDB_RELLOCK();

    return rc;
}

int bdb_lite_fetch_partial(bdb_state_type *bdb_state, void *key_in, int klen_in,
                           void *key_out, int *fnd, int *bdberr)
{
    DBT dbt_key = {0}, dbt_data = {0};
    DB *db;
    DBC *dbcp = NULL;
    int rc;
    int ixlen = bdb_state->ixlen[0];
    void *fullkey = NULL;

    *bdberr = BDBERR_NOERROR;
    *fnd = 0;

    dbt_key.flags = DB_DBT_USERMEM;
    dbt_key.ulen = ixlen;
    dbt_data.flags = DB_DBT_PARTIAL;

    db = bdb_state->dbp_data[0][0];
    rc = db->cursor(db, NULL, &dbcp, 0);
    if (rc != 0) {
        bdb_cursor_error(bdb_state, NULL, rc, bdberr, __func__);
        return -1;
    }
    // dbt_key.data = key_in;
    dbt_key.data = fullkey = malloc(ixlen);
    memcpy(dbt_key.data, key_in, klen_in);
    dbt_key.size = klen_in;

    rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_SET_RANGE);
    /* didn't find, or found a different key? not found */
    if (rc == DB_NOTFOUND || memcmp(dbt_key.data, key_in, klen_in) != 0) {
        /* not found, but not an error */
        rc = 0;
        *fnd = 0;
        goto done;
    } else if (rc) {
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    /* lite tables have fixed key size, so we'll assume the caller is passing
     * a sufficiently large buffer */
    memcpy(key_out, dbt_key.data, dbt_key.size);
    *fnd = 1;

done:
    if (fullkey)
        free(fullkey);
    if (dbcp) {
        rc = dbcp->c_close(dbcp);
        if (rc == DB_LOCK_DEADLOCK) {
            *bdberr = BDBERR_BADARGS;
            rc = -1;
        } else if (rc) {
            *bdberr = BDBERR_MISC;
            logmsg(LOGMSG_ERROR, "%s: c_close rc %d\n", __func__, rc);
            rc = -1;
        }
    }
    return rc;
}

/* Fetch 1 or more keys from the database. */
static int bdb_lite_fetch_keys_int(bdb_state_type *bdb_state, tran_type *tran,
                                   void *firstkey, int direction, void *fndkeys,
                                   int maxfnd, int *numfnd, int *bdberr)
{
    DB *db;
    DBT dbt_key, dbt_data;
    DBC *dbcp;
    int ixlen, rc = 0;
    u_int32_t flags;
    char *nextkeyptr;
    DB_TXN *tid = NULL;

    *bdberr = BDBERR_NOERROR;

    if (tran) {
        tid = tran->tid;
    }

    /* buffer must allow for at least one record */
    if (maxfnd < 1) {
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    switch (direction) {
    case FETCH_INT_NEXT:
        flags = DB_NEXT;
        break;

    case FETCH_INT_PREV:
        flags = DB_PREV;
        break;

    default:
        *bdberr = BDBERR_BADARGS;
        return -1;
    }

    db = bdb_state->dbp_data[0][0];
    rc = db->cursor(db, tid, &dbcp, 0);
    if (rc != 0) {
        bdb_cursor_error(bdb_state, tid, rc, bdberr, "bdb_lite_fetch_keys_int");
        return -1;
    }

    ixlen = bdb_state->ixlen[0];
    *numfnd = 0;

    /* we don't want any data to be returned - we just want the keys */
    memset(&dbt_data, 0, sizeof(dbt_data));
    dbt_data.flags = DB_DBT_PARTIAL;

    nextkeyptr = fndkeys;

    /* go to the specified starting point if it is given */
    if (firstkey) {
        memset(&dbt_key, 0, sizeof(dbt_key));

        /* use first item in return buffer to give bdb somewhere to write to */
        memcpy(fndkeys, firstkey, ixlen);
        dbt_key.data = nextkeyptr;
        dbt_key.size = ixlen;
        dbt_key.ulen = ixlen;
        dbt_key.flags = DB_DBT_USERMEM;

        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_SET_RANGE);

        /* if we are past the last record and going in reverse: position
         * cursor on last record (which is also our first result) */
        if (direction == FETCH_INT_PREV && rc == DB_NOTFOUND) {
            rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, DB_LAST);

            /* if no error this is our first result, other error codes handled
             * below */
            if (rc == 0) {
                nextkeyptr += ixlen;
                (*numfnd)++;
            }
        }

        if (rc == DB_NOTFOUND) {
            dbcp->c_close(dbcp);
            return 0;
        } else if (rc != 0) {
            bdb_c_get_error(bdb_state, tid, &dbcp, rc, BDBERR_FETCH_IX, bdberr,
                            "bdb_lite_fetch_keys_int");
            return -1;
        }

        /* set cursor to start point ok.  on a forward op, if key isn't what
         * we expected then we are on the next highest key and we want to return
         * it. */
        if (direction == FETCH_INT_NEXT &&
            memcmp(dbt_key.data, firstkey, ixlen) != 0) {
            nextkeyptr += ixlen;
            (*numfnd)++;
        }
    }
    /* TODO set cursor to DB_LAST if going in reverse and no start point given
     */

    while (*numfnd < maxfnd) {
        memset(&dbt_key, 0, sizeof(dbt_key));

        dbt_key.data = nextkeyptr;
        dbt_key.ulen = ixlen;
        dbt_key.flags = DB_DBT_USERMEM;

        rc = dbcp->c_get(dbcp, &dbt_key, &dbt_data, flags);

        if (rc == DB_NOTFOUND) {
            /* no more data to be found */
            break;
        } else if (rc != 0) {
            bdb_c_get_error(bdb_state, tid, &dbcp, rc, BDBERR_FETCH_IX, bdberr,
                            "bdb_lite_fetch_keys_int");
            return -1;
        } else {
            (*numfnd)++;
            nextkeyptr += ixlen;
        }
    }

    /* a happy ending */
    dbcp->c_close(dbcp);
    return 0;
}

int bdb_lite_fetch_keys_fwd_tran(bdb_state_type *bdb_state, tran_type *tran,
                                 void *firstkey, void *fndkeys, int maxfnd,
                                 int *numfnd, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_fetch_keys_fwd");
    rc = bdb_lite_fetch_keys_int(bdb_state, tran, firstkey, FETCH_INT_NEXT,
                                 fndkeys, maxfnd, numfnd, bdberr);
    BDB_RELLOCK();

    return rc;
}

int bdb_lite_fetch_keys_fwd(bdb_state_type *bdb_state, void *firstkey,
                            void *fndkeys, int maxfnd, int *numfnd, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_fetch_keys_fwd");
    rc = bdb_lite_fetch_keys_int(bdb_state, NULL /*tran*/, firstkey,
                                 FETCH_INT_NEXT, fndkeys, maxfnd, numfnd,
                                 bdberr);
    BDB_RELLOCK();

    return rc;
}

int bdb_lite_fetch_keys_bwd_tran(bdb_state_type *bdb_state, tran_type *tran,
                                 void *firstkey, void *fndkeys, int maxfnd,
                                 int *numfnd, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_fetch_keys_bwd");
    rc = bdb_lite_fetch_keys_int(bdb_state, tran, firstkey, FETCH_INT_PREV,
                                 fndkeys, maxfnd, numfnd, bdberr);
    BDB_RELLOCK();

    return rc;
}

int bdb_lite_fetch_keys_bwd(bdb_state_type *bdb_state, void *firstkey,
                            void *fndkeys, int maxfnd, int *numfnd, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_fetch_keys_bwd");
    rc = bdb_lite_fetch_keys_int(bdb_state, NULL /*tran*/, firstkey,
                                 FETCH_INT_PREV, fndkeys, maxfnd, numfnd,
                                 bdberr);
    BDB_RELLOCK();

    return rc;
}

static int bdb_lite_add_int(bdb_state_type *bdb_state, tran_type *tran,
                            void *dtaptr, int dtalen, void *key, int *bdberr)
{
    int rc;
    u_int32_t flags = 0;
    DBT dbt_key, dbt_data;
    DB_TXN *tid;

    if (!bdb_state->read_write) {
        *bdberr = BDBERR_READONLY;
        return 0;
    }

    *bdberr = BDBERR_NOERROR;

    if (tran) {
        tid = tran->tid;
    } else {
        tid = NULL;
    }

    if (NULL == tid)
        flags |= DB_AUTO_COMMIT;

    memset(&dbt_key, 0, sizeof(dbt_key));
    memset(&dbt_data, 0, sizeof(dbt_data));
    dbt_key.flags |= DB_DBT_USERMEM;
    dbt_data.flags |= DB_DBT_USERMEM;

    dbt_key.data = key;
    dbt_key.size = bdb_state->ixlen[0];

    dbt_data.data = dtaptr;
    dbt_data.size = dtalen;

    rc =
        bdb_state->dbp_data[0][0]->put(bdb_state->dbp_data[0][0], tid, &dbt_key,
                                       &dbt_data, flags | DB_NOOVERWRITE);

    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        case DB_KEYEXIST:
            *bdberr = BDBERR_ADD_DUPE;
            break;
        default:
            logmsg(LOGMSG_ERROR, "bdb_lite_add_int x3.  rc=%d\n", rc);
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }

    return 0;
}

int bdb_lite_add(bdb_state_type *bdb_state, tran_type *tran, void *dtaptr,
                 int dtalen, void *key, int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_add");
    rc = bdb_lite_add_int(bdb_state, tran, dtaptr, dtalen, key, bdberr);
    BDB_RELLOCK();

    return rc;
}

static int bdb_lite_exact_del_int(bdb_state_type *bdb_state, tran_type *tran,
                                  void *key, int *bdberr)
{
    int rc;
    DBT dbt_key;
    u_int32_t flags = 0;
    DB_TXN *tid;

    if (!bdb_state->read_write) {
        *bdberr = BDBERR_READONLY;
        return 0;
    }

    if (tran) {
        tid = tran->tid;
        /*
        if(NULL == tid)
            fprintf(stderr, "Put a breakpoint here!\n");
         */
    } else {
        tid = NULL;
    }

    if (NULL == tid)
        flags |= DB_AUTO_COMMIT;

    *bdberr = BDBERR_NOERROR;

    memset(&dbt_key, 0, sizeof(dbt_key));
    dbt_key.data = key;
    dbt_key.size = bdb_state->ixlen[0];
    dbt_key.flags |= DB_DBT_USERMEM;

    rc = bdb_state->dbp_data[0][0]->del(bdb_state->dbp_data[0][0], tid,
                                        &dbt_key, flags);

    if (rc != 0) {
        switch (rc) {
        case DB_REP_HANDLE_DEAD:
        case DB_LOCK_DEADLOCK:
            *bdberr = BDBERR_DEADLOCK;
            break;
        case DB_NOTFOUND:
            *bdberr = BDBERR_DEL_DTA;
            break;
        default:
            logmsg(LOGMSG_ERROR, "bdb_lite_exact_del rc=%d\n", rc);
            *bdberr = BDBERR_MISC;
            break;
        }
        return -1;
    }

    return 0;
}

int bdb_lite_exact_del(bdb_state_type *bdb_state, tran_type *tran, void *key,
                       int *bdberr)
{
    int rc;

    BDB_READLOCK("bdb_lite_exact_del");
    rc = bdb_lite_exact_del_int(bdb_state, tran, key, bdberr);
    BDB_RELLOCK();

    return rc;
}

int bdb_lite_list_records(bdb_state_type *bdb_state,
                          int (*userfunc)(bdb_state_type *bdb_state, void *key,
                                          int keylen, void *data, int datalen,
                                          int *bdberr),
                          int *bdberr)
{
    DB *db;
    DBC *dbcp;
    DBT key, data;
    int rc = 0;

    *bdberr = 0;

    db = bdb_state->dbp_data[0][0];

    rc = db->cursor(db, NULL, &dbcp, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d: failed to open cursor rc=%d\n", __FILE__,
                __LINE__, rc);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    bzero(&key, sizeof(key));
    bzero(&data, sizeof(data));
    key.flags = data.flags = DB_DBT_MALLOC;

    rc = dbcp->c_get(dbcp, &key, &data, DB_FIRST);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d: failed to get first record rc=%d\n", __FILE__,
                __LINE__, rc);
        *bdberr = BDBERR_MISC;
        rc = -1;
        goto done;
    }

    do {
        rc = userfunc(bdb_state, key.data, key.size, data.data, data.size,
                      bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d: failed userfunc rc=%d bdberr=%d\n",
                    __FILE__, __LINE__, rc, *bdberr);
            goto done;
        }

        rc = dbcp->c_get(dbcp, &key, &data, DB_NEXT);
    } while (rc == 0);

    if (rc == DB_NOTFOUND) {
        rc = 0;
    }

done:
    dbcp->c_close(dbcp);
    return rc;
}
