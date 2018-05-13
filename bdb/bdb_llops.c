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
#include <string.h>
#include <stddef.h>

#include "bdb_api.h"
#include "bdb_int.h"

#include <fsnapf.h>
#include "util.h"

extern char *comdb2_asprintf(const char *fmt, ...);

int bdb_llop_add(bdb_state_type *bdb_state, void *trans, int raw, int stripe,
                 int dtafile, int ix, void *key, int keylen, void *data,
                 int datalen, void *dtacopy, int dtacopylen, char **errstr)
{
    DBC *dbc = NULL;
    DBT dkey = {0}, ddata = {0};
    int rc;
    tran_type *t = (tran_type *)trans;
    DB *db;
    unsigned long long genid;
    int made_trans = 0;

    *errstr = NULL;

    if (t == NULL) {
        t = bdb_tran_begin(bdb_state, NULL, &rc);
        if (t == NULL) {
            *errstr = comdb2_asprintf("can't start transaction rc %d\n", rc);
            rc = -1;
            goto done;
        }
        made_trans = 1;
    }

    DB_TXN *txn = t->tid;

    /* which db? */
    if (ix != -1) {
        if (ix >= bdb_state->numix) {
            *errstr =
                comdb2_asprintf("index out of range (%d, table has %d indices)",
                                ix, bdb_state->numix);
            rc = -1;
            goto done;
        }
        db = bdb_state->dbp_ix[ix];
        if (raw) {
            rc = db->cursor(db, txn, &dbc, 0);
            if (rc) {
                *errstr = comdb2_asprintf(
                    "can't allocate cursor on %s index %d, rc %d",
                    bdb_state->name, ix, rc);
                rc = -1;
                goto done;
            }
        }
    } else {
        if (stripe < 0 || stripe >= bdb_state->attr->dtastripe) {
            *errstr =
                comdb2_asprintf("stripe out of range (%d, db has %d stripes)",
                                stripe, bdb_state->attr->dtastripe);
            rc = -1;
            goto done;
        }
        if (dtafile < 0 && dtafile >= bdb_state->numdtafiles) {
            *errstr =
                comdb2_asprintf("dtafile out of range (%d, db has %d dtafiles)",
                                dtafile, bdb_state->numdtafiles);
            rc = -1;
            goto done;
        }
        if (stripe > 0 && dtafile > 0 && !bdb_state->attr->blobstripe) {
            *errstr = comdb2_asprintf(
                "stripe is set for a blob, but database isn't blobstripe");
            rc = -1;
            goto done;
        }
        db = bdb_state->dbp_data[dtafile][stripe];
        if (raw) {
            rc = db->cursor(db, txn, &dbc, 0);
            if (rc) {
                *errstr = comdb2_asprintf(
                    "can't allocate cursor on %s dtafile %d stripe %d, rc %d",
                    bdb_state->name, dtafile, stripe, rc);
                rc = -1;
                goto done;
            }
        }
    }
    dkey.data = key;
    dkey.size = keylen;
    ddata.data = data;
    ddata.size = datalen;

    if (raw) {
        /* note that this overrides any recordin the db with the same key value
         */
        rc = dbc->c_put(dbc, &dkey, &ddata, DB_KEYFIRST);
    } else {
        if (ix == -1)
            rc = bdb_put_pack(bdb_state, dtafile > 0 ? 1 : 0, db, txn, &dkey,
                              &ddata, 0);
        else {
            int bdberr;

            if (ddata.size != sizeof(sizeof(unsigned long long))) {
                *errstr =
                    comdb2_asprintf("non-raw record, and key payload isn't a "
                                    "genid (expected size %d, got %d)",
                                    (int)sizeof(unsigned long long), datalen);
                rc = -1;
                goto done;
            }
            if (dkey.size != bdb_state->ixlen[ix]) {
                *errstr = comdb2_asprintf(
                    "unexpected key size for %s ix %d (expected %d, got %d)",
                    bdb_state->name, ix, bdb_state->ixlen[ix], dkey.size);
                rc = -1;
                goto done;
            }
            memcpy(&genid, ddata.data, sizeof(unsigned long long));

            rc = bdb_prim_addkey_genid(bdb_state, t, dkey.data, ix, 2, genid,
                                       dtacopy, dtacopylen, 0, &bdberr);
            if (rc) {
                *errstr = comdb2_asprintf(
                    "bdb_prim_addkey_genid rc %d bdberr %d", rc, bdberr);
                rc = -1;
                goto done;
            }
        }
    }
    if (rc) {
        *errstr = comdb2_asprintf("add rc %d", rc);
        rc = -1;
        goto done;
    }

    rc = 0;

done:
    if (dbc) {
        int crc;
        crc = dbc->c_close(dbc);
        if (crc) {
            if (!(*errstr))
                *errstr = comdb2_asprintf("close cursor rc %d\n", rc);
            rc = crc;
        }
    }
    /* if we made a transaction, commit or abort it */
    if (made_trans && t) {
        if (rc == 0) {
            int bdberr;
            int crc;
            crc = bdb_tran_commit(bdb_state, t, &bdberr);
            if (crc) {
                if (!(*errstr))
                    *errstr =
                        comdb2_asprintf("commit rc %d bdberr %d\n", rc, bdberr);
                rc = crc;
            }
        } else {
            int bdberr;
            int crc;
            crc = bdb_tran_abort(bdb_state, t, &bdberr);
            if (crc) {
                if (!(*errstr))
                    *errstr =
                        comdb2_asprintf("commit rc %d bdberr %d\n", rc, bdberr);
                /* leave rc alone */
            }
        }
    }

    return rc;
}

int bdb_llop_del(bdb_state_type *bdb_state, void *trans, int stripe,
                 int dtafile, int ix, void *key, int keylen, char **errstr)
{
    DBC *dbc = NULL;
    DBT dkey = {0}, ddata = {0};
    int rc = 0;
    tran_type *t = (tran_type *)trans;
    DB *db;
    unsigned long long genid;
    int made_trans = 0;

    *errstr = NULL;

    if (t == NULL) {
        t = bdb_tran_begin(bdb_state, NULL, &rc);
        if (t == NULL) {
            *errstr = comdb2_asprintf("can't start transaction rc %d\n", rc);
            rc = -1;
            goto done;
        }
        made_trans = 1;
    }

    DB_TXN *txn = t->tid;

    /* which db? */
    if (ix != -1) {
        if (ix >= bdb_state->numix) {
            *errstr =
                comdb2_asprintf("index out of range (%d, table has %d indices)",
                                ix, bdb_state->numix);
            rc = -1;
            goto done;
        }
        db = bdb_state->dbp_ix[ix];
        rc = db->cursor(db, txn, &dbc, 0);
        if (rc) {
            *errstr =
                comdb2_asprintf("can't allocate cursor on %s index %d, rc %d",
                                bdb_state->name, ix, rc);
            rc = -1;
            goto done;
        }
    } else {
        if (stripe < 0 || stripe >= bdb_state->attr->dtastripe) {
            *errstr =
                comdb2_asprintf("stripe out of range (%d, db has %d stripes)",
                                stripe, bdb_state->attr->dtastripe);
            rc = -1;
            goto done;
        }
        if (dtafile < 0 && dtafile >= bdb_state->numdtafiles) {
            *errstr =
                comdb2_asprintf("dtafile out of range (%d, db has %d dtafiles)",
                                dtafile, bdb_state->numdtafiles);
            rc = -1;
            goto done;
        }
        if (stripe > 0 && dtafile > 0 && !bdb_state->attr->blobstripe) {
            *errstr = comdb2_asprintf(
                "stripe is set for a blob, but database isn't blobstripe");
            rc = -1;
            goto done;
        }
        db = bdb_state->dbp_data[dtafile][stripe];
        rc = db->cursor(db, txn, &dbc, 0);
        if (rc) {
            *errstr = comdb2_asprintf(
                "can't allocate cursor on %s dtafile %d stripe %d, rc %d",
                bdb_state->name, dtafile, stripe, rc);
            rc = -1;
            goto done;
        }
    }
    dkey.data = key;
    dkey.size = keylen;
    ddata.flags = DB_DBT_MALLOC;
    rc = dbc->c_get(dbc, &dkey, &ddata, DB_SET);
    if (rc) {
        *errstr = comdb2_asprintf("find rc %d", rc);
        goto done;
    }
    rc = dbc->c_del(dbc, 0);
    if (rc) {
        *errstr = comdb2_asprintf("delete rc %d", rc);
        goto done;
    }
    free(ddata.data);

done:
    if (dbc) {
        int crc;
        crc = dbc->c_close(dbc);
        if (rc == 0) {
            *errstr = comdb2_asprintf("close cursor rc %d\n", crc);
            rc = crc;
        }
    }
    if (made_trans && t) {
        int crc;
        if (rc == 0) {
            int bdberr;
            rc = bdb_tran_commit(bdb_state, t, &bdberr);
        } else {
            int bdberr;
            crc = bdb_tran_abort(bdb_state, t, &bdberr);
            /* don't override the original return code */
        }
    }

    return rc;
}

void *bdb_llop_find(bdb_state_type *bdb_state, void *trans, int raw, int stripe,
                    int dtafile, int ix, void *key, int keylen, int *fndlen,
                    uint8_t *ver, char **errstr)
{
    DBC *dbc = NULL;
    DBT dkey = {0}, ddata = {0};
    int rc;
    DB *db;
    unsigned long long genid;
    tran_type *t = (tran_type *)trans;
    DB_TXN *txn;

    *errstr = NULL;

    if (t)
        txn = t->tid;
    else
        txn = NULL;

    /* which db? */
    if (ix != -1) {
        if (ix >= bdb_state->numix) {
            *errstr =
                comdb2_asprintf("index out of range (%d, table has %d indices)",
                                ix, bdb_state->numix);
            rc = -1;
            goto done;
        }
        db = bdb_state->dbp_ix[ix];
        rc = db->cursor(db, txn, &dbc, 0);
        if (rc) {
            *errstr =
                comdb2_asprintf("can't allocate cursor on %s index %d, rc %d",
                                bdb_state->name, ix, rc);
            rc = -1;
            goto done;
        }
    } else {
        if (stripe < 0 || stripe >= bdb_state->attr->dtastripe) {
            *errstr =
                comdb2_asprintf("stripe out of range (%d, db has %d stripes)",
                                stripe, bdb_state->attr->dtastripe);
            rc = -1;
            goto done;
        }
        if (dtafile < 0 && dtafile >= bdb_state->numdtafiles) {
            *errstr =
                comdb2_asprintf("dtafile out of range (%d, db has %d dtafiles)",
                                dtafile, bdb_state->numdtafiles);
            rc = -1;
            goto done;
        }
        if (stripe > 0 && dtafile > 0 && !bdb_state->attr->blobstripe) {
            *errstr = comdb2_asprintf(
                "stripe is set for a blob, but database isn't blobstripe");
            rc = -1;
            goto done;
        }
        db = bdb_state->dbp_data[dtafile][stripe];
        rc = db->cursor(db, txn, &dbc, 0);
        if (rc) {
            *errstr = comdb2_asprintf(
                "can't allocate cursor on %s dtafile %d stripe %d, rc %d",
                bdb_state->name, dtafile, stripe, rc);
            rc = -1;
            goto done;
        }
    }
    dkey.data = key;
    dkey.size = keylen;
    ddata.flags = DB_DBT_MALLOC;
    if (raw || ix != -1) {
        rc = dbc->c_get(dbc, &dkey, &ddata, DB_SET);
    } else {
        uint8_t ver;
        rc = bdb_cget_unpack(bdb_state, dbc, &dkey, &ddata, &ver, DB_SET);
    }
    if (rc) {
        *errstr = comdb2_asprintf("find rc %d", rc);
        goto done;
    }
    *fndlen = ddata.size;

done:
    if (dbc) {
        int crc;
        crc = dbc->c_close(dbc);
        if (rc == 0)
            rc = crc;
    }
    if (rc == 0)
        return ddata.data;
    else
        return NULL;
}
