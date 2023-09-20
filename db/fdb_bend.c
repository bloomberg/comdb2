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

/**
 * Foreign db back-end
 *
 *
 * This is the code running on remote server
 * The functions here are callbacks from communication layer
 *
 */

/**
 * Managing remote cursors at destination requires:
 * - handling transactions
 * - handling cursors for each transaction
 * - handling connectivity
 */

/*
 * NOTE: at Alex request, all of this has been simplified to use a socket per
 *cursor.  Lets hope we have enough ephemerals, or will run on socket fumes.
 *
 * Design details
 * Trying to eliminate the dependency on single socket
 * Each request is self-contained, and identified by cursor id (or cid)
 * A socket can be shared between multiple transactions and/or cursors
 * We will guarantee a header and message are sent atomically
 * End points will collect and store the replies
 * TODO: queuing
 *
 */
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <alloca.h>
#include <poll.h>

#include <plhash.h>
#include "debug_switches.h"

#include "util.h"
#include "bdb_api.h"
#include "genid.h"
#include "bdb_fetch.h"
#include "sql.h"
#include "bdb_api.h"
#include "bdb_cursor.h"
#include "tag.h"
#include "fdb_comm.h"
#include "fdb_bend.h"
#include "fdb_bend_sql.h"
#include "fdb_fend.h"
#include <flibc.h>
#include "comdb2uuid.h"

#include "logmsg.h"

extern int gbl_fdb_track;

struct svc_cursor {
    char *cid;    /* cursor id */
    char *tid;    /* trans id */
    int rid;      /* next receive id expected */
    int sid;      /* next send id will send */
    int code_rel; /* code release */

    int tblnum;             /* which table */
    int ixnum;              /* which index */
    bdb_cursor_ifn_t *bdbc; /* bdb cursor */
    char *outbuf;           /* temporary storage for output row */
    int outbuflen;          /* outbuf length */

    dbtran_type dbtran;

    int autocommit; /* set for no user controlled transactions */

    LINKC_T(
        struct svc_cursor) lnk; /* list of cursors of the same transaction */
    uuid_t ciduuid;
    uuid_t tiduuid;
};

typedef struct svc_center {
    hash_t *cursors_hash;
    pthread_rwlock_t cursors_rwlock;
} svc_center_t;

static svc_center_t *center;

static int fdb_convert_data(svc_cursor_t *cur, unsigned long long *genid,
                            char **data, int *datalen);
static int fdb_unp_to_p(Mem *m, int ncols, int hdrsz, int datasz, char *out,
                        int outlen);
/**
   ======== API ===========

   openCursor(transactionId, cursorId, tableName, tableRootpage,
transactionMode)
   closeCursor(cursorId)

   beginTransaction(transactionId)
   commitTransaction(transactionId)
   rollbackTransaction(transactionId)


   cursorFind(rowValue)
   cursorMove(op[First,Next,Prev,Last], pred[RowSet,Row]);

   cursorInsert(rowValue)
   cursorDelete()
   cursorUpdate(rowSet, rowValue)

**/

/**
 beginTransaction
 - allocate trans
 - wlock trans hash and add a new svc_trans; release lock
 */

/**
 openCursor
 - allocate rem_cur
 - link rem_cur to svc_trans
 - wlock cursors_hash and add rem_cur; release lock
 */

/**
 closeCursor

*/

unsigned int cidhash(const void *key, int len)
{
    /* steal from plhash */
    unsigned hash = 0;
    enum { PRIME = 8388013 };
    svc_cursor_t *cur = (svc_cursor_t *)key;
    key = cur->cid;
    for (int i = 0; i < 8; i++)
        hash = ((hash % PRIME) << 8) + (((uint8_t *)key)[i]);
    return hash;
}

int cidcmp(const void *key1, const void *key2, int len)
{
    svc_cursor_t *c1 = (svc_cursor_t *)key1, *c2 = (svc_cursor_t *)key2;

    return memcmp(c1->cid, c2->cid, sizeof(unsigned long long));
}

int fdb_svc_init(void)
{
    center = (svc_center_t *)calloc(1, sizeof(svc_center_t));
    if (!center) {
        logmsg(LOGMSG_ERROR, "%s: malloc %zu\n", __func__,
               sizeof(svc_center_t));
        return -1;
    }

    Pthread_rwlock_init(&center->cursors_rwlock, NULL);
    center->cursors_hash =
        hash_init_o(offsetof(svc_cursor_t, ciduuid), sizeof(uuid_t));

    return 0;
}

void fdb_svc_destroy(void)
{
    hash_free(center->cursors_hash);
    Pthread_rwlock_destroy(&center->cursors_rwlock);
}

svc_cursor_t *fdb_svc_cursor_open(char *tid, char *cid, int code_release,
                                  int version, int flags, int seq,
                                  struct sqlclntstate **pclnt)
{
    struct sqlclntstate *tran_clnt;
    svc_cursor_t *cur;
    int rc;
    uuid_t zerouuid;
    fdb_tran_t *trans;
    uuidstr_t cus, tus;

    comdb2uuid_clear(zerouuid);
    comdb2uuidstr((unsigned char *)cid, cus);
    comdb2uuidstr((unsigned char *)tid, tus);

    /* create cursor */
    cur = (svc_cursor_t *)calloc(1, sizeof(svc_cursor_t));
    if (!cur) {
        logmsg(LOGMSG_ERROR, "%s failed to create cursor\n", __func__);
        return NULL;
    }
    cur->cid = (char *)cur->ciduuid;
    cur->tid = (char *)cur->tiduuid;
    cur->code_rel = code_release;
    cur->tblnum = -1;
    cur->ixnum = -1;

    if (comdb2uuidcmp((unsigned char *)tid, zerouuid)) {
        cur->autocommit = 0;
    } else {
        cur->autocommit = 1;
    }

    rc = fdb_svc_cursor_open_sql(tid, cid, code_release, version, flags,
                                 pclnt);
    if (rc) {
        free(cur);
        logmsg(LOGMSG_ERROR, "%s failed open cursor sql rc=%d\n", __func__, rc);
        return NULL;
    }

    if (cur->autocommit == 0) {
        /* surprise, if tran_clnt != NULL, it is LOCKED, PINNED, FROZED */
        tran_clnt = fdb_svc_trans_get(tid);
        /* check of out-of-order rollbacks */
        if (!tran_clnt || tran_clnt->dbtran.rollbacked) {
            if (!tran_clnt)
                logmsg(LOGMSG_ERROR, "%s: missing client transaction %s!\n",
                       __func__, tus);
            else {
                logmsg(LOGMSG_ERROR, "%s: out of order rollback %s!\n",
                       __func__, tus);
                Pthread_mutex_unlock(&tran_clnt->dtran_mtx);
            }
            free(cur);
            free(*pclnt);
            return NULL;
        }

        (*pclnt)->dbtran.mode = tran_clnt->dbtran.mode;
        (*pclnt)->dbtran.shadow_tran = tran_clnt->dbtran.shadow_tran;

        trans = tran_clnt->dbtran.dtran->fdb_trans.top; /* there is only one! */

        /* link the guy in the transaction */
        listc_abl(&trans->cursors, cur);

        /* ok, we got the transaction begin, we should wait for the proper
           sequence number
           so that the magic of replicated consistency HAPPENS! whoever branded
           OCC lockless
           must have been selling snake oil in a previous life
         */
        fdb_sequence_request(tran_clnt, trans, seq);

        /* free this guy */
        Pthread_mutex_unlock(&tran_clnt->dtran_mtx);
    }

    memcpy(cur->cid, cid, sizeof(uuid_t));
    memcpy(cur->tid, tid, sizeof(uuid_t));

    if (gbl_fdb_track)
        logmsg(LOGMSG_INFO, "added %p to tid=%s cid=%s\n", cur, tus, cus);

    Pthread_rwlock_wrlock(&center->cursors_rwlock);
    hash_add(center->cursors_hash, cur);
    Pthread_rwlock_unlock(&center->cursors_rwlock);

    return cur;
}

int fdb_svc_cursor_close(char *cid, struct sqlclntstate **pclnt)
{
    struct sqlclntstate *tran_clnt;
    svc_cursor_t *cur;
    int rc = 0;
    int bdberr = 0;
    uuidstr_t us;

    comdb2uuidstr((unsigned char *)cid, us);

    /* retrieve cursor */
    Pthread_rwlock_wrlock(&center->cursors_rwlock);
    cur = hash_find(center->cursors_hash, cid);
    if (!cur) {
        Pthread_rwlock_unlock(&center->cursors_rwlock);

        logmsg(LOGMSG_ERROR, "%s: missing cursor %s\n", __func__, us);
        return -1;
    }

    hash_del(center->cursors_hash, cur);

    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "%p: CLosing rem cursor cid=%s autocommit=%d\n",
               (void *)pthread_self(), us, cur->autocommit);

    Pthread_rwlock_unlock(&center->cursors_rwlock);

    if (cur->bdbc) {
        rc = cur->bdbc->close(cur->bdbc, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed closing cursor rc=%d bdberr=%d\n",
                    __func__, rc, bdberr);
        }
        cur->bdbc = NULL;
    }

    /* take care of dedicated clnt structure */
    if (pclnt) {
        /* if transaction is autocommit, commit transaction */
        if (cur->autocommit == 0) {
            /* remove from transaction */

            /* NOTE: we don't synchonize cursor close with the updates, per
             * design */
            tran_clnt = fdb_svc_trans_get(cur->tid);
            if (!tran_clnt) {
                logmsg(LOGMSG_ERROR, "%s: missing client transaction %s!\n",
                        __func__, us);
            } else {
                /* link the guy in the transaction */
                listc_rfl(&tran_clnt->dbtran.dtran->fdb_trans.top->cursors,
                          cur);

                /* free this guy */
                Pthread_mutex_unlock(&tran_clnt->dtran_mtx);
            }

            (*pclnt)->dbtran.shadow_tran = NULL;
        }

        cleanup_clnt(*pclnt);

        free(*pclnt);

        *pclnt = NULL;
    }

    /* close cursor */
    free(cur);

    return rc;
}

/**
 * Move cursor
 *
 */
int fdb_svc_cursor_move(enum svc_move_types type, char *cid, char **data,
                        int *datalen, unsigned long long *genid,
                        char **datacopy, int *datacopylen)
{
    svc_cursor_t *cur;
    int bdberr = 0;
    int rc = 0;
    int irc = 0;

    /* retrieve cursor */
    Pthread_rwlock_rdlock(&center->cursors_rwlock);
    cur = hash_find_readonly(center->cursors_hash, cid);
    Pthread_rwlock_unlock(&center->cursors_rwlock);

    /* TODO: we assumed here nobody can close this cursor except ourselves; pls
       review
       when adding critical cursor termination */
    if (cur && cur->bdbc) {
        /* TODO: handle bdberr-s, mainly BDBERR_DEADLOCK */
        do {
            switch (type) {
            case SVC_MOVE_FIRST:
                rc = cur->bdbc->first(cur->bdbc, &bdberr);
                break;
            case SVC_MOVE_NEXT:
                rc = cur->bdbc->next(cur->bdbc, &bdberr);
                break;
            case SVC_MOVE_PREV:
                rc = cur->bdbc->prev(cur->bdbc, &bdberr);
                break;
            case SVC_MOVE_LAST:
                rc = cur->bdbc->last(cur->bdbc, &bdberr);
                break;
            default:
                logmsg(LOGMSG_FATAL, "%s: unknown move %d\n", __func__, type);
                abort();
            }
        } while (0); /* here loop until no deadlock */

        if (rc == IX_FND || rc == IX_NOTFND) {
            irc = fdb_convert_data(cur, genid, data, datalen);
            if (irc) {
                logmsg(LOGMSG_ERROR, "%s: failed to convert data rc=%d\n", __func__,
                        irc);
            }

            *datacopy = NULL; /* TODO: review datacopy */
            *datacopylen = 0;
        } else if (rc == IX_EMPTY || rc == IX_PASTEOF) {
            *datalen = 0;
            *data = NULL;
            *datacopylen = 0;
            *datacopy = NULL;
            *genid = -1LL;
        } else {
            logmsg(LOGMSG_ERROR, "%s: failed move %d\n", __func__, rc);
        }

        /* TODO: prefetching */
    } else {
        logmsg(LOGMSG_ERROR, "%s: unknown cursor id %llx\n", __func__,
                *(unsigned long long *)cid);
        rc = -1;
    }
    return rc;
}

static int fdb_fetch_blob_into_sqlite_mem(svc_cursor_t *cur, struct schema *sc,
                                          int fnum, Mem *m,
                                          unsigned long long genid)
{
    logmsg(LOGMSG_DEBUG, "%s\n", __func__);

    struct sql_thread *thd = NULL;
    struct field *f;
    int blobnum;
    blob_status_t blobs;
    int bdberr;

    int rc;
    int nretries = 0;

    bdb_state_type *state;

    f = &sc->member[fnum];
    blobnum = f->blob_index + 1;

    state = thedb->dbs[cur->tblnum]->handle;

again:
    if (is_genid_synthetic(genid)) {
        abort(); /*TODO: support snapisol */
        /*
        rc = osql_fetch_shadblobs_by_genid(pCur, &blobnum, &blobs, &bdberr);
        */
    } else {
        bdb_fetch_args_t args = {0};
        rc = bdb_fetch_blobs_by_rrn_and_genid_cursor(
            state, 2, genid, 1, &blobnum, blobs.bloblens, blobs.bloboffs,
            (void **)blobs.blobptrs, cur->bdbc, &args, &bdberr);
    }

    if (rc) {
        if (bdberr == BDBERR_DEADLOCK) {
            nretries++;

            if (NULL == thd) {
                thd = pthread_getspecific(query_info_key);
            }

            if (recover_deadlock(thedb->bdb_env, thd, NULL, 0)) {
                if (!gbl_rowlocks)
                    logmsg(LOGMSG_ERROR, "%s: %p failed dd recovery\n", __func__, (void *)pthread_self());
                return SQLITE_DEADLOCK;
            }

            if (nretries >= gbl_maxretries) {
                logmsg(LOGMSG_ERROR, "too much contention fetching "
                                     "tbl %s blob %s tried %d times\n",
                       thedb->dbs[cur->tblnum]->tablename, f->name, nretries);
                return SQLITE_DEADLOCK;
            }
            goto again;
        }
        return SQLITE_DEADLOCK;
    }

    if (blobs.blobptrs[0] == NULL) {
        m->z = NULL;
        m->flags = MEM_Null;
    } else {
        m->z = blobs.blobptrs[0];
        m->n = blobs.bloblens[0];
        m->flags = MEM_Dyn;
        m->xDel = free;

        if (f->type == SERVER_VUTF8) {
            m->flags |= MEM_Str;
            if (m->n > 0)
                --m->n; /* sqlite string lengths do not include NULL */
        } else
            m->flags |= MEM_Blob;
    }

    return 0;
}

static int fdb_get_data_int(svc_cursor_t *cur, struct schema *sc, char *in,
                            int fnum, Mem *m, uint8_t flip_orig,
                            unsigned long long genid)
{
    int null;
    i64 ival;
    double dval;
    int outdtsz = 0;
    int rc = 0;
    struct field *f = &(sc->member[fnum]);
    char *in_orig = in = in + f->offset;

    if (f->flags & INDEX_DESCEND) {
        if (gbl_sort_nulls_correctly) {
            in_orig[0] = ~in_orig[0];
        }
        if (flip_orig) {
            xorbufcpy(&in[1], &in[1], f->len - 1);
        } else {
            switch (f->type) {
            case SERVER_BINT:
            case SERVER_UINT:
            case SERVER_BREAL:
            case SERVER_DATETIME:
            case SERVER_DATETIMEUS:
            case SERVER_INTVYM:
            case SERVER_INTVDS:
            case SERVER_INTVDSUS:
            case SERVER_DECIMAL: {
                /* This way we don't have to flip them back. */
                char *p = alloca(f->len);
                p[0] = in[0];
                xorbufcpy(&p[1], &in[1], f->len - 1);
                in = p;
                break;
            }
            default:
                /* For byte and cstring, set the MEM_Xor flag and
                 * sqlite will flip the field */
                break;
            }
        }
    }

    switch (f->type) {

    case SERVER_UINT:
        rc = SERVER_UINT_to_CLIENT_INT(
            in, f->len, NULL /*convopts */, NULL /*blob */, &ival, sizeof(ival),
            &null, &outdtsz, NULL /*convopts */, NULL /*blob */);
        ival = flibc_ntohll(ival);
        m->u.i = ival;
        if (rc == -1)
            goto done;
        if (null)
            m->flags = MEM_Null;
        else
            m->flags = MEM_Int;
        break;

    case SERVER_BINT:
        rc = SERVER_BINT_to_CLIENT_INT(
            in, f->len, NULL /*convopts */, NULL /*blob */, &ival, sizeof(ival),
            &null, &outdtsz, NULL /*convopts */, NULL /*blob */);
        ival = flibc_ntohll(ival);
        m->u.i = ival;
        if (rc == -1)
            goto done;
        if (null) {
            m->flags = MEM_Null;
        } else {
            m->flags = MEM_Int;
        }
        break;

    case SERVER_BREAL:
        rc = SERVER_BREAL_to_CLIENT_REAL(
            in, f->len, NULL /*convopts */, NULL /*blob */, &dval, sizeof(dval),
            &null, &outdtsz, NULL /*convopts */, NULL /*blob */);
        dval = flibc_ntohd(dval);
        m->u.r = dval;
        if (rc == -1)
            goto done;
        if (null)
            m->flags = MEM_Null;
        else
            m->flags = MEM_Real;
        break;

    case SERVER_BCSTR:
        null = stype_is_null(in);
        if (null) {
            m->z = "";
            m->n = 0;
            m->flags = MEM_Null;
        } else {
            /* point directly at the ondisk string */
            m->z = &in[1]; /* skip header byte in front */
            if (flip_orig || !(f->flags & INDEX_DESCEND)) {
                m->n = cstrlenlim(&in[1], f->len - 1);
            } else {
                m->n = cstrlenlimflipped((unsigned char *)&in[1], f->len - 1);
            }
            m->flags = MEM_Str | MEM_Ephem;
        }
        break;

    case SERVER_BYTEARRAY:
        /* just point to bytearray directly */
        m->z = &in[1];
        m->n = f->len - 1;
        if (stype_is_null(in))
            m->flags = MEM_Null;
        else
            m->flags = MEM_Blob | MEM_Ephem;
        break;

    case SERVER_DATETIME:
        if (debug_switch_support_datetimes()) {
            assert(sizeof(server_datetime_t) == f->len);
            if (stype_is_null(in)) {
                m->flags = MEM_Null;
            } else {

                db_time_t sec = 0;
                unsigned short msec = 0;

                bzero(&m->du.dt, sizeof(dttz_t));

                /* TMP BROKEN DATETIME */
                if (in[0] == 0) {
                    memcpy(&sec, &in[1], sizeof(sec));
                    memcpy(&msec, &in[1] + sizeof(db_time_t), sizeof(msec));
                    sec = flibc_ntohll(sec);
                    msec = ntohs(msec);
                    m->du.dt.dttz_sec = sec;
                    m->du.dt.dttz_frac = msec;
                    m->du.dt.dttz_prec = DTTZ_PREC_MSEC;
                    m->flags = MEM_Datetime;
                    /*m->tz = NULL;*/
                } else {
                    rc = SERVER_BINT_to_CLIENT_INT(
                        in, sizeof(db_time_t) + 1, NULL /*convopts */,
                        NULL /*blob */, &(m->du.dt.dttz_sec),
                        sizeof(m->du.dt.dttz_sec), &null, &outdtsz,
                        NULL /*convopts */, NULL /*blob */);
                    if (rc == -1)
                        goto done;

                    memcpy(&msec, &in[1] + sizeof(db_time_t), sizeof(msec));
                    m->du.dt.dttz_sec = flibc_ntohll(m->du.dt.dttz_sec);
                    msec = ntohs(msec);
                    m->du.dt.dttz_frac = msec;
                    m->du.dt.dttz_prec = DTTZ_PREC_MSEC;
                    m->flags = MEM_Datetime;
                    /*m->tz = NULL;*/
                }
            }

        } else {
            /* previous broken case, treat as bytearay */
            m->z = &in[1];
            m->n = f->len - 1;
            if (stype_is_null(in))
                m->flags = MEM_Null;
            else
                m->flags = MEM_Blob;
        }
        break;

    case SERVER_DATETIMEUS:
        if (debug_switch_support_datetimes()) {
            assert(sizeof(server_datetimeus_t) == f->len);
            if (stype_is_null(in)) {
                m->flags = MEM_Null;
            } else {

                db_time_t sec = 0;
                unsigned int usec = 0;

                bzero(&m->du.dt, sizeof(dttz_t));

                /* TMP BROKEN DATETIME */
                if (in[0] == 0) {
                    memcpy(&sec, &in[1], sizeof(sec));
                    memcpy(&usec, &in[1] + sizeof(db_time_t), sizeof(usec));
                    sec = flibc_ntohll(sec);
                    usec = ntohs(usec);
                    m->du.dt.dttz_sec = sec;
                    m->du.dt.dttz_frac = usec;
                    m->du.dt.dttz_prec = DTTZ_PREC_USEC;
                    m->flags = MEM_Datetime;
                    /*m->tz = NULL;*/
                } else {
                    rc = SERVER_BINT_to_CLIENT_INT(
                        in, sizeof(db_time_t) + 1, NULL /*convopts */,
                        NULL /*blob */, &(m->du.dt.dttz_sec),
                        sizeof(m->du.dt.dttz_sec), &null, &outdtsz,
                        NULL /*convopts */, NULL /*blob */);
                    if (rc == -1)
                        goto done;

                    memcpy(&usec, &in[1] + sizeof(db_time_t), sizeof(usec));
                    m->du.dt.dttz_sec = flibc_ntohll(m->du.dt.dttz_sec);
                    usec = ntohs(usec);
                    m->du.dt.dttz_frac = usec;
                    m->du.dt.dttz_prec = DTTZ_PREC_USEC;
                    m->flags = MEM_Datetime;
                    /*m->tz = NULL;*/
                }
            }

        } else {
            /* previous broken case, treat as bytearay */
            m->z = &in[1];
            m->n = f->len - 1;
            if (stype_is_null(in))
                m->flags = MEM_Null;
            else
                m->flags = MEM_Blob;
        }
        break;

    case SERVER_INTVYM: {
        cdb2_client_intv_ym_t ym;
        rc = SERVER_INTVYM_to_CLIENT_INTVYM(in, f->len, NULL, NULL, &ym,
                                            sizeof(ym), &null, &outdtsz, NULL,
                                            NULL);
        if (rc)
            goto done;

        if (null) {
            m->flags = MEM_Null;
        } else {
            m->flags = MEM_Interval;
            m->du.tv.type = INTV_YM_TYPE;
            m->du.tv.sign = ntohl(ym.sign);
            m->du.tv.u.ym.years = ntohl(ym.years);
            m->du.tv.u.ym.months = ntohl(ym.months);
        }
        break;
    }

    case SERVER_INTVDS: {
        cdb2_client_intv_ds_t ds;

        rc = SERVER_INTVDS_to_CLIENT_INTVDS(in, f->len, NULL, NULL, &ds,
                                            sizeof(ds), &null, &outdtsz, NULL,
                                            NULL);
        if (rc)
            goto done;

        if (null)
            m->flags = MEM_Null;
        else {

            m->flags = MEM_Interval;
            m->du.tv.type = INTV_DS_TYPE;
            m->du.tv.sign = ntohl(ds.sign);
            m->du.tv.u.ds.days = ntohl(ds.days);
            m->du.tv.u.ds.hours = ntohl(ds.hours);
            m->du.tv.u.ds.mins = ntohl(ds.mins);
            m->du.tv.u.ds.sec = ntohl(ds.sec);
            m->du.tv.u.ds.frac = ntohl(ds.msec);
            m->du.tv.u.ds.prec = DTTZ_PREC_MSEC;
        }
        break;
    }

    case SERVER_INTVDSUS: {
        cdb2_client_intv_dsus_t ds;

        rc = SERVER_INTVDSUS_to_CLIENT_INTVDSUS(in, f->len, NULL, NULL, &ds,
                                                sizeof(ds), &null, &outdtsz,
                                                NULL, NULL);
        if (rc)
            goto done;

        if (null)
            m->flags = MEM_Null;
        else {

            m->flags = MEM_Interval;
            m->du.tv.type = INTV_DSUS_TYPE;
            m->du.tv.sign = ntohl(ds.sign);
            m->du.tv.u.ds.days = ntohl(ds.days);
            m->du.tv.u.ds.hours = ntohl(ds.hours);
            m->du.tv.u.ds.mins = ntohl(ds.mins);
            m->du.tv.u.ds.sec = ntohl(ds.sec);
            m->du.tv.u.ds.frac = ntohl(ds.usec);
            m->du.tv.u.ds.prec = DTTZ_PREC_USEC;
        }
        break;
    }

    case SERVER_VUTF8: {
        int len;
        /* get the length of the vutf8 string */
        memcpy(&len, &in[1], 4);
        len = ntohl(len);

        /* TODO use types.c's enum for header length */
        /* if the string is small enough to be stored in the record */
        if (len <= f->len - 5) {
            /* point directly at the ondisk string */
            null = stype_is_null(in);

            /* TODO use types.c's enum for header length */
            m->z = &in[5];

            /* sqlite string lengths do not include NULL */
            m->n = (len > 0) ? len - 1 : 0;

            /*fprintf(stderr, "m->n = %d\n", m->n); */

            if (null)
                m->flags = MEM_Null;
            else
                m->flags = MEM_Str | MEM_Ephem;
            break;
        } /* else fall through to blob code */
    }

    case SERVER_BLOB:
        rc = fdb_fetch_blob_into_sqlite_mem(cur, sc, fnum, m, genid);
        break;

    case SERVER_DECIMAL: {
        null = stype_is_null(in);
        if (null) {
            m->flags = MEM_Null;
        } else {
            m->flags = MEM_Interval;
            m->du.tv.type = INTV_DECIMAL_TYPE;
            m->du.tv.sign = 0;
            decimal_ondisk_to_sqlite(in, f->len, (decQuad *)&m->du.tv.u.dec,
                                     &null);
        }
        break;
    }

    default:
        logmsg(LOGMSG_ERROR, "fdb_get_data_int: unhandled type %d\n", f->type);
        break;
    }

done:
    if (flip_orig)
        return rc;

    if (f->flags & INDEX_DESCEND) {
        if (gbl_sort_nulls_correctly) {
            in_orig[0] = ~in_orig[0];
        }
        switch (f->type) {
        case SERVER_BCSTR:
        case SERVER_BYTEARRAY:
            m->flags |= MEM_Xor;
            break;
        }
    }

    return rc;
}

static int fdb_ondisk_to_unpacked(struct dbtable *db, struct schema *s,
                                  svc_cursor_t *cur, char *in,
                                  unsigned long long genid, Mem *m, int nMems,
                                  int *p_hdrsz, int *p_datasz, int *p_ncols)
{
    int datasz = 0, hdrsz = 0;
    int fnum;
    u32 type;
    int sz;
    int rc;
    int ncols = 0;
    u32 len;

    for (fnum = 0; fnum < nMems - 1; fnum++) {
        bzero(&m[fnum], sizeof(Mem));
        rc = fdb_get_data_int(cur, s, in, fnum, &m[fnum], 1, genid);
        if (rc)
            goto done;
        type =
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len);
        sz = sqlite3VdbeSerialTypeLen(type);
        datasz += sz;
        hdrsz += sqlite3VarintLen(type);
        /*fprintf( stderr, "%s:%d type=%d size=%d datasz=%d hdrsz=%d\n",
          __FILE__, __LINE__, type, sz, datasz, hdrsz);*/
    }
    ncols = fnum;

    if (genid) {
        bzero(&m[fnum], sizeof(Mem));
        m[fnum].u.i = genid;
        m[fnum].flags = MEM_Int;

        type =
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len);
        sz = sqlite3VdbeSerialTypeLen(type);
        datasz += sz;
        hdrsz += sqlite3VarintLen(sz);
        ncols++;
    }

    /* to account for size of header in header */
    hdrsz += sqlite3VarintLen(hdrsz);

    *p_hdrsz = hdrsz;
    *p_datasz = datasz;
    *p_ncols = ncols;

done:
    return rc;
}

static int fdb_ondisk_to_packed_sqlite_tz(struct dbtable *db, struct schema *s,
                                          char *in, unsigned long long genid,
                                          char *out, int maxout, int *reqsize,
                                          svc_cursor_t *cur)
{
    Mem *m;
    int i;
    int rc = 0;
    int datasz = 0;
    int hdrsz = 0;
    int ncols = 0;
    int nField;
    int rec_srt_off = gbl_sort_nulls_correctly ? 0 : 1;

#if 0 
   /* Raw index optimization */
   if (pCur && pCur->nCookFields >= 0)
      nField = pCur->nCookFields;
   else
#endif

    nField = s->nmembers; /* TODO: add skip/include logic */

    m = (Mem *)alloca((nField + 1 /*just in case for genid*/) * sizeof(Mem));

    *reqsize = 0;

    rc = fdb_ondisk_to_unpacked(db, s, cur, in, genid, m, nField + 1, &hdrsz,
                                &datasz, &ncols);

/*
   fprintf( stderr, "%s:%d hdrsz=%d ncols=%d maxout=%d\n",
   __FILE__, __LINE__, hdrsz, ncols, maxout);*/

    /* return real length */
    *reqsize = datasz + hdrsz;

    rc = fdb_unp_to_p(m, ncols, hdrsz, datasz, out, maxout);
    if (rc) {
        logmsg(LOGMSG_ERROR, "AAAAA!?!?\n");
        rc = -2;
        goto done;
    }

done:
    /* revert back the flipped fields */
    for (i = 0; i < nField; i++) {
        struct field *f = &s->member[i];
        if (f->flags & INDEX_DESCEND) {
            xorbuf(in + f->offset + rec_srt_off, f->len - rec_srt_off);
        }
    }
    return rc;
}

static int fdb_convert_data(svc_cursor_t *cur, unsigned long long *genid,
                            char **data, int *datalen)
{
    struct dbtable *db = thedb->dbs[cur->tblnum];
    struct schema *sc =
        (cur->ixnum < 0) ? db->schema : db->ixschema[cur->ixnum];
    uint8_t ver;
    int rrn;
    char *outdata;
    int outdatalen;
    int reqlen;
    int rc;

    *data = NULL;
    *datalen = 0;

    /* retrieve data from cursor */
    cur->bdbc->get_found_data(cur->bdbc, &rrn, genid, &outdatalen,
                              (void **)&outdata, &ver);

    /*convert this to sqlite */
    /* TODO: (NC) Where do we alloc cur->outbuf ? */
    rc = fdb_ondisk_to_packed_sqlite_tz(db, sc, outdata, *genid, cur->outbuf,
                                        cur->outbuflen, &reqlen, cur);
    if (rc == -2) {
        logmsg(LOGMSG_ERROR, "%s: preallocated buffer too small %d, needed %d\n",
                __func__, cur->outbuflen, reqlen);
        return -1;
    } else if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed ondisk_to_sqlite %d\n", __func__, rc);
        return -1;
    }

    *data = cur->outbuf;
    *datalen = reqlen;

    return 0;
}

struct key_mem_info {
    struct schema *s;
    Mem *m;
    int null;
    struct field_conv_opts_tz *convopts;
    const char *tzname;
    struct convert_failure *fail_reason;
    int fldidx;
};

static int fdb_packed_sqlite_to_ondisk_key_tz(struct dbtable *db, struct schema *s,
                                              char *in,
                                              unsigned long long genid,
                                              char *out, int maxout,
                                              int *reqsize, svc_cursor_t *cur)
{
    return -1;
}

/**
 * Find row matching a key, or the last match
 *
 */
int fdb_svc_cursor_find(char *cid, int keylen, char *key, int last,
                        unsigned long long *genid, int *datalen, char **data,
                        char **datacopy, int *datacopylen)
{
    svc_cursor_t *cur;
    struct dbtable *db;
    struct schema *sc;
    struct convert_failure convfail;
    int bdberr = 0;
    int rc = 0;
    int irc = 0;
    int use_keylen;
    char *use_key;

    /* retrieve cursor */
    Pthread_rwlock_rdlock(&center->cursors_rwlock);
    cur = hash_find_readonly(center->cursors_hash, cid);
    Pthread_rwlock_unlock(&center->cursors_rwlock);

    if (cur) {
        if (cur->ixnum < 0) {
            rc = fdb_packedsqlite_extract_genid(key, &use_keylen, cur->outbuf);
            if (rc < 0) {
                logmsg(LOGMSG_ERROR, "%s:%d: failed to convert key rc=%d\n",
                        __func__, __LINE__, rc);
                return -1;
            }
            use_key = cur->outbuf;
        } else {
            db = thedb->dbs[cur->tblnum];
            sc = (cur->ixnum < 0) ? db->schema : db->ixschema[cur->ixnum];

            /* convert key to ondisk */
            rc = sqlite_to_ondisk(sc, key, keylen, cur->outbuf, NULL, NULL, 0,
                                  &convfail, NULL);
            if (rc <= 0) {
                logmsg(LOGMSG_ERROR, "%s: failed to convert key rc=%d\n", __func__,
                        rc);
                return -1;
            }

            use_keylen = rc;
            use_key = cur->outbuf;
        }

        if (!last) {
            rc = cur->bdbc->find(cur->bdbc, use_key, use_keylen, 0, &bdberr);
        } else {
            assert(cur->ixnum >= 0); /* this path for indexes only */

            int keymax = thedb->dbs[cur->tblnum]->ix_keylen[cur->ixnum];

            rc = cur->bdbc->find_last_dup(cur->bdbc, use_key, use_keylen,
                                          keymax, 0, &bdberr);
        }
        if (rc != IX_FND && rc != IX_NOTFND && rc != IX_PASTEOF &&
            rc != IX_EMPTY) {
            logmsg(LOGMSG_ERROR, "%s failed find%s rc=%d bdberr=%d\n", __func__,
                    (last) ? "_last_dup" : "", rc, bdberr);
            return -1;
        }
        if (rc == IX_FND || (rc == IX_NOTFND && cur->ixnum >= 0)) {
            irc = fdb_convert_data(cur, genid, data, datalen);
            if (irc) {
                logmsg(LOGMSG_ERROR, "%s: failed to convert data rc=%d\n", __func__,
                        irc);
            }
            *datacopy = NULL;
            *datacopylen = 0;
        } else {
            *datalen = 0;
            *data = NULL;
            *datacopy = NULL;
            *datacopylen = 0;
            *genid = -1LL;
        }
    } else {
        uuidstr_t us;
        comdb2uuidstr((unsigned char*)cid, us);
        logmsg(LOGMSG_ERROR, "%s: unknown cursor id %s\n", __func__, us);
    }

    return rc;
}

/**
 * Transaction cursors support
 *
 * Init routine
 */
int fdb_svc_trans_init(struct sqlclntstate *clnt, const char *tid,
                       enum transaction_level lvl, int seq)
{
    trans_t *trans = NULL;
    fdb_tran_t *fdb_tran = NULL;

    trans = &clnt->dbtran;

    assert(trans != NULL);

    Pthread_mutex_lock(&clnt->dtran_mtx);

    if (lvl == TRANLEVEL_SOSQL || lvl == TRANLEVEL_RECOM ||
        lvl == TRANLEVEL_SERIAL || lvl == TRANLEVEL_SNAPISOL) {
        trans->mode = lvl;
    } else {
        trans->mode = TRANLEVEL_INVALID;
        Pthread_mutex_unlock(&clnt->dtran_mtx);
        logmsg(LOGMSG_ERROR, "%s: invalid transaction isolation level (%d)!\n",
               __func__, lvl);
        return -1;
    }

    trans->dtran = (fdb_distributed_tran_t *)calloc(
        1, sizeof(fdb_distributed_tran_t) + sizeof(fdb_tran_t));
    if (!trans->dtran) {
        Pthread_mutex_unlock(&clnt->dtran_mtx);
        logmsg(LOGMSG_ERROR, "%s: calloc!\n", __func__);
        return -1;
    }

    fdb_tran =
        (fdb_tran_t *)((char *)trans->dtran + sizeof(fdb_distributed_tran_t));
    fdb_tran->tid = (char *)fdb_tran->tiduuid;

    listc_init(&trans->dtran->fdb_trans, offsetof(struct fdb_tran, lnk));
    trans->dtran->remoted = 1;
    fdb_tran->seq = seq;
    comdb2uuidcpy((unsigned char *)fdb_tran->tid, (unsigned char *)tid);
    listc_atl(&trans->dtran->fdb_trans, fdb_tran);
    listc_init(&fdb_tran->cursors, offsetof(svc_cursor_t, lnk));

    /* explicit bump of sequence number, waiting for this */
    fdb_tran->seq++;

    Pthread_mutex_unlock(&clnt->dtran_mtx);

    return 0;
}

/**
 * Transaction cursors support
 *
 * Destroy routine
 */
void fdb_svc_trans_destroy(struct sqlclntstate *clnt)
{
    free(clnt->dbtran.dtran);
    clnt->dbtran.dtran = NULL;
}

/**
 * Retrieve a transaction, if any, for a cid
 *
 */
int fdb_svc_trans_get_tid(char *cid, char *tid)
{
    svc_cursor_t *cur;

    *tid = 0ULL;

    /* retrieve cursor */
    Pthread_rwlock_rdlock(&center->cursors_rwlock);
    cur = hash_find_readonly(center->cursors_hash, cid);
    if (cur) {
        memcpy(tid, cur->tiduuid, sizeof(uuid_t));
    }
    Pthread_rwlock_unlock(&center->cursors_rwlock);

    if (!cur) {
        uuidstr_t us;
        comdb2uuidstr((unsigned char *)cid, us);
        logmsg(LOGMSG_ERROR, "%s: looking for missing fdb tran for cid=%s\n",
               __func__, us);
        return -1;
    }

    return 0;
}

/**
 * Requests for the same transaction can come of differetn sockets
 * and will run in parallel.
 * We need to maintain the internal ordering for certain requests
 * to maintain to transaction consistency and semantics
 *
 * NOTE: this is called with dtran_mtx locked, and returns it LOCKED!
 */
void fdb_sequence_request(struct sqlclntstate *tran_clnt, fdb_tran_t *trans,
                          int seq)
{
    while (trans->seq < seq) {
        /* free this guy */
        Pthread_mutex_unlock(&tran_clnt->dtran_mtx);

        /* wait for all the updates to arrive; we could skip that in socksql,
           but I am
           not gonna do that now.  Recom ftw */
        poll(NULL, 0, 10);

        Pthread_mutex_lock(&tran_clnt->dtran_mtx);
    }

    /* bump the transactional sequence */
    trans->seq++;
}

/* pack an sqlite unpacked row to a packed row */
static int fdb_unp_to_p(Mem *m, int ncols, int hdrsz, int datasz, char *out,
                        int outlen)
{
    char *hdrbuf, *dtabuf;
    int fnum;
    int sz;
    u32 len;

    hdrbuf = out;
    dtabuf = out + hdrsz;

    /* enough room? */
    if ((datasz + hdrsz) > outlen) {
        return -1;
    }

    /* put header size in header */
    sz = sqlite3PutVarint((unsigned char *)hdrbuf, hdrsz);
    hdrbuf += sz;

    for (fnum = 0; fnum < ncols; fnum++) {
        sz = sqlite3VdbeSerialPut(
            (unsigned char *)dtabuf, &m[fnum],
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len));
        dtabuf += sz;
        sz = sqlite3PutVarint(
            (unsigned char *)hdrbuf,
            sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len));
        hdrbuf += sz;
        assert(hdrbuf <= (out + hdrsz));
    }

    return 0;
}
