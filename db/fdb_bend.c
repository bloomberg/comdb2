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
#include "fdb_util.h"
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
    int isuuid;
};

typedef struct svc_center {
    hash_t *cursors_hash;
    hash_t *cursorsuuid_hash;
    pthread_rwlock_t cursors_rwlock;
} svc_center_t;

static svc_center_t *center;

static int fdb_convert_data(svc_cursor_t *cur, unsigned long long *genid,
                            char **data, int *datalen);

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
        logmsg(LOGMSG_ERROR, "%s: malloc %d\n", __func__, sizeof(svc_center_t));
        return -1;
    }

    pthread_rwlock_init(&center->cursors_rwlock, NULL);
    center->cursors_hash = hash_init_user(cidhash, cidcmp, 0, 0);
    center->cursorsuuid_hash =
        hash_init_o(offsetof(svc_cursor_t, ciduuid), sizeof(uuid_t));

    return 0;
}

void fdb_svc_destroy(void)
{
    hash_free(center->cursors_hash);
    hash_free(center->cursorsuuid_hash);
    pthread_rwlock_destroy(&center->cursors_rwlock);
}

svc_cursor_t *fdb_svc_cursor_open_master(char *tid, char *cid, int version)
{
    /* inline processing */

    return NULL;
}

svc_cursor_t *fdb_svc_cursor_open(char *tid, char *cid, int code_release,
                                  int version, int flags, int seq, int isuuid,
                                  struct sqlclntstate **pclnt)
{
    struct sqlclntstate *tran_clnt;
    svc_cursor_t *cur;
    int rc;
    uuid_t zerouuid;
    fdb_tran_t *trans;

    comdb2uuid_clear(zerouuid);
    assert(flags == FDB_MSG_CURSOR_OPEN_SQL);

    /* create cursor */
    cur = (svc_cursor_t *)calloc(1, sizeof(svc_cursor_t));
    if (!cur) {
        logmsg(LOGMSG_ERROR, "%s failed to create cursor\n", __func__);
        return NULL;
    }
    cur->cid = cur->ciduuid;
    cur->tid = cur->tiduuid;
    cur->code_rel = code_release;
    cur->tblnum = -1;
    cur->ixnum = -1;

    if (isuuid) {
        if (comdb2uuidcmp(tid, zerouuid)) {
            cur->autocommit = 0;
        } else {
            cur->autocommit = 1;
        }
    } else {
        if (*(unsigned long long *)tid != 0ULL) {
            cur->autocommit = 0;
        } else {
            cur->autocommit = 1;
        }
    }

    rc = fdb_svc_cursor_open_sql(tid, cid, code_release, version, flags, isuuid,
                                 pclnt);
    if (rc) {
        free(cur);
        logmsg(LOGMSG_ERROR, "%s failed open cursor sql rc=%d\n", __func__, rc);
        return NULL;
    }

    if (cur->autocommit == 0) {
        /* surprise, if tran_clnt != NULL, it is LOCKED, PINNED, FROZED */
        tran_clnt = fdb_svc_trans_get(tid, isuuid);
        if (!tran_clnt) {
            logmsg(LOGMSG_ERROR, "%s: missing client transaction %llx!\n", __func__,
                    *(unsigned long long *)tid);
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
        pthread_mutex_unlock(&tran_clnt->dtran_mtx);
    }

    if (isuuid) {
        memcpy(cur->cid, cid, sizeof(uuid_t));
        memcpy(cur->tid, tid, sizeof(uuid_t));
    } else {
        memcpy(cur->cid, cid, sizeof(unsigned long long));
        memcpy(cur->tid, tid, sizeof(unsigned long long));
    }

    if (gbl_fdb_track)
       logmsg(LOGMSG_ERROR, "added %p to tid=%llx cid=%llx\n", cur,
               *(unsigned long long *)cur->tid,
               *(unsigned long long *)cur->cid);

    pthread_rwlock_wrlock(&center->cursors_rwlock);
    if (isuuid)
        hash_add(center->cursorsuuid_hash, cur);
    else
        hash_add(center->cursors_hash, cur);
    pthread_rwlock_unlock(&center->cursors_rwlock);

    return cur;
}

#if 0
svc_cursor_t* fdb_svc_cursor_open(char *tid, char *cid, int rootpage, int version, int flags)
{
   struct db      *db;
   struct schema  *sc;
   svc_cursor_t   *cur;
   bdb_state_type *state;
   int            bdberr = 0;
   int            tblnum;
   int            ixnum;
   int            outlen;
   int            rc;

   if (flags != FDB_MSG_CURSOR_OPEN_SQL)
   {
      /* TODO lock the table pointed by rootpage as part of the transaction */
      /* TODO: check the version here, after transaction is created */

      struct sql_thread *thd = pthread_getspecific(query_info_key);
      get_sqlite_tblnum_and_ixnum(thd, rootpage, &tblnum, &ixnum);

      if (tblnum<0 || tblnum >=thedb->num_dbs || thedb->dbs[tblnum]->nix < ixnum)
      {
         fprintf(stderr, "%s: failed to retrieve bdb_state for table rootpage=%d\n", 
               __func__, rootpage);
         return NULL;
      }

      db = thedb->dbs[tblnum];
      sc = (ixnum<0)?db->schema:db->ixschema[ixnum];
      state = thedb->dbs[tblnum]->handle;

      outlen = schema_var_size(sc);
   }
   else
   {
      tblnum = -1;
      ixnum = -1;

      db = NULL;
      sc = NULL;
      state = NULL;
      outlen = 0; /* ?*/
   }


   assert(flags == FDB_MSG_CURSOR_OPEN_SQL);

   /* create cursor */
   cur = (svc_cursor_t*)calloc(1, sizeof(svc_cursor_t) + outlen);
   if(!cur)
   {
      fprintf(stderr, "%s failed to create cursor\n", __func__);
      return NULL;
   }

   /* create a transaction for cursor */
   rc = fdb_svc_trans_begin_2(cur);
   if (rc)
   {
      free(cur);
      return NULL;
   }
   cur->autocommit = 1; /* for read path */

   memcpy(cur->cid, cid, sizeof(cur->cid));
   memcpy(cur->tid, tid, sizeof(cur->tid));
   cur->rootpage = rootpage;
   cur->tblnum = tblnum;
   cur->ixnum = ixnum;
   if (gbl_fdb_track)
      printf("added %p to tid=%llx cid=%llx\n",
            cur,
            *(unsigned long long*)cur->tid,
            *(unsigned long long*)cur->cid);
   cur->rid = cur->sid = 0;
   cur->outbuf = ((char*)cur)+sizeof(svc_cursor_t);
   cur->outbuflen = outlen;

   if (flags != FDB_MSG_CURSOR_OPEN_SQL)
   {
      /* TODO: handle different transaction modes here */
      cur->bdbc = bdb_cursor_open(state, 
            cur->dbtran.cursor_tran, 
            NULL /*trans->dbtran.shadow_tran*/, 
            cur->ixnum,  
            BDB_OPEN_REAL,
            NULL, 
            0, /*TODO : review page order scan */
            gbl_rowlocks,
            NULL,
            NULL,
            NULL,
            NULL,
            NULL,
            0,
            &bdberr
            );
   }

   pthread_rwlock_wrlock(&center->cursors_rwlock);
   hash_add(center->cursors_hash, cur);
   pthread_rwlock_unlock(&center->cursors_rwlock);

   return cur;
}
#endif

int fdb_svc_cursor_close(char *cid, int isuuid, struct sqlclntstate **pclnt)
{
    struct sqlclntstate *tran_clnt;
    svc_cursor_t *cur;
    int rc = 0;
    int bdberr = 0;
    int commit;
    uuidstr_t us;

    /* retrieve cursor */
    pthread_rwlock_wrlock(&center->cursors_rwlock);
    if (isuuid) {
        uuidstr_t us;
        cur = hash_find(center->cursorsuuid_hash, cid);
        if (!cur) {
            comdb2uuidstr(cid, us);
            pthread_rwlock_unlock(&center->cursors_rwlock);

            logmsg(LOGMSG_ERROR, "%s: missing cursor %s\n", __func__, us);
            return -1;
        }

        hash_del(center->cursorsuuid_hash, cur);
    } else {
        cur = hash_find(center->cursors_hash, cid);
        if (!cur) {
            pthread_rwlock_unlock(&center->cursors_rwlock);

            logmsg(LOGMSG_ERROR, "%s: missing cursor %llx\n", __func__,
                    *(unsigned long long *)cid);
            return -1;
        }

        hash_del(center->cursors_hash, cur);
    }

    if (gbl_fdb_track)
        logmsg(LOGMSG_USER, "%d: CLosing rem cursor cid=%llx autocommit=%d\n",
               pthread_self(), *(unsigned long long *)cur->cid,
               cur->autocommit);

    pthread_rwlock_unlock(&center->cursors_rwlock);

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
            tran_clnt = fdb_svc_trans_get(cur->tid, isuuid);
            if (!tran_clnt) {
                logmsg(LOGMSG_ERROR, "%s: missing client transaction %llx!\n",
                        __func__, *(unsigned long long *)cur->tid);
            } else {
                /* link the guy in the transaction */
                listc_rfl(&tran_clnt->dbtran.dtran->fdb_trans.top->cursors,
                          cur);

                /* free this guy */
                pthread_mutex_unlock(&tran_clnt->dtran_mtx);
            }

            (*pclnt)->dbtran.shadow_tran = NULL;
        }

        reset_clnt(*pclnt, NULL, 0);

        pthread_mutex_destroy(&(*pclnt)->wait_mutex);
        pthread_cond_destroy(&(*pclnt)->wait_cond);
        pthread_mutex_destroy(&(*pclnt)->write_lock);
        pthread_mutex_destroy(&(*pclnt)->dtran_mtx);

        free(*pclnt);

        *pclnt = NULL;
    }

    /* close cursor */
    free(cur);

    return rc;
}

#if 0
int fdb_svc_trans_rollback(char *tid)
{
   int         rc = 0;
   int         bdberr = 0;


   if(gbl_fdb_track)
      printf("%d rolling back tid=%llx\n", pthread_self(),
         *(unsigned long long*)cur->tid);
   /* destroying curstran */
   if(cur->dbtran.cursor_tran)
   {
      rc = bdb_put_cursortran(thedb->bdb_env, cur->dbtran.cursor_tran, &bdberr);
      if(rc || bdberr)
      {
         fprintf(stderr, "%s: failed releasing the curstran rc=%d bdberr=%d\n",
               __func__, rc, bdberr);
      }
      cur->dbtran.cursor_tran = NULL;
   }
   else
   {
      fprintf(stderr, "%s: missing trans %llx\n", *(unsigned long long*)cur->tid);
   }

   return 0;
}
#endif

/**
 * Move cursor
 *
 */
int fdb_svc_cursor_move(enum svc_move_types type, char *cid, char **data,
                        int *datalen, unsigned long long *genid,
                        char **datacopy, int *datacopylen, int isuuid)
{
    svc_cursor_t *cur;
    int bdberr = 0;
    int rc = 0;
    int irc = 0;

    /* retrieve cursor */
    pthread_rwlock_rdlock(&center->cursors_rwlock);
    cur = hash_find_readonly(center->cursors_hash, cid);
    pthread_rwlock_unlock(&center->cursors_rwlock);

    /* TODO: we assumed here nobody can close this cursor except ourselves; pls
       review
       when adding critical cursor termination */
    if (cur) {
        assert(cur->bdbc != NULL);
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
                logmsg(LOGMSG_FATAL, "%s: unknown move\n", __func__, type);
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

    struct ireq iq;
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
                    logmsg(LOGMSG_ERROR, "%s: %d failed dd recovery\n", __func__,
                            pthread_self());
                return SQLITE_DEADLOCK;
            }

            if (nretries >= gbl_maxretries) {
               logmsg(LOGMSG_ERROR, "too much contention fetching "
                       "tbl %s blob %s tried %d times\n",
                       thedb->dbs[cur->tblnum]->dbname, f->name, nretries);
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
    Vdbe *vdbe = NULL;
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
                m->n = cstrlenlimflipped(&in[1], f->len - 1);
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
        int i;
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
        logmsg(LOGMSG_ERROR, "unhandled type %d\n", f->type);
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

static int fdb_ondisk_to_unpacked(struct db *db, struct schema *s,
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

static int fdb_ondisk_to_packed_sqlite_tz(struct db *db, struct schema *s,
                                          char *in, unsigned long long genid,
                                          char *out, int maxout, int *reqsize,
                                          svc_cursor_t *cur)
{
    Mem *m;
    int fnum;
    int i;
    int rc = 0;
    int datasz = 0;
    int hdrsz = 0;
    int remainingsz = 0;
    int sz;
    unsigned char *hdrbuf, *dtabuf;
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

#if 0
   for (fnum = 0; fnum < nField; fnum++) {
      bzero(&m[fnum], sizeof(Mem));
      rc = fdb_get_data_int(cur, s, in, fnum, &m[fnum], 1, genid);
      if (rc) goto done;
      type = sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len);
      sz = sqlite3VdbeSerialTypeLen(type);
      datasz += sz;
      hdrsz += sqlite3VarintLen(type);
      /*fprintf( stderr, "%s:%d type=%d size=%d datasz=%d hdrsz=%d\n",
        __FILE__, __LINE__, type, sz, datasz, hdrsz);*/
   }
   ncols = fnum;

   if (genid)
   {
      bzero(&m[fnum], sizeof(Mem));
      m[fnum].u.i = genid;
      m[fnum].flags = MEM_Int;

      type = sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len);
      sz = sqlite3VdbeSerialTypeLen(type);
      datasz += sz;
      hdrsz += sqlite3VarintLen(sz);
      ncols++;
   }

   /* to account for size of header in header */
   hdrsz += sqlite3VarintLen(hdrsz);

#endif

    rc = fdb_ondisk_to_unpacked(db, s, cur, in, genid, m, nField + 1, &hdrsz,
                                &datasz, &ncols);

/*
   fprintf( stderr, "%s:%d hdrsz=%d ncols=%d maxout=%d\n",
   __FILE__, __LINE__, hdrsz, ncols, maxout);*/

#if 0
   hdrbuf = out;
   dtabuf = out + hdrsz;

   /* enough room? */
   if (maxout > 0 && (datasz + hdrsz) > maxout) {
      fprintf (stderr, "AAAAA!?!?\n");
      rc = -2;
      *reqsize = datasz + hdrsz;
      goto done;
   }

   /* put header size in header */

   sz = sqlite3PutVarint(hdrbuf, hdrsz);
   hdrbuf += sz;

   /* keep track of the size remaining */
   remainingsz = datasz;

   for (fnum = 0; fnum < ncols; fnum++) {
      sz = sqlite3VdbeSerialPut(dtabuf, &m[fnum], sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len));
      dtabuf += sz;
      remainingsz -= sz;
      sz = sqlite3PutVarint(hdrbuf, sqlite3VdbeSerialType(&m[fnum], SQLITE_DEFAULT_FILE_FORMAT, &len));
      hdrbuf += sz;
      assert( hdrbuf <= (out + hdrsz) );
   }
   
   /* return real length */
   *reqsize = datasz + hdrsz;
   rc = 0;
#endif

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
    struct db *db = thedb->dbs[cur->tblnum];
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

static int fdb_packed_sqlite_to_ondisk_key_tz(struct db *db, struct schema *s,
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
                        char **datacopy, int *datacopylen, int isuuid)
{
    svc_cursor_t *cur;
    struct db *db;
    struct schema *sc;
    struct convert_failure convfail;
    int bdberr = 0;
    int rc = 0;
    int irc = 0;
    int use_keylen;
    char *use_key;

    /* retrieve cursor */
    pthread_rwlock_rdlock(&center->cursors_rwlock);
    cur = hash_find_readonly(center->cursors_hash, cid);
    pthread_rwlock_unlock(&center->cursors_rwlock);

    if (cur) {
        if (cur->ixnum < 0) {
            rc = fdb_packedsqlite_extract_genid(key, &use_keylen, cur->outbuf);
            if (rc <= 0) {
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
        logmsg(LOGMSG_ERROR, "%s: unknown cursor id %llx\n", __func__,
                *(unsigned long long *)cid);
    }

    return rc;
}

/**
 * Transaction cursors support
 *
 * Init routine
 */
int fdb_svc_trans_init(struct sqlclntstate *clnt, const char *tid,
                       enum transaction_level lvl, int seq, int isuuid)
{
    trans_t *trans = NULL;
    fdb_tran_t *fdb_tran = NULL;

    trans = &clnt->dbtran;

    assert(trans == NULL);

    pthread_mutex_lock(&clnt->dtran_mtx);

    trans->dtran = (fdb_distributed_tran_t *)calloc(
        1, sizeof(fdb_distributed_tran_t) + sizeof(fdb_tran_t));
    if (!trans->dtran) {
        pthread_mutex_unlock(&clnt->dtran_mtx);
        logmsg(LOGMSG_ERROR, "%s: calloc!\n", __func__);
        return -1;
    }

    fdb_tran =
        (fdb_tran_t *)((char *)trans->dtran + sizeof(fdb_distributed_tran_t));
    fdb_tran->tid = fdb_tran->tiduuid;

    listc_init(&trans->dtran->fdb_trans, offsetof(struct fdb_tran, lnk));
    trans->dtran->remoted = 1;
    fdb_tran->seq = seq;
    if (isuuid) {
        comdb2uuidcpy(fdb_tran->tid, (unsigned char *)tid);
    } else {
        memcpy(fdb_tran->tid, tid, sizeof(unsigned long long));
    }
    listc_atl(&trans->dtran->fdb_trans, fdb_tran);
    listc_init(&fdb_tran->cursors, offsetof(svc_cursor_t, lnk));

    trans->mode = lvl;

    /* explicit bump of sequence number, waiting for this */
    fdb_tran->seq++;

    pthread_mutex_unlock(&clnt->dtran_mtx);

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
int fdb_svc_trans_get_tid(const char *cid, char *tid, int isuuid)
{
    svc_cursor_t *cur;

    *tid = 0ULL;

    /* retrieve cursor */
    pthread_rwlock_rdlock(&center->cursors_rwlock);
    if (isuuid)
        cur = hash_find_readonly(center->cursorsuuid_hash, cid);
    else
        cur = hash_find_readonly(center->cursors_hash, cid);

    if (cur) {
        if (isuuid)
            memcpy(tid, cur->tiduuid, sizeof(uuid_t));
        else
            memcpy(tid, cur->tid, sizeof(unsigned long long));
    }

    pthread_rwlock_unlock(&center->cursors_rwlock);

    if (!cur) {
        if (isuuid) {
            uuidstr_t us;
            comdb2uuidstr((unsigned char *)cid, us);
            logmsg(LOGMSG_ERROR, "%s: looking for missing fdb tran for cid=%s\n",
                    __func__, us);
        } else
            logmsg(LOGMSG_ERROR, "%s: looking for missing fdb tran for cid=%llx\n",
                    __func__, *(unsigned long long *)cid);
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
        pthread_mutex_unlock(&tran_clnt->dtran_mtx);

        /* wait for all the updates to arrive; we could skip that in socksql,
           but I am
           not gonna do that now.  Recom ftw */
        poll(NULL, 0, 10);

        pthread_mutex_lock(&tran_clnt->dtran_mtx);
    }

    /* bump the transactional sequence */
    trans->seq++;
}

