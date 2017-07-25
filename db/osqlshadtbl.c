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

#include <epochlib.h>
#include "sql.h"
#include "osqlshadtbl.h"
#include "osqlcomm.h"
#include "sqloffload.h"
#include "bdb_osqlcur.h"
#include <assert.h>
#include <list.h>
#include <plhash.h>

#include <genid.h>
#include <net_types.h>
#include "comdb2uuid.h"
#include "logmsg.h"

extern int g_osql_max_trans;
extern int gbl_partial_indexes;
extern int gbl_expressions_indexes;

typedef struct blob_key {
    unsigned long long seq; /* tbl->seq identifying the owning row */
    unsigned long long id;  /* blob index in the row */
} blob_key_t;

typedef struct updCols_key {
    unsigned long long seq; /* thd->seq identifying the owning row */
    unsigned long long id;  /* -1 to differentiate from blobs */
} updCols_key_t;

struct rec_dirty_keys {
    unsigned long long seq;
    unsigned long long dirty_keys;
};

typedef struct index_key {
    unsigned long long seq;   /* tbl->seq identifying the owning row */
    unsigned long long ixnum; /* index num in the row */
} index_key_t;

static shad_tbl_t *get_shadtbl(struct BtCursor *pCur);
static shad_tbl_t *open_shadtbl(struct BtCursor *pCur);
static shad_tbl_t *create_shadtbl(struct BtCursor *pCur,
                                  struct sqlclntstate *clnt);
static int create_tablecursor(bdb_state_type *bdb_env, struct tmp_table **ptbl,
                              struct temp_cursor **pcur, int *bdberr,
                              int skipcursor);
static int destroy_tablecursor(bdb_state_type *bdb_env, struct temp_cursor *cur,
                               struct tmp_table *tbl, int *bdberr);
static int truncate_tablecursor(bdb_state_type *bdb_env,
                                struct temp_cursor **cur,
                                struct temp_table *tbl, int *bdberr);

static int process_local_shadtbl_usedb(struct sqlclntstate *clnt,
                                       char *tablename);
static int process_local_shadtbl_skp(struct sqlclntstate *clnt, shad_tbl_t *tbl,
                                     int *bdberr, int crt_nops);
static int process_local_shadtbl_qblob(struct sqlclntstate *clnt,
                                       shad_tbl_t *tbl, int *updCols,
                                       int *bdberr, unsigned long long seq,
                                       char *record);
static int process_local_shadtbl_index(struct sqlclntstate *clnt,
                                       shad_tbl_t *tbl, int *bdberr,
                                       unsigned long long seq, int is_delete);
static int process_local_shadtbl_add(struct sqlclntstate *clnt, shad_tbl_t *tbl,
                                     int *bdberr, int crt_nops);
static int process_local_shadtbl_upd(struct sqlclntstate *clnt, shad_tbl_t *tbl,
                                     int *bdberr, int crt_nops);
static int process_local_shadtbl_recgenids(struct sqlclntstate *clnt,
                                           int *bdberr);
static int process_local_shadtbl_sc(struct sqlclntstate *clnt, int *bdberr);
static int process_local_shadtbl_dbq(struct sqlclntstate *, int *bdberr,
                                     int *crt_nops);

static int insert_record_indexes(BtCursor *pCur, struct sql_thread *thd,
                                 int64_t nKey, int *bdberr);
static int delete_record_indexes(BtCursor *pCur, char *dta, int dtasize,
                                 struct sql_thread *thd, int *bdberr);
static int delete_synthetic_row(struct BtCursor *pCur, struct sql_thread *thd,
                                shad_tbl_t *tbl);
static int blb_tbl_cmp(void *, int, const void *, int, const void *);
static int idx_tbl_cmp(void *, int, const void *, int, const void *);

static int osql_create_verify(bdb_state_type *bdb_state,
                              struct sqlclntstate *clnt, int *bdberr);
static int osql_destroy_verify(bdb_state_type *bdb_state,
                               struct sqlclntstate *clnt);

static int osql_create_schemachange(bdb_state_type *bdb_state,
                                    struct sqlclntstate *clnt, int *bdberr);
static int osql_destroy_schemachange(bdb_state_type *bdb_state,
                                     struct sqlclntstate *clnt);

static int free_it(void *obj, void *arg)
{
    free(obj);
    return 0;
}
static void destroy_idx_hash(hash_t *h)
{
    hash_for(h, free_it, NULL);
    hash_clear(h);
    hash_free(h);
}

static int destroy_shadtbl(shad_tbl_t *tbl)
{

    int bdberr = 0;

    if (tbl->blb_tbl)
        destroy_tablecursor(tbl->env->bdb_env, tbl->blb_cur, tbl->blb_tbl,
                            &bdberr);
    if (tbl->add_tbl)
        destroy_tablecursor(tbl->env->bdb_env, tbl->add_cur, tbl->add_tbl,
                            &bdberr);
    if (tbl->upd_tbl)
        destroy_tablecursor(tbl->env->bdb_env, tbl->upd_cur, tbl->upd_tbl,
                            &bdberr);
    if (tbl->addidx_hash)
        destroy_idx_hash(tbl->addidx_hash);
    if (tbl->delidx_hash)
        destroy_idx_hash(tbl->delidx_hash);
    if (tbl->delidx_tbl)
        destroy_tablecursor(tbl->env->bdb_env, tbl->delidx_cur, tbl->delidx_tbl,
                            &bdberr);
    if (tbl->insidx_tbl)
        destroy_tablecursor(tbl->env->bdb_env, tbl->insidx_cur, tbl->insidx_tbl,
                            &bdberr);

    /*
       This is now down when we close the session (or should it be when I
       commit?)
       free(tbl);
     */
    /*fprintf(stdout, "++++ Destroyed shattbl for %d\n", pthread_self());*/

    return 0;
}

static int reset_for_selectv_shadtbl(shad_tbl_t *tbl)
{

    int bdberr = 0;
    int rc = 0;

    tbl->updcols = 0;
    tbl->nops = 0;

    if (tbl->blb_tbl) {
        destroy_tablecursor(tbl->env->bdb_env, tbl->blb_cur, tbl->blb_tbl,
                            &bdberr);
        rc = create_tablecursor(tbl->env->bdb_env, &tbl->blb_tbl, &tbl->blb_cur,
                                &bdberr, 1);
        if (rc)
            return rc;
        bdb_temp_table_set_cmp_func(tbl->blb_tbl->table, blb_tbl_cmp);
    }

    if (tbl->add_tbl) {
        destroy_tablecursor(tbl->env->bdb_env, tbl->add_cur, tbl->add_tbl,
                            &bdberr);
        rc = create_tablecursor(tbl->env->bdb_env, &tbl->add_tbl, &tbl->add_cur,
                                &bdberr, 1);
        if (rc) {
            destroy_tablecursor(tbl->env->bdb_env, tbl->blb_cur, tbl->blb_tbl,
                                &bdberr);
            return rc;
        }
    }

    if (tbl->upd_tbl) {
        destroy_tablecursor(tbl->env->bdb_env, tbl->upd_cur, tbl->upd_tbl,
                            &bdberr);
        rc = create_tablecursor(tbl->env->bdb_env, &tbl->upd_tbl, &tbl->upd_cur,
                                &bdberr, 1);
        if (rc) {
            destroy_tablecursor(tbl->env->bdb_env, tbl->blb_cur, tbl->blb_tbl,
                                &bdberr);
            destroy_tablecursor(tbl->env->bdb_env, tbl->add_cur, tbl->add_tbl,
                                &bdberr);
            return rc;
        }
    }

    if (tbl->delidx_tbl) {
        destroy_tablecursor(tbl->env->bdb_env, tbl->delidx_cur, tbl->delidx_tbl,
                            &bdberr);
        rc = create_tablecursor(tbl->env->bdb_env, &tbl->delidx_tbl,
                                &tbl->delidx_cur, &bdberr, 1);
        if (rc) {
            destroy_tablecursor(tbl->env->bdb_env, tbl->blb_cur, tbl->blb_tbl,
                                &bdberr);
            destroy_tablecursor(tbl->env->bdb_env, tbl->add_cur, tbl->add_tbl,
                                &bdberr);
            destroy_tablecursor(tbl->env->bdb_env, tbl->upd_cur, tbl->upd_tbl,
                                &bdberr);
            return rc;
        }
        bdb_temp_table_set_cmp_func(tbl->delidx_tbl->table, idx_tbl_cmp);
    }

    if (tbl->insidx_tbl) {
        destroy_tablecursor(tbl->env->bdb_env, tbl->insidx_cur, tbl->insidx_tbl,
                            &bdberr);
        rc = create_tablecursor(tbl->env->bdb_env, &tbl->insidx_tbl,
                                &tbl->insidx_cur, &bdberr, 1);
        if (rc) {
            destroy_tablecursor(tbl->env->bdb_env, tbl->blb_cur, tbl->blb_tbl,
                                &bdberr);
            destroy_tablecursor(tbl->env->bdb_env, tbl->add_cur, tbl->add_tbl,
                                &bdberr);
            destroy_tablecursor(tbl->env->bdb_env, tbl->upd_cur, tbl->upd_tbl,
                                &bdberr);
            destroy_tablecursor(tbl->env->bdb_env, tbl->delidx_cur,
                                tbl->delidx_tbl, &bdberr);
            return rc;
        }
        bdb_temp_table_set_cmp_func(tbl->insidx_tbl->table, idx_tbl_cmp);
    }

    /*
       This is now down when we close the session (or should it be when I
       commit?)
       fprintf(stdout, "++++ Reset selectv shattbl for %d\n", pthread_self());
     */
    return 0;
}

static shad_tbl_t *open_shadtbl(struct BtCursor *pCur)
{

    shad_tbl_t *tbl = NULL;
    struct sql_thread *thd = pthread_getspecific(query_info_key);

    tbl = get_shadtbl(pCur);
    if (!tbl) {
        struct sqlclntstate *clnt = thd->sqlclntstate;
        if (!clnt) {
            /* this is a bug */
            static int once = 0;
            if (!once) {
                once = 1;
                logmsg(LOGMSG_ERROR, "%s: sql_thread has no sqlclntstate!\n",
                        __func__);
            }
            return NULL;
        }

        tbl = create_shadtbl(pCur, clnt);
        if (!tbl) {
            logmsg(LOGMSG_ERROR, "%s: unable to allocated %d bytes!\n", __func__,
                    sizeof(shad_tbl_t));
            return NULL;
        }
    }

    return tbl;
}

/* Open shadow tables.  Return the add-table cursor. */
void *osql_open_shadtbl_addtbl(struct BtCursor *pCur)
{
    shad_tbl_t *tbl = open_shadtbl(pCur);

    if (tbl)
        return tbl->add_cur;

    return NULL;
}

/**
 * Truncate all the shadow tables but for selectv records
 *
 */
int osql_shadtbl_reset_for_selectv(struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    shad_tbl_t *tbl = NULL, *tmp = NULL;
    int rc = 0;
    int bdberr = 0;

    /* close selectv cursor but keep table */
    if (osql->verify_cur) {
        rc = bdb_temp_table_close_cursor(thedb->bdb_env, osql->verify_cur,
                                         &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s: bdb_temp_table_close failed, rc=%d bdberr=%d\n",
                    __func__, rc, bdberr);
            return -1;
        }

        /* new code doesn't open the cursor unless I am saving in it */
        osql->verify_cur = bdb_temp_table_cursor(
            thedb->bdb_env, osql->verify_tbl, NULL, &bdberr);
        if (!osql->verify_cur) {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_cursor failed, bdberr=%d\n",
                    __func__, bdberr);
            bdb_temp_table_close(thedb->bdb_env, osql->verify_tbl, &bdberr);
            return -1;
        }
    }
    /* close sc cursor but keep table */
    if (osql->sc_cur) {
        rc = bdb_temp_table_close_cursor(thedb->bdb_env, osql->sc_cur, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_temp_table_close failed, rc=%d bdberr=%d\n",
                   __func__, rc, bdberr);
            return -1;
        }

        /* new code doesn't open the cursor unless I am saving in it */
        osql->sc_cur =
            bdb_temp_table_cursor(thedb->bdb_env, osql->sc_tbl, NULL, &bdberr);
        if (!osql->sc_cur) {
            logmsg(LOGMSG_ERROR,
                   "%s: bdb_temp_table_cursor failed, bdberr=%d\n", __func__,
                   bdberr);
            bdb_temp_table_close(thedb->bdb_env, osql->sc_tbl, &bdberr);
            return -1;
        }
    }

    LISTC_FOR_EACH_SAFE(&osql->shadtbls, tbl, tmp, linkv)
    {
        rc = reset_for_selectv_shadtbl(tbl);
        if (rc) {
            return rc;
        }
    }
    return 0;
}

/**
 * Blob table comparison function
 */
static int blb_tbl_cmp(void *usermem, int key1len, const void *key1,
                       int key2len, const void *key2)
{
    const updCols_key_t *k1 = (updCols_key_t *)key1;
    const updCols_key_t *k2 = (updCols_key_t *)key2;

    assert(sizeof(updCols_key_t) == key1len);
    assert(sizeof(updCols_key_t) == key2len);

    if (k1->seq < k2->seq) {
        return -1;
    }

    if (k1->seq > k2->seq) {
        return 1;
    }

    if (k1->id < k2->id) {
        return -1;
    }

    if (k1->id > k2->id) {
        return 1;
    }

    return 0;
}

/**
 * Index table comparison function
 */
static int idx_tbl_cmp(void *usermem, int key1len, const void *key1,
                       int key2len, const void *key2)
{
    const index_key_t *k1 = (index_key_t *)key1;
    const index_key_t *k2 = (index_key_t *)key2;

    assert(sizeof(index_key_t) == key1len);
    assert(sizeof(index_key_t) == key2len);

    if (k1->seq < k2->seq) {
        return -1;
    }

    if (k1->seq > k2->seq) {
        return 1;
    }

    if (k1->ixnum < k2->ixnum) {
        return -1;
    }

    if (k1->ixnum > k2->ixnum) {
        return 1;
    }

    return 0;
}

static shad_tbl_t *create_shadtbl(struct BtCursor *pCur,
                                  struct sqlclntstate *clnt)
{
    shad_tbl_t *tbl;
    unsigned long long rqid;
    struct dbtable *db = pCur->db;
    struct dbenv *env = pCur->db->dbenv;
    int numblobs = pCur->numblobs;
    int rc = 0;
    int bdberr = 0;

    /*pBt->vdbe = pthread_getspecific(sqlite3VDBEkey);*/

    tbl = calloc(1, sizeof(shad_tbl_t));
    if (!tbl)
        return NULL;

    tbl->seq = 0;
    tbl->env = env;
    tbl->db = db;

    /* create table add and its cursor */
    rc = create_tablecursor(env->bdb_env, &tbl->add_tbl, &tbl->add_cur, &bdberr,
                            0);
    if (rc)
        return NULL;

    /* create update and its cursor */
    rc = create_tablecursor(env->bdb_env, &tbl->upd_tbl, &tbl->upd_cur, &bdberr,
                            0);
    if (rc) {
        destroy_tablecursor(env->bdb_env, tbl->add_cur, tbl->add_tbl, &bdberr);
        return NULL;
    }

    tbl->nblobs = numblobs;
    tbl->updcols = 0;
    tbl->nops = 0;
    if (pCur->bdbcur) {
        tbl->dbnum = pCur->bdbcur->dbnum(pCur->bdbcur);
    } else {
        tbl->dbnum = get_dbnum_by_handle(db->handle);
    }

    /* We store updCols in the blob-table so always create this- it's generally
     * useful */
    rc = create_tablecursor(env->bdb_env, &tbl->blb_tbl, &tbl->blb_cur, &bdberr,
                            0);
    if (rc) {
        destroy_tablecursor(env->bdb_env, tbl->upd_cur, tbl->upd_tbl, &bdberr);
        destroy_tablecursor(env->bdb_env, tbl->add_cur, tbl->add_tbl, &bdberr);
        return NULL;
    }
    rc = create_tablecursor(env->bdb_env, &tbl->delidx_tbl, &tbl->delidx_cur,
                            &bdberr, 0);
    if (rc) {
        destroy_tablecursor(env->bdb_env, tbl->upd_cur, tbl->upd_tbl, &bdberr);
        destroy_tablecursor(env->bdb_env, tbl->add_cur, tbl->add_tbl, &bdberr);
        destroy_tablecursor(env->bdb_env, tbl->blb_cur, tbl->blb_tbl, &bdberr);
        return NULL;
    }
    rc = create_tablecursor(env->bdb_env, &tbl->insidx_tbl, &tbl->insidx_cur,
                            &bdberr, 0);
    if (rc) {
        destroy_tablecursor(env->bdb_env, tbl->upd_cur, tbl->upd_tbl, &bdberr);
        destroy_tablecursor(env->bdb_env, tbl->add_cur, tbl->add_tbl, &bdberr);
        destroy_tablecursor(env->bdb_env, tbl->blb_cur, tbl->blb_tbl, &bdberr);
        destroy_tablecursor(env->bdb_env, tbl->delidx_cur, tbl->delidx_tbl,
                            &bdberr);
        return NULL;
    }
    assert(tbl->blb_cur);
    bdb_temp_table_set_cmp_func(tbl->blb_tbl->table, blb_tbl_cmp);
    bdb_temp_table_set_cmp_func(tbl->delidx_tbl->table, idx_tbl_cmp);
    bdb_temp_table_set_cmp_func(tbl->insidx_tbl->table, idx_tbl_cmp);

    tbl->addidx_hash = hash_init_o(offsetof(struct rec_dirty_keys, seq),
                                   sizeof(unsigned long long));
    tbl->delidx_hash = hash_init_o(offsetof(struct rec_dirty_keys, seq),
                                   sizeof(unsigned long long));

    listc_abl(&clnt->osql.shadtbls, tbl);
    pCur->shadtbl = tbl;

    assert(tbl->blb_cur);
    assert(tbl->delidx_cur);
    assert(tbl->insidx_cur);

    /*fprintf(stdout, "++++ Created shattbl for %d\n", pthread_self());*/

    return tbl;
}

static int destroy_tablecursor(bdb_state_type *bdb_env, struct temp_cursor *cur,
                               struct tmp_table *tbl, int *bdberr)
{

    int rc = 0;

    if (cur) {
        rc = bdb_temp_table_close_cursor(bdb_env, cur, bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: fail to close cursor bdberr=%d\n", __func__,
                    *bdberr);
    }

    if (tbl) {
        rc = bdb_temp_table_close(bdb_env, tbl->table, bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: fail to bdberr=%d\n", __func__, *bdberr);

        free(tbl);
    }

    return rc;
}

static int truncate_tablecursor(bdb_state_type *bdb_env,
                                struct temp_cursor **cur,
                                struct temp_table *tbl, int *bdberr)
{

    int rc = 0;

    /* we need to close the cursor before we truncate, will reopen */
    if (*cur) {
        rc = bdb_temp_table_close_cursor(bdb_env, *cur, bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: fail to close cursor bdberr=%d\n", __func__,
                    *bdberr);
    }

    if (tbl) {
        rc = bdb_temp_table_truncate(bdb_env, tbl, bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: error truncating add temp_table rc=%d bdberr=%d\n",
                    __func__, rc, *bdberr);
    }

    *cur = bdb_temp_table_cursor(bdb_env, tbl, NULL, bdberr);
    if (!*cur) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_cursor failed, bdberr=%d\n",
                __func__, *bdberr);
        return -1;
    }

    return rc;
}

/* this fetches just one blob, indexed by blobnum */
int osql_fetch_shadblobs_by_genid(BtCursor *pCur, int *blobnum,
                                  blob_status_t *blobs, int *bdberr)
{

    int rc = 0;
    /*int   i = 0;*/
    shad_tbl_t *tbl = NULL;
    /* key gets set into cur->key, and is freed when a new key is
       submitted or when the cursor is closed */
    blob_key_t *key = (blob_key_t *)malloc(sizeof(blob_key_t));

    if (!(tbl = open_shadtbl(pCur)) || !tbl->blb_cur) {
        logmsg(LOGMSG_ERROR, "%s: error getting shadtbl for \'%s\'\n", __func__,
                pCur->db->dbname);
        if (key)
            free(key);
        return -1;
    }

    key->seq = pCur->genid;
    key->id = *blobnum - 1;

    rc = bdb_temp_table_find_exact(tbl->env->bdb_env, tbl->blb_cur, key,
                                   sizeof(*key), bdberr);
    if (rc == IX_EMPTY || rc == IX_NOTFND) {
        free(key);
        key = NULL;

        blobs->bloblens[0] = 0;
        blobs->bloboffs[0] = 0;
        blobs->blobptrs[0] = NULL;
        rc = 0;

    } else if (!rc) {

        blobs->bloblens[0] = bdb_temp_table_datasize(tbl->blb_cur);
        blobs->bloboffs[0] = 0;
        blobs->blobptrs[0] = bdb_temp_table_data(tbl->blb_cur);

        /* reset data pointer in cursor; blob will be freed when blobs is freed
         */
        bdb_temp_table_reset_datapointers(tbl->blb_cur);

    } else {
        free(key);
        key = NULL;

        return rc;
    }

#if 0
   for(i=0; i<tbl->nblobs; i++) {

      /* key gets set into cur->key, and is freed when a new key is
         submitted or when the cursor is closed */
      blob_key_t  *key = (blob_key_t*)malloc(sizeof(blob_key_t));

      key->seq = pCur->genid;
      key->id = i;

      rc = bdb_temp_table_find_exact(tbl->env->bdb_env, tbl->blb_cur, key, sizeof(*key), bdberr);
      if(rc == IX_EMPTY || rc == IX_NOTFND)
      {
         free(key);
         key = NULL;

         blobs->bloblens[i] = 0;
         blobs->bloboffs[i] = 0;
         blobs->blobptrs[i] = NULL;
         rc = 0;

      } else if(!rc) {

         blobs->bloblens[i]   = bdb_temp_table_datasize(tbl->blb_cur);
         blobs->bloboffs[i]   = 0;
         blobs->blobptrs[i]   = bdb_temp_table_data(tbl->blb_cur);

         /* reset data pointer in cursor; blob will be freed when blobs is freed */
         bdb_temp_table_reset_datapointers(tbl->blb_cur);

      } else 
      {
         free(key);
         key = NULL;

         return rc;
      }
   }
#endif

    return rc;
}

int osql_get_shadowdata(BtCursor *pCur, unsigned long long genid, void **buf,
                        int *buflen, int *bdberr)
{

    int rc = 0;
    shad_tbl_t *tbl = NULL;

    if (pCur->ixnum != -1)
        /* this is for data only, for now*/
        return -1;

    if (!(tbl = open_shadtbl(pCur)) || !tbl->add_cur) {
        logmsg(LOGMSG_ERROR, "%s: error getting shadtbl for \'%s\'\n", __func__,
                pCur->db->dbname);
        return -1;
    }

    if (!is_genid_synthetic(genid)) {
        return -1;
    }

    unsigned long long *key =
        (unsigned long long *)malloc(sizeof(unsigned long long));
    *key = genid;

    rc = bdb_temp_table_find_exact(tbl->env->bdb_env, tbl->add_cur, key,
                                   sizeof(*key), bdberr);
    if (rc != IX_FND) {
        free(key);
        return -1;
    }

    *buflen = bdb_temp_table_datasize(tbl->add_cur);
    *buf = bdb_temp_table_data(tbl->add_cur);

    return 0;
}

static shad_tbl_t *get_shadtbl(struct BtCursor *pCur)
{
    struct sql_thread *thd;
    struct sqlclntstate *clnt;

    /* we keep the shadow with the pCur, if any */
    if (pCur && pCur->shadtbl) {
        return (shad_tbl_t *)pCur->shadtbl;
    }

    /* pCur has not cached this; maybe it was opened in the
       meantime? case in mind, index and data both recording
       genids */
    thd = pthread_getspecific(query_info_key);
    clnt = thd->sqlclntstate;

    return osql_get_shadow_bydb(clnt, pCur->db);
}

static int create_tablecursor(bdb_state_type *bdb_env, struct tmp_table **ptbl,
                              struct temp_cursor **pcur, int *bdberr,
                              int skip_cursor)
{

    struct tmp_table *tbl =
        (struct tmp_table *)calloc(1, sizeof(struct tmp_table));

    if (!tbl) {
        logmsg(LOGMSG_ERROR, "%s: unable to allocate %d bytes\n", __func__,
                sizeof(struct tmp_table));
        return -1;
    }

    tbl->table = bdb_temp_table_create(bdb_env, bdberr);

    if (!tbl->table) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_create failed, bderr=%d\n",
                __func__, *bdberr);
        free(tbl);
        return -1;
    }
    if (skip_cursor) {
        *pcur = NULL;
    } else {
        *pcur = bdb_temp_table_cursor(bdb_env, tbl->table, NULL, bdberr);
        if (!*pcur) {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_cursor failed, bdberr=%d\n",
                    __func__, *bdberr);
            bdb_temp_table_close(bdb_env, tbl->table, bdberr);
            free(tbl);
            return -1;
        }
    }

    *ptbl = tbl;

    return 0;
}

void *osql_get_shadtbl_addtbl_newcursor(struct BtCursor *pCur)
{
    int bdberr = 0;
    struct temp_cursor *cur;
    shad_tbl_t *tbl = get_shadtbl(pCur);
    bdb_state_type *bdbenv = NULL;
    /* If we've found the table, create a new cursor against it. */
    if (tbl) {
        bdbenv = tbl->env->bdb_env;
        cur = bdb_temp_table_cursor(bdbenv, tbl->add_tbl->table, NULL, &bdberr);
        if (!cur) {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_cursor failed, bdberr=%d\n",
                    __func__, bdberr);
        }
        return cur;
    }
    return NULL;
}

void *osql_get_shadtbl_addtbl(struct BtCursor *pCur)
{

    shad_tbl_t *tbl = get_shadtbl(pCur);

    if (tbl)
        return tbl->add_cur;

    return NULL;
}

void *osql_get_shadtbl_updtbl(struct BtCursor *pCur)
{

    shad_tbl_t *tbl = get_shadtbl(pCur);

    if (tbl)
        return tbl->upd_cur;

    return NULL;
}

static int save_dirty_keys(struct sqlclntstate *clnt, shad_tbl_t *tbl,
                           unsigned long long seq,
                           int ins /* 1 for add, 0 for del */)
{
    struct rec_dirty_keys rdk;
    struct rec_dirty_keys *prdk;
    hash_t *h;
    rdk.seq = seq;
    h = ins ? tbl->addidx_hash : tbl->delidx_hash;

    assert(h);
    assert(hash_find(h, &rdk) == NULL);

    prdk = calloc(1, sizeof(struct rec_dirty_keys));
    if (!prdk) {
        logmsg(LOGMSG_ERROR, "%s: unable to allocate %d bytes\n", __func__,
                sizeof(struct rec_dirty_keys));
        return -1;
    }

    prdk->seq = seq;
    prdk->dirty_keys = ins ? clnt->ins_keys : clnt->del_keys;
    hash_add(h, prdk);

    return 0;
}

static int save_ins_keys(struct sqlclntstate *clnt, shad_tbl_t *tbl,
                         unsigned long long seq)
{
    return save_dirty_keys(clnt, tbl, seq, 1);
}

static int save_del_keys(struct sqlclntstate *clnt, shad_tbl_t *tbl,
                         unsigned long long seq)
{
    return save_dirty_keys(clnt, tbl, seq, 0);
}

static unsigned long long get_dirty_keys(struct sqlclntstate *clnt,
                                         shad_tbl_t *tbl,
                                         unsigned long long seq,
                                         int ins /* 1 for add, 0 for del */)
{
    struct rec_dirty_keys rdk;
    struct rec_dirty_keys *prdk;
    hash_t *h;
    rdk.seq = seq;
    h = ins ? tbl->addidx_hash : tbl->delidx_hash;

    prdk = hash_find(h, &rdk);
    if (prdk)
        return prdk->dirty_keys;

    return 0ULL;
}

static unsigned long long get_ins_keys(struct sqlclntstate *clnt,
                                       shad_tbl_t *tbl, unsigned long long seq)
{
    return get_dirty_keys(clnt, tbl, seq, 1);
}

static unsigned long long get_del_keys(struct sqlclntstate *clnt,
                                       shad_tbl_t *tbl, unsigned long long seq)
{
    return get_dirty_keys(clnt, tbl, seq, 0);
}

/*
 * NOTE:
 * Handle upd table for multiple updates of synthetic rows
 * Need to retrieve the original value and have it percolated
 * to the new row (case when we update a real row multiple times)
 * Need to delete existing row as well; a new entry will be added
 * by the caller
 *
 */
int osql_save_updrec(struct BtCursor *pCur, struct sql_thread *thd, char *pData,
                     int nData)
{

    osqlstate_t *osql = &thd->sqlclntstate->osql;
    int rc = 0;
    int bdberr = 0;
    shad_tbl_t *tbl = NULL;
    bdb_state_type *bdbenv = NULL;
    unsigned long long tmp = 0;
    unsigned long long genid = 0;

    if (!(tbl = open_shadtbl(pCur)) || !tbl->upd_cur) {
        logmsg(LOGMSG_ERROR, "%s: error getting shadtbl for \'%s\'\n", __func__,
                pCur->db->dbname);
        return -1;
    }

    /* generate a new synthetic genid */
    tmp = tbl->seq;
    bdbenv = tbl->env->bdb_env;

    set_genid_upd(&tmp);

    if (is_genid_synthetic(pCur->genid)) {
        if (thd->sqlclntstate->dbtran.shadow_tran)
            bdb_set_check_shadows(thd->sqlclntstate->dbtran.shadow_tran);

        /* Need to know if the synthetic genid is generated as a result of a
           insert or update
           The following contraction rules apply:
           INSERT + UPDATE = INSERT
           UPDATE + UPDATE = UPDATE

           upd table tells us the origin of synthetic genid
         */
        unsigned long long *pgenid =
            (unsigned long long *)malloc(sizeof(*pgenid));
        if (!pgenid) {
            logmsg(LOGMSG_ERROR, "malloc %d\n", sizeof(*pgenid));
            return -1;
        }
        *pgenid = pCur->genid;

        rc = bdb_temp_table_find_exact(bdbenv, tbl->upd_cur, pgenid,
                                       sizeof(*pgenid), &bdberr);
        if (bdberr) {
            logmsg(LOGMSG_ERROR, "%s: fail to update genid %llx (%lld) rc=%d bdberr=%d (1)\n",
                __func__, tmp, *pgenid, rc, bdberr);
            free(pgenid);
            return -1;
        }

        if (rc == IX_FND) {
            /* this was an update of a real row, preserve the original genid */

            genid = *(unsigned long long *)bdb_temp_table_data(tbl->upd_cur);

            /* we delete the original upd entry */
            rc = bdb_temp_table_delete(bdbenv, tbl->upd_cur, &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to update genid %llx (%lld) rc=%d "
                                "bdberr=%d (2)\n",
                        __func__, tmp, *pgenid, rc, bdberr);
                return -1;
            }

            /* add the new entry in upd table */
            rc = bdb_temp_table_put(bdbenv, tbl->upd_tbl->table, &tmp,
                                    sizeof(tmp), &genid, sizeof(genid), NULL,
                                    &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to update genid %llx (%lld) rc=%d "
                                "bdberr=%d (3)\n",
                        __func__, tmp, genid, rc, bdberr);
                return -1;
            }
        } else {
            /* this was an insert; no need to touch anything in upd table */
            free(pgenid);
        }

        /* delete the original index from add and its indexes */
        rc = delete_synthetic_row(pCur, thd, tbl);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: fail to update genid %llx (%lld) rc=%d bdberr=%d (4)\n",
                __func__, tmp, pCur->genid, rc, bdberr);
            return -1;
        }
    } else {
        genid = pCur->genid;

        /* we have to mark the row as an update (which also give us the original
         * genid) */
        rc = bdb_temp_table_put(bdbenv, tbl->upd_tbl->table, &tmp, sizeof(tmp),
                                &genid, sizeof(genid), NULL, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: fail to update genid %llx (%lld) rc=%d bdberr=%d (5)\n",
                __func__, tmp, genid, rc, bdberr);
            return -1;
        }

        /* mark the real row as deleted */
        rc = bdb_tran_deltbl_setdeleted(pCur->bdbcur, pCur->genid, &tmp,
                                        sizeof(tmp), &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: fail to update genid %llx (%lld) rc=%d bdberr=%d (6)\n",
                __func__, tmp, pCur->genid, rc, bdberr);
            return -1;
        }
    }

    /* update add table */
    rc = bdb_temp_table_put(bdbenv, tbl->add_tbl->table, &tmp, sizeof(tmp),
                            (char *)pData, nData, NULL, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s: fail to update genid %llx (%lld) rc=%d bdberr=%d (7)\n",
                __func__, tmp, tmp, rc, bdberr);
        return -1;
    }

    /* add  the new indexes */
    rc = insert_record_indexes(pCur, thd, tmp, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s: fail to update genid %llx (%lld) rc=%d bdberr=%d (8)\n",
                __func__, tmp, pCur->genid, rc, bdberr);
        return -1;
    }

    if (gbl_partial_indexes && pCur->db->ix_partial &&
        (save_ins_keys(thd->sqlclntstate, tbl, tmp) ||
         save_del_keys(thd->sqlclntstate, tbl, pCur->genid))) {
        logmsg(LOGMSG_ERROR, "%s: error saving the shadow dirty keys\n", __func__);
        return -1;
    }

#ifdef TEST_OSQL
    uuidstr_t us;
    fprintf(stdout,
            "[%llu %s] Updated genid=%llu (%u) rc=%d pCur->genid=%llu\n",
            osql->rqid, comdb2uuidstr(osql->uuid, us), pCur->genid,
            pthread_self(), rc, pCur->genid);
#endif

    tbl->seq = increment_seq(tbl->seq);

    return 0;
}

int osql_save_insrec(struct BtCursor *pCur, struct sql_thread *thd, char *pData,
                     int nData)
{

    int rc = 0;
    int bdberr = 0;
    shad_tbl_t *tbl = NULL;
    unsigned long long tmp = 0;

    if (!(tbl = open_shadtbl(pCur)) || !tbl->add_cur) {
        logmsg(LOGMSG_ERROR, "%s: error getting shadtbl for \'%s\'\n", __func__,
                pCur->db->dbname);
        return -1;
    }

    tmp = tbl->seq;

    /* mark this as synthetic genid */
    set_genid_add(&tmp);

    rc = bdb_temp_table_put(tbl->env->bdb_env, tbl->add_tbl->table, &tmp,
                            sizeof(tmp), (char *)pData, nData, NULL, &bdberr);

#ifdef TEST_OSQL
    uuidstr_t us;
    fprintf(stdout, "[%llu %s] Inserted seq=%llu (%u) rc=%d pCur->genid=%llu\n",
            thd->sqlclntstate->osql.rqid,
            comdb2uuidstr(thd->sqlclntstate->osql.uuid, us), tmp,
            pthread_self(), rc, pCur->genid);
#endif

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: fail to insert genid %llx (%lld) rc=%d bdberr=%d\n",
                __func__, tmp, pCur->genid, rc, bdberr);
        return -1;
    }

    if (thd->sqlclntstate->dbtran.shadow_tran)
        bdb_set_check_shadows(thd->sqlclntstate->dbtran.shadow_tran);

    /* if this is recom, snapisol or serial, we need to update the index shadows
     */
    if (insert_record_indexes(pCur, thd, tmp, &bdberr)) {
        logmsg(LOGMSG_ERROR, "%s: error updating the shadow indexes bdberr = %d\n",
                __func__, bdberr);
        return -1;
    }

    if (gbl_partial_indexes && pCur->db->ix_partial &&
        save_ins_keys(thd->sqlclntstate, tbl, tmp)) {
        logmsg(LOGMSG_ERROR, "%s: error saving the shadow dirty keys\n", __func__);
        return -1;
    }

    tbl->seq = increment_seq(tbl->seq);
    /*++tbl->seq;*/

    thd->sqlclntstate->osql.dirty = 1;

    return 0;
}

int osql_save_delrec(struct BtCursor *pCur, struct sql_thread *thd)
{
    shad_tbl_t *tbl = NULL;
    int rc = 0;
    int bdberr = 0;

    /* this is stupid, but until I better integrate bdb_osql and
       osqlshadtbl, this will do */
    if (!(tbl = open_shadtbl(pCur)) || !tbl->blb_cur) {
        logmsg(LOGMSG_ERROR, "%s: error getting shadtbl\n", __func__);
        return -1;
    }

    if (is_genid_synthetic(pCur->genid)) {
        rc = delete_synthetic_row(pCur, thd, tbl);
    } else {
        rc = bdb_tran_deltbl_setdeleted(pCur->bdbcur, pCur->genid, NULL, 0,
                                        &bdberr);
    }
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: fail to delete genid %llx (%lld) rc=%d bdberr=%d (5)\n",
                __func__, tbl->seq, pCur->genid, rc, bdberr);
        return -1;
    }

    if (gbl_partial_indexes && pCur->db->ix_partial &&
        save_del_keys(thd->sqlclntstate, tbl, pCur->genid)) {
        logmsg(LOGMSG_ERROR, "%s: error saving the shadow dirty keys\n", __func__);
        return -1;
    }

    thd->sqlclntstate->osql.dirty = 1;

    return 0;
}

int osql_save_index(struct BtCursor *pCur, struct sql_thread *thd,
                    int is_update, int is_delete)
{
    struct sqlclntstate *clnt = thd->sqlclntstate;
    osqlstate_t *osql = &clnt->osql;
    shad_tbl_t *tbl = NULL;
    int rc = 0;
    int bdberr = 0;
    int i;
    unsigned long long tmp = 0;
    unsigned long long dirty_key = -1ULL;
    struct tmp_table *tmp_tbl = NULL;
    struct temp_cursor *tmp_cur = NULL;
    unsigned char **index = NULL;

    if (!(tbl = open_shadtbl(pCur))) {
        logmsg(LOGMSG_ERROR, "%s: error getting shadtbl\n", __func__);
        return -1;
    }

    if (is_delete) {
        tmp_tbl = tbl->delidx_tbl;
        tmp_cur = tbl->delidx_cur;
        index = clnt->idxDelete;
        tmp = pCur->genid;
        dirty_key = clnt->del_keys;
    } else {
        tmp_tbl = tbl->insidx_tbl;
        tmp_cur = tbl->insidx_cur;
        index = clnt->idxInsert;
        tmp = tbl->seq;
        dirty_key = clnt->ins_keys;
    }

    if (!tmp_cur) {
        logmsg(LOGMSG_ERROR, "%s: error getting shadtbl cursor\n", __func__);
        return -1;
    }

    if (!is_delete) {
        if (is_update)
            set_genid_upd(&tmp);
        else
            set_genid_add(&tmp);
    }

    for (i = 0; i < pCur->db->nix && rc == SQLITE_OK; i++) {
        index_key_t key;
        key.seq = tmp;
        key.ixnum = i;
        /* only save keys when told */
        if (gbl_partial_indexes && pCur->db->ix_partial &&
            !(dirty_key & (1ULL << i)))
            continue;
        rc = bdb_temp_table_put(tbl->env->bdb_env, tmp_tbl->table, &key,
                                sizeof(key), index[i], getkeysize(pCur->db, i),
                                NULL, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: fail to insert seq %llu rc=%d bdberr=%d\n",
                    __func__, key.seq, rc, bdberr);
            return -1;
        }
    }

    return 0;
}

int osql_save_dbq_consume(struct sqlclntstate *clnt, const char *spname,
                          genid_t genid)
{
    shadbq_t *shad = &clnt->osql.shadbq;
    shad->spname = spname;
    shad->genid = genid;
    return 0;
}

int osql_save_updcols(struct BtCursor *pCur, struct sql_thread *thd,
                      int *updCols)
{
    osqlstate_t *osql = &thd->sqlclntstate->osql;
    int bdberr = 0;
    shad_tbl_t *tbl = NULL;
    unsigned long long tmp = 0;
    updCols_key_t key;
    int len = 0;
    int rc;
    int updated_synthetic = 0;

    /* verify that there's something to update */
    if (NULL == updCols || updCols[0] <= 0) {
        return 0;
    }

    if (!(tbl = open_shadtbl(pCur)) || !tbl->blb_cur) {
        logmsg(LOGMSG_ERROR, "osql_save_updcols: error getting shadtbl\n");
        return -1;
    }

    tmp = tbl->seq;
    set_genid_upd(&tmp);

    /* find the old map, if it exists */
    if (is_genid_synthetic(pCur->genid)) {
        /* union of updCols here, if updCols exists */
        updCols_key_t *pkey = (updCols_key_t *)malloc(sizeof(*pkey));
        if (!pkey) {
            logmsg(LOGMSG_ERROR, "malloc %d\n", sizeof(*pkey));
            return -1;
        }

        pkey->seq = pCur->genid;
        pkey->id = -1ULL;

        rc = bdb_temp_table_find_exact(tbl->env->bdb_env, tbl->blb_cur, pkey,
                                       sizeof(*pkey), &bdberr);
        if (bdberr) {
            logmsg(LOGMSG_ERROR, 
                    "%s: fail to update genid %llx (%lld) rc=%d bdberr=%d (1)\n",
                    __func__, tmp, pkey->seq, rc, bdberr);
            free(pkey);
            return -1;
        }

        if (rc == IX_FND) {
            int *oldUpdCols = (int *)bdb_temp_table_data(tbl->blb_cur);
            int oldUpdCols_len = bdb_temp_table_datasize(tbl->blb_cur);
            int i;

            assert((oldUpdCols[0] + 1) * sizeof(int) == oldUpdCols_len);
            assert(updCols[0] == oldUpdCols[0]);

            for (i = 0; i < updCols[0]; i++) {
                if (oldUpdCols[i + 1] ==
                    -1) /* use the new setting if this was not updated */
                    oldUpdCols[i + 1] = updCols[i + 1];
            }

            rc =
                bdb_temp_table_delete(tbl->env->bdb_env, tbl->blb_cur, &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: failed to delete old updcols rc=%d bdberr=%d\n",
                        __func__, rc, bdberr);
            }
            updCols = oldUpdCols;
        }
    }

    /* insert into the blobs table with a blobid of -1 */
    key.id = -1ULL;
    key.seq = tmp;

    len = (updCols[0] + 1) * sizeof(int);

    rc = bdb_temp_table_put(tbl->env->bdb_env, tbl->blb_tbl->table, &key,
                            sizeof(key), updCols, len, NULL, &bdberr);

#ifdef TEST_OSQL
    uuidstr_t us;
    fprintf(stdout, "[%llu %s] Inserted updcol seq=%llu id=%d len=%d (%u) "
                    "rc=%d pCur->genid=%llu\n",
            osql->rqid, comdb2uuidstr(osql->uuid, us), key.seq, key.id, len,
            pthread_self(), rc, pCur->genid);
#endif

    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: fail to insert seq %llu rc=%d bdberr=%d\n",
                __func__, key.seq, rc, bdberr);
        return -1;
    }

    tbl->updcols = 1;

    thd->sqlclntstate->osql.dirty = 1;

    return 0;
}

int osql_save_qblobs(struct BtCursor *pCur, struct sql_thread *thd,
                     blob_buffer_t *blobs, int maxblobs, int is_update)
{

    osqlstate_t *osql = &thd->sqlclntstate->osql;
    int rc = 0;
    int bdberr = 0;
    shad_tbl_t *tbl = NULL;
    int i;
    int idx;
    int ncols;
    unsigned long long tmp = 0;

    if (pCur->numblobs == 0) {
        /* no blobs */
        return 0;
    }
    if (!(tbl = open_shadtbl(pCur))) {
        logmsg(LOGMSG_ERROR, "%s: error getting shadtbl\n", __func__);
        return -1;
    }
    if (!tbl->blb_cur) {
        logmsg(LOGMSG_ERROR, "%s: error getting shadtbl cursor\n", __func__);
        return -1;
    }

    tmp = tbl->seq;
    if (is_update)
        set_genid_upd(&tmp);
    else
        set_genid_add(&tmp);

    for (i = 0; i < maxblobs && rc == SQLITE_OK; i++) {

        if (blobs[i].exists == 1) {

            blob_key_t key;

            key.id = i;
            /* if it is an update, we index blobs using original genid
               if it is an insert, we index blobs using temptable seq number
             */
            key.seq = tmp;

            rc = bdb_temp_table_put(tbl->env->bdb_env, tbl->blb_tbl->table,
                                    &key, sizeof(key), blobs[i].data,
                                    blobs[i].length, NULL, &bdberr);

#ifdef TEST_OSQL
            fprintf(stdout, "[%llu] Inserted blob seq=%llu id=%d len=%d (%u) "
                            "rc=%d pCur->genid=%llu\n",
                    osql->rqid, key.seq, key.id, blobs[i].length,
                    pthread_self(), rc, pCur->genid);
#endif

            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: fail to insert seq %llu rc=%d bdberr=%d\n",
                        __func__, key.seq, rc, bdberr);
                return -1;
            }
        }
    }

    return 0;
}

void *osql_get_shadow_bydb(struct sqlclntstate *clnt, struct dbtable *db)
{
    void *ret = NULL;
    shad_tbl_t *tbl = NULL;

    LISTC_FOR_EACH(&clnt->osql.shadtbls, tbl, linkv)
    {
        if (tbl->db == db) {
            ret = tbl;
            break;
        }
    }

    return ret;
}

/**
 * Scan the shadow tables for the current transaction
 * and send to the master the ops
 */
int osql_shadtbl_process(struct sqlclntstate *clnt, int *nops, int *bdberr)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    shad_tbl_t *tbl = NULL;
    int i = 0;

    *nops = 0;

    /* OPTIMIZATION: if there are only SELECTV, configurably,
       we have the option to skip master transaction! This is fixing
       consumer-producer pullers that run selectv!
     */
    if (!osql->dirty &&
        !bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DISABLE_SELECTVONLY_TRAN_NOP) &&
        !osql->sc_tbl) {
        return -3;
    }

    if (osql_shadtbl_empty(clnt)) {
        return -2;
    }

    rc = process_local_shadtbl_recgenids(clnt, bdberr);
    if (rc)
        return -1;
    rc = process_local_shadtbl_sc(clnt, bdberr);
    if (rc) return -1;

    LISTC_FOR_EACH(&osql->shadtbls, tbl, linkv)
    {
        /* we need to reset any cached nops in tbl */
        tbl->nops = 0;

        /* set the table we operate on */
        rc = process_local_shadtbl_usedb(clnt, tbl->db->dbname);
        if (rc)
            return -1;

        rc = process_local_shadtbl_skp(clnt, tbl, bdberr, *nops);
        if (rc == SQLITE_TOOBIG) {
            *nops += tbl->nops;
            return rc;
        }
        if (rc)
            return -1;

        rc = process_local_shadtbl_add(clnt, tbl, bdberr, *nops);
        if (rc == SQLITE_TOOBIG) {
            *nops += tbl->nops;
            return rc;
        }
        if (rc)
            return -1;

        rc = process_local_shadtbl_upd(clnt, tbl, bdberr, *nops);
        if (rc == SQLITE_TOOBIG) {
            *nops += tbl->nops;
            return rc;
        }
        if (rc)
            return -1;

        *nops += tbl->nops;
    }

    rc = process_local_shadtbl_dbq(clnt, bdberr, nops);
    if (rc == SQLITE_TOOBIG) {
        return rc;
    }
    if (rc)
        return -1;

    return rc;
}

/**
 * Clear the rows from the shadow tables at the end of a transaction
 *
 */
int osql_shadtbl_cleartbls(struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    shad_tbl_t *tbl = NULL;
    int bdberr = 0;
    int rc = 0;

    /* reset the verify */
    if (osql->verify_tbl) {
        truncate_tablecursor(thedb->bdb_env, &osql->verify_cur,
                             osql->verify_tbl, &bdberr);
    }
    /* reset the sc */
    if (osql->sc_tbl) {
        truncate_tablecursor(thedb->bdb_env, &osql->sc_cur, osql->sc_tbl,
                             &bdberr);
    }

    /* close the temporary bdb structures first */
    LISTC_FOR_EACH(&osql->shadtbls, tbl, linkv)
    {
        if (tbl->add_tbl) {
            truncate_tablecursor(thedb->bdb_env, &tbl->add_cur,
                                 tbl->add_tbl->table, &bdberr);
        }

        if (tbl->upd_tbl) {
            truncate_tablecursor(thedb->bdb_env, &tbl->upd_cur,
                                 tbl->upd_tbl->table, &bdberr);
        }

        if (tbl->blb_tbl) {
            truncate_tablecursor(thedb->bdb_env, &tbl->blb_cur,
                                 tbl->blb_tbl->table, &bdberr);
        }

        if (tbl->delidx_tbl) {
            truncate_tablecursor(thedb->bdb_env, &tbl->delidx_cur,
                                 tbl->delidx_tbl->table, &bdberr);
        }

        if (tbl->insidx_tbl) {
            truncate_tablecursor(thedb->bdb_env, &tbl->insidx_cur,
                                 tbl->insidx_tbl->table, &bdberr);
        }
    }

    return rc;
}

/****************************************** INTERNALS
 * **************************************/

static int process_local_shadtbl_usedb(struct sqlclntstate *clnt,
                                       char *tablename)
{

    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    int osql_nettype = tran2netrpl(clnt->dbtran.mode);

    rc = osql_send_usedb(osql->host, osql->rqid, osql->uuid, tablename,
                         osql_nettype, osql->logsb);

    return rc;
}

static int process_local_shadtbl_skp(struct sqlclntstate *clnt, shad_tbl_t *tbl,
                                     int *bdberr, int crt_nops)
{
    osqlstate_t *osql = &clnt->osql;
    struct temp_cursor *cur = NULL;
    int rc = 0;
    int osql_nettype = tran2netrpl(clnt->dbtran.mode);
    unsigned long long genid = 0;
    char *data = NULL;
    int datalen = 0;

    cur = bdb_tran_deltbl_first(tbl->env->bdb_env, clnt->dbtran.shadow_tran,
                                tbl->dbnum, &genid, &data, &datalen, bdberr);
    if (!cur) {
        if (*bdberr == 0)
            return 0;

        logmsg(LOGMSG_ERROR, "%s: bdb_tran_deltbl_first failed rc=%d bdberr=%d\n",
                __func__, rc, *bdberr);
        return SQLITE_INTERNAL;
    }

    while (rc == 0) {

        if (datalen == 0) { /* this a delete, not an update */

            tbl->nops++;

            if ((tbl->nops + crt_nops) > g_osql_max_trans) {
                return SQLITE_TOOBIG;
            }

            rc = process_local_shadtbl_index(clnt, tbl, bdberr, genid, 1);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: error writting index record to master in "
                                "offload mode %d!\n",
                        __func__, rc);
                return SQLITE_INTERNAL;
            }

            rc = osql_send_delrec(osql->host, osql->rqid, osql->uuid, genid,
                                  (gbl_partial_indexes && tbl->db->ix_partial)
                                      ? get_del_keys(clnt, tbl, genid)
                                      : -1ULL,
                                  osql_nettype, osql->logsb);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: error writting record to master in offload mode %d!\n",
                    __func__, rc);
                return SQLITE_INTERNAL;
            }
        }

        rc = bdb_tran_deltbl_next(tbl->env->bdb_env, clnt->dbtran.shadow_tran,
                                  cur, &genid, &data, &datalen, bdberr);
    }
    if (rc == IX_PASTEOF || rc == IX_EMPTY) {
        rc = 0;
    } else {
        logmsg(LOGMSG_ERROR, "%s:%d bdb_temp_table_next failed rc=%d bdberr=%d\n",
                __func__, __LINE__, rc, *bdberr);
        /* fall-through */
    }

    return rc;
}

static int process_local_shadtbl_updcols(struct sqlclntstate *clnt,
                                         shad_tbl_t *tbl, int **updcolsout,
                                         int *bdberr, unsigned long long seq)
{

    osqlstate_t *osql = &clnt->osql;
    long long savkey;
    int *cdata = NULL;
    int ldata = 0;
    int cksz;
    int rc;
    int osql_nettype = tran2netrpl(clnt->dbtran.mode);

    if (!tbl->updcols)
        return 0;

    updCols_key_t *key = (updCols_key_t *)malloc(sizeof(updCols_key_t));
    savkey = key->seq = seq;
    key->id = -1;

    rc = bdb_temp_table_find_exact(tbl->env->bdb_env, tbl->blb_cur, key,
                                   sizeof(*key), bdberr);
    if (rc < 0) {
        free(key);
        return rc;
    }

    if (IX_EMPTY == rc || IX_NOTFND == rc) {
        free(key);
        return 0;
    }

    cdata = bdb_temp_table_data(tbl->blb_cur);
    ldata = bdb_temp_table_datasize(tbl->blb_cur);

    if (ldata < 4) {
        logmsg(LOGMSG_ERROR, "%s: invalid size for updcol object: %d!\n", __func__,
                ldata);
        return SQLITE_INTERNAL;
    }

    cksz = (cdata[0] + 1) * sizeof(int);
    if (ldata != cksz) {
        logmsg(LOGMSG_USER, 
                "%s: mismatched size for updcol object: got %d should be %d!\n",
                __func__, ldata, cksz);
        return SQLITE_INTERNAL;
    }

    if (updcolsout) {
        *updcolsout = (int *)malloc(cksz);
        memcpy(*updcolsout, cdata, cksz);
    }

    rc = osql_send_updcols(osql->host, osql->rqid, osql->uuid, savkey,
                           osql_nettype, &cdata[1], cdata[0], osql->logsb);

    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s: error writting record to master in offload mode %d!\n",
                __func__, rc);
        return SQLITE_INTERNAL;
    }

    return rc;
}

static int process_local_shadtbl_qblob(struct sqlclntstate *clnt,
                                       shad_tbl_t *tbl, int *updCols,
                                       int *bdberr, unsigned long long seq,
                                       char *record)
{

    osqlstate_t *osql = &clnt->osql;
    int i = 0;
    int ldata = 0;
    char *data = NULL;
    int rc = 0;
    int idx;
    int ncols;
    int osql_nettype = tran2netrpl(clnt->dbtran.mode);

    /* identify the number of blobs */
    for (i = 0; i < tbl->nblobs; i++) {
        blob_key_t *key;

        if (updCols && gbl_osql_blob_optimization) {
            idx = get_schema_blob_field_idx(tbl->db->dbname, ".ONDISK", i);
            ncols = updCols[0];
            if (idx >= 0 && idx < ncols && -1 == updCols[idx + 1]) {
                rc = osql_send_qblob(osql->host, osql->rqid, osql->uuid, i, seq,
                                     osql_nettype, NULL, -2, osql->logsb);
                continue;
            }
        }

        /* key gets set into cur->key, and is freed when a new key is
           submitted or when the cursor is closed */
        key = (blob_key_t *)malloc(sizeof(blob_key_t));

        key->seq = seq;
        key->id = i;

        rc = bdb_temp_table_find_exact(tbl->env->bdb_env, tbl->blb_cur, key,
                                       sizeof(*key), bdberr);
        if (rc == IX_EMPTY || rc == IX_NOTFND) {
            /* null blob */
            data = NULL;
            ldata = -1;
            free(key);
        } else if (rc == IX_FND) {
            data = bdb_temp_table_data(tbl->blb_cur);
            ldata = bdb_temp_table_datasize(tbl->blb_cur);
        } else {
            free(key);
            return SQLITE_INTERNAL;
        }

        rc = osql_send_qblob(osql->host, osql->rqid, osql->uuid, i, seq,
                             osql_nettype, data, ldata, osql->logsb);

        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s: error writting record to master in offload mode %d!\n",
                    __func__, rc);
            return SQLITE_INTERNAL;
        }

    } /* for */

    return rc;
}

static int process_local_shadtbl_index(struct sqlclntstate *clnt,
                                       shad_tbl_t *tbl, int *bdberr,
                                       unsigned long long seq, int is_delete)
{
    osqlstate_t *osql = &clnt->osql;
    int i = 0;
    int lindex = 0;
    char *index = NULL;
    int rc = 0;
    int idx;
    int ncols;
    int osql_nettype = tran2netrpl(clnt->dbtran.mode);
    struct temp_cursor *tmp_cur = NULL;

    if (!gbl_expressions_indexes || !tbl->db->ix_expr)
        return 0;

    if (is_delete) {
        tmp_cur = tbl->delidx_cur;
    } else {
        tmp_cur = tbl->insidx_cur;
    }

    for (i = 0; i < tbl->db->nix; i++) {
        index_key_t *key;
        /* key gets set into cur->key, and is freed when a new key is
           submitted or when the cursor is closed */
        key = (index_key_t *)malloc(sizeof(index_key_t));
        key->seq = seq;
        key->ixnum = i;

        rc = bdb_temp_table_find_exact(tbl->env->bdb_env, tmp_cur, key,
                                       sizeof(*key), bdberr);
        if (rc == IX_FND) {
            index = bdb_temp_table_data(tmp_cur);
            lindex = bdb_temp_table_datasize(tmp_cur);
        } else {
            logmsg(LOGMSG_ERROR, "%s: error missing index record!\n", __func__);
            free(key);
            return SQLITE_INTERNAL;
        }

        rc = osql_send_index(osql->host, osql->rqid, osql->uuid, seq, is_delete,
                             i, index, lindex, osql_nettype, osql->logsb);

        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: error writting record to master in offload mode %d!\n",
                    __func__, rc);
            return SQLITE_INTERNAL;
        }
    }
    return 0;
}

static int process_local_shadtbl_add(struct sqlclntstate *clnt, shad_tbl_t *tbl,
                                     int *bdberr, int crt_nops)
{

    osqlstate_t *osql = &clnt->osql;
    unsigned long long *seq = NULL;
    int rc = 0;
    int osql_nettype = tran2netrpl(clnt->dbtran.mode);

    rc = bdb_temp_table_first(tbl->env->bdb_env, tbl->add_cur, bdberr);
    if (rc == IX_EMPTY)
        return 0;
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_first failed rc=%d bdberr=%d\n",
                __func__, rc, *bdberr);
        return SQLITE_INTERNAL;
    }

    while (rc == 0) {
        char *data = bdb_temp_table_data(tbl->add_cur);
        int ldata = bdb_temp_table_datasize(tbl->add_cur);

        seq = (unsigned long long *)malloc(sizeof(unsigned long long));

        *seq = *(unsigned long long *)bdb_temp_table_key(tbl->add_cur);

        /*
         * If this isn't a synthetic genid, then it's a logfile update to a
         * page-order cursor- ignore that here.
         */
        if (!is_genid_synthetic(*seq)) {
            rc = bdb_temp_table_next(tbl->env->bdb_env, tbl->add_cur, bdberr);
            continue;
        }

        /* lookup the upd_cur to see if this is an actual update, skip it if so
  TODO: we could package and ship it rite here, rite now (later)
         */
        rc = bdb_temp_table_find_exact(tbl->env->bdb_env, tbl->upd_cur, seq,
                                       sizeof(*seq), bdberr);
        if (rc < 0) {
            free(seq);
            return rc;
        }

        if (rc != IX_FND) {

            rc = process_local_shadtbl_index(clnt, tbl, bdberr, *seq, 0);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: error writting index record to master in "
                                "offload mode!\n",
                        __func__);
                free(seq);
                break;
            }

            rc = process_local_shadtbl_qblob(clnt, tbl, NULL, bdberr, *seq,
                                             data);
            if (rc) {
                free(seq);
                break;
            }

            tbl->nops++;

            if ((tbl->nops + crt_nops) > g_osql_max_trans) {
                free(seq);
                return SQLITE_TOOBIG;
            }

            rc = osql_send_insrec(osql->host, osql->rqid, osql->uuid, *seq,
                                  (gbl_partial_indexes && tbl->db->ix_partial)
                                      ? get_ins_keys(clnt, tbl, *seq)
                                      : -1ULL,
                                  data, ldata, osql_nettype, osql->logsb);

            if (rc) {
                logmsg(LOGMSG_USER, 
                        "%s: error writting record to master in offload mode!\n",
                        __func__);
                free(seq);
                return SQLITE_INTERNAL;
            }
            free(seq);
        }

        rc = bdb_temp_table_next(tbl->env->bdb_env, tbl->add_cur, bdberr);
    }

    if (rc == IX_PASTEOF || rc == IX_EMPTY) {
        rc = 0;
    } else {
        logmsg(LOGMSG_ERROR, "%s:%d bdb_temp_table_next failed rc=%d bdberr=%d\n",
                __func__, __LINE__, rc, *bdberr);
        /* fall-through */
    }

    return rc;
}

static int process_local_shadtbl_upd(struct sqlclntstate *clnt, shad_tbl_t *tbl,
                                     int *bdberr, int crt_nops)
{

    osqlstate_t *osql = &clnt->osql;
    unsigned long long *seq = NULL;
    int *updCols = NULL;
    int rc = 0;
    unsigned long long genid = 0;
    int osql_nettype = tran2netrpl(clnt->dbtran.mode);

    rc = bdb_temp_table_first(tbl->env->bdb_env, tbl->upd_cur, bdberr);
    if (rc == IX_EMPTY)
        return 0;
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_first failed rc=%d bdberr=%d\n",
                __func__, rc, *bdberr);
        return SQLITE_INTERNAL;
    }

    while (rc == 0) {
        char *data = NULL;
        int ldata = 0;

        seq = (unsigned long long *)malloc(sizeof(unsigned long long));

        *seq = *(unsigned long long *)bdb_temp_table_key(tbl->upd_cur);
        genid = *(unsigned long long *)bdb_temp_table_data(tbl->upd_cur);

        /* locate the row in the add_cur */
        rc = bdb_temp_table_find_exact(tbl->env->bdb_env, tbl->add_cur, seq,
                                       sizeof(*seq), bdberr);
        if (rc != IX_FND) {
            logmsg(LOGMSG_ERROR, "%s: this genid %llu must exist! bug rc = %d\n",
                    __func__, *seq, rc);
            free(seq);
            return SQLITE_INTERNAL;
        }

        data = bdb_temp_table_data(tbl->add_cur);
        ldata = bdb_temp_table_datasize(tbl->add_cur);

        /* counting operations */
        tbl->nops++;

        if ((tbl->nops + crt_nops) > g_osql_max_trans) {
            return SQLITE_TOOBIG;
        }

        rc = process_local_shadtbl_updcols(clnt, tbl, &updCols, bdberr, *seq);
        if (rc)
            return SQLITE_INTERNAL;

        /* indexes to delete */
        rc = process_local_shadtbl_index(clnt, tbl, bdberr, genid, 1);
        if (rc)
            return SQLITE_INTERNAL;
        /* indexes to add */
        rc = process_local_shadtbl_index(clnt, tbl, bdberr, *seq, 0);
        if (rc)
            return SQLITE_INTERNAL;

        rc =
            process_local_shadtbl_qblob(clnt, tbl, updCols, bdberr, *seq, data);
        if (rc)
            return SQLITE_INTERNAL;

        if (updCols) {
            free(updCols);
            updCols = NULL;
        }

        rc = osql_send_updrec(osql->host, osql->rqid, osql->uuid, genid,
                              (gbl_partial_indexes && tbl->db->ix_partial)
                                  ? get_ins_keys(clnt, tbl, *seq)
                                  : -1ULL,
                              (gbl_partial_indexes && tbl->db->ix_partial)
                                  ? get_del_keys(clnt, tbl, genid)
                                  : -1ULL,
                              data, ldata, osql_nettype, osql->logsb);

        if (rc) {
            rc = SQLITE_INTERNAL;
            logmsg(LOGMSG_ERROR, "%s: error writting record to master in offload mode!\n",
                    __func__);
            break;
        }

        rc = bdb_temp_table_next(tbl->env->bdb_env, tbl->upd_cur, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d bdb_temp_table_next failed rc=%d bdberr=%d\n",
                   __func__, __LINE__, rc, *bdberr);
            break;
        }
    }
    if (rc == IX_PASTEOF || rc == IX_EMPTY) {
        rc = 0;
    }

    return rc;
}

/*
** We can consume only one item at a time (the item at the head of the DBQ).
** Setting up a shadow tmptbl to store dbq name and one genid, seems a bit
** overkill. I will just save this info in sqlclntstate.
*/
static int process_local_shadtbl_dbq(struct sqlclntstate *clnt, int *bdberr,
                                     int *crt_nops)
{
    if (*crt_nops >= g_osql_max_trans) {
        return SQLITE_TOOBIG;
    }
    shadbq_t *shadbq = &clnt->osql.shadbq;
    if (shadbq->spname && shadbq->genid) {
        osql_dbq_consume(clnt, shadbq->spname, shadbq->genid);
        ++*crt_nops;
    }
    return SQLITE_OK;
}

static int insert_record_indexes(BtCursor *pCur, struct sql_thread *thd,
                                 int64_t nKey, int *bdberr)
{
    bdb_cursor_ifn_t *tmpcur;
    int ix;
    int rc = SQLITE_OK;
    char key[MAXKEYLEN];
    char namebuf[MAXTAGLEN];
    char *datacopy;
    int datacopylen;

    if (thd->sqlclntstate && thd->sqlclntstate->dbtran.mode == TRANLEVEL_SOSQL)
        return 0;

    /* Add all the keys to the shadow indices */
    for (ix = 0; ix < pCur->db->nix; ix++) {
        /* only add keys when told */
        if (gbl_partial_indexes && pCur->db->ix_partial &&
            !(thd->sqlclntstate->ins_keys & (1ULL << ix)))
            continue;

        snprintf(namebuf, sizeof(namebuf), ".ONDISK_IX_%d", ix);
        if (gbl_expressions_indexes && pCur->db->ix_expr) {
            memcpy(key, thd->sqlclntstate->idxInsert[ix],
                   pCur->db->ix_keylen[ix]);
        } else {
            rc = stag_to_stag_buf(pCur->db->dbname, ".ONDISK", pCur->ondisk_buf,
                                  namebuf, key, NULL);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR, "insert_record:stag_to_stag_buf ix %d\n", ix);
                return SQLITE_INTERNAL;
            }
        }

        tmpcur = bdb_cursor_open(
            pCur->db->handle, thd->sqlclntstate->dbtran.cursor_tran,
            thd->sqlclntstate->dbtran.shadow_tran, ix, BDB_OPEN_SHAD,
            osql_get_shadtbl_addtbl_newcursor(pCur), 0, 0, NULL, NULL, NULL,
            NULL, NULL, thd->sqlclntstate->bdb_osql_trak, bdberr);
        if (tmpcur == NULL) {
            logmsg(LOGMSG_ERROR, "%s: bdb_cursor_open ix %d rc %d\n", __func__, ix, *bdberr);
            return SQLITE_INTERNAL;
        }

        if (pCur->db->ix_datacopy[ix]) {
            datacopy = pCur->ondisk_buf;
            datacopylen = getdatsize(pCur->db);
        } else if (pCur->db->ix_collattr[ix]) {
            datacopy = alloca(4 * pCur->db->ix_collattr[ix]);

            rc = extract_decimal_quantum(pCur->db, ix, pCur->ondisk_buf, datacopy,
                                       4 * pCur->db->ix_collattr[ix], 
                                       &datacopylen);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s: failed to construct decimal index rc=%d\n",
                        __func__, rc);
                tmpcur->close(tmpcur, bdberr);
                return SQLITE_INTERNAL;
            }
        } else {
            datacopy = NULL;
            datacopylen = 0;
        }

        rc = tmpcur->insert(tmpcur, nKey, key, pCur->db->ix_keylen[ix],
                            datacopy, datacopylen, bdberr);
        if (rc) {
           logmsg(LOGMSG_ERROR, "%s: bdb_cursor_insert ix %d rc %d\n", __func__, ix,
                   *bdberr);
            tmpcur->close(tmpcur, bdberr);
            return SQLITE_INTERNAL;
        }

        rc = tmpcur->close(tmpcur, bdberr);
        if (rc) {
           logmsg(LOGMSG_ERROR, "%s: bdb_cursor_close ix %d rc %d\n", __func__, ix, *bdberr);
            return SQLITE_INTERNAL;
        }
    }

    return SQLITE_OK;
}

static int delete_record_indexes(BtCursor *pCur, char *pdta, int dtasize,
                                 struct sql_thread *thd, int *bdberr)
{

    int ix = 0;
    char namebuf[MAXTAGLEN];
    struct dbtable *db = pCur->db;
    char key[MAXKEYLEN];
    void *tran = thd->sqlclntstate->dbtran.shadow_tran;
    bdb_cursor_ifn_t *tmpcur = NULL;
    int rc = 0;
    unsigned long long genid = pCur->genid;
    void *dta = pCur->dtabuf;

    if (thd->sqlclntstate && thd->sqlclntstate->dbtran.mode == TRANLEVEL_SOSQL)
        return 0;

    /* none synthetic genids are added to the skip list
       are not deleted perse, therefore their indexes
       do not need to be updated either */
    if (!is_genid_synthetic(pCur->genid))
        return 0;

    if (pCur->ixnum >= 0) {
        /* index cursor, we saved dta row on the side */
        dta = pdta;
    }

    /* delete the keys */
    for (ix = 0; ix < db->nix; ix++) {
        /* only delete keys when told */
        if (gbl_partial_indexes && db->ix_partial &&
            !(thd->sqlclntstate->del_keys & (1ULL << ix)))
            continue;

        snprintf(namebuf, sizeof(namebuf), ".ONDISK_IX_%d", ix);
        if (gbl_expressions_indexes && db->ix_expr) {
            memcpy(key, thd->sqlclntstate->idxDelete[ix], db->ix_keylen[ix]);
        } else {
            rc = stag_to_stag_buf(db->dbname, ".ONDISK", dta, namebuf, key,
                                  NULL);
            if (rc == -1) {
                logmsg(LOGMSG_ERROR, "%s:stag_to_stag_buf ix %d\n", __func__, ix);
                return -1;
            }
        }

        tmpcur = bdb_cursor_open(
            db->handle, thd->sqlclntstate->dbtran.cursor_tran,
            thd->sqlclntstate->dbtran.shadow_tran, ix, BDB_OPEN_SHAD,
            osql_get_shadtbl_addtbl_newcursor(pCur), 0, 0, NULL, NULL, NULL,
            NULL, NULL, thd->sqlclntstate->bdb_osql_trak, bdberr);
        if (tmpcur == NULL) {
            logmsg(LOGMSG_ERROR, "%s:bdb_cursor_open ix %d rc %d\n", __func__, ix, *bdberr);
            return -1;
        }

        rc = tmpcur->find(tmpcur, key, db->ix_keylen[ix], 0, bdberr);
        if (rc) {
            int newbdberr;
            logmsg(LOGMSG_ERROR, "%s:bdb_cursor_find ix %d rc %d bdberr %d\n", __func__, ix,
                   rc, *bdberr);

            rc = tmpcur->close(tmpcur, &newbdberr);
            if (rc)
               logmsg(LOGMSG_ERROR, "%s:bdb_cursor_close ix %d rc %d bdberr %d\n", __func__,
                       ix, rc, newbdberr);

            return -1;
        }
        if (memcmp(key, tmpcur->data(tmpcur), db->ix_keylen[ix]) != 0) {
            logmsg(LOGMSG_ERROR, "%s:bdb_cursor_find did not find shadow index !\n",
                   __func__);
            return -1;
        }

        rc = tmpcur->delete (tmpcur, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:bdb_cursor_delete ix %d bdberr %d\n", __func__, ix,
                   *bdberr);

            rc = tmpcur->close(tmpcur, bdberr);
            if (rc)
                logmsg(LOGMSG_ERROR, "%s:bdb_cursor_close ix %d rc %d bdberr %d\n", __func__,
                       ix, rc, *bdberr);

            return -1;
        }

        rc = tmpcur->close(tmpcur, bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:bdb_cursor_close ix %d bdberr %d\n", __func__, ix,
                   *bdberr);
            rc = -1;
        }
    }
    return 0;
}

static inline void osql_destroy_dbq(osqlstate_t *osql)
{
    osql->shadbq.spname = NULL;
    osql->shadbq.genid = 0;
}

/**
 * Frees shadow tables used by this sql client
 *
 */
void osql_shadtbl_close(struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    shad_tbl_t *tbl = NULL, *tmp = NULL;

    osql_destroy_verify(thedb->bdb_env, clnt);
    osql_destroy_dbq(osql);
    osql_destroy_schemachange(thedb->bdb_env, clnt);

    LISTC_FOR_EACH_SAFE(&osql->shadtbls, tbl, tmp, linkv)
    {
        listc_rfl(&osql->shadtbls, tbl);
        destroy_shadtbl(tbl);
        free(tbl);
    }
}

static int reopen_shadtbl_cursors(bdb_state_type *bdb_env, osqlstate_t *osql,
                                  struct temp_table *shad_tbl,
                                  struct temp_cursor **shad_cur)
{

    int bdberr = 0;

    if (shad_tbl) {

        if (*shad_cur) {
            logmsg(LOGMSG_ERROR, "%s: bug, bug, bug, cursor not closed\n", __func__);
            /* This should be fixed now */
            abort();
        } else {
            *shad_cur = bdb_temp_table_cursor(bdb_env, shad_tbl, NULL, &bdberr);
            if (!*shad_cur) {
                logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_cursor failed, bdberr=%d\n",
                        __func__, bdberr);
                return -1;
            }
        }
    }
    return 0;
}

/**
 * Open cursors for shadow tables, if any are present
 *
 */
int osql_shadtbl_begin_query(bdb_state_type *bdb_env, struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    shad_tbl_t *tbl = NULL;

    /*printf( "Opening %d locals\n", osql->shadtbls.count); */
    if (reopen_shadtbl_cursors(bdb_env, osql, osql->verify_tbl,
                               &osql->verify_cur))
        return -1;
    if (reopen_shadtbl_cursors(bdb_env, osql, osql->sc_tbl, &osql->sc_cur))
        return -1;

    /* close the temporary bdb structures first */
    LISTC_FOR_EACH(&osql->shadtbls, tbl, linkv)
    {
        if (tbl->add_tbl &&
            reopen_shadtbl_cursors(bdb_env, osql, tbl->add_tbl->table,
                                   &tbl->add_cur))
            return -1;
        if (tbl->upd_tbl &&
            reopen_shadtbl_cursors(bdb_env, osql, tbl->upd_tbl->table,
                                   &tbl->upd_cur))
            return -1;
        if (tbl->blb_tbl &&
            reopen_shadtbl_cursors(bdb_env, osql, tbl->blb_tbl->table,
                                   &tbl->blb_cur))
            return -1;
        if (tbl->delidx_tbl &&
            reopen_shadtbl_cursors(bdb_env, osql, tbl->delidx_tbl->table,
                                   &tbl->delidx_cur))
            return -1;
        if (tbl->insidx_tbl &&
            reopen_shadtbl_cursors(bdb_env, osql, tbl->insidx_tbl->table,
                                   &tbl->insidx_cur))
            return -1;
    }

    return 0;
}

static int close_shadtbl_cursors(bdb_state_type *bdb_env, osqlstate_t *osql,
                                 struct temp_table *shad_tbl,
                                 struct temp_cursor **shad_cur)
{

    int bdberr = 0;
    int rc = 0;

    if (shad_tbl && *shad_cur) {

        rc = bdb_temp_table_close_cursor(bdb_env, *shad_cur, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s: bdb_temp_table_close failed, rc=%d bdberr=%d\n",
                    __func__, rc, bdberr);
            return -1;
        }
        *shad_cur = NULL;
    }
    return 0;
}

/**
 * Close cursors for shadow tables, if any are present
 *
 */
int osql_shadtbl_done_query(bdb_state_type *bdb_env, struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    shad_tbl_t *tbl = NULL;

    /*printf( "Cleaning %d locals\n", osql->shadtbls.count); */
    close_shadtbl_cursors(bdb_env, osql, osql->verify_tbl, &osql->verify_cur);
    close_shadtbl_cursors(bdb_env, osql, osql->sc_tbl, &osql->sc_cur);

    /* close the temporary bdb structures first */
    LISTC_FOR_EACH(&osql->shadtbls, tbl, linkv)
    {
        if (tbl->add_tbl)
            close_shadtbl_cursors(bdb_env, osql, tbl->add_tbl->table,
                                  &tbl->add_cur);
        if (tbl->upd_tbl)
            close_shadtbl_cursors(bdb_env, osql, tbl->upd_tbl->table,
                                  &tbl->upd_cur);
        if (tbl->blb_tbl)
            close_shadtbl_cursors(bdb_env, osql, tbl->blb_tbl->table,
                                  &tbl->blb_cur);
        if (tbl->delidx_tbl)
            close_shadtbl_cursors(bdb_env, osql, tbl->delidx_tbl->table,
                                  &tbl->delidx_cur);
        if (tbl->insidx_tbl)
            close_shadtbl_cursors(bdb_env, osql, tbl->insidx_tbl->table,
                                  &tbl->insidx_cur);
    }
    return 0;
}

static int _saved_dta_row(struct temp_cursor *cur, char **row, int *rowlen)
{
    char *saved_dta_row = NULL;
    char *saved_dta_row_orig = NULL;
    int saved_dta_row_len = 0;

    saved_dta_row_len = bdb_temp_table_datasize(cur);
    if (saved_dta_row_len <= 0) {
        logmsg(LOGMSG_ERROR, "%s:%d: fail to retrieve datasize (3.1)\n", __FILE__,
                __LINE__);
        return -1;
    }
    saved_dta_row_orig = bdb_temp_table_data(cur);
    if (!saved_dta_row_orig) {
        logmsg(LOGMSG_ERROR, "%s:%d: fail to retrieve data (3.2)\n", __FILE__,
                __LINE__);
        return -1;
    }

    saved_dta_row = (char *)calloc(1, saved_dta_row_len);
    if (!saved_dta_row) {
        logmsg(LOGMSG_ERROR, "%s:%d: OOM (3.2)\n", __FILE__, __LINE__);
        return -1;
    }
    memcpy(saved_dta_row, saved_dta_row_orig, saved_dta_row_len);

    *row = saved_dta_row;
    *rowlen = saved_dta_row_len;

    return 0;
}

static int delete_synthetic_row(struct BtCursor *pCur, struct sql_thread *thd,
                                shad_tbl_t *tbl)
{
    unsigned long long *genid =
        (unsigned long long *)malloc(sizeof(unsigned long long));
    unsigned long long *genid2 =
        (unsigned long long *)malloc(sizeof(unsigned long long));
    bdb_state_type *bdbenv = tbl->env->bdb_env;
    int rc = 0;
    int bdberr = 0;
    char *saved_dta_row = NULL;
    int saved_dta_row_len = 0;

    *genid = pCur->genid;
    *genid2 = pCur->genid;

    /* if this is an update, please delete also update record */
    if (tbl->upd_cur) {
        rc = bdb_temp_table_find_exact(bdbenv, tbl->upd_cur, genid2,
                                       sizeof(*genid2), &bdberr);
        if (rc == IX_FND) {
            rc = bdb_temp_table_delete(bdbenv, tbl->upd_cur, &bdberr);
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s:%d: fail to delete genid %llx (%lld) rc=%d "
                                "bdberr=%d (3)\n",
                        __FILE__, __LINE__, *genid2, pCur->genid, rc, bdberr);

                free(genid);
                return rc;
            }
            /* we might as well leak the blobs here, as the table will get
             * truncated anyway */

            /* check the original genid; if this was a pre-existing row that was
               updated,
               replace the update with an actual delete */
            genid2 = (unsigned long long *)bdb_temp_table_data(tbl->upd_cur);
            if (!is_genid_synthetic(*genid2)) {
                rc = bdb_tran_deltbl_setdeleted(pCur->bdbcur, *genid2, NULL, 0,
                                                &bdberr);
                if (rc) {
                    logmsg(LOGMSG_ERROR, "%s: fail to delete genid %llx (%lld) "
                                    "rc=%d bdberr=%d (5)\n",
                            __func__, *genid2, pCur->genid, rc, bdberr);
                }
            }
        } else {
            free(genid2);

            if (rc != IX_NOTFND && rc != IX_PASTEOF && rc != IX_EMPTY) {
                logmsg(LOGMSG_ERROR, 
                        "%s: fail to find genid %llx (%lld) rc=%d bdberr=%d\n",
                        __func__, *genid, pCur->genid, rc, bdberr);
                free(genid);
                return rc;
            }
        }
    }

    /* find the add table entry */
    rc = bdb_temp_table_find_exact(bdbenv, tbl->add_cur, genid, sizeof(*genid),
                                   &bdberr);
    if (rc != IX_FND) {
        logmsg(LOGMSG_ERROR, "%s: fail to find genid %llx (%lld) rc=%d bdberr=%d\n",
                __func__, *genid, pCur->genid, rc, bdberr);
        free(genid);
        return rc;
    }

    if (pCur->ixnum >= 0) {
        rc = _saved_dta_row(tbl->add_cur, &saved_dta_row, &saved_dta_row_len);
        if (rc)
            return rc;
    }

    /* delete entry from add table */
    rc = bdb_temp_table_delete(bdbenv, tbl->add_cur, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s: fail to delete genid %llx (%lld) rc=%d bdberr=%d (3)\n",
                __func__, *genid, pCur->genid, rc, bdberr);
        return rc;
    }

    /* delete the indexes */
    rc = delete_record_indexes(pCur, saved_dta_row, saved_dta_row_len, thd,
                               &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, 
                "%s: fail to update genid %llx (%lld) rc=%d bdberr=%d (4)\n",
                __func__, *genid, pCur->genid, rc, bdberr);
    }

    return rc;
}

#ifdef _AIX
#pragma options align = packed
#else
#pragma pack(1)
#endif

typedef struct recgenid_key {
    int tblnum;
    unsigned long long genid;
} recgenid_key_t;

#ifdef _AIX
#pragma options align = full
#else
#pragma pack() /* return to normal alignment */
#endif

int osql_save_recordgenid(struct BtCursor *pCur, struct sql_thread *thd,
                          unsigned long long genid)
{
    osqlstate_t *osql = &thd->sqlclntstate->osql;
    int rc = 0;
    int bdberr = 0;
    shad_tbl_t *tbl = NULL;
    recgenid_key_t key;

    /*create a common verify */
    if (!osql->verify_tbl) {
        rc = osql_create_verify(thedb->bdb_env, thd->sqlclntstate, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to create verify rc=%d bdberr=%d\n",
                    __func__, rc, bdberr);
            return -1;
        }
    }

    if (!osql->verify_tbl || !osql->verify_cur) {
        logmsg(LOGMSG_ERROR, "%s: error getting verify table for \'%s\'\n", __func__,
                pCur->db->dbname);
        return -1;
    }

    memcpy(&key.tblnum, &pCur->tblnum, sizeof(key.tblnum));
    memcpy(&key.genid, &genid, sizeof(key.genid));

    /*printf("RECGENID SAVING %d : %llx\n", pCur->tblnum, genid);*/

    rc = bdb_temp_table_put(thedb->bdb_env, osql->verify_tbl, &key, sizeof(key),
                            NULL, 0, NULL, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: fail to save genid %llx\n", __func__, genid);
        return -1;
    }

    return 0;
}

static int process_local_shadtbl_recgenids(struct sqlclntstate *clnt,
                                           int *bdberr)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    int osql_nettype = tran2netrpl(clnt->dbtran.mode);
    unsigned long long genid;
    int tblnum;
    int old_tblnum;
    bdb_state_type *bdb_state = thedb->bdb_env;
    struct temp_cursor *cur = osql->verify_cur;
    recgenid_key_t *key;

    if (!cur) {
        return 0;
    }

    rc = bdb_temp_table_first(bdb_state, cur, bdberr);
    if (rc == IX_EMPTY)
        return 0;
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_first failed rc=%d bdberr=%d\n",
                __func__, rc, *bdberr);
        return SQLITE_INTERNAL;
    }

    old_tblnum = -1;
    while (rc == 0) {
        key = (recgenid_key_t *)bdb_temp_table_key(cur);
        memcpy(&tblnum, &key->tblnum, sizeof(tblnum));
        memcpy(&genid, &key->genid, sizeof(genid));

        /* do we need to send a new usedb? */
        if (old_tblnum != tblnum) {
            /*printf("RECGENID SENDING USEDB= %d %s\n", tblnum,
             * thedb->dbs[tblnum]->dbname);*/
            rc = process_local_shadtbl_usedb(clnt, thedb->dbs[tblnum]->dbname);
            if (rc) {
                logmsg(LOGMSG_ERROR, 
                        "%s:%d: error writting record to master in offload mode!\n",
                        __func__, __LINE__);
                return SQLITE_INTERNAL;
            }
            old_tblnum = tblnum;
        }

#if 0
      uuidstr_t us;
      comdb2uuidstr(osql->uuid, us);
      /*printf("RECGENID SENDING %d : %llx %s\n", tblnum, genid, us);*/
#endif
        rc = osql_send_recordgenid(osql->host, osql->rqid, osql->uuid, genid,
                                   osql_nettype, osql->logsb);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s: error writting record to master in offload mode!\n",
                    __func__);
            return SQLITE_INTERNAL;
        }

        rc = bdb_temp_table_next(bdb_state, cur, bdberr);
    }

    if (rc == IX_PASTEOF || rc == IX_EMPTY) {
        rc = 0;
    } else {
        logmsg(LOGMSG_ERROR, "%s:%d bdb_temp_table_next failed rc=%d bdberr=%d\n",
                __func__, __LINE__, rc, *bdberr);
        /* fall-through */
    }

    return rc;
}

int osql_save_schemachange(struct sql_thread *thd,
                           struct schema_change_type *sc)
{
    struct sqlclntstate *clnt = thd->sqlclntstate;
    osqlstate_t *osql = &thd->sqlclntstate->osql;
    int rc = 0;
    int bdberr = 0;
    void *packed_sc_data = NULL;
    size_t packed_sc_data_len;
    int count = 0;

    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_SC_RESUME_AUTOCOMMIT) &&
        !clnt->in_client_trans)
        return 0;

    if (!osql->sc_tbl) {
        rc = osql_create_schemachange(thedb->bdb_env, thd->sqlclntstate,
                                      &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: failed to create sc rc=%d bdberr=%d\n",
                   __func__, rc, bdberr);
            return -1;
        }
    }

    if (!osql->sc_tbl || !osql->sc_cur) {
        logmsg(LOGMSG_ERROR, "%s: error getting sc table for \'%s\'\n",
               __func__, sc->table);
        return -1;
    }
    sc->rqid = osql->rqid;
    comdb2uuidcpy(sc->uuid, osql->uuid);

    if (pack_schema_change_type(sc, &packed_sc_data, &packed_sc_data_len)) {
        logmsg(LOGMSG_ERROR, "%s: error packing sc table for \'%s\'\n",
               __func__, sc->table);
        return -1;
    }
    if (clnt->ddl_tables)
        hash_info(clnt->ddl_tables, NULL, NULL, NULL, NULL, &count, NULL, NULL);
    rc = bdb_temp_table_put(thedb->bdb_env, osql->sc_tbl, &count, sizeof(int),
                            packed_sc_data, packed_sc_data_len, NULL, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: error saving sc table for \'%s\'\n", __func__,
               sc->table);
        return -1;
    }

    free(packed_sc_data);
    return 0;
}

static int process_local_shadtbl_sc(struct sqlclntstate *clnt, int *bdberr)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    bdb_state_type *bdb_state = thedb->bdb_env;
    struct temp_cursor *cur = osql->sc_cur;
    void *packed_sc_data = NULL;
    size_t packed_sc_data_len;

    if (!cur) {
        return 0;
    }

    rc = bdb_temp_table_first(bdb_state, cur, bdberr);
    if (rc == IX_EMPTY) return 0;
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "%s: bdb_temp_table_first failed rc=%d bdberr=%d\n", __func__,
               rc, *bdberr);
        return SQLITE_INTERNAL;
    }

    while (rc == 0) {
        struct schema_change_type *sc = NULL;
        packed_sc_data = bdb_temp_table_data(cur);
        packed_sc_data_len = bdb_temp_table_datasize(cur);

        sc = new_schemachange_type();
        if (!sc) {
            logmsg(LOGMSG_ERROR, "%s: ran out of memory\n", __func__);
            return -1;
        }
        if (unpack_schema_change_type(sc, packed_sc_data, packed_sc_data_len)) {
            logmsg(LOGMSG_ERROR, "%s: failed to unpack sc\n", __func__);
            return -1;
        }

        rc = osql_send_schemachange(osql->host, osql->rqid, osql->uuid, sc,
                                    NET_OSQL_BLOCK_RPL_UUID, osql->logsb);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s: error writting record to master in offload mode!\n",
                   __func__);
            return SQLITE_INTERNAL;
        }
        free_schema_change_type(sc);

        rc = bdb_temp_table_next(bdb_state, cur, bdberr);
    }

    if (rc == IX_PASTEOF || rc == IX_EMPTY) {
        rc = 0;
    } else {
        logmsg(LOGMSG_ERROR,
               "%s:%d bdb_temp_table_next failed rc=%d bdberr=%d\n", __func__,
               __LINE__, rc, *bdberr);
        /* fall-through */
    }

    return rc;
}

/**
 *  Check of a shadow table transaction has cached selectv records
 *
 */
int osql_shadtbl_has_selectv(struct sqlclntstate *clnt, int *bdberr)
{
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;

    if (osql->verify_tbl && osql->verify_cur) {
        rc = bdb_temp_table_first(thedb->bdb_env, osql->verify_cur, bdberr);
        if (rc < 0) {
            return rc;
        }
        if (rc == IX_FND) {
            /* found */
            return 1;
        }
    }
    /* here we get with rc<0 or 0(IX_FND) if break, or IX_EMTPY */
    if (rc == IX_EMPTY) {
        rc = 0;
    }
    return rc;
}

static int osql_create_temptbl(bdb_state_type *bdb_state,
                               struct temp_table **out_table,
                               struct temp_cursor **out_cursor, int *bdberr)
{
    struct temp_table *table;
    struct temp_cursor *cursor;

    table = bdb_temp_table_create(bdb_state, bdberr);
    if (!table) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_create failed, bderr=%d\n",
                __func__, *bdberr);
        return -1;
    }

    cursor = bdb_temp_table_cursor(bdb_state, table, NULL, bdberr);
    if (!cursor) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_cursor failed, bdberr=%d\n",
                __func__, *bdberr);
        bdb_temp_table_close(bdb_state, table, bdberr);
        return -1;
    }

    *out_table = table;
    *out_cursor = cursor;

    return 0;
}

static int osql_destroy_temptbl(bdb_state_type *bdb_state,
                                struct temp_table **table,
                                struct temp_cursor **cursor)
{
    int rc = 0;
    int bdberr = 0;

    if (*cursor) {
        rc = bdb_temp_table_close_cursor(bdb_state, *cursor, &bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: fail to close cursor bdberr=%d\n", __func__,
                    bdberr);
    }

    if (*table) {
        rc = bdb_temp_table_close(bdb_state, *table, &bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s: fail to bdberr=%d\n", __func__, bdberr);
    }

    *cursor = NULL;
    *table = NULL;

    return 0; /* not sure what value is returning rc at this point */
}

static int osql_create_verify(bdb_state_type *bdb_state,
                              struct sqlclntstate *clnt, int *bdberr)
{
    osqlstate_t *osql = &clnt->osql;
    return osql_create_temptbl(bdb_state, &osql->verify_tbl, &osql->verify_cur,
                               bdberr);
}

static int osql_destroy_verify(bdb_state_type *bdb_state,
                               struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    return osql_destroy_temptbl(bdb_state, &osql->verify_tbl,
                                &osql->verify_cur);
}

static int osql_create_schemachange(bdb_state_type *bdb_state,
                                    struct sqlclntstate *clnt, int *bdberr)
{
    osqlstate_t *osql = &clnt->osql;
    return osql_create_temptbl(bdb_state, &osql->sc_tbl, &osql->sc_cur, bdberr);
}

int osql_destroy_schemachange(bdb_state_type *bdb_state,
                              struct sqlclntstate *clnt)
{
    osqlstate_t *osql = &clnt->osql;
    return osql_destroy_temptbl(bdb_state, &osql->sc_tbl, &osql->sc_cur);
}

int osql_shadtbl_empty(struct sqlclntstate *clnt)
{
    return LIST_EMPTY(&clnt->osql.shadtbls) && !clnt->osql.verify_tbl &&
           !clnt->osql.sc_tbl;
}

int osql_shadtbl_usedb_only(struct sqlclntstate *clnt)
{
    int rc = 0;
    int bdberr = 0;
    struct temp_cursor *cur = NULL;
    shad_tbl_t *tbl = NULL;
    osqlstate_t *osql = &clnt->osql;
    unsigned long long genid = 0;
    char *data = NULL;
    int datalen = 0;
    if (LIST_EMPTY(&clnt->osql.shadtbls) && !clnt->osql.verify_tbl &&
        !clnt->osql.sc_tbl)
        return 1;
    LISTC_FOR_EACH(&osql->shadtbls, tbl, linkv)
    {
        cur =
            bdb_tran_deltbl_first(tbl->env->bdb_env, clnt->dbtran.shadow_tran,
                                  tbl->dbnum, &genid, &data, &datalen, &bdberr);
        if (cur)
            return 0;
        rc = bdb_temp_table_first(tbl->env->bdb_env, tbl->add_cur, &bdberr);
        if (rc != IX_EMPTY)
            return 0;
        rc = bdb_temp_table_first(tbl->env->bdb_env, tbl->upd_cur, &bdberr);
        if (rc != IX_EMPTY)
            return 0;
    }
    return 1;
}
