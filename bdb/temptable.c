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

#include <alloca.h>

#include <errno.h>
#include <pthread.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <unistd.h>
#include <assert.h>
#include <openssl/rand.h>

#include <build/db.h> /* berk db.h */
#include <net.h>
#include <sbuf2.h>
#include "bdb_int.h"
#include <list.h>
#include <plhash.h>

#ifdef _LINUX_SOURCE
#include <execinfo.h>
#endif
#include <logmsg.h>
#include <util.h>
#include "comdb2_atomic.h"

/* One of the difference between using b-tree and hash is that in using hash, we
 * get pointer of data in hash table,
 * but in case of B-Tree new data is allocated. So far haven't seen any
 * problems. */

#include "locks.h"
#include "bdb_int.h"

#ifdef __GLIBC__
extern int backtrace(void **, int);
extern void backtrace_symbols_fd(void *const *, int, int);
#else
#define backtrace(A, B) 1
#define backtrace_symbols_fd(A, B, C)
#endif

extern char *gbl_crypto;

struct hashobj {
    int len;
    unsigned char data[/*len*/];
};

unsigned int hashfunc(const void *key, int len)
{
    struct hashobj *o = (struct hashobj *)key;
    return hash_default_fixedwidth(o->data, o->len);
}

int hashcmpfunc(const void *key1, const void *key2, int len)
{
    struct hashobj *o1, *o2;
    o1 = (struct hashobj *)key1;
    o2 = (struct hashobj *)key2;
    int minlen = o1->len < o2->len ? o1->len : o2->len;
    int cmp = memcmp(o1->data, o2->data, minlen);
    if (cmp)
        return cmp;
    if (o1->len == o2->len)
        return 0;
    if (o1->len > o2->len)
        return 1;
    return -1;
}

struct temp_list_node {
    LINKC_T(struct temp_list_node) lnk;
    void *data;
};

/* code for SQL temp table support */
struct temp_cursor {
    DBC *cur;
    void *key;
    int keylen;
    void *data;
    int datalen;
    int valid;
    /* void *usermem; */
    struct temp_table *tbl;
    int curid;
    struct temp_list_node *list_cur;
    void *hash_cur;
    unsigned int hash_cur_buk;
    LINKC_T(struct temp_cursor) lnk;
};

enum { TEMP_TABLE_TYPE_BTREE, TEMP_TABLE_TYPE_HASH, TEMP_TABLE_TYPE_LIST };

struct temp_table {
    DB_ENV *dbenv_temp;

    int temp_table_type;
    DB *tmpdb; /* in-memory table */
    LISTC_T(struct temp_list_node) temp_tbl_list;
    hash_t *temp_hash_tbl;

    tmptbl_cmp cmpfunc;
    void *usermem;
    char filename[512];
    int tblid;
    unsigned long long rowid;

    int num_mem_entries;
    int max_mem_entries;
    LISTC_T(struct temp_cursor) cursors;
    void *next;
};

enum { TMPTBL_PRIORITY, TMPTBL_WAIT };

static int curid; /* for debug trace only */

#ifdef DEBUGMODE
extern void dbgtrace(int, const char *fmt, ...);
extern void dbghexdump(int, void *buf, size_t len);
#else
static void dbgtrace(int flag, const char *fmt, ...) {}
static void dbghexdump(int flag, void *buf, size_t len) {}
#endif

static int key_memcmp(void *usermem, int key1len, const void *key1, int key2len,
                      const void *key2);
static int temp_table_compare(DB *db, const DBT *dbt1, const DBT *dbt2);

/* refactored both insert and put code paths here */
static int bdb_temp_table_insert_put(bdb_state_type *, struct temp_table *,
                                     void *key, int keylen, void *data,
                                     int dtalen, int *bdberr);

void *bdb_temp_table_get_cur(struct temp_cursor *skippy) { return skippy->cur; }

static int histcmpfunc(const void *key1, const void *key2, int len)
{
    return !pthread_equal(*(pthread_t *)key1, *(pthread_t *)key2);
}

hash_t *bdb_temp_table_histhash_init(void)
{
    return hash_init_user((hashfunc_t *)hash_default_fixedwidth, histcmpfunc, 0,
                          sizeof(pthread_t));
}

static int bdb_hash_table_copy_to_temp_db(bdb_state_type *bdb_state,
                                          struct temp_table *tbl, int *bdberr)
{
    int rc = 0;
    int num_recs = 0;
    DBT dbt_key, dbt_data;
    bzero(&dbt_key, sizeof(DBT));
    bzero(&dbt_data, sizeof(DBT));
    struct temp_cursor *cur;
    void *hash_cur;
    unsigned int hash_cur_buk;
    char *data;

    /* copy the hash to a btree */
    data = hash_first(tbl->temp_hash_tbl, &hash_cur, &hash_cur_buk);
    while (data) {
        int keylen = *(int *)data;
        void *key = data + sizeof(int);
#ifdef _SUN_SOURCE
        int datalen;
        memcpy(&datalen, data + keylen + sizeof(int), sizeof(int));
#else
        int datalen = *(int *)(data + keylen + sizeof(int));
        ;
#endif
        void *d_data = data + keylen + 2 * sizeof(int);

        dbt_key.ulen = dbt_key.size = keylen;
        dbt_data.ulen = dbt_data.size = datalen;
        dbt_data.data = d_data;
        dbt_key.data = key;

        rc = tbl->tmpdb->put(tbl->tmpdb, NULL, &dbt_key, &dbt_data, 0);
        num_recs++;
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d put rc %d\n", __FILE__, __LINE__, rc);
            return rc;
        }
        data = hash_next(tbl->temp_hash_tbl, &hash_cur, &hash_cur_buk);
    }

    /* get rid of the hash */
    data = hash_first(tbl->temp_hash_tbl, &hash_cur, &hash_cur_buk);
    while (data) {
        free(data);
        data = hash_next(tbl->temp_hash_tbl, &hash_cur, &hash_cur_buk);
    }
    hash_clear(tbl->temp_hash_tbl);
    hash_free(tbl->temp_hash_tbl);
    tbl->temp_hash_tbl = hash_init_user(hashfunc, hashcmpfunc, 0, 0);

    /* its now a btree! */
    tbl->temp_table_type = TEMP_TABLE_TYPE_BTREE;

    /* Reset all the cursors for this table.
       For now don't care about position. */
    LISTC_FOR_EACH(&tbl->cursors, cur, lnk)
    {
        rc = tbl->tmpdb->cursor(tbl->tmpdb, NULL, &cur->cur, 0);
        if (rc) {
            cur->cur = NULL;
            /* not sure it's safe to proceed after this point, actually */
            logmsg(LOGMSG_ERROR, "%s:%d cursor rc %d\n", __FILE__, __LINE__, rc);
            goto done;
        }

        /* New cursor does not point to any data */
        cur->key = cur->data = NULL;
        cur->keylen = cur->datalen = 0;
    }

done:
    return rc;
}

static int bdb_temp_table_init_temp_db(bdb_state_type *bdb_state,
                                       struct temp_table *tbl, int *bdberr)
{
    DB *db;
    int rc;

    if (tbl->tmpdb) {
        rc = tbl->tmpdb->close(tbl->tmpdb, 0);
        if (rc) {
            *bdberr = rc;
            rc = -1;
            logmsg(LOGMSG_ERROR, "%s:%d close rc %d\n", __FILE__, __LINE__, rc);
            tbl->tmpdb = NULL;
            goto done;
        }

        // set to NULL all cursors that this table has open
        struct temp_cursor *cur;
        LISTC_FOR_EACH(&tbl->cursors, cur, lnk) { cur->cur = NULL; }
    }

    rc = db_create(&db, tbl->dbenv_temp, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d bdb_temp_table_init_real_db failed rc %d\n", __FILE__,
               __LINE__, rc);
        *bdberr = rc;
        goto done;
    }
    if (gbl_crypto) {
        if ((rc = db->set_flags(db, DB_ENCRYPT)) != 0) {
            fprintf(stderr, "error enabling crypto for temp table\n");
            *bdberr = rc;
            goto done;
        }
    }
    db->set_bt_compare(db, temp_table_compare);
    db->app_private = tbl;
    db->set_pagesize(db, 65536);

    rc = db->open(db, NULL, NULL, NULL, DB_BTREE,
                  DB_CREATE | DB_TRUNCATE | DB_TEMPTABLE, 0666);
    if (rc) {
        logmsg(LOGMSG_ERROR, "bdb_temp_table_init_temp_db rc %d\n", rc);
        *bdberr = rc;
        goto done;
    }

    tbl->tmpdb = db;
    /* Start with rowid 2 */
    tbl->rowid = 2;

    tbl->num_mem_entries = 0;

done:
    return rc;
}

static int bdb_temp_table_env_close(bdb_state_type *bdb_state,
                                    struct temp_table *tbl, int *bdberr)
{
    int rc;

    if (tbl->tmpdb) {
        rc = tbl->tmpdb->close(tbl->tmpdb, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d close rc %d\n", __FILE__, __LINE__, rc);
            *bdberr = rc;
            return -1;
        }
        tbl->tmpdb = NULL;
    }
    rc = tbl->dbenv_temp->close(tbl->dbenv_temp, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: failed to close dbenv_temp rc=%d\n", __func__, rc);
        *bdberr = rc;
        return -1;
    }
    *bdberr = 0;
    return 0;
}

pthread_key_t current_sql_query_key;
int gbl_debug_temptables = 0;

static struct temp_table *bdb_temp_table_create_main(bdb_state_type *bdb_state,
                                                     int *bdberr)
{
    int rc;
    struct temp_table *tbl;
    bdb_state_type *parent;
    int id;
    DB_ENV *dbenv_temp;
    unsigned int gb = 0, bytes = 0;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    tbl = malloc(sizeof(struct temp_table));
    tbl->next = NULL;
    tbl->tmpdb = NULL;
    tbl->cmpfunc = key_memcmp;

    rc = db_env_create(&dbenv_temp, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "couldnt create temp table env\n");
        free(tbl);
        tbl = NULL;
        goto done;
    }

    if (gbl_crypto) {
        // generate random password for temp tables
        char passwd[64]; passwd[0] = 0;
        while (passwd[0] == 0) {
            RAND_bytes((unsigned char *)passwd, 63);
        }
        passwd[63] = 0;
        if ((rc = dbenv_temp->set_encrypt(dbenv_temp, passwd,
                                          DB_ENCRYPT_AES)) != 0) {
            fprintf(stderr, "%s set_encrypt rc:%d\n", __func__, rc);
            free(tbl);
            tbl = NULL;
            goto done;
        }
        memset(passwd, 0xff, sizeof(passwd));
    }

    rc = dbenv_temp->set_is_tmp_tbl(dbenv_temp, 1);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "couldnt set property is_tmp_tbl\n");
        free(tbl);
        tbl = NULL;
        goto done;
    }

    bytes = bdb_state->attr->temptable_cachesz;

    /* 512k minimim cache */
    if (bytes < 524288)
        bytes = 524288;

    rc = dbenv_temp->set_tmp_dir(dbenv_temp, parent->tmpdir);
    if (rc) {
        logmsg(LOGMSG_ERROR, "can't set temp table environment's temp directory");
        /* continue anyway */
    }

    rc = dbenv_temp->set_cachesize(dbenv_temp, gb, bytes, 1);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "invalid set_cache_size call: gb %d bytes %d\n", gb, bytes);
        free(tbl);
        *bdberr = rc;
        tbl = NULL;
        goto done;
    }

    rc = dbenv_temp->set_tmp_dir(dbenv_temp, parent->tmpdir);
    if (rc) {
        logmsg(LOGMSG_ERROR, "can't set temp table environment's temp directory");
        /* continue anyway */
    }

    rc = dbenv_temp->open(dbenv_temp, parent->tmpdir,
                          DB_INIT_MPOOL | DB_CREATE | DB_PRIVATE, 0666);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "couldnt open temp table env\n");
        free(tbl);
        tbl = NULL;
        goto done;
    }

    tbl->dbenv_temp = dbenv_temp;

    if (gbl_temptable_pool_capacity == 0) {
        Pthread_mutex_lock(&parent->temp_list_lock);
        id = parent->temp_table_id++;
        parent->num_temp_tables++;
        Pthread_mutex_unlock(&parent->temp_list_lock);
    } else {
        id = parent->temp_table_id++;
        parent->num_temp_tables++;
    }
    snprintf(tbl->filename, sizeof(tbl->filename), "%s/_temp_%d.db",
             parent->tmpdir, id);
    tbl->tblid = id;

    listc_init(&tbl->cursors, offsetof(struct temp_cursor, lnk));

    tbl->max_mem_entries = bdb_state->attr->temptable_mem_threshold;

    rc = bdb_temp_table_init_temp_db(bdb_state, tbl, bdberr);
    if (rc) {
        free(tbl);
        tbl = NULL;
        goto done;
    }

    listc_init(&tbl->temp_tbl_list, offsetof(struct temp_list_node, lnk));

    tbl->temp_hash_tbl = hash_init_user(hashfunc, hashcmpfunc, 0, 0);

#ifdef _LINUX_SOURCE
    if (gbl_debug_temptables) {
        char *sql;
        sql = pthread_getspecific(current_sql_query_key);
        if (sql)
            logmsg(LOGMSG_USER, "creating a temp table object %p: %s\n", tbl, sql);
        else {
            int nframes;
            void *stack[100];
            logmsg(LOGMSG_USER, "creating a temp table object %p: ", tbl);
            nframes = backtrace(stack, 100);
            for (int i = 0; i < nframes; i++)
               logmsg(LOGMSG_USER, "%p ", stack[i]);
           logmsg(LOGMSG_USER, "\n");
        }
    }
#endif

done:
    dbgtrace(3, "temp_table_create(%s) = %d", tbl ? tbl->filename : "failed",
             tbl ? tbl->tblid : -1);
    return tbl;
}

int bdb_temp_table_create_pool_wrapper(void **tblp, void *bdb_state_arg)
{
    int bdberr = 0;
    *tblp =
        bdb_temp_table_create_main((bdb_state_type *)bdb_state_arg, &bdberr);
    return bdberr;
}

/*
   pull from a list of these things kept on parent bdb_state.
   if list empty, run create code.
   reset flags on item pulled from list
*/
static struct temp_table *bdb_temp_table_create_type(bdb_state_type *bdb_state,
                                                     int temp_table_type,
                                                     int *bdberr)
{
    struct temp_table *table = NULL;
    int rc;

    /* needed by temptable pool */
    extern pthread_key_t query_info_key;
    void *sql_thread;
    int action;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (gbl_temptable_pool_capacity == 0) {
        /* temptable pool not enabled. fall back to linked list cache. */
        Pthread_mutex_lock(&(bdb_state->temp_list_lock));
        if (bdb_state->temp_list) {
            table = bdb_state->temp_list;
            bdb_state->temp_list = table->next;
            Pthread_mutex_unlock(&(bdb_state->temp_list_lock));
        } else {
            Pthread_mutex_unlock(&(bdb_state->temp_list_lock));
            table = bdb_temp_table_create_main(bdb_state, bdberr);
            if (!table)
                return NULL;
        }
    } else {
        rc = 0;
        action = TMPTBL_WAIT;
        sql_thread = pthread_getspecific(query_info_key);

        if (sql_thread == NULL) {
            /*
            ** a sql thread may be waiting for the completion of a non-sql
            *thread (tag, for example).
            ** so don't block non-sql.
            */
            action = TMPTBL_PRIORITY;
        } else if (bdb_state->haspriosqlthr) {
            /* there is a priority thread. there might be a dirty read here but
             * wouldn't matter */
            action = pthread_equal(pthread_self(), bdb_state->priosqlthr)
                         ? TMPTBL_PRIORITY
                         : TMPTBL_WAIT;
        } else if (!comdb2_objpool_available(bdb_state->temp_table_pool)) {
            rc = pthread_mutex_lock(&bdb_state->temp_list_lock);
            if (rc == 0) {
                if (!bdb_state->haspriosqlthr) {
                    bdb_state->haspriosqlthr = 1;
                    bdb_state->priosqlthr = pthread_self();
                    action = TMPTBL_PRIORITY;
                }
                rc = pthread_mutex_unlock(&bdb_state->temp_list_lock);
            }
        }

        if (rc != 0)
            return NULL;

        switch (action) {
        case TMPTBL_PRIORITY:
            rc = comdb2_objpool_forcedborrow(bdb_state->temp_table_pool,
                                             (void **)&table);
            break;
        case TMPTBL_WAIT:
            rc = comdb2_objpool_borrow(bdb_state->temp_table_pool,
                                       (void **)&table);
            break;
        }
    }

    table->num_mem_entries = 0;
    table->cmpfunc = key_memcmp;
    table->temp_table_type = temp_table_type;

    return table;
}

struct temp_table *bdb_temp_table_create_flags(bdb_state_type *bdb_state,
                                               int flags, int *bdberr)
{
    int temptype;

    temptype = TEMP_TABLE_TYPE_BTREE;

    return bdb_temp_table_create_type(bdb_state, temptype, bdberr);
}

struct temp_table *bdb_temp_table_create(bdb_state_type *bdb_state, int *bdberr)
{
    return bdb_temp_table_create_type(bdb_state, TEMP_TABLE_TYPE_BTREE, bdberr);
}

struct temp_table *bdb_temp_list_create(bdb_state_type *bdb_state, int *bdberr)
{
    return bdb_temp_table_create_type(bdb_state, TEMP_TABLE_TYPE_LIST, bdberr);
}

struct temp_table *bdb_temp_hashtable_create(bdb_state_type *bdb_state,
                                             int *bdberr)
{
    return bdb_temp_table_create_type(bdb_state, TEMP_TABLE_TYPE_HASH, bdberr);
}

struct temp_cursor *bdb_temp_table_cursor(bdb_state_type *bdb_state,
                                          struct temp_table *tbl, void *usermem,
                                          int *bdberr)
{
    struct temp_cursor *cur;
    int rc;

    cur = calloc(1, sizeof(struct temp_cursor));
    /* set up compare routine and callback */
    cur->tbl = tbl;
    tbl->usermem = usermem;
    cur->curid = curid++;
    cur->cur = NULL;

    switch (tbl->temp_table_type) {

    case TEMP_TABLE_TYPE_LIST:
        cur->list_cur = NULL;
        rc = 0;
        break;

    case TEMP_TABLE_TYPE_HASH:
        cur->hash_cur = NULL;
        cur->hash_cur_buk = 0;
        rc = 0;
        break;

    case TEMP_TABLE_TYPE_BTREE:
        rc = tbl->tmpdb->cursor(tbl->tmpdb, NULL, &cur->cur, 0);
        break;
    }

    if (rc) {
        *bdberr = rc;
        free(cur);
        cur = NULL;
        goto done;
    }

#if 0
   printf("%p Nulling %p\n", cur, cur->key);
#endif
    cur->key = NULL;
    cur->data = NULL;
/*pthread_setspecific(tbl->curkey, cur);*/
done:
    if (cur) {
        listc_abl(&tbl->cursors, cur);
    }

    dbgtrace(3, "temp_table_cursor(table %d) = %d", tbl->tblid,
             cur ? cur->curid : -1);
    return cur;
}

int bdb_temp_table_insert(bdb_state_type *bdb_state, struct temp_cursor *cur,
                          void *key, int keylen, void *data, int dtalen,
                          int *bdberr)
{
    DBT dkey, ddata;
    struct temp_table *tbl = cur->tbl;

    int rc = bdb_temp_table_insert_put(bdb_state, tbl, key, keylen, data,
                                       dtalen, bdberr);
    if (rc <= 0)
        goto done;

    /*pthread_setspecific(cur->tbl->curkey, cur);*/
    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));
    dkey.flags = ddata.flags = DB_DBT_USERMEM;
    dkey.ulen = dkey.size = keylen;
    ddata.ulen = ddata.size = dtalen;
    ddata.data = data;
    dkey.data = key;

    assert(cur->cur != NULL);
    rc = cur->cur->c_put(cur->cur, &dkey, &ddata, DB_KEYFIRST);
    if (rc && rc != DB_KEYEXIST) {
        *bdberr = rc;
        /*free(dkey.data);*/ /*... some routines pass in a stack variable
                               for key, so this would be bad! */
        rc = -1;
        goto done;
    }

done:
    dbghexdump(3, key, keylen);
    dbgtrace(3, "temp_table_insert(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

int bdb_temp_table_update(bdb_state_type *bdb_state, struct temp_cursor *cur,
                          void *key, int keylen, void *data, int dtalen,
                          int *bdberr)
{
    DBT dkey, ddata;
    int rc = 0;

    if (cur->tbl->temp_table_type != TEMP_TABLE_TYPE_BTREE) {
        logmsg(LOGMSG_ERROR, "bdb_temp_table_update operation "
                        "only supported for btree.\n");
        return -1;
    }

    /*pthread_setspecific(cur->tbl->curkey, cur);*/

    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));
    dkey.flags = ddata.flags = DB_DBT_USERMEM;
    dkey.ulen = dkey.size = keylen;
    ddata.ulen = ddata.size = dtalen;
    ddata.data = data;
    dkey.data = key;
    /*
      printf( "Updating data %p %d key %p %d\n",
            ddata.data, ddata.size, dkey.data, dkey.size);
     */

    assert(cur->cur != NULL);
    rc = cur->cur->c_put(cur->cur, &dkey, &ddata, DB_CURRENT);
    if (rc) {
        *bdberr = rc;
        rc = -1;
    }

    dbghexdump(3, key, keylen);
    dbgtrace(3, "temp_table_update(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

unsigned long long bdb_temp_table_new_rowid(struct temp_table *tbl)
{
    return ++tbl->rowid;
}

int bdb_temp_table_put(bdb_state_type *bdb_state, struct temp_table *tbl,
                       void *key, int keylen, void *data, int dtalen,
                       void *unpacked, int *bdberr)
{
    DBT dkey, ddata;

    int rc = bdb_temp_table_insert_put(bdb_state, tbl, key, keylen, data,
                                       dtalen, bdberr);
    if (rc <= 0)
        goto done;

    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));
    dkey.flags = ddata.flags = DB_DBT_USERMEM;
    dkey.ulen = dkey.size = keylen;
    ddata.ulen = ddata.size = dtalen;
    ddata.data = data;
    dkey.data = key;
    dkey.app_data = unpacked;

    rc = tbl->tmpdb->put(tbl->tmpdb, NULL, &dkey, &ddata, 0);
    if (rc) {
        *bdberr = rc;
        rc = -1;
        goto done;
    }

done:
    dbghexdump(3, key, keylen);
    dbgtrace(3, "temp_table_insert = %d\n", rc);
    return rc;
}

static int bdb_temp_table_first_last(bdb_state_type *bdb_state,
                                     struct temp_cursor *cur, int *bdberr,
                                     int how)
{
    DBT dkey, ddata;
    int rc;

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_LIST) {
        cur->valid = 0;
        if (how != DB_FIRST) {
            logmsg(LOGMSG_ERROR, "bdb_temp_table_first_last operation not supported "
                            "for temp list.\n");
            return -1;
        }
        if (listc_size(&(cur->tbl->temp_tbl_list)) == 0) {
            return IX_EMPTY;
        }
        cur->list_cur = cur->tbl->temp_tbl_list.top;
        cur->data = cur->list_cur->data;
        cur->valid = 1;
        return 0;
    }

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_HASH) {
        cur->valid = 0;
        char *data;
        if (how != DB_FIRST) {
            logmsg(LOGMSG_ERROR, "bdb_temp_table_first_last operation not supported "
                            "for temp list.\n");
            return -1;
        }

        data = hash_first(cur->tbl->temp_hash_tbl, &cur->hash_cur,
                          &cur->hash_cur_buk);
        if (data) {
            cur->keylen = *(int *)data;
            cur->key = data + sizeof(int);
            cur->datalen = *(int *)(data + cur->keylen + sizeof(int));
            ;
            cur->data = data + cur->keylen + 2 * sizeof(int);
            cur->valid = 1;
        } else {
            return IX_EMPTY;
        }
        return 0;
    }

    /* if cursor was deleted, need to reopen */
    if (cur->cur == NULL) {
        int rc = cur->tbl->tmpdb->cursor(cur->tbl->tmpdb, NULL, &cur->cur, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: cursor create returned rc=%d\n", __func__, rc);
        }
    }

    /*pthread_setspecific(cur->tbl->curkey, cur);*/

    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));
    if (cur->key) {
#if 0
      printf("%p Freeing %p\n", cur, cur->key);
#endif
        free(cur->key);
        cur->key = NULL;
    }
    if (cur->data) {
        free(cur->data);
        cur->data = NULL;
    }
    cur->valid = 0;
    dkey.flags = ddata.flags = DB_DBT_MALLOC;
    rc = cur->cur->c_get(cur->cur, &dkey, &ddata, how);
    if (rc == DB_NOTFOUND)
        return IX_EMPTY;
    else if (rc) {
        *bdberr = rc;
        return -1;
    }
    cur->valid = 1;
    cur->key = dkey.data;
#if 0
   printf("%p Setting %p\n", cur, cur->key);
#endif
    cur->keylen = dkey.size;
    cur->data = ddata.data;
    cur->datalen = ddata.size;
    rc = 0;
    return rc;
}

static int bdb_temp_table_next_prev_norewind(bdb_state_type *bdb_state,
                                             struct temp_cursor *cur,
                                             int *bdberr, int how)
{
    DBT ddata, dkey;
    int rc;

    if (!cur->valid)
        return IX_PASTEOF;
    cur->valid = 0;

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_LIST) {
        if (how != DB_NEXT) {
            logmsg(LOGMSG_ERROR, "bdb_temp_table_next_prev_norewind operation not "
                            "supported for temp list.\n");
            return -1;
        }
        cur->list_cur = cur->list_cur->lnk.next;
        if (cur->list_cur == 0) {
            return IX_PASTEOF;
        }
        cur->data = cur->list_cur->data;
        cur->valid = 1;
        return 0;
    }

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_HASH) {
        cur->valid = 0;
        if (how != DB_NEXT) {
            logmsg(LOGMSG_ERROR, "bdb_temp_table_first_last operation not supported "
                            "for temp list.\n");
            return -1;
        }
        char *data = hash_next(cur->tbl->temp_hash_tbl, &cur->hash_cur,
                               &cur->hash_cur_buk);
        if (data) {
            cur->keylen = *(int *)data;
            cur->key = data + sizeof(int);
            cur->datalen = *(int *)(data + cur->keylen + sizeof(int));
            ;
            cur->data = data + cur->keylen + 2 * sizeof(int);
            cur->valid = 1;
        } else {
            return IX_PASTEOF;
        }
        return 0;
    }

    /* if cursor was deleted, need to reopen */
    if (cur->cur == NULL) {
        int rc = cur->tbl->tmpdb->cursor(cur->tbl->tmpdb, NULL, &cur->cur, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s: cursor create returned rc=%d\n", __func__, rc);
        }
    }

    /*pthread_setspecific(cur->tbl->curkey, cur);*/

    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));

    ddata.flags = dkey.flags = DB_DBT_MALLOC;
    rc = cur->cur->c_get(cur->cur, &dkey, &ddata, how);
    if (rc == DB_NOTFOUND)
        return IX_PASTEOF;
    else if (rc) {
        *bdberr = rc;
        return -1;
    }
    if (cur->key) {
#if 0
       printf("%p Freeing %p\n", cur, cur->key);
#endif
        free(cur->key);
        cur->key = NULL;
    }
    if (cur->data) {
        free(cur->data);
        cur->data = NULL;
    }
    cur->valid = 1;
    cur->data = ddata.data;
    cur->datalen = ddata.size;
    cur->key = dkey.data;
#if 0
    printf("%p Setting %p\n", cur, cur->key);
#endif
    cur->keylen = dkey.size;
    return IX_FND;
}

inline static int bdb_temp_table_next_prev(bdb_state_type *bdb_state,
                                    struct temp_cursor *cur, int *bdberr,
                                    int how)
{

    /*pthread_setspecific(cur->tbl->curkey, cur);*/
    if (!cur->valid) {
        if (how == DB_NEXT)
            return bdb_temp_table_first_last(bdb_state, cur, bdberr, DB_FIRST);
        else
            return bdb_temp_table_first_last(bdb_state, cur, bdberr, DB_LAST);
    }

    return bdb_temp_table_next_prev_norewind(bdb_state, cur, bdberr, how);
}

int bdb_temp_table_first(bdb_state_type *bdb_state, struct temp_cursor *cur,
                         int *bdberr)
{
    int rc;
    rc = bdb_temp_table_first_last(bdb_state, cur, bdberr, DB_FIRST);
    dbgtrace(3, "temp_table_first(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

int bdb_temp_table_last(bdb_state_type *bdb_state, struct temp_cursor *cur,
                        int *bdberr)
{
    int rc;
    rc = bdb_temp_table_first_last(bdb_state, cur, bdberr, DB_LAST);
    dbgtrace(3, "temp_table_last(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

int bdb_temp_table_next(bdb_state_type *bdb_state, struct temp_cursor *cur,
                        int *bdberr)
{
    int rc;
    rc = bdb_temp_table_next_prev(bdb_state, cur, bdberr, DB_NEXT);
    dbgtrace(3, "temp_table_next(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

int bdb_temp_table_prev(bdb_state_type *bdb_state, struct temp_cursor *cur,
                        int *bdberr)
{
    int rc;
    rc = bdb_temp_table_next_prev(bdb_state, cur, bdberr, DB_PREV);
    dbgtrace(3, "temp_table_prev(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

int bdb_temp_table_next_norewind(bdb_state_type *bdb_state,
                                 struct temp_cursor *cur, int *bdberr)
{
    int rc;
    rc = bdb_temp_table_next_prev_norewind(bdb_state, cur, bdberr, DB_NEXT);
    dbgtrace(3, "temp_table_next(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

int bdb_temp_table_prev_norewind(bdb_state_type *bdb_state,
                                 struct temp_cursor *cur, int *bdberr)
{
    int rc;
    rc = bdb_temp_table_next_prev_norewind(bdb_state, cur, bdberr, DB_PREV);
    dbgtrace(3, "temp_table_prev(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

int bdb_temp_table_keysize(struct temp_cursor *cur)
{
    int rc;
    if (!cur->valid) {
        rc = -1;
        goto done;
    }
    rc = cur->keylen;
done:
    dbgtrace(3, "temp_table_keysize(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

int bdb_temp_table_datasize(struct temp_cursor *cur)
{
    int rc;
    if (!cur->valid) {
        rc = -1;
        goto done;
    }
    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_LIST) {
        logmsg(LOGMSG_ERROR, 
                "bdb_temp_table_datasize operation not supported for temp list.\n");
        return -1;
    }
    rc = cur->datalen;
done:
    dbgtrace(3, "temp_table_datasize(cursor %d) = %d\n", cur->curid, rc);
    return rc;
}

void *bdb_temp_table_key(struct temp_cursor *cur)
{
    void *rc;
    if (!cur->valid) {
        rc = NULL;
        goto done;
    }
    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_LIST) {
        logmsg(LOGMSG_ERROR, 
                "bdb_temp_table_key operation not supported for temp list.\n");
        return (void *)-1;
    }

#if 0
    printf( "%p Retrieving key %p\n", cur, cur->key);
#endif
    rc = cur->key;
done:
    if (rc)
        dbghexdump(3, cur->key, cur->keylen);
    dbgtrace(3, "temp_table_key(cursor %d) = %p\n", cur->curid, rc);
    return rc;
}

void *bdb_temp_table_data(struct temp_cursor *cur)
{
    void *rc;
    if (!cur->valid) {
        rc = NULL;
        goto done;
    }
    rc = cur->data;
done:
    if (rc)
        dbghexdump(3, cur->data, cur->datalen);
    dbgtrace(3, "temp_table_data(cursor %d) = %p\n", cur->curid, rc);
    return rc;
}

/* berkeley extension/kludge */
extern long long __db_filesz(DB *dbp);

static int bdb_temp_table_truncate_temp_db(bdb_state_type *bdb_state,
                                           struct temp_table *tbl, int *bdberr)
{
    int rc, rc2;
    DBC *dbcur;
    DBT dbt_key, dbt_data;
    DB *db;

    if (tbl->num_mem_entries == 0) {
        return 0;
    }

    /*fprintf(stderr, "bdb_temp_table_truncate_temp_db\n");*/

    bzero(&dbt_key, sizeof(DBT));
    bzero(&dbt_data, sizeof(DBT));
    dbt_key.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;
    dbt_data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    rc = tbl->tmpdb->cursor(tbl->tmpdb, NULL, &dbcur, 0);
    if (rc != 0) {
        logmsg(LOGMSG_FATAL, "bdb_temp_table_init_temp_db couldnt get cursor\n");
        exit(1);
    }

    rc = dbcur->c_get(dbcur, &dbt_key, &dbt_data, DB_FIRST);
    if (rc == 0) {
        /*fprintf(stderr, "deleting\n");*/
        rc2 = dbcur->c_del(dbcur, 0);
    }

    while (rc == 0) {
        rc = dbcur->c_get(dbcur, &dbt_key, &dbt_data, DB_NEXT);
        rc2 = dbcur->c_del(dbcur, 0);
        /*fprintf(stderr, "deleting\n");*/
    }

    dbcur->c_close(dbcur);

    rc = 0;

done:
    return rc;
}

int bdb_temp_table_truncate(bdb_state_type *bdb_state, struct temp_table *tbl,
                            int *bdberr)
{
    if (tbl == NULL)
        return 0;
    DB *db = NULL;
    int rc = 0;
    unsigned int discarded = 0;

    switch (tbl->temp_table_type) {
    case TEMP_TABLE_TYPE_LIST: {
        struct temp_list_node *c_node = NULL;
        do {
            c_node = (struct temp_list_node *)listc_rtl(&(tbl->temp_tbl_list));
            if (c_node) {
                if (c_node->data)
                    free(c_node->data);
                free(c_node);
            }
        } while (c_node);
    } break;

    case TEMP_TABLE_TYPE_HASH:
        if (tbl->temp_hash_tbl) {
            void *hash_cur;
            unsigned int hash_cur_buk;
            char *data;

            data = hash_first(tbl->temp_hash_tbl, &hash_cur, &hash_cur_buk);
            while (data) {
                free(data);
                data = hash_next(tbl->temp_hash_tbl, &hash_cur, &hash_cur_buk);
            }
            hash_clear(tbl->temp_hash_tbl);
            hash_free(tbl->temp_hash_tbl);
            tbl->temp_hash_tbl = hash_init_user(hashfunc, hashcmpfunc, 0, 0);
        }
        break;

    case TEMP_TABLE_TYPE_BTREE:

        if (tbl->num_mem_entries < 100)
            rc = bdb_temp_table_truncate_temp_db(bdb_state, tbl, bdberr);
        else
            rc = bdb_temp_table_init_temp_db(bdb_state, tbl, bdberr);

        if (rc) {
            *bdberr = rc;
            rc = -1;
            goto done;
        }
        break;
    }

done:

    dbgtrace(3, "temp_table_truncate() = %d", rc);
    return rc;
}

/* XXX todo - call bdb_temp_table_truncate() and put on a list at parent
   bdb_state */
int bdb_temp_table_close(bdb_state_type *bdb_state, struct temp_table *tbl,
                         int *bdberr)
{
    struct temp_cursor *cur, *temp;
    DB *db;
    DB_MPOOL_STAT *tmp;
    int rc;

    extern pthread_key_t query_info_key;

    if (tbl == NULL)
        return 0;

    rc = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    LISTC_FOR_EACH_SAFE(&tbl->cursors, cur, temp, lnk)
    {
        if ((rc = bdb_temp_table_close_cursor(bdb_state, cur, bdberr)) != 0)
            return rc;
    }

    rc = bdb_temp_table_truncate(bdb_state, tbl, bdberr);

    Pthread_mutex_lock(&(bdb_state->temp_list_lock));

    if ((tbl->dbenv_temp->memp_stat(tbl->dbenv_temp, &tmp, NULL,
                                    DB_STAT_CLEAR)) == 0) {
        bdb_state->temp_stats->st_gbytes += tmp->st_gbytes;
        bdb_state->temp_stats->st_bytes += tmp->st_bytes;
        bdb_state->temp_stats->st_ncache += tmp->st_ncache;
        bdb_state->temp_stats->st_regsize += tmp->st_regsize;
        bdb_state->temp_stats->st_map += tmp->st_map;
        bdb_state->temp_stats->st_cache_hit += tmp->st_cache_hit;
        bdb_state->temp_stats->st_cache_miss += tmp->st_cache_miss;
        bdb_state->temp_stats->st_cache_ihit += tmp->st_cache_ihit;
        bdb_state->temp_stats->st_cache_imiss += tmp->st_cache_imiss;
        bdb_state->temp_stats->st_cache_lhit += tmp->st_cache_lhit;
        bdb_state->temp_stats->st_cache_lmiss += tmp->st_cache_lmiss;
        bdb_state->temp_stats->st_page_create += tmp->st_page_create;
        bdb_state->temp_stats->st_page_pf_in += tmp->st_page_pf_in;
        bdb_state->temp_stats->st_page_in += tmp->st_page_in;
        bdb_state->temp_stats->st_page_out += tmp->st_page_out;
        bdb_state->temp_stats->st_ro_merges += tmp->st_ro_merges;
        bdb_state->temp_stats->st_rw_merges += tmp->st_rw_merges;
        bdb_state->temp_stats->st_ro_evict += tmp->st_ro_evict;
        bdb_state->temp_stats->st_rw_evict += tmp->st_rw_evict;
        bdb_state->temp_stats->st_pf_evict += tmp->st_pf_evict;
        bdb_state->temp_stats->st_rw_evict_skip += tmp->st_rw_evict_skip;
        bdb_state->temp_stats->st_page_trickle += tmp->st_page_trickle;
        bdb_state->temp_stats->st_pages += tmp->st_pages;
        ATOMIC_ADD(bdb_state->temp_stats->st_page_dirty, tmp->st_page_dirty);
        bdb_state->temp_stats->st_page_clean += tmp->st_page_clean;
        bdb_state->temp_stats->st_hash_buckets += tmp->st_hash_buckets;
        bdb_state->temp_stats->st_hash_searches += tmp->st_hash_searches;
        bdb_state->temp_stats->st_hash_longest += tmp->st_hash_longest;
        bdb_state->temp_stats->st_hash_examined += tmp->st_hash_examined;
        bdb_state->temp_stats->st_region_nowait += tmp->st_region_nowait;
        bdb_state->temp_stats->st_region_wait += tmp->st_region_wait;
        bdb_state->temp_stats->st_alloc += tmp->st_alloc;
        bdb_state->temp_stats->st_alloc_buckets += tmp->st_alloc_buckets;
        bdb_state->temp_stats->st_alloc_max_buckets +=
            tmp->st_alloc_max_buckets;
        bdb_state->temp_stats->st_alloc_pages += tmp->st_alloc_pages;
        bdb_state->temp_stats->st_alloc_max_pages += tmp->st_alloc_max_pages;
        bdb_state->temp_stats->st_ckp_pages_sync += tmp->st_ckp_pages_sync;
        bdb_state->temp_stats->st_ckp_pages_skip += tmp->st_ckp_pages_skip;

        free(tmp);
    }

    if (gbl_temptable_pool_capacity == 0) {
        tbl->next = bdb_state->temp_list;
        bdb_state->temp_list = tbl;
    }

    Pthread_mutex_unlock(&(bdb_state->temp_list_lock));

    if (gbl_temptable_pool_capacity > 0) {
        rc = comdb2_objpool_return(bdb_state->temp_table_pool, tbl);
        if (rc == 0) {
            if (bdb_state->haspriosqlthr &&
                pthread_equal(pthread_self(), bdb_state->priosqlthr)) {
                Pthread_mutex_lock(&(bdb_state->temp_list_lock));
                if (bdb_state->haspriosqlthr &&
                    pthread_equal(pthread_self(), bdb_state->priosqlthr))
                    bdb_state->haspriosqlthr = 0;
                Pthread_mutex_unlock(&(bdb_state->temp_list_lock));
            }
        }
    }

done:
    dbgtrace(3, "temp_table_close() = %d %s", rc, db_strerror(rc));
    return rc;
}

int bdb_temp_table_destroy_lru(struct temp_table *tbl,
                               bdb_state_type *bdb_state, int *last,
                               int *bdberr)
{
    DB *db;
    DB_MPOOL_STAT *tmp;
    int rc;

    rc = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    /* unlink */
    Pthread_mutex_lock(&(bdb_state->temp_list_lock));

    if (tbl == NULL)
        tbl = bdb_state->temp_list;
    if (!tbl) {
        *last = 1;
        Pthread_mutex_unlock(&(bdb_state->temp_list_lock));
        return 0;
    }
    bdb_state->temp_list = tbl->next;
    *last = 0;

    if ((tbl->dbenv_temp->memp_stat(tbl->dbenv_temp, &tmp, NULL,
                                    DB_STAT_CLEAR)) == 0) {
        bdb_state->temp_stats->st_gbytes += tmp->st_gbytes;
        bdb_state->temp_stats->st_bytes += tmp->st_bytes;
        bdb_state->temp_stats->st_ncache += tmp->st_ncache;
        bdb_state->temp_stats->st_regsize += tmp->st_regsize;
        bdb_state->temp_stats->st_map += tmp->st_map;
        bdb_state->temp_stats->st_cache_hit += tmp->st_cache_hit;
        bdb_state->temp_stats->st_cache_miss += tmp->st_cache_miss;
        bdb_state->temp_stats->st_cache_ihit += tmp->st_cache_ihit;
        bdb_state->temp_stats->st_cache_imiss += tmp->st_cache_imiss;
        bdb_state->temp_stats->st_cache_lhit += tmp->st_cache_lhit;
        bdb_state->temp_stats->st_cache_lmiss += tmp->st_cache_lmiss;
        bdb_state->temp_stats->st_page_create += tmp->st_page_create;
        bdb_state->temp_stats->st_page_pf_in += tmp->st_page_pf_in;
        bdb_state->temp_stats->st_page_in += tmp->st_page_in;
        bdb_state->temp_stats->st_page_out += tmp->st_page_out;
        bdb_state->temp_stats->st_ro_merges += tmp->st_ro_merges;
        bdb_state->temp_stats->st_rw_merges += tmp->st_rw_merges;
        bdb_state->temp_stats->st_ro_evict += tmp->st_ro_evict;
        bdb_state->temp_stats->st_rw_evict += tmp->st_rw_evict;
        bdb_state->temp_stats->st_pf_evict += tmp->st_pf_evict;
        bdb_state->temp_stats->st_rw_evict_skip += tmp->st_rw_evict_skip;
        bdb_state->temp_stats->st_page_trickle += tmp->st_page_trickle;
        bdb_state->temp_stats->st_pages += tmp->st_pages;
        ATOMIC_ADD(bdb_state->temp_stats->st_page_dirty, tmp->st_page_dirty);
        bdb_state->temp_stats->st_page_clean += tmp->st_page_clean;
        bdb_state->temp_stats->st_hash_buckets += tmp->st_hash_buckets;
        bdb_state->temp_stats->st_hash_searches += tmp->st_hash_searches;
        bdb_state->temp_stats->st_hash_longest += tmp->st_hash_longest;
        bdb_state->temp_stats->st_hash_examined += tmp->st_hash_examined;
        bdb_state->temp_stats->st_region_nowait += tmp->st_region_nowait;
        bdb_state->temp_stats->st_region_wait += tmp->st_region_wait;
        bdb_state->temp_stats->st_alloc += tmp->st_alloc;
        bdb_state->temp_stats->st_alloc_buckets += tmp->st_alloc_buckets;
        bdb_state->temp_stats->st_alloc_max_buckets +=
            tmp->st_alloc_max_buckets;
        bdb_state->temp_stats->st_alloc_pages += tmp->st_alloc_pages;
        bdb_state->temp_stats->st_alloc_max_pages += tmp->st_alloc_max_pages;
        bdb_state->temp_stats->st_ckp_pages_sync += tmp->st_ckp_pages_sync;
        bdb_state->temp_stats->st_ckp_pages_skip += tmp->st_ckp_pages_skip;

        free(tmp);
    }

    Pthread_mutex_unlock(&(bdb_state->temp_list_lock));

    switch (tbl->temp_table_type) {
    case TEMP_TABLE_TYPE_LIST: {
        struct temp_list_node *c_node = NULL;
        do {
            c_node = (struct temp_list_node *)listc_rtl(&(tbl->temp_tbl_list));
            if (c_node) {
                if (c_node->data)
                    free(c_node->data);
                free(c_node);
            }
        } while (c_node);
    } break;

    case TEMP_TABLE_TYPE_HASH: {
        void *hash_cur;
        unsigned int hash_cur_buk;
        char *data;

        data = hash_first(tbl->temp_hash_tbl, &hash_cur, &hash_cur_buk);
        while (data) {
            free(data);
            data = hash_next(tbl->temp_hash_tbl, &hash_cur, &hash_cur_buk);
        }
        hash_clear(tbl->temp_hash_tbl);
    } break;

    case TEMP_TABLE_TYPE_BTREE:
        break;
    }

    hash_free(tbl->temp_hash_tbl);
    tbl->temp_hash_tbl = NULL;

    /* close the environments*/
    rc = bdb_temp_table_env_close(bdb_state, tbl, bdberr);

    free(tbl);

done:
    dbgtrace(3, "temp_table_destroy_lru() = %d %s", rc, db_strerror(rc));
    return rc;
}

inline int bdb_temp_table_destroy_pool_wrapper(void *tbl, void *bdb_state_arg)
{
    int last, bdberr;
    return bdb_temp_table_destroy_lru(tbl, bdb_state_arg, &last, &bdberr);
}

int bdb_temp_table_delete(bdb_state_type *bdb_state, struct temp_cursor *cur,
                          int *bdberr)
{
    int rc;

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_LIST) {
        listc_rtl(&(cur->tbl->temp_tbl_list));
        return 0;
    }

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_HASH) {
        // AZ: address of data returned by hash_find: cur->key - sizeof(int)
        rc = hash_del(cur->tbl->temp_hash_tbl, cur->key - sizeof(int));
        goto done;
    }

    /*pthread_setspecific(cur->tbl->curkey, cur);*/
    if (!cur->valid) {
        rc = -1;
        goto done;
    }

    assert(cur->cur != NULL);
    rc = cur->cur->c_del(cur->cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "c_del rc %d\n", rc);
        *bdberr = rc;
        return -1;
    }
done:
    dbgtrace(3, "temp_table_delete(cursor %d) = %d", cur->curid, rc);
    return rc;
}

void bdb_temp_table_set_cmp_func(struct temp_table *tbl, tmptbl_cmp cmpfunc)
{
    tbl->cmpfunc = cmpfunc;
    /* default to memcmp semantics (for keys) */
    if (tbl->cmpfunc == NULL)
        tbl->cmpfunc = key_memcmp;
}

/* compare btree keys */
static int temp_table_compare(DB *db, const DBT *dbt1, const DBT *dbt2)
{
    struct temp_table *tbl;
    struct temp_cursor *cur;
    void *pKeyInfo = NULL;

    tbl = (struct temp_table *)db->app_private;

    if (dbt1->app_data)
        return -tbl->cmpfunc(NULL, dbt2->size, dbt2->data, -1, dbt1->app_data);

    /*
    cur = pthread_getspecific(tbl->curkey);
    if(cur) pKeyInfo = cur->usermem;
    */

    pKeyInfo = tbl->usermem;
    return tbl->cmpfunc(pKeyInfo, dbt1->size, dbt1->data, dbt2->size,
                        dbt2->data);
}


static int bdb_temp_table_find_hash(struct temp_cursor *cur, const void *key, int keylen)
{
        char *data = NULL;
        cur->valid = 0;
        if (!cur->tbl->temp_hash_tbl) {
            return IX_EMPTY;
        }
        if (keylen != 0 && key != NULL) {
            struct hashobj *o;
            int should_free = 0;

            if (keylen + sizeof(int) < 64*1024)
                o = alloca(keylen + sizeof(int));
            else {
                o = malloc(keylen + sizeof(int));
                should_free = 1;
            }
            o->len = keylen;
            memcpy(o->data, key, keylen);

            data = hash_find(cur->tbl->temp_hash_tbl, o);
            if (should_free)
                free(o);
        }
        if (!data) { /* find anything at all if possible */
            data = hash_first(cur->tbl->temp_hash_tbl, &cur->hash_cur,
                              &cur->hash_cur_buk);
        }

        if (data) {
            cur->keylen = *(int *)data;
            cur->key = data + sizeof(int);
            cur->datalen = *(int *)(data + cur->keylen + sizeof(int));
            ;
            cur->data = data + cur->keylen + 2 * sizeof(int);
            cur->valid = 1;
        } else {
            return IX_EMPTY;
        }
        return 0;
    }


int bdb_temp_table_find(bdb_state_type *bdb_state, struct temp_cursor *cur,
                        const void *key, int keylen, void *unpacked,
                        int *bdberr)
{
    int rc;
    DBT dkey, ddata;

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_LIST) {
        logmsg(LOGMSG_ERROR, 
                "bdb_temp_table_find operation not supported for temp list.\n");
        return -1;
    }
    else if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_HASH) {
        return bdb_temp_table_find_hash(cur, key, keylen);
    }

    assert(cur->cur != NULL);

    /*pthread_setspecific(cur->tbl->curkey, cur);*/

    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));
    dkey.flags = ddata.flags = DB_DBT_MALLOC;
    dkey.data = (void *)key;
    dkey.size = keylen;
    dkey.app_data = unpacked;
    cur->valid = 0;
    rc = cur->cur->c_get(cur->cur, &dkey, &ddata, DB_SET_RANGE);
    if (rc == DB_NOTFOUND) {
        rc = bdb_temp_table_last(bdb_state, cur, bdberr);
        /* find anything at all if possible */
        if (rc == IX_PASTEOF || rc == IX_EMPTY || rc == IX_FND)
            goto done;
        else {
            *bdberr = rc;
            rc = -1;
            goto done;
        }
    } else if (rc) {
        *bdberr = rc;
        rc = -1;
        goto done;
    }

    if (cur->key) {
#if 0
      printf( "%p Freeing %p\n", cur, cur->key);
#endif
        free(cur->key);
        cur->key = NULL;
    }
    if (cur->data) {
        free(cur->data);
        cur->data = NULL;
    }

    cur->key = dkey.data;
#if 0
   printf( "%p Setting %p\n", cur, cur->key);
#endif
    cur->keylen = dkey.size;
    cur->data = ddata.data;
    cur->datalen = ddata.size;
    cur->valid = 1;
/* higher level needs to check for match/higher/lower if needed */

done:
    dbghexdump(3, (void *)key, keylen);
    dbgtrace(3, "temp_table_find(cursor %d) = %d", cur->curid, rc);

    return rc;
}


static int bdb_temp_table_find_exact_hash(struct temp_cursor *cur, 
                                             void *key, int keylen)
{
    struct hashobj *o;
    int should_free = 0;

    if (keylen < 64*1024)
        o = alloca(keylen + sizeof(int));
    else {
        o = malloc(keylen + sizeof(int));
        should_free = 1;
    }

    cur->valid = 0;
    o->len = keylen;
    memcpy(o->data, key, keylen);
    char *data = hash_find(cur->tbl->temp_hash_tbl, o);
    if (should_free)
        free(o);

    if (data) {
        cur->keylen = *(int *)data;
        cur->key = data + sizeof(int);
        cur->datalen = *(int *)(data + cur->keylen + sizeof(int));
        ;
        cur->data = data + cur->keylen + 2 * sizeof(int);
        cur->valid = 1;
    } else {
        return IX_NOTFND;
    }
    return 0;


}


int bdb_temp_table_find_exact(bdb_state_type *bdb_state,
                              struct temp_cursor *cur, void *key, int keylen,
                              int *bdberr)
{
    int rc;
    DBT dkey, ddata;
    int exists = 0;

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_LIST) {
        logmsg(LOGMSG_ERROR, "bdb_temp_table_find_exact operation not supported for "
                        "temp list.\n");
        return -1;
    }
    else if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_HASH) {
        return bdb_temp_table_find_exact_hash(cur, key, keylen);
    }

    /*pthread_setspecific(cur->tbl->curkey, cur);*/

    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));
    dkey.flags = ddata.flags = DB_DBT_MALLOC;
    dkey.data = key;
    dkey.size = keylen;

    assert(cur->cur != NULL);
    cur->valid = 0;
    rc = cur->cur->c_get(cur->cur, &dkey, &ddata, DB_SET);
    /*
    printf("Got data %p %d key %p %d\n",
          ddata.data, ddata.size, dkey.data, dkey.size);
          */
    if (!rc) {
        exists = 1;
    } else if (rc == DB_NOTFOUND) {
        exists = 0;
        goto done;
    } else {
        *bdberr = rc;
        rc = -1;
        goto done;
    }

    if (cur->key && cur->key != dkey.data) {
#if 0
      printf( "%p Freeing key %p\n", cur, cur->key);
#endif
        free(cur->key);
        cur->key = NULL;
    }
    if (cur->data) {
        free(cur->data);
        cur->data = NULL;
    }

    cur->key = dkey.data;
#if 0
   printf( "%p Setting %p\n", cur, cur->key);
#endif
    cur->keylen = dkey.size;
    cur->data = ddata.data;
    cur->datalen = ddata.size;
    cur->valid = 1;
/* higher level needs to check for match/higher/lower if needed */

done:
    dbghexdump(3, key, keylen);
    dbgtrace(3, "temp_table_find(cursor %d) = %d", cur->curid, rc);
    return (exists) ? IX_FND : IX_NOTFND;
}

static int key_memcmp(void *_, int key1len, const void *key1, int key2len,
                      const void *key2)
{
    int len;
    int rc;
    len = (key1len < key2len) ? key1len : key2len;
    rc = memcmp(key1, key2, len);
    if (rc == 0) {
        if (key1len == key2len)
            rc = 0;
        else if (key1len > key2len)
            rc = 1;
        else
            rc = -1;
    }
    return rc;
}

int bdb_temp_table_close_cursor(bdb_state_type *bdb_state,
                                struct temp_cursor *cur, int *bdberr)
{
    int rc = 0;
    struct temp_table *tbl;
    tbl = cur->tbl;

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_BTREE) {
        if (cur->key) {
#if 0
          printf( "%p Freeing %p\n", cur, cur->key);
#endif
            free(cur->key);
            cur->key = NULL;
        }

        if (cur->data) {
            free(cur->data);
            cur->data = NULL;
        }

        if (cur->cur) {
            rc = cur->cur->c_close(cur->cur);
            if (rc) {
                *bdberr = rc;
                rc = -1;
            }
            cur->cur = NULL;
        }

        /* Note: we can't do this until the cursor is closed.
           Closing a cursor may invoke a search if we deleted items through that
           cursor.  The search (custom search routine)
           will need access to thread-specific data. */
        /*pthread_setspecific(cur->tbl->curkey, NULL);*/
    }

    listc_rfl(&tbl->cursors, cur);
    free(cur);
    return rc;
}

/* Run this carefully, it basically leave the task of freeing the
   data pointer to the caller that should have called bdb_temp_table_data by now
 */
inline void bdb_temp_table_reset_datapointers(struct temp_cursor *cur)
{
    if (cur) {
        cur->datalen = 0;
        cur->data = NULL;
    }
}

/* the only move routine you should have */
inline int bdb_temp_table_move(bdb_state_type *bdb_state, struct temp_cursor *cursor,
                        int how, int *bdberr)
{
    switch (how) {
    case DB_FIRST:
        return bdb_temp_table_first(bdb_state, cursor, bdberr);
    case DB_NEXT:
        return bdb_temp_table_next(bdb_state, cursor, bdberr);
    case DB_PREV:
        return bdb_temp_table_prev(bdb_state, cursor, bdberr);
    case DB_LAST:
        return bdb_temp_table_last(bdb_state, cursor, bdberr);
    }
    logmsg(LOGMSG_ERROR, "%s: argh?\n", __func__);
    *bdberr = BDBERR_BUG_KILLME;
    return -1;
}

void bdb_temp_table_debug_dump(bdb_state_type *bdb_state, tmpcursor_t *cur)
{
    int rc = 0;
    int bdberr = 0;
    int rowid = 0;
    int keysize_sd;
    char *key_sd;
    int dtasize_sd;
    char *dta_sd;

    logmsg(LOGMSG_USER, "TMPTABLE:\n");
    rc = bdb_temp_table_first(bdb_state, cur, &bdberr);
    while (!rc) {

        key_sd = bdb_temp_table_key(cur);
        keysize_sd = bdb_temp_table_keysize(cur);
        dta_sd = bdb_temp_table_data(cur);
        dtasize_sd = bdb_temp_table_datasize(cur);

        logmsg(LOGMSG_USER, " ROW %d:\n\tkeylen=%d\n\tkey=\"", rowid, keysize_sd);
        hexdump(LOGMSG_USER, key_sd, keysize_sd);
        logmsg(LOGMSG_USER, "\"\n\tdatalen=%d\n\tdata=\"", dtasize_sd);
        hexdump(LOGMSG_USER, dta_sd, dtasize_sd);
        logmsg(LOGMSG_USER, "\"\n");

        rowid++;

        rc = bdb_temp_table_next(bdb_state, cur, &bdberr);
    }
}

inline int bdb_is_hashtable(struct temp_table *tt)
{
    return (tt->temp_table_type == TEMP_TABLE_TYPE_HASH);
}

static int bdb_temp_table_insert_put(bdb_state_type *bdb_state,
                                     struct temp_table *tbl, void *key,
                                     int keylen, void *data, int dtalen,
                                     int *bdberr)
{
    int rc;

    if (tbl->temp_table_type == TEMP_TABLE_TYPE_LIST) {
        struct temp_list_node *c_node = malloc(sizeof(struct temp_list_node));
        void *list_data = malloc(dtalen);
        memcpy(list_data, data, dtalen);
        c_node->data = list_data;
        listc_abl(&(tbl->temp_tbl_list), c_node);
        return 0;
    }

    if (tbl->temp_table_type == TEMP_TABLE_TYPE_HASH) {
        void *hash_data;
        void *old;

        hash_data = malloc(keylen + dtalen + 2 * sizeof(int));
        memcpy(hash_data, &keylen, sizeof(int));
        memcpy((uint8_t *)hash_data + sizeof(int), key, keylen);
        old = hash_find(tbl->temp_hash_tbl, hash_data);

        if (old == NULL) {
            memcpy((uint8_t *)hash_data + keylen + sizeof(int), &dtalen,
                   sizeof(int));
            memcpy((uint8_t *)hash_data + keylen + 2 * sizeof(int), data,
                   dtalen);
            hash_add(tbl->temp_hash_tbl, hash_data);
            tbl->num_mem_entries++;
        } else {
            free(hash_data);
        }

        if (tbl->num_mem_entries > tbl->max_mem_entries) {
            rc = bdb_hash_table_copy_to_temp_db(bdb_state, tbl, bdberr);
            if (unlikely(rc)) {
                return -1;
            }
        }

        return 0;
    }

    assert (tbl->temp_table_type == TEMP_TABLE_TYPE_BTREE);
    tbl->num_mem_entries++;

    return 1;
}

inline const char *bdb_temp_table_filename(struct temp_table *tbl)
{
    if (tbl)
        return tbl->filename;
    return NULL;
}

inline void bdb_temp_table_flush(struct temp_table *tbl)
{
    DB *db = tbl->tmpdb;
    db->sync(db, 0);
}

int bdb_temp_table_stat(bdb_state_type *bdb_state, DB_MPOOL_STAT **gspp)
{
    bdb_state_type *parent;
    struct temp_table *tbl;
    DB_MPOOL_STAT *sp;
    DB_MPOOL_STAT *tmp;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    if (gspp == NULL)
        return 1;

    *gspp = calloc(1, sizeof(**gspp));

    sp = *gspp;

    sp->st_gbytes = parent->temp_stats->st_gbytes;
    sp->st_bytes = parent->temp_stats->st_bytes;
    sp->st_ncache = parent->temp_stats->st_ncache;
    sp->st_regsize = parent->temp_stats->st_regsize;
    sp->st_map = parent->temp_stats->st_map;
    sp->st_cache_hit = parent->temp_stats->st_cache_hit;
    sp->st_cache_miss = parent->temp_stats->st_cache_miss;
    sp->st_cache_ihit = parent->temp_stats->st_cache_ihit;
    sp->st_cache_imiss = parent->temp_stats->st_cache_imiss;
    sp->st_cache_lhit = parent->temp_stats->st_cache_lhit;
    sp->st_cache_lmiss = parent->temp_stats->st_cache_lmiss;
    sp->st_page_create = parent->temp_stats->st_page_create;
    sp->st_page_pf_in = parent->temp_stats->st_page_pf_in;
    sp->st_page_in = parent->temp_stats->st_page_in;
    sp->st_page_out = parent->temp_stats->st_page_out;
    sp->st_ro_merges = parent->temp_stats->st_ro_merges;
    sp->st_rw_merges = parent->temp_stats->st_rw_merges;
    sp->st_ro_evict = parent->temp_stats->st_ro_evict;
    sp->st_rw_evict = parent->temp_stats->st_rw_evict;
    sp->st_pf_evict = parent->temp_stats->st_pf_evict;
    sp->st_rw_evict_skip = parent->temp_stats->st_rw_evict_skip;
    sp->st_page_trickle = parent->temp_stats->st_page_trickle;
    sp->st_pages = parent->temp_stats->st_pages;
    sp->st_page_dirty = parent->temp_stats->st_page_dirty;
    sp->st_page_clean = parent->temp_stats->st_page_clean;
    sp->st_hash_buckets = parent->temp_stats->st_hash_buckets;
    sp->st_hash_searches = parent->temp_stats->st_hash_searches;
    sp->st_hash_longest = parent->temp_stats->st_hash_longest;
    sp->st_hash_examined = parent->temp_stats->st_hash_examined;
    sp->st_region_nowait = parent->temp_stats->st_region_nowait;
    sp->st_region_wait = parent->temp_stats->st_region_wait;
    sp->st_alloc = parent->temp_stats->st_alloc;
    sp->st_alloc_buckets = parent->temp_stats->st_alloc_buckets;
    sp->st_alloc_max_buckets = parent->temp_stats->st_alloc_max_buckets;
    sp->st_alloc_pages = parent->temp_stats->st_alloc_pages;
    sp->st_alloc_max_pages = parent->temp_stats->st_alloc_max_pages;
    sp->st_ckp_pages_sync = parent->temp_stats->st_ckp_pages_sync;
    sp->st_ckp_pages_skip = parent->temp_stats->st_ckp_pages_skip;

    return 0;
}



int bdb_temp_table_insert_test(bdb_state_type *bdb_state, int recsz, int maxins)
{
    bdb_state_type *parent;
    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    //create
    int bdberr;
    struct temp_table *db = bdb_temp_table_create(parent, &bdberr);
    if (!db || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to create temp table bdberr=%d\n",
               __func__, bdberr);
        return -1;
    }

    if (recsz < 8) recsz = 8; //force it to be min 8 bytes
    if (recsz * maxins > 10000000) return -1; //limit the temptbl size

    //read one random string into key, note that reading from urandom is
    //slow so we get one full record from urandom, then override the first 
    //4 byte from random()
    int rc;
    FILE *urandom;
    if ((urandom = fopen("/dev/urandom", "r")) == NULL) {
        logmsgperror("fopen");
        return -2;
    }

    uint8_t rkey[recsz];
    if ((rc = fread(rkey, sizeof(rkey), 1, urandom)) != 1 && ferror(urandom)) {
        logmsgperror("fread");
    }
    fclose(urandom);

    //insert: replace first 4 bytes with a new random value, payload is same val
    for (int cnt = 0; cnt < maxins; cnt++) {
        int x = rand();
        *((int*)rkey) = x;
        rc = bdb_temp_table_put(parent, db, &rkey, sizeof(rkey),
                              &x, sizeof(x), NULL, &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s: fail to put into temp tbl rc=%d bdberr=%d\n",
                    __func__, rc, bdberr);
            break; 
        }
    }

    //cleanup
    rc = bdb_temp_table_close(parent, db, &bdberr);
    return rc;
}
