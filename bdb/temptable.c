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
#include <sys/time.h>

#ifdef _LINUX_SOURCE
#include <execinfo.h>
#endif
#include <logmsg.h>
#include <tohex.h>
#include "comdb2_atomic.h"

/* One of the difference between using b-tree and hash is that in using hash, we
 * get pointer of data in hash table,
 * but in case of B-Tree new data is allocated. So far haven't seen any
 * problems. */

#include <schema_lk.h>
#include "locks.h"
#include "sys_wrap.h"
#include "bdb_int.h"
#include "strbuf.h"

extern int recover_deadlock_simple(bdb_state_type *bdb_state);

#ifdef __GLIBC__
extern int backtrace(void **, int);
extern void backtrace_symbols_fd(void *const *, int, int);
#else
#define backtrace(A, B) 1
#define backtrace_symbols_fd(A, B, C)
#endif

#define REOPEN_CURSOR(c)                                                       \
    do {                                                                       \
        if ((c)->cur == NULL) {                                                \
            int rc =                                                           \
                (c)->tbl->tmpdb->cursor((c)->tbl->tmpdb, NULL, &(c)->cur, 0);  \
            if (rc) {                                                          \
                logmsg(LOGMSG_ERROR, "%s: cursor create returned rc=%d\n",     \
                       __func__, rc);                                          \
                return rc;                                                     \
            }                                                                  \
        }                                                                      \
    } while (0)

extern char *gbl_crypto;
extern int64_t gbl_temptable_created;
extern int64_t gbl_temptable_create_reqs;
extern int64_t gbl_temptable_spills;

struct hashobj {
    int len;
    unsigned char data[/*len*/];
};

uint32_t gbl_temptable_count;

static char *get_stack_backtrace(void)
{
    void *stack[100] = {0};
    int nFrames = backtrace(stack, sizeof(stack) / sizeof(void*));
    if (nFrames > 0) {
        strbuf *pStr = strbuf_new();
        for (int i = 0; i < nFrames; i++) {
            if (i > 0) strbuf_append(pStr, " ");
            strbuf_appendf(pStr, "%p", stack[i]);
        }
        char *zBacktrace = strbuf_disown(pStr);
        strbuf_free(pStr);
        return zBacktrace;
    }
    return NULL;
}

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
    void *hash_cur;
    unsigned int hash_cur_buk;
    LINKC_T(struct temp_cursor) lnk;
    int ind;
    int keymalloclen;
    int datamalloclen;
};

typedef struct arr_elem {
    int keylen;
    int dtalen;
    uint8_t *key;
    uint8_t *dta;
} arr_elem_t;

#define COPY_KV_TO_CUR(c)                                                      \
    do {                                                                       \
        arr_elem_t *elem = &(c)->tbl->elements[(c)->ind];                      \
        int keylen = elem->keylen, dtalen = elem->dtalen;                      \
        if ((c)->key == NULL || (c)->keymalloclen < keylen) {                  \
            (c)->key = malloc_resize((c)->key, keylen);                        \
            (c)->keymalloclen = keylen;                                        \
        }                                                                      \
        if ((c)->key == NULL) {                                                \
            (c)->valid = 0;                                                    \
            return -1;                                                         \
        }                                                                      \
        if ((c)->data == NULL || (c)->datamalloclen < dtalen) {                \
            (c)->data = malloc_resize((c)->data, dtalen);                      \
            (c)->datamalloclen = dtalen;                                       \
        }                                                                      \
        if ((c)->data == NULL) {                                               \
            free((c)->data);                                                   \
            (c)->valid = 0;                                                    \
            return -1;                                                         \
        }                                                                      \
        (c)->keylen = keylen;                                                  \
        (c)->datalen = dtalen;                                                 \
        memcpy((c)->key, elem->key, keylen);                                   \
        memcpy((c)->data, elem->dta, dtalen);                                  \
        (c)->valid = 1;                                                        \
    } while (0);

/* A temparray is a lightweight replacement of a temptable. It is simply
   a sorted array. If the number of elements is greater than a threshold,
   or the in-memory data size exceeds a pre-configured cache size,
   a temparray will fall back to a temptable.
   A temparray is more efficient than a temptable. Besides, it uses far
   less memory than a temptable for small and medium-sized requests. */
enum {
    TEMP_TABLE_TYPE_BTREE,
    TEMP_TABLE_TYPE_HASH,
    TEMP_TABLE_TYPE_ARRAY
};

struct temp_table {
    DB_ENV *dbenv_temp;

    int temp_table_type;
    DB *tmpdb; /* in-memory table */
    hash_t *temp_hash_tbl;

    tmptbl_cmp cmpfunc;
    void *usermem;
    char filename[512];
    int tblid;
    unsigned long long rowid;
    char *sql;

    unsigned long long num_mem_entries;
    int max_mem_entries;
    LISTC_T(struct temp_cursor) cursors;
    void *next;

    unsigned long long inmemsz;
    unsigned long long cachesz;
    arr_elem_t *elements;
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

static int bdb_temp_table_init_temp_db(bdb_state_type *bdb_state,
                                       struct temp_table *tbl, int *bdberr);

static int create_temp_db_env(bdb_state_type *bdb_state, struct temp_table *tbl,
                              int *bdberr)
{
    int rc;
    DB_ENV *dbenv_temp;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    rc = db_env_create(&dbenv_temp, 0);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "couldnt create temp table env\n");
        *bdberr = BDBERR_MISC;
        return rc;
    }

    if (gbl_crypto) {
        // generate random password for temp tables
        char passwd[64];
        passwd[0] = 0;
        while (passwd[0] == 0) {
            RAND_bytes((unsigned char *)passwd, 63);
        }
        passwd[63] = 0;
        if ((rc = dbenv_temp->set_encrypt(dbenv_temp, passwd,
                                          DB_ENCRYPT_AES)) != 0) {
            logmsg(LOGMSG_ERROR, "%s set_encrypt rc:%d\n", __func__, rc);
            goto error;
        }
        memset(passwd, 0xff, sizeof(passwd));
    }

    rc = dbenv_temp->set_is_tmp_tbl(dbenv_temp, 1);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "couldnt set property is_tmp_tbl\n");
        goto error;
    }

    rc = dbenv_temp->set_tmp_dir(dbenv_temp, bdb_state->tmpdir);
    if (rc) {
        logmsg(LOGMSG_ERROR,
               "can't set temp table environment's temp directory");
        /* continue anyway */
    }

    rc = dbenv_temp->set_cachesize(dbenv_temp, 0, tbl->cachesz, 1);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "invalid set_cache_size call: bytes %llu\n",
               tbl->cachesz);
        goto error;
    }

    rc = dbenv_temp->open(dbenv_temp, bdb_state->tmpdir,
                          DB_INIT_MPOOL | DB_CREATE | DB_PRIVATE, 0666);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "couldnt open temp table env\n");
        goto error;
    }

    tbl->dbenv_temp = dbenv_temp;
    rc = bdb_temp_table_init_temp_db(bdb_state, tbl, bdberr);
    if (rc != 0)
        goto error;
    return 0;

error:
    *bdberr = BDBERR_MISC;
    tbl->dbenv_temp = NULL;
    dbenv_temp->close(dbenv_temp, 0);
    return rc;
}

static int bdb_array_copy_to_temp_db(bdb_state_type *bdb_state,
                                     struct temp_table *tbl, int *bdberr)
{
    int rc = 0, ii;
    DBT dbt_key, dbt_data;
    struct temp_cursor *cur;
    arr_elem_t *elem;
    unsigned long long nents = tbl->num_mem_entries;

    bzero(&dbt_key, sizeof(DBT));
    bzero(&dbt_data, sizeof(DBT));

    if (tbl->dbenv_temp == NULL &&
        create_temp_db_env(bdb_state, tbl, bdberr) != 0) {
        bdb_temp_table_destroy_pool_wrapper(tbl, bdb_state);
    }

    for (ii = 0; ii != nents; ++ii) {
        elem = &tbl->elements[ii];
        dbt_key.flags = dbt_data.flags = DB_DBT_USERMEM;
        dbt_key.ulen = dbt_key.size = elem->keylen;
        dbt_data.ulen = dbt_data.size = elem->dtalen;
        dbt_data.data = elem->dta;
        dbt_key.data = elem->key;

        rc = tbl->tmpdb->put(tbl->tmpdb, NULL, &dbt_key, &dbt_data, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR, "%s:%d put rc %d\n", __FILE__, __LINE__, rc);
            return rc;
        }
    }

    for (ii = 0; ii != nents; ++ii) {
        elem = &tbl->elements[ii];
        free(elem->key);
    }
    tbl->inmemsz = 0;
    tbl->num_mem_entries = nents;

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
            logmsg(LOGMSG_ERROR, "%s:%d cursor rc %d\n", __FILE__, __LINE__,
                   rc);
            goto done;
        }

        /* New cursor does not point to any data */
        cur->key = cur->data = NULL;
        cur->keylen = cur->datalen = 0;
    }

done:
    return rc;
}

static int bdb_hash_table_copy_to_temp_db(bdb_state_type *bdb_state,
                                          struct temp_table *tbl, int *bdberr)
{
    int rc = 0;
    DBT dbt_key, dbt_data;
    bzero(&dbt_key, sizeof(DBT));
    bzero(&dbt_data, sizeof(DBT));
    struct temp_cursor *cur;
    void *hash_cur;
    unsigned int hash_cur_buk;
    char *data;

    if (tbl->dbenv_temp == NULL &&
        create_temp_db_env(bdb_state, tbl, bdberr) != 0) {
        bdb_temp_table_destroy_pool_wrapper(tbl, bdb_state);
    }

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

static void bdb_temp_table_reset(struct temp_table *tbl)
{
    tbl->rowid = 0;
    tbl->num_mem_entries = 0;
}

static int bdb_temp_table_reset_cursors( bdb_state_type *bdb_state, struct temp_table *tbl, int *bdberr);
static int bdb_temp_table_reset_cursor(bdb_state_type *bdb_state, struct temp_cursor *cur, int *bdberr);

static int bdb_temp_table_init_temp_db(bdb_state_type *bdb_state,
                                       struct temp_table *tbl, int *bdberr)
{
    DB *db;
    int rc;

    if (tbl->tmpdb) {
        /* Close all cursors that this table has open. */
        rc = bdb_temp_table_reset_cursors(bdb_state, tbl, bdberr);
        if (rc)
            goto done;

        rc = tbl->tmpdb->close(tbl->tmpdb, 0);
        if (rc) {
            *bdberr = rc;
            rc = -1;
            logmsg(LOGMSG_ERROR, "%s:%d close rc %d\n", __FILE__, __LINE__, rc);
            tbl->tmpdb = NULL;
            goto done;
        }
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

    bdb_temp_table_reset(tbl);

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

extern pthread_key_t current_sql_query_key;
int gbl_debug_temptables = 0;

static struct temp_table *bdb_temp_table_create_main(bdb_state_type *bdb_state,
                                                     int *bdberr)
{
    bdb_state_type *parent;
    int id;

    if (bdb_state->parent)
        parent = bdb_state->parent;
    else
        parent = bdb_state;

    struct temp_table *tbl = calloc(1, sizeof(struct temp_table));
    if (!tbl) {
        logmsg(LOGMSG_ERROR, "%s:%d: Failed calloc", __func__, __LINE__);
        *bdberr = BDBERR_MALLOC;
        return NULL;
    }

    if (gbl_debug_temptables) {
        char *sql = pthread_getspecific(current_sql_query_key);
        if (sql) tbl->sql = strdup(sql);
    }

    tbl->cachesz = bdb_state->attr->temptable_cachesz;

    /* 512k minimim cache */
    if (tbl->cachesz < 524288)
        tbl->cachesz = 524288;

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

#ifdef _LINUX_SOURCE
    if (gbl_debug_temptables) {
        char *zBacktrace = get_stack_backtrace();
        logmsg(LOGMSG_USER, "creating a temp table object %p (%d): %s, %s\n",
               tbl, tbl ? 0 : -1, tbl ? tbl->sql : 0, zBacktrace);
        free(zBacktrace);
    }
#endif
    ++gbl_temptable_created;
    ATOMIC_ADD32(gbl_temptable_count, 1);
    dbgtrace(3, "temp_table_create(%s) = %d", tbl->filename, tbl->tblid);
    return tbl;
}

int bdb_temp_table_clear_list(bdb_state_type *bdb_state)
{
    int rc = 0;
    int last = 0;
    while (rc == 0 && last == 0) {
        int bdberr = 0;
        rc = bdb_temp_table_destroy_lru(NULL, bdb_state, &last, &bdberr);
        if (rc != 0) {
            logmsg(LOGMSG_USER,
                   "%s: failed to destroy a temp table object rc=%d, bdberr=%d\n",
                   __func__, rc, bdberr);
        }
    }
    return rc;
}

int bdb_temp_table_clear_pool(bdb_state_type *bdb_state)
{
    comdb2_objpool_t op = bdb_state->temp_table_pool;
    if (op == NULL) return EINVAL;
    comdb2_objpool_clear(op);
    return 0;
}

int bdb_temp_table_clear_cache(bdb_state_type *bdb_state)
{
    if (gbl_temptable_pool_capacity == 0) {
        return bdb_temp_table_clear_list(bdb_state);
    } else {
        return bdb_temp_table_clear_pool(bdb_state);
    }
}

int bdb_temp_table_create_pool_wrapper(void **tblp, void *bdb_state_arg)
{
    int bdberr = 0;
    *tblp =
        bdb_temp_table_create_main((bdb_state_type *)bdb_state_arg, &bdberr);
    return bdberr;
}

int bdb_temp_table_notify_pool_wrapper(void **tblp, void *bdb_state_arg)
{
    bdb_state_type *bdb_state = (bdb_state_type *)bdb_state_arg;
    int rc1 = bdb_temp_table_maybe_set_priority_thread(bdb_state);
    if (rc1 == TMPTBL_PRIORITY) { /* Are we going to end up waiting? */
        return OP_FORCE_NOW; /* No, we are forcing object creation. */
    } else {
        int rc2;

        /* This is in the middle of prepare.  We can't call recover deadlock,
         * as releasing and re-acquiring the bdblock while holding the schema
         * lock violates lock order.  We also can't prevent upgrades. */
        if (have_schema_lock()) {
            return OP_FAIL_NOW;
        }
        rc2 = recover_deadlock_simple(bdb_state);
        if (rc2 != 0) {
            logmsg(LOGMSG_WARN, "%s: recover_deadlock rc=%d\n", __func__, rc2);
        }
        return OP_WAIT_AGAIN; /* Yes, and we give up locks. */
    }
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

    /* needed by temptable pool */
    extern pthread_key_t query_info_key;
    void *sql_thread;
    int action;

    ++gbl_temptable_create_reqs;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    if (gbl_temptable_pool_capacity == 0) {
        /* temptable pool not enabled. fall back to linked list cache. */
        Pthread_mutex_lock(&(bdb_state->temp_list_lock));
        if (bdb_state->temp_list) {
            table = bdb_state->temp_list;
            bdb_state->temp_list = table->next;
            table->next = NULL;
            Pthread_mutex_unlock(&(bdb_state->temp_list_lock));
        } else {
            Pthread_mutex_unlock(&(bdb_state->temp_list_lock));
            table = bdb_temp_table_create_main(bdb_state, bdberr);
            if (!table)
                return NULL;
        }
    } else {
        sql_thread = pthread_getspecific(query_info_key);

        if (sql_thread == NULL) {
            /*
            ** a sql thread may be waiting for the completion of a non-sql
            *thread (tag, for example).
            ** so don't block non-sql.
            */
            action = TMPTBL_PRIORITY;
        } else {
            action = bdb_temp_table_maybe_set_priority_thread(bdb_state);
        }

        int rc = 0;
        switch (action) {
        case TMPTBL_PRIORITY:
            rc = comdb2_objpool_forcedborrow(bdb_state->temp_table_pool, (void **)&table);
            break;
        case TMPTBL_WAIT:
            rc = comdb2_objpool_borrow(bdb_state->temp_table_pool, (void **)&table);
            break;
        }
        if (rc) {
            logmsg(LOGMSG_ERROR, "comdb2_objpool_borrow (or forcedborrow) returns rc %d %s\n", rc, db_strerror(rc));
            *bdberr = BDBERR_MISC;
        } else if (!table) {
            logmsg(LOGMSG_ERROR, "comdb2_objpool_borrow (or forcedborrow) returns NULL table object\n");
            *bdberr = BDBERR_MISC;
        }
    }

    if (table != NULL) {
        switch (temp_table_type) {
        case TEMP_TABLE_TYPE_BTREE:
            if (table->dbenv_temp == NULL) {
                if (create_temp_db_env(bdb_state, table, bdberr) != 0) {
                    bdb_temp_table_destroy_pool_wrapper(table, bdb_state);
                    return NULL;
                }
            }
            break;
        case TEMP_TABLE_TYPE_HASH:
            if (table->temp_hash_tbl == NULL) {
                table->temp_hash_tbl = hash_init_user(hashfunc, hashcmpfunc, 0, 0);
                if (table->temp_hash_tbl == NULL) {
                    bdb_temp_table_destroy_pool_wrapper(table, bdb_state);
                    return NULL;
                }
            }
            break;
        case TEMP_TABLE_TYPE_ARRAY:
            if (table->elements == NULL) {
                table->elements = calloc(table->max_mem_entries, sizeof(arr_elem_t));
                if (table->elements == NULL) {
                    bdb_temp_table_destroy_pool_wrapper(table, bdb_state);
                    return NULL;
                }
            }
            break;
        }

        table->num_mem_entries = 0;
        table->cmpfunc = key_memcmp;
        table->temp_table_type = temp_table_type;
    }

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

struct temp_table *bdb_temp_hashtable_create(bdb_state_type *bdb_state,
                                             int *bdberr)
{
    return bdb_temp_table_create_type(bdb_state, TEMP_TABLE_TYPE_HASH, bdberr);
}

struct temp_table *bdb_temp_array_create(bdb_state_type *bdb_state, int *bdberr)
{
    return bdb_temp_table_create_type(bdb_state, TEMP_TABLE_TYPE_ARRAY, bdberr);
}

struct temp_cursor *bdb_temp_table_cursor(bdb_state_type *bdb_state,
                                          struct temp_table *tbl, void *usermem,
                                          int *bdberr)
{
    struct temp_cursor *cur;
    int rc = 0;

    cur = calloc(1, sizeof(struct temp_cursor));
    /* set up compare routine and callback */
    cur->tbl = tbl;
    tbl->usermem = usermem;
    cur->curid = curid++;
    cur->cur = NULL;

    switch (tbl->temp_table_type) {
    case TEMP_TABLE_TYPE_HASH:
        cur->hash_cur = NULL;
        cur->hash_cur_buk = 0;
        break;

    case TEMP_TABLE_TYPE_BTREE:
        rc = tbl->tmpdb->cursor(tbl->tmpdb, NULL, &cur->cur, 0);
        break;

    case TEMP_TABLE_TYPE_ARRAY:
        cur->ind = 0;
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
/*Pthread_setspecific(tbl->curkey, cur);*/
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

    REOPEN_CURSOR(cur);

    /*Pthread_setspecific(cur->tbl->curkey, cur);*/
    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));
    dkey.flags = ddata.flags = DB_DBT_USERMEM;
    dkey.ulen = dkey.size = keylen;
    ddata.ulen = ddata.size = dtalen;
    ddata.data = data;
    dkey.data = key;

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
    arr_elem_t *elem;
    uint8_t *keycopy, *dtacopy;

    if (cur->tbl->temp_table_type != TEMP_TABLE_TYPE_BTREE &&
        cur->tbl->temp_table_type != TEMP_TABLE_TYPE_ARRAY) {
        logmsg(LOGMSG_ERROR, "bdb_temp_table_update operation "
                             "only supported for btree or array.\n");
        return -1;
    }

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_ARRAY) {
        if (!cur->valid)
            return -1;

        /* Free the existing elements and update the memory footprint. */
        elem = &cur->tbl->elements[cur->ind];
        free(elem->key);
        cur->tbl->inmemsz -= (elem->keylen + elem->dtalen);

        /* malloc and copy */
        keycopy = malloc(keylen + dtalen);
        if (keycopy == NULL)
            return -1;
        dtacopy = keycopy + keylen;
        memcpy(keycopy, key, keylen);
        memcpy(dtacopy, data, dtalen);

        /* Update the element and the memory footprint. */
        elem->keylen = keylen;
        elem->key = keycopy;
        elem->dtalen = dtalen;
        elem->dta = dtacopy;
        cur->tbl->inmemsz += (elem->keylen + elem->dtalen);
    }

    REOPEN_CURSOR(cur);

    /*Pthread_setspecific(cur->tbl->curkey, cur);*/

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
    DBC *cur;
    void *ent;
    unsigned int bkt;

    switch (tbl->temp_table_type) {
    case TEMP_TABLE_TYPE_BTREE:
        if (tbl->tmpdb->cursor(tbl->tmpdb, NULL, &cur, 0) == 0) {
            DBT key, data;
            memset(&key, 0, sizeof(DBT));
            memset(&data, 0, sizeof(DBT));
            key.flags = DB_DBT_USERMEM;
            data.flags = DB_DBT_USERMEM;
            if (cur->c_get(cur, &key, &data, DB_FIRST) == DB_NOTFOUND)
                tbl->rowid = 0;
            cur->c_close(cur);
        }
        break;
    case TEMP_TABLE_TYPE_ARRAY:
        if (tbl->num_mem_entries == 0)
            tbl->rowid = 0;
        break;
    case TEMP_TABLE_TYPE_HASH:
        if (hash_first(tbl->temp_hash_tbl, &ent, &bkt) == NULL)
            tbl->rowid = 0;
    }

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
    int rc, arrlen;

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

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_ARRAY) {
        arrlen = cur->tbl->num_mem_entries;
        if (arrlen == 0) {
            cur->valid = 0;
            return IX_EMPTY;
        }

        cur->ind = (how == DB_LAST) ? (arrlen - 1) : 0;
        COPY_KV_TO_CUR(cur);
        return 0;
    }

    REOPEN_CURSOR(cur);

    /*Pthread_setspecific(cur->tbl->curkey, cur);*/

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

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_ARRAY) {
        if ((how == DB_NEXT && ++cur->ind >= cur->tbl->num_mem_entries) ||
            (how == DB_PREV && --cur->ind < 0)) {
            cur->valid = 0;
            return IX_PASTEOF;
        }

        COPY_KV_TO_CUR(cur);
        return 0;
    }

    REOPEN_CURSOR(cur);

    /*Pthread_setspecific(cur->tbl->curkey, cur);*/

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

    /*Pthread_setspecific(cur->tbl->curkey, cur);*/
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

static int bdb_temp_table_reset_cursors( bdb_state_type *bdb_state, struct temp_table *tbl, int *bdberr) {
    int rc;
    struct temp_cursor *cur;

    LISTC_FOR_EACH(&tbl->cursors, cur, lnk)
    {
        if ((rc = bdb_temp_table_reset_cursor(bdb_state, cur, bdberr)) != 0) {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_reset_cursor(%p, %p) rc %d\n", __func__, tbl, cur, rc);
            return rc;
        }
    }
    return 0;
}


static int bdb_temp_table_truncate_temp_db(bdb_state_type *bdb_state,
                                           struct temp_table *tbl, int *bdberr)
{
    int rc, rc2;
    DBC *dbcur;
    DBT dbt_key, dbt_data;

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
        logmsg(LOGMSG_FATAL,
               "bdb_temp_table_truncate_temp_db couldnt get cursor\n");
        exit(1);
    }

    rc = dbcur->c_get(dbcur, &dbt_key, &dbt_data, DB_FIRST);
    while (rc == 0) {
        rc2 = dbcur->c_del(dbcur, 0);
        if (rc2)
            logmsg(LOGMSG_WARN, "%s:%d rc2=%d\n", __func__, __LINE__, rc2);
        /*fprintf(stderr, "deleting\n");*/
        rc = dbcur->c_get(dbcur, &dbt_key, &dbt_data, DB_NEXT);
    }
    // assert(rc == DB_KEYEMPTY || rc == DB_NOTFOUND);

    rc = dbcur->c_close(dbcur);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d close rc=%d\n", __func__, __LINE__, rc);
        *bdberr = BDBERR_MISC;
        return -1;
    }
    return bdb_temp_table_reset_cursors(bdb_state, tbl, bdberr);
}

int bdb_temp_table_truncate(bdb_state_type *bdb_state, struct temp_table *tbl,
                            int *bdberr)
{
    if (tbl == NULL)
        return 0;
    int rc = 0, ii = 0;
    arr_elem_t *elem;

    switch (tbl->temp_table_type) {
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

    case TEMP_TABLE_TYPE_ARRAY:
        for (; ii != tbl->num_mem_entries; ++ii) {
            elem = &tbl->elements[ii];
            free(elem->key);
        }
        tbl->inmemsz = 0;
        tbl->num_mem_entries = 0;
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
    if (rc == 0)
        tbl->num_mem_entries = 0;


    dbgtrace(3, "temp_table_truncate() = %d", rc);
    return rc;
}

/* XXX todo - call bdb_temp_table_truncate() and put on a list at parent
   bdb_state */
int bdb_temp_table_close(bdb_state_type *bdb_state, struct temp_table *tbl,
                         int *bdberr)
{
    struct temp_cursor *cur, *temp;
    DB_MPOOL_STAT *tmp;
    int rc;

    if (tbl == NULL)
        return 0;

    rc = 0;

    if (bdb_state->parent)
        bdb_state = bdb_state->parent;

    LISTC_FOR_EACH_SAFE(&tbl->cursors, cur, temp, lnk)
    {
        if ((rc = bdb_temp_table_close_cursor(bdb_state, cur, bdberr)) != 0) {
            logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_close_cursor(%p, %p) rc %d\n",
                   __func__, tbl, cur, rc);
            return rc;
        }
    }

    rc = bdb_temp_table_truncate(bdb_state, tbl, bdberr);

    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_truncate rc = %d\n",
               __func__, rc);
    }

    /*
    ** Check for type instead of dbenv. A temparray has a dbenv too if it's
    ** previously spilled to a btree. Do not double-count the btree statistics.
    */
    if (tbl->temp_table_type == TEMP_TABLE_TYPE_BTREE) {
        Pthread_mutex_lock(&(bdb_state->temp_list_lock));

        if ((tbl->dbenv_temp->memp_stat(tbl->dbenv_temp, &tmp, NULL,
                                        DB_STAT_CLEAR)) == 0) {
            bdb_state->temp_stats->st_gbytes += tmp->st_gbytes;
            bdb_state->temp_stats->st_bytes += tmp->st_bytes;
            bdb_state->temp_stats->st_total_bytes += tmp->st_total_bytes;
            bdb_state->temp_stats->st_used_bytes += tmp->st_used_bytes;
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
            ATOMIC_ADD32(bdb_state->temp_stats->st_page_dirty,
                       tmp->st_page_dirty);
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
            bdb_state->temp_stats->st_alloc_max_pages +=
                tmp->st_alloc_max_pages;
            bdb_state->temp_stats->st_ckp_pages_sync += tmp->st_ckp_pages_sync;
            bdb_state->temp_stats->st_ckp_pages_skip += tmp->st_ckp_pages_skip;

            free(tmp);
        }

        Pthread_mutex_unlock(&(bdb_state->temp_list_lock));
    }

    bdb_temp_table_reset(tbl);

    if (gbl_temptable_pool_capacity > 0) {
        rc = comdb2_objpool_return(bdb_state->temp_table_pool, tbl);
    } else {
        Pthread_mutex_lock(&(bdb_state->temp_list_lock));
        assert(tbl->next == NULL);
        tbl->next = bdb_state->temp_list;
        bdb_state->temp_list = tbl;
        Pthread_mutex_unlock(&(bdb_state->temp_list_lock));
    }

    dbgtrace(3, "temp_table_close() = %d %s", rc, db_strerror(rc));
    return rc;
}

int bdb_temp_table_destroy_lru(struct temp_table *tbl,
                               bdb_state_type *bdb_state, int *last,
                               int *bdberr)
{
    DB_MPOOL_STAT *tmp;
    int rc, ii;
    arr_elem_t *elem;

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
    tbl->next = NULL;

    *last = 0;

    /* See comments in bdb_temp_table_close(). */
    if ((tbl->temp_table_type == TEMP_TABLE_TYPE_BTREE) &&
        (tbl->dbenv_temp->memp_stat(tbl->dbenv_temp, &tmp, NULL,
                                    DB_STAT_CLEAR)) == 0) {
        bdb_state->temp_stats->st_gbytes += tmp->st_gbytes;
        bdb_state->temp_stats->st_bytes += tmp->st_bytes;
        bdb_state->temp_stats->st_total_bytes += tmp->st_total_bytes;
        bdb_state->temp_stats->st_used_bytes += tmp->st_used_bytes;
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
        ATOMIC_ADD32(bdb_state->temp_stats->st_page_dirty, tmp->st_page_dirty);
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

    case TEMP_TABLE_TYPE_ARRAY:
        for (ii = 0; ii != tbl->num_mem_entries; ++ii) {
            elem = &tbl->elements[ii];
            free(elem->key);
        }
        break;

    case TEMP_TABLE_TYPE_BTREE:
        break;
    }

    if (tbl->temp_hash_tbl != NULL)
        hash_free(tbl->temp_hash_tbl);
    free(tbl->elements);

    /* close the environments*/
    if (tbl->dbenv_temp != NULL)
        rc = bdb_temp_table_env_close(bdb_state, tbl, bdberr);

#ifdef _LINUX_SOURCE
    if (gbl_debug_temptables) {
        char *zBacktrace = get_stack_backtrace();
        logmsg(LOGMSG_USER, "closing a temp table object %p (%d): %s, %s\n",
               tbl, rc, tbl ? tbl->sql : 0, zBacktrace);
        free(zBacktrace);
    }
#endif

    if (rc == 0) {
        ATOMIC_ADD32(gbl_temptable_count, -1);
    } else {
        logmsg(LOGMSG_ERROR, "%s: bdb_temp_table_env_close(%p) rc %d\n",
               __func__, tbl, rc);
    }

    if (tbl->sql) free(tbl->sql);
    free(tbl);

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
    arr_elem_t *elem;
    struct temp_cursor *opencur;
    if (!cur->valid) {
        rc = -1;
        goto done;
    }

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_HASH) {
        // AZ: address of data returned by hash_find: cur->key - sizeof(int)
        rc = hash_del(cur->tbl->temp_hash_tbl, cur->key - sizeof(int));
        --cur->tbl->num_mem_entries;
        goto done;
    }

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_ARRAY) {
        elem = &cur->tbl->elements[cur->ind];
        free(elem->key);
        --cur->tbl->num_mem_entries;
        cur->tbl->inmemsz -= (elem->keylen + elem->dtalen);
        memmove(elem, elem + 1,
                sizeof(arr_elem_t) * (cur->tbl->num_mem_entries - cur->ind));
        /* Move backward all open cursors to the right of deletion point
         * by one position */
        LISTC_FOR_EACH(&cur->tbl->cursors, opencur, lnk)
        {
            if (opencur->ind >= cur->ind)
                --opencur->ind;
        }
        rc = 0;
        goto done;
    }

    REOPEN_CURSOR(cur);

    rc = cur->cur->c_del(cur->cur, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "c_del rc %d\n", rc);
        *bdberr = rc;
        return -1;
    }
    if (cur->tbl->num_mem_entries > 0)
        --cur->tbl->num_mem_entries;

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
    void *pKeyInfo = NULL;

    tbl = (struct temp_table *)db->app_private;

    if (dbt1->app_data)
        return -tbl->cmpfunc(NULL, dbt2->size, dbt2->data, -1, dbt1->app_data);

    /*
    struct temp_cursor *cur;
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
    int rc, cmp, lo, hi, mid, found;
    tmptbl_cmp cmpfn;
    arr_elem_t *elem;
    DBT dkey, ddata;

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_HASH) {
        return bdb_temp_table_find_hash(cur, key, keylen);
    }

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_ARRAY) {

        /* Find the 1st occurrence of `key'. If `key' is not found,
           find the smallest element greater than `key' */

        if (cur->tbl->num_mem_entries == 0) {
            cur->valid = 0;
            return IX_EMPTY;
        }

        lo = 0;
        hi = cur->tbl->num_mem_entries - 1;
        found = -1;
        cmpfn = cur->tbl->cmpfunc;

        while (lo <= hi) {
            mid = (lo + hi) >> 1;
            elem = &cur->tbl->elements[mid];
            cmp = cmpfn(NULL, elem->keylen, elem->key, keylen, key);

            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else {
                found = mid;
                hi = mid - 1;
            }
        }

        if (found != -1)
            cur->ind = found;
        else if (lo < cur->tbl->num_mem_entries)
            cur->ind = lo;
        else {
            cur->valid = 0;
            return IX_NOTFND;
        }

        COPY_KV_TO_CUR(cur);
        return 0;
    }

    REOPEN_CURSOR(cur);

    /*Pthread_setspecific(cur->tbl->curkey, cur);*/

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
    int rc, cmp, lo, hi, mid, found;
    tmptbl_cmp cmpfn;
    DBT dkey, ddata;
    int exists = 0;
    arr_elem_t *elem;
    void *keydup;

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_HASH) {
        return bdb_temp_table_find_exact_hash(cur, key, keylen);
    }

    if (cur->tbl->temp_table_type == TEMP_TABLE_TYPE_ARRAY) {

        /* Find the 1st occurrence of `key'. */

        if (cur->tbl->num_mem_entries == 0) {
            cur->valid = 0;
            return IX_EMPTY;
        }

        lo = 0;
        hi = cur->tbl->num_mem_entries - 1;
        found = -1;
        cmpfn = cur->tbl->cmpfunc;

        while (lo <= hi) {
            mid = (lo + hi) >> 1;
            elem = &cur->tbl->elements[mid];
            cmp = cmpfn(NULL, elem->keylen, elem->key, keylen, key);

            if (cmp < 0)
                lo = mid + 1;
            else if (cmp > 0)
                hi = mid - 1;
            else {
                found = mid;
                hi = mid - 1;
            }
        }

        if (found == -1) {
            cur->valid = 0;
            return IX_NOTFND;
        }

        cur->ind = found;
        COPY_KV_TO_CUR(cur);
        return 0;
    }

    REOPEN_CURSOR(cur);

    /* Make a copy of the user key */
    if ((keydup = malloc(keylen)) == NULL)
        return ENOMEM;

    memcpy(keydup, key, keylen);

    memset(&dkey, 0, sizeof(DBT));
    memset(&ddata, 0, sizeof(DBT));
    dkey.flags = ddata.flags = DB_DBT_MALLOC;
    dkey.data = keydup;
    dkey.size = keylen;

    cur->valid = 0;
    rc = cur->cur->c_get(cur->cur, &dkey, &ddata, DB_SET);

    if (rc == DB_NOTFOUND) {
        goto done;
    } else if (rc) {
        *bdberr = rc;
        rc = -1;
        goto done;
    }

    exists = 1; /* rc is 0; key was found */

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
    return (exists) ? IX_FND : (free(keydup), IX_NOTFND);
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

static int bdb_temp_table_reset_cursor(bdb_state_type *bdb_state, struct temp_cursor *cur, int *bdberr)
{
    int rc = 0;
    struct temp_table *tbl;
    tbl = cur->tbl;

    if (tbl->temp_table_type == TEMP_TABLE_TYPE_BTREE ||
        tbl->temp_table_type == TEMP_TABLE_TYPE_ARRAY) {
        if (cur->key) {
            free(cur->key);
            cur->key = NULL;
        }

        if (cur->data) {
            free(cur->data);
            cur->data = NULL;
        }

        /* A cursor on a temparray will not have `cur'. */
        if (cur->cur) {
            rc = cur->cur->c_close(cur->cur);
            if (rc) {
                *bdberr = rc;
                rc = -1;
            }
            cur->cur = NULL;
        }
    }

    return rc;
}

/* The function closes the underlying berkdb cursor, removes the temp cursor from the temp table and frees it. */
int bdb_temp_table_close_cursor(bdb_state_type *bdb_state, struct temp_cursor *cur, int *bdberr)
{
    int rc;
    struct temp_table *tbl = cur->tbl;

    listc_rfl(&tbl->cursors, cur);
    rc = bdb_temp_table_reset_cursor(bdb_state, cur, bdberr);
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

void bdb_temp_table_debug_dump(bdb_state_type *bdb_state, tmpcursor_t *cur,
                               int level)
{
    int rc = 0;
    int bdberr = 0;
    int rowid = 0;
    int keysize_sd;
    char *key_sd;
    int dtasize_sd;
    char *dta_sd;

    logmsg(level, "TMPTABLE:\n");
    rc = bdb_temp_table_first(bdb_state, cur, &bdberr);
    while (!rc) {
        key_sd = bdb_temp_table_key(cur);
        keysize_sd = bdb_temp_table_keysize(cur);
        dta_sd = bdb_temp_table_data(cur);
        dtasize_sd = bdb_temp_table_datasize(cur);

        logmsg(level, " ROW %d:\n\tkeylen=%d\n\tkey=\"", rowid, keysize_sd);
        hexdump(level, key_sd, keysize_sd);
        logmsg(level, "\"\n\tdatalen=%d\n\tdata=\"", dtasize_sd);
        hexdump(level, dta_sd, dtasize_sd);
        logmsg(level, "\"\n");

        rowid++;

        rc = bdb_temp_table_next(bdb_state, cur, &bdberr);
    }
}

inline int bdb_is_hashtable(struct temp_table *tt)
{
    return (tt->temp_table_type == TEMP_TABLE_TYPE_HASH);
}

int bdb_temp_table_maybe_set_priority_thread(bdb_state_type *bdb_state)
{
    int rc = TMPTBL_WAIT;
    if (bdb_state) {
        int wasSet = 0;
        Pthread_mutex_lock(&bdb_state->temp_list_lock);
        if (bdb_state->haspriosqlthr) {
            rc = pthread_equal(pthread_self(), bdb_state->priosqlthr)
                         ? TMPTBL_PRIORITY
                         : TMPTBL_WAIT;
        } else {
            bdb_state->haspriosqlthr = 1;
            bdb_state->priosqlthr = pthread_self();
            rc = TMPTBL_PRIORITY;
            wasSet = 1;
        }
        Pthread_mutex_unlock(&bdb_state->temp_list_lock);
        if (gbl_debug_temptables) {
            if (wasSet) {
                logmsg(LOGMSG_DEBUG, "%s: thd %p NOW HAS PRIORITY\n",
                       __func__, (void*)pthread_self());
            } else if (rc == TMPTBL_PRIORITY) {
                logmsg(LOGMSG_DEBUG, "%s: thd %p STILL HAS PRIORITY\n",
                       __func__, (void*)pthread_self());
            }
        }
    }
    return rc;
}

int bdb_temp_table_maybe_reset_priority_thread(bdb_state_type *bdb_state,
                                               int notify)
{
    int rc = 0;
    if (bdb_state) {
        if (bdb_state->haspriosqlthr &&
                pthread_equal(pthread_self(), bdb_state->priosqlthr)) {
            Pthread_mutex_lock(&(bdb_state->temp_list_lock));
            if (bdb_state->haspriosqlthr &&
                    pthread_equal(pthread_self(), bdb_state->priosqlthr)) {
                bdb_state->haspriosqlthr = 0;
                bdb_state->priosqlthr = 0;
                rc = 1; /* yes, thread was reset. */
            }
            Pthread_mutex_unlock(&(bdb_state->temp_list_lock));
            if (gbl_debug_temptables && rc) {
                logmsg(LOGMSG_DEBUG, "%s: thd %p NO LONGER HAS PRIORITY\n",
                       __func__, (void*)pthread_self());
            }
        }
        if (notify && rc) {
            int rc2 = comdb2_objpool_notify(bdb_state->temp_table_pool, 1);
            if (rc2 != 0) {
                logmsg(LOGMSG_ERROR, "%s: comdb2_objpool_notify rc=%d\n",
                       __func__, rc2);
            }
        }
    }
    return rc;
}

static int bdb_temp_table_insert_put(bdb_state_type *bdb_state,
                                     struct temp_table *tbl, void *key,
                                     int keylen, void *data, int dtalen,
                                     int *bdberr)
{
    int rc, cmp, lo, hi, mid;
    tmptbl_cmp cmpfn;
    arr_elem_t *elem;
    uint8_t *keycopy, *dtacopy;

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
            gbl_temptable_spills++;
            rc = bdb_hash_table_copy_to_temp_db(bdb_state, tbl, bdberr);
            if (unlikely(rc)) {
                return -1;
            }
        }

        return 0;
    }

    if (tbl->temp_table_type == TEMP_TABLE_TYPE_ARRAY) {

        /* Insert `key' into the sorted array.
           If 1 or more elements of the same key already exist,
           insert it after the last one of those elements. */

        keycopy = malloc(keylen + dtalen);
        if (keycopy == NULL)
            return -1;
        dtacopy = keycopy + keylen;
        memcpy(keycopy, key, keylen);
        memcpy(dtacopy, data, dtalen);

        lo = 0;
        hi = tbl->num_mem_entries - 1;
        if (hi >= 0) {
            cmpfn = tbl->cmpfunc;

            while (lo <= hi) {
                mid = (lo + hi) >> 1;
                elem = &tbl->elements[mid];
                cmp = cmpfn(NULL, elem->keylen, elem->key, keylen, key);

                if (cmp < 0)
                    lo = mid + 1;
                else if (cmp > 0)
                    hi = mid - 1;
                else
                    lo = mid + 1;
            }

            elem = &tbl->elements[lo];
            memmove(elem + 1, elem,
                    sizeof(arr_elem_t) * (tbl->num_mem_entries - lo));
        }

        elem = &tbl->elements[lo];
        elem->keylen = keylen;
        elem->key = keycopy;
        elem->dtalen = dtalen;
        elem->dta = dtacopy;

        ++tbl->num_mem_entries;
        tbl->inmemsz += (keylen + dtalen);

        if (tbl->num_mem_entries == tbl->max_mem_entries ||
            tbl->inmemsz > tbl->cachesz) {
            gbl_temptable_spills++;
            rc = bdb_array_copy_to_temp_db(bdb_state, tbl, bdberr);
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
    DB_MPOOL_STAT *sp;

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
    sp->st_total_bytes = parent->temp_stats->st_total_bytes;
    sp->st_used_bytes = parent->temp_stats->st_used_bytes;
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
    int bdberr = 0;
    struct temp_table *db = bdb_temp_table_create(parent, &bdberr);
    if (!db || bdberr) {
        logmsg(LOGMSG_ERROR, "%s: failed to create temp table bdberr=%d\n",
               __func__, bdberr);
        return -1;
    }

    struct temp_cursor *cur = bdb_temp_table_cursor(parent, db, NULL, &bdberr);
    if (!cur) {
        logmsg(LOGMSG_ERROR, "%s: failed to create cursor bdberr=%d\n",
               __func__, bdberr);
        return -1;
    }

    if (recsz < 16) recsz = 16; //force it to be min 16 bytes
    if (maxins > 10000000 || recsz * maxins > 100000000) {
        logmsg(LOGMSG_USER, "Too much data to write %d records\n", maxins);
        return -1; // limit the temptbl size
    }

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

    struct timeval t1;
    gettimeofday(&t1, NULL);

    //insert: replace first 4 bytes with a new random value, payload is same val
    for (int cnt = 0; cnt < maxins; cnt++) {
        int x = rand();
        ((int *)rkey)[0] = 123456789;
        ((int *)rkey)[1] = 123456789;
        ((int *)rkey)[2] = 123456789;
        ((int *)rkey)[3] = x;
        /* 
         * Can either use insert or put as follows:
         * rc = bdb_temp_table_put(parent, db, &rkey, sizeof(rkey),
         *                         &x, sizeof(x), NULL, &bdberr);
         */
        rc = bdb_temp_table_insert(parent, cur, &rkey, sizeof(rkey),
                                   &x, sizeof(x), &bdberr);
        if (rc) {
            logmsg(LOGMSG_ERROR, 
                    "%s: fail to put into temp tbl rc=%d bdberr=%d\n",
                    __func__, rc, bdberr);
            break; 
        }
    }

    rc = bdb_temp_table_first(parent, cur, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: first error bdberr=%d\n",
               __func__, bdberr);
        return -1;
    }

    while (rc == IX_OK) {
        uint8_t *keyp = bdb_temp_table_key(cur);
        uint8_t *datap = bdb_temp_table_data(cur);
        if (((int *)keyp)[3] != *(int *)datap)
            abort();
        rc = bdb_temp_table_next(parent, cur, &bdberr);
    }

    struct timeval t2;
    gettimeofday(&t2, NULL);

    int sec = (t2.tv_sec - t1.tv_sec) * 1000000;
    int msec = (t2.tv_usec - t1.tv_usec);
    logmsg(LOGMSG_USER, "Wrote %d records in %f sec\n", maxins,
           (float)(sec + msec) / 1000000);

    //cleanup
    rc = bdb_temp_table_close_cursor(parent, cur, &bdberr);
    rc = bdb_temp_table_close(parent, db, &bdberr);
    return rc;
}
