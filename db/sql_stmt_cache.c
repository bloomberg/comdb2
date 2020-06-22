/*
   Copyright 2020 Bloomberg Finance L.P.

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

#include <sqliteInt.h>
#include "sql_stmt_cache.h"
#include "sql.h"
#include "lrucache.h"

int gbl_max_sqlcache = 10;
int gbl_enable_sql_stmt_caching = STMT_CACHE_ALL;

extern int gbl_debug_temptables;

static int stmt_cache_finalize_entry_int(void *stmt_entry, void *args)
{
    stmt_cache_entry_t *entry = (stmt_cache_entry_t *)stmt_entry;
    sqlite3_finalize(entry->stmt);
    if (entry->query && gbl_debug_temptables) {
        free(entry->query);
        entry->query = NULL;
    }
    sqlite3_free(entry);
    return 0;
}

/* Teardown statement cache */
int stmt_cache_delete(stmt_cache_t *stmt_cache)
{
    assert(stmt_cache && stmt_cache->hash);
    /* iterate through the hash table and finalize all the statements */
    hash_for(stmt_cache->hash, stmt_cache_finalize_entry_int, NULL);
    hash_clear(stmt_cache->hash);
    hash_free(stmt_cache->hash);
    return 0;
}

static int strcmpfunc_stmt(char *a, char *b, int len)
{
    return strcmp(a, b);
}

static u_int strhashfunc_stmt(u_char *keyp, int len)
{
    unsigned hash;
    u_char *key = keyp;
    for (hash = 0; *key; key++)
        hash = ((hash % 8388013) << 8) + ((*key));
    return hash;
}

/* Initialize the specified stmt cache object and/or return a new one. */
stmt_cache_t *stmt_cache_new(stmt_cache_t *in_stmt_cache)
{
    stmt_cache_t *stmt_cache;

    stmt_cache = (in_stmt_cache) ? in_stmt_cache : malloc(sizeof(stmt_cache_t));
    if (!stmt_cache) {
        logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__, __LINE__);
        return NULL;
    }

    stmt_cache->hash = hash_init_user(
        (hashfunc_t *)strhashfunc_stmt, (cmpfunc_t *)strcmpfunc_stmt,
        offsetof(stmt_cache_entry_t, sql), MAX_HASH_SQL_LENGTH);
    if (!stmt_cache->hash) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to initialized stmt_cache\n",
               __func__, __LINE__);
        free(stmt_cache);
        return NULL;
    }
    listc_init(&(stmt_cache->param_stmt_list),
               offsetof(stmt_cache_entry_t, lnk));
    listc_init(&(stmt_cache->noparam_stmt_list),
               offsetof(stmt_cache_entry_t, lnk));
    return stmt_cache;
}

/* Requeue a stmt that was previously removed from the queues by calling
 * stmt_cache_remove_entry(). Called by put_prepared_stmt_int() after we are
 * done running stmt and by add_stmt_table() after it allocates the new entry.
 */
static int stmt_cache_requeue_entry(stmt_cache_t *stmt_cache,
                                    stmt_cache_entry_t *entry)
{
    if (hash_find(stmt_cache->hash, entry->sql) != NULL) {
        return -1; // already there, don't add again
    }

    if (hash_add(stmt_cache->hash, entry) != 0) {
        return -1;
    }

    int rc = sqlite3_reset(entry->stmt); // reset vdbe when adding to hash tbl
    assert(rc == SQLITE_OK);
    if (rc != SQLITE_OK) {
        logmsg(LOGMSG_ERROR, "%s:%d sqlite3_reset(%p) error, rc = %d\n",
               __func__, __LINE__, entry->stmt, rc);
    }
    rc = sqlite3_clear_bindings(entry->stmt);
    assert(rc == SQLITE_OK);
    if (rc != SQLITE_OK) {
        logmsg(LOGMSG_ERROR,
               "%s:%d sqlite3_clear_bindings(%p) error, rc = %d\n", __func__,
               __LINE__, entry->stmt, rc);
    }

    void *list;
    if (sqlite3_bind_parameter_count(entry->stmt)) {
        list = &stmt_cache->param_stmt_list;
    } else {
        list = &stmt_cache->noparam_stmt_list;
    }
    listc_atl(list, entry);

    return 0;
}

static void stmt_cache_free_entry(stmt_cache_entry_t *entry)
{
    if (entry->query) {
        free(entry->query);
        entry->query = NULL;
    }
    sqlite3_free(entry);
}

static int stmt_cache_finalize_entry(stmt_cache_entry_t *entry)
{
    sqlite3_finalize(entry->stmt);
    stmt_cache_free_entry(entry);
    return 0;
}

static int stmt_cache_delete_last_entry(stmt_cache_t *stmt_cache, void *list)
{
    int rc;
    stmt_cache_entry_t *entry = listc_rbl(list);
    rc = hash_del(stmt_cache->hash, entry);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to delete entry (rc: %d)\n",
               __func__, __LINE__, rc);
    }
    stmt_cache_finalize_entry(entry);
    return rc;
}

/* Remove from queue and stmt_cache->hash this entry so that subsequent finds of
 * the same sql will not find it but rather create a new stmt (to avoid having
 * stmt vdbe used by two sql at the same time). */
static int stmt_cache_remove_entry(stmt_cache_t *stmt_cache,
                                   stmt_cache_entry_t *entry, int noComplain)
{
    assert(entry);

    void *list;
    if (sqlite3_bind_parameter_count(entry->stmt)) {
        list = &stmt_cache->param_stmt_list;
    } else {
        list = &stmt_cache->noparam_stmt_list;
    }

    listc_maybe_rfl(list, entry);
    int rc = hash_del(stmt_cache->hash, entry->sql);
    if (!noComplain && rc) {
        logmsg(LOGMSG_ERROR, "%s:%d failed to delete entry (rc: %d)\n",
               __func__, __LINE__, rc);
        return rc;
    }
    return 0;
}

/* This will call stmt_cache_requeue_entry() after it has allocated memory for
 * the new entry. On error will return non zero and caller will need to
 * finalize_stmt(). */
int stmt_cache_add_entry(stmt_cache_t *stmt_cache, const char *sql,
                         const char *actual_sql, sqlite3_stmt *stmt)
{
    if (strlen(sql) >= MAX_HASH_SQL_LENGTH) {
        return -1;
    }

    assert(stmt_cache->hash);

    /* stored procedure can call same stmt from a lua thread more than once so
     * we should not add stmt that exists already */
    if (hash_find(stmt_cache->hash, sql) != NULL) {
        return -1;
    }

    void *list = NULL;
    if (sqlite3_bind_parameter_count(stmt)) {
        list = &stmt_cache->param_stmt_list;
    } else {
        list = &stmt_cache->noparam_stmt_list;
    }

    /* remove older entries to make room for new ones */
    if (gbl_max_sqlcache <= listc_size(list)) {
        stmt_cache_delete_last_entry(stmt_cache, list);
    }

    stmt_cache_entry_t *entry = sqlite3_malloc(sizeof(stmt_cache_entry_t));
    strncpy(entry->sql, sql, MAX_HASH_SQL_LENGTH - 1);
    entry->stmt = stmt;

    if (actual_sql && gbl_debug_temptables)
        entry->query = strdup(actual_sql);
    else
        entry->query = NULL;

    return stmt_cache_requeue_entry(stmt_cache, entry);
}

int stmt_cache_find_entry(stmt_cache_t *stmt_cache, const char *sql,
                          stmt_cache_entry_t **entry)
{
    if (stmt_cache->hash == NULL)
        return -1;

    if (strlen(sql) >= MAX_HASH_SQL_LENGTH)
        return -1;

    *entry = hash_find(stmt_cache->hash, sql);

    if (*entry == NULL)
        return -1;

    stmt_cache_remove_entry(stmt_cache, *entry, 0); // will add again when done

    return 0;
}

int stmt_cache_reset(stmt_cache_t *stmt_cache)
{
    if (!stmt_cache)
        return 0;

    stmt_cache_delete(stmt_cache);
    if (!stmt_cache_new(stmt_cache)) {
        return 1;
    }
    return 0;
}

/** Table which stores sql strings and sql hints
 * We will hit this table if the thread running the queries from
 * certain sql control changes.
 **/

lrucache *sql_hints = NULL;

/* sql_hint/sql_str/tag all point to mem (tag can also be NULL) */
typedef struct {
    char *sql_hint;
    char *sql_str;
    lrucache_link lnk;
    char mem[0];
} sql_hint_hash_entry_type;

void delete_sql_hint_table()
{
    lrucache_destroy(sql_hints);
}

static unsigned int sqlhint_hash(const void *p, int len)
{
    unsigned char *s;
    unsigned h = 0;

    memcpy(&s, p, sizeof(char *));

    while (*s) {
        h = ((h % 8388013) << 8) + (*s);
        s++;
    }
    return h;
}

int sqlhint_cmp(const void *key1, const void *key2, int len)
{
    char *s1, *s2;
    memcpy(&s1, key1, sizeof(char *));
    memcpy(&s2, key2, sizeof(char *));
    return strcmp((char *)s1, (char *)s2);
}

void init_sql_hint_table()
{
    sql_hints = lrucache_init(sqlhint_hash, sqlhint_cmp, free,
                              offsetof(sql_hint_hash_entry_type, lnk),
                              offsetof(sql_hint_hash_entry_type, sql_hint),
                              sizeof(char *), gbl_max_sql_hint_cache);
}

void reinit_sql_hint_table()
{
    Pthread_mutex_lock(&gbl_sql_lock);
    {
        delete_sql_hint_table();
        init_sql_hint_table();
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
}

static int has_sql_hint_table(char *sql_hint)
{
    int ret;
    Pthread_mutex_lock(&gbl_sql_lock);
    {
        ret = lrucache_hasentry(sql_hints, &sql_hint);
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
    return ret;
}

#define SQLCACHEHINT "/*+ RUNCOMDB2SQL"

int has_sqlcache_hint(const char *sql, const char **pstart, const char **pend)
{
    char *start, *end;
    start = strstr(sql, SQLCACHEHINT);
    if (pstart)
        *pstart = start;
    if (start) {
        end = strstr(start, " */");
        if (pend)
            *pend = end;
        if (end) {
            end += 3;
            if (pend)
                *pend = end;
            return 1;
        }
    }
    return 0;
}

int extract_sqlcache_hint(const char *sql, char *hint, int hintlen)
{
    const char *start = NULL;
    const char *end = NULL;
    int length;
    int ret;

    ret = has_sqlcache_hint(sql, &start, &end);

    if (ret) {
        length = end - start;
        if (length >= hintlen) {
            logmsg(LOGMSG_WARN, "Query has very long hint! \"%s\"\n", sql);
            length = hintlen - 1;
        }
        strncpy(hint, start, length);
        hint[length] = '\0';
    }
    return ret;
}

static int find_sql_hint_table(char *sql_hint, char **sql_str)
{
    sql_hint_hash_entry_type *entry;
    Pthread_mutex_lock(&gbl_sql_lock);
    {
        entry = lrucache_find(sql_hints, &sql_hint);
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
    if (entry) {
        *sql_str = entry->sql_str;
        return 0;
    }
    return -1;
}

static void add_sql_hint_table(char *sql_hint, char *sql_str)
{
    int sql_hint_len = strlen(sql_hint) + 1;
    int sql_len = strlen(sql_str) + 1;
    int len = sql_hint_len + sql_len;
    sql_hint_hash_entry_type *entry = malloc(sizeof(*entry) + len);

    entry->sql_hint = entry->mem;
    memcpy(entry->sql_hint, sql_hint, sql_hint_len);

    entry->sql_str = entry->sql_hint + sql_hint_len;
    memcpy(entry->sql_str, sql_str, sql_len);

    Pthread_mutex_lock(&gbl_sql_lock);
    {
        if (lrucache_hasentry(sql_hints, &sql_hint) == 0) {
            lrucache_add(sql_hints, entry);
        } else {
            free(entry);
            logmsg(LOGMSG_ERROR,
                   "Client BUG: Two threads using same SQL tag.\n");
        }
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
}

static void dump_sql_hint_entry(void *item, void *p)
{
    int *count = (int *)p;
    sql_hint_hash_entry_type *entry = (sql_hint_hash_entry_type *)item;

    logmsg(LOGMSG_USER, "%d hit %d ref %d   %s  => %s\n", *count,
           entry->lnk.hits, entry->lnk.ref, entry->sql_hint, entry->sql_str);
    (*count)++;
}

void sql_dump_hints(void)
{
    int count = 0;
    Pthread_mutex_lock(&gbl_sql_lock);
    {
        lrucache_foreach(sql_hints, dump_sql_hint_entry, &count);
    }
    Pthread_mutex_unlock(&gbl_sql_lock);
}

int stmt_cache_get(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                   struct sql_state *rec, int prepFlags)
{
    rec->status = CACHE_DISABLED;
    if (gbl_enable_sql_stmt_caching == STMT_CACHE_NONE)
        return 0;
    if (gbl_enable_sql_stmt_caching == STMT_CACHE_PARAM &&
        param_count(clnt) == 0)
        return 0;
    if (extract_sqlcache_hint(rec->sql, rec->cache_hint, HINT_LEN)) {
        rec->status = CACHE_HAS_HINT;
        if (stmt_cache_find_entry(thd->stmt_cache, rec->cache_hint,
                                  &rec->stmt_entry) == 0) {
            rec->status |= CACHE_FOUND_STMT;
            rec->stmt = rec->stmt_entry->stmt;
        } else {
            /* We are not able to find the statement in cache, and this is a
             * partial statement. Try to find sql string stored in hash table */
            if (find_sql_hint_table(rec->cache_hint, (char **)&rec->sql) == 0) {
                rec->status |= CACHE_FOUND_STR;
            }
        }
    } else {
        if (stmt_cache_find_entry(thd->stmt_cache, rec->sql,
                                  &rec->stmt_entry) == 0) {
            rec->status = CACHE_FOUND_STMT;
            rec->stmt = rec->stmt_entry->stmt;
        }
    }

    if (rec->stmt) {
        rec->sql = sqlite3_sql(rec->stmt); // save expanded query
        if ((prepFlags & PREPARE_ONLY) == 0) {
            int rc = sqlite3LockStmtTables(rec->stmt);
            if (rc) {
                stmt_cache_finalize_entry(rec->stmt_entry);
                rec->stmt = NULL;
            }
        }
    }
    return 0;
}

/* This is called at the time of put_prepared_stmt_int()
 * to determine whether the given sql should be cached.
 * We should not cache ddl stmts, analyze commands,
 * rebuild commands, truncate commands, explain commands.
 * Ddl stmts and explain commands should not get to
 * put_prepared_stmt_int() so are not handled in this function.
 * However, most of these cases are now handled via the custom
 * authorizer callback.  This function only needs to handle the
 * EXPLAIN case.
 */
static inline int dont_cache_this_sql(struct sql_state *rec)
{
    return sqlite3_stmt_isexplain(rec->stmt);
}

/* return code of 1 means we encountered an error and the caller
 * needs to cleanup this rec->stmt */
int stmt_cache_put(struct sqlthdstate *thd, struct sqlclntstate *clnt,
                   struct sql_state *rec, int noCache, int outrc,
                   int distributed)
{
    if (noCache) {
        goto cleanup;
    }
    if (gbl_enable_sql_stmt_caching == STMT_CACHE_NONE) {
        goto cleanup;
    }
    if (distributed || clnt->conns || clnt->plugin.state) {
        goto cleanup;
    }
    if (thd && thd->authState.numDdls > 0) { /* NOTE: Never cache DDL. */
        goto cleanup;
    }
    if (dont_cache_this_sql(rec)) {
        goto cleanup;
    }
    sqlite3_stmt *stmt = rec->stmt;
    if (stmt == NULL) {
        goto cleanup;
    }
    if (gbl_enable_sql_stmt_caching == STMT_CACHE_PARAM &&
        param_count(clnt) == 0) {
        goto cleanup;
    }
    if (bdb_attr_get(thedb->bdb_attr, BDB_ATTR_DISABLE_CACHING_STMT_WITH_FDB) &&
        sqlite3_stmt_has_remotes(stmt)) {
        goto cleanup;
    }
    if (rec->stmt_entry != NULL) { /* we found this stmt in the cache */
        if (stmt_cache_requeue_entry(
                thd->stmt_cache, rec->stmt_entry)) /* put back in queue... */
            stmt_cache_finalize_entry(
                rec->stmt_entry); /* ...and on error, cleanup */

        return 0;
    }

    /* this is a new stmt (never was in cache before) so create cache object */
    const char *sqlptr = clnt->sql;
    if (rec->sql)
        sqlptr = rec->sql;

    if (rec->status & CACHE_HAS_HINT) {
        sqlptr = rec->cache_hint;
        if (!(rec->status & CACHE_FOUND_STR)) {
            add_sql_hint_table(rec->cache_hint, clnt->sql);
        }
    }

    if ((rec->status & CACHE_HAS_HINT) && (rec->status & CACHE_FOUND_STR)) {
        char *k = rec->cache_hint;
        Pthread_mutex_lock(&gbl_sql_lock);
        {
            lrucache_release(sql_hints, &k);
        }
        Pthread_mutex_unlock(&gbl_sql_lock);
    }

    return stmt_cache_add_entry(thd->stmt_cache, sqlptr,
                                gbl_debug_temptables ? rec->sql : NULL, stmt);
cleanup:
    if (rec->stmt_entry != NULL) {
        stmt_cache_remove_entry(thd->stmt_cache, rec->stmt_entry, 1);
        stmt_cache_free_entry(rec->stmt_entry);
        rec->stmt_entry = NULL;
    }

    if ((rec->status & CACHE_HAS_HINT) && (rec->status & CACHE_FOUND_STR)) {
        char *k = rec->cache_hint;
        Pthread_mutex_lock(&gbl_sql_lock);
        {
            lrucache_release(sql_hints, &k);
        }
        Pthread_mutex_unlock(&gbl_sql_lock);
    }

    return 1;
}
