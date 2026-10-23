/*
   Copyright 2026 Bloomberg Finance L.P.

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
 * The tables a modsnap snapshot sees, when a schema change committed after
 * its start LSN.
 *
 * The catalog of such a snapshot is read from llmeta as of the start LSN.  A
 * table whose csc2 version and file versions are the same now is the current
 * table.  Any other table is opened again from its csc2 and its files as of
 * the start LSN: the files of a dropped or rebuilt table stay on disk until
 * the log that removed them is deleted, and the snapshot holds that log.  A
 * table created after the start LSN is not in the catalog.
 *
 * sqlite sees the catalog through its own sqlite_master entries, and
 * get_dbtable_by_name() returns its tables on the sql thread of the snapshot.
 * The snapshot cannot write an old table.
 *
 * The views and the time partitions of the catalog are read from llmeta as
 * of the start LSN too.  A rollout that truncates a shard gives the shard new files, so
 * the snapshot reads the shard as an old table.  A truncate partition made
 * from an existing table names that table after its first shard in sql (see
 * timepart_views_sqlalias()); an old table gets the name as of the start
 * LSN.
 */

#include <inttypes.h>

#include "comdb2.h"
#include "sql.h"
#include "tag.h"
#include "macc_glue.h"
#include "comdb2_atomic.h"
#include "snap_catalog.h"
#include "views.h"
#include <bdb_api.h>
#include <logmsg.h>
#include <sys_wrap.h>

extern pthread_mutex_t csc2_subsystem_mtx;

/* An old table: a table as a snapshot saw it before a schema change */
struct snap_table {
    struct snap_table *next;
    char *name;
    int dbopen_gen;
    struct dbtable *db;
    int refs;
};

struct snap_catalog_ent {
    char *name;
    char *alias; /* sql name, if not name */
    struct snap_table *old; /* NULL: the current table */
};

struct snap_catalog {
    uint64_t id;
    int64_t sc_gen;  /* sc_gen when built */
    int dbopen_gen;
    int nents;       /* 0: the snapshot sees the current tables */
    struct snap_catalog_ent *ents;
    master_entry_t *master;
    int nmaster;
    timepart_views_t *views; /* the time partitions */
    hash_t *view_hash;       /* the views */
};

/* every committed schema change is before sc_lsn; unknown while
 * sc_lsn_known is 0 */
static pthread_mutex_t sc_lsn_lk = PTHREAD_MUTEX_INITIALIZER;
static int sc_lsn_known;
static unsigned int sc_lsn_file, sc_lsn_offset;
static int64_t sc_gen = 1;

static pthread_mutex_t snap_tables_lk = PTHREAD_MUTEX_INITIALIZER;
static struct snap_table *snap_tables;
static uint64_t snap_seq;

void snap_catalog_schema_changed(void)
{
    Pthread_mutex_lock(&sc_lsn_lk);
    sc_lsn_known = 0;
    sc_gen++;
    Pthread_mutex_unlock(&sc_lsn_lk);
}

/* Return 1 if a schema change may have committed after the LSN.  Call with
 * the schema lock held for read: a schema change that made sc_lsn unknown
 * committed before it released the lock, so the end of the log is after
 * it. */
static int sc_after_lsn(uint32_t file, uint32_t offset, int64_t *gen)
{
    Pthread_mutex_lock(&sc_lsn_lk);
    if (!sc_lsn_known) {
        bdb_get_current_lsn(thedb->bdb_env, &sc_lsn_file, &sc_lsn_offset);
        sc_lsn_known = 1;
    }
    int after = file < sc_lsn_file || (file == sc_lsn_file && offset < sc_lsn_offset);
    *gen = sc_gen;
    Pthread_mutex_unlock(&sc_lsn_lk);
    return after;
}

/* 1 if db has the sql name alias (NULL: its own name), the schema csc2 and
 * the files that llmeta names through tran.  The csc2 version alone does not
 * tell: a table without instant schema change keeps version 1 through an
 * index change. */
static int same_table(tran_type *tran, struct dbtable *db, const char *alias, const char *csc2)
{
    unsigned long long vers;
    int bdberr;

    if (db->sqlaliasname ? alias == NULL || strcasecmp(db->sqlaliasname, alias) != 0 : alias != NULL)
        return 0;
    if (db->csc2_schema == NULL || strcmp(db->csc2_schema, csc2) != 0)
        return 0;
    for (int i = 0; i < db->numblobs + 1; i++) {
        if (bdb_get_file_version_data_by_name(tran, db->tablename, i, &vers, &bdberr) < 0 ||
            vers != bdb_get_open_file_version(db->handle, 1, i))
            return 0;
    }
    for (int i = 0; i < db->nix; i++) {
        if (bdb_get_file_version_index_by_name(tran, db->tablename, i, &vers, &bdberr) < 0 ||
            vers != bdb_get_open_file_version(db->handle, 0, i))
            return 0;
    }
    return 1;
}

static void close_old_table(struct dbtable *db)
{
    if (db->handle)
        bdb_close_snapshot(db->handle);
    db->handle = NULL;
    delete_schema(db->tag_tablename);
    free(db->tag_tablename);
    db->tag_tablename = NULL;
    freedb(db);
    free(db);
}

/* Open table name, with sql name alias, schema csc2 and version csc2_vers,
 * as llmeta names it through tran */
static struct dbtable *open_old_table(tran_type *tran, const char *name, const char *alias, const char *csc2,
                                      int csc2_vers)
{
    struct dbtable *db = NULL;
    char tagname[MAXTABLELEN];
    int bdberr, compr = 0, compr_blobs = 0, datacopy_odh = 0;

    /* the tags of the table are under a name of its own */
    snprintf(tagname, sizeof(tagname), "$snap%" PRIu64, ATOMIC_ADD64(snap_seq, 1));
    struct errstat err = {0};
    Pthread_mutex_lock(&csc2_subsystem_mtx);
    db = create_new_dbtable(thedb, tagname, (char *)csc2, 0, 0, 1, 0, &err);
    Pthread_mutex_unlock(&csc2_subsystem_mtx);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "%s: table %s version %d: %s\n", __func__, name, csc2_vers, err.errstr);
        return NULL;
    }
    db->tag_tablename = db->tablename;
    db->tablename = strdup(name);
    if (alias)
        db->sqlaliasname = strdup(alias);
    db->schema_version = csc2_vers;

    db->handle = bdb_open_snapshot(name, thedb->basedir, db->lrl, db->nix, (short *)db->ix_keylen, db->ix_dupes,
                                   db->ix_recnums, db->ix_datacopy, db->ix_datacopylen, db->ix_collattr,
                                   db->ix_nullsallowed, db->numblobs + 1, thedb->bdb_env, tran, &bdberr);
    if (db->handle == NULL) {
        logmsg(LOGMSG_ERROR, "%s: table %s: cannot open old files, bdberr %d\n", __func__, name, bdberr);
        goto err;
    }

    if (get_db_odh_tran(db, &db->odh, tran) == 0 && db->odh) {
        get_db_compress_tran(db, &compr, tran);
        get_db_compress_blobs_tran(db, &compr_blobs, tran);
        get_db_instant_schema_change_tran(db, &db->instant_schema_change, tran);
        get_db_inplace_updates_tran(db, &db->inplace_updates, tran);
        get_db_datacopy_odh_tran(db, &datacopy_odh, tran);
    } else {
        db->odh = 0;
        db->instant_schema_change = 0;
        db->inplace_updates = 0;
    }
    set_bdb_option_flags(db, db->odh, db->inplace_updates, db->instant_schema_change, db->schema_version, compr,
                         compr_blobs, datacopy_odh);

    if (load_csc2_versions(db, tran)) {
        logmsg(LOGMSG_ERROR, "%s: table %s: cannot load schema versions\n", __func__, name);
        goto err;
    }
    update_dbstore(db);
    if (bdb_table_version_select(name, tran, &db->tableversion, &bdberr))
        db->tableversion = 0;

    if (create_datacopy_array(db) || create_sqlmaster_record(db, NULL)) {
        logmsg(LOGMSG_ERROR, "%s: table %s: cannot create sql schema\n", __func__, name);
        goto err;
    }
    return db;

err:
    close_old_table(db);
    return NULL;
}

/* Return a reference to table name as llmeta names it through tran */
static struct snap_table *get_old_table(tran_type *tran, const char *name, const char *alias, const char *csc2,
                                        int csc2_vers)
{
    const int dbopen_gen = bdb_get_dbopen_gen();
    struct snap_table *st;

    Pthread_mutex_lock(&snap_tables_lk);
    for (st = snap_tables; st; st = st->next) {
        if (strcasecmp(st->name, name) == 0 && st->dbopen_gen == dbopen_gen && same_table(tran, st->db, alias, csc2)) {
            st->refs++;
            break;
        }
    }
    Pthread_mutex_unlock(&snap_tables_lk);
    if (st)
        return st;

    struct dbtable *db = open_old_table(tran, name, alias, csc2, csc2_vers);
    if (db == NULL)
        return NULL;
    st = calloc(1, sizeof(*st));
    st->name = strdup(name);
    st->dbopen_gen = dbopen_gen;
    st->db = db;
    st->refs = 1;

    Pthread_mutex_lock(&snap_tables_lk);
    st->next = snap_tables;
    snap_tables = st;
    Pthread_mutex_unlock(&snap_tables_lk);

    logmsg(LOGMSG_INFO, "%s: opened table %s version %d as of a snapshot\n", __func__, name, csc2_vers);
    return st;
}

/* An open old table keeps its files open: close it when the last snapshot
 * that reads it ends, so the files can be removed */
static void put_old_table(struct snap_table *st)
{
    struct snap_table **p;

    Pthread_mutex_lock(&snap_tables_lk);
    if (--st->refs > 0) {
        Pthread_mutex_unlock(&snap_tables_lk);
        return;
    }
    for (p = &snap_tables; *p != st; p = &(*p)->next)
        ;
    *p = st->next;
    Pthread_mutex_unlock(&snap_tables_lk);

    close_old_table(st->db);
    free(st->name);
    free(st);
}

/* Make cat the current tables */
static void clear_catalog(struct snap_catalog *cat)
{
    for (int i = 0; i < cat->nents; i++) {
        free(cat->ents[i].name);
        free(cat->ents[i].alias);
        if (cat->ents[i].old)
            put_old_table(cat->ents[i].old);
    }
    free(cat->ents);
    cat->ents = NULL;
    cat->nents = 0;
    destroy_sqlite_master(cat->master, cat->nmaster);
    cat->master = NULL;
    cat->nmaster = 0;
    timepart_views_free(cat->views);
    cat->views = NULL;
    if (cat->view_hash)
        free_view_hash(cat->view_hash);
    cat->view_hash = NULL;
}

void snap_catalog_free(struct snap_catalog **pcat)
{
    struct snap_catalog *cat = *pcat;
    if (cat == NULL)
        return;
    clear_catalog(cat);
    free(cat);
    *pcat = NULL;
}

/* 1 if the views a and b have the same names and definitions */
static int same_views(hash_t *a, hash_t *b)
{
    void *ent;
    unsigned int bkt;

    if (hash_get_num_entries(a) != hash_get_num_entries(b))
        return 0;
    for (struct dbview *v = hash_first(a, &ent, &bkt); v; v = hash_next(a, &ent, &bkt)) {
        struct dbview *w = hash_find_readonly(b, &v->view_name);
        if (w == NULL || strcmp(v->view_def, w->view_def) != 0)
            return 0;
    }
    return 1;
}

static int build_catalog(struct sqlclntstate *clnt, struct snap_catalog *cat)
{
    char **names = calloc(MAX_NUM_TABLES, sizeof(char *));
    int *dbnums = calloc(MAX_NUM_TABLES, sizeof(int));
    struct dbtable **dbs = NULL;
    int ntables = 0, changed = 0, nlive = 0, rc = -1, bdberr;

    tran_type *tran = bdb_tran_begin(thedb->bdb_env, NULL, &bdberr);
    if (names == NULL || dbnums == NULL || tran == NULL)
        goto done;
    bdb_tran_set_llmeta_snap(tran, clnt->modsnap_start_lsn_file, clnt->modsnap_start_lsn_offset,
                             clnt->last_checkpoint_lsn_file, clnt->last_checkpoint_lsn_offset);

    if (bdb_llmeta_get_tables(tran, names, dbnums, MAX_NUM_TABLES, &ntables, &bdberr) || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "%s: cannot read the tables as of %u:%u, bdberr %d\n", __func__,
               clnt->modsnap_start_lsn_file, clnt->modsnap_start_lsn_offset, bdberr);
        goto done;
    }

    cat->ents = calloc(ntables, sizeof(*cat->ents));
    dbs = calloc(ntables, sizeof(*dbs));
    if (ntables && (cat->ents == NULL || dbs == NULL))
        goto done;

    cat->views = views_create_all_views_tran(tran);
    cat->view_hash = llmeta_read_views(tran);
    if (cat->views == NULL || cat->view_hash == NULL)
        goto done;

    for (int i = 0; i < ntables; i++) {
        struct snap_catalog_ent *ent = &cat->ents[cat->nents];
        char *csc2 = NULL;
        int csc2_vers;

        if (bdb_get_csc2_highest(tran, names[i], &csc2_vers, &bdberr) || bdberr != BDBERR_NOERROR ||
            get_csc2_file_tran(names[i], csc2_vers, &csc2, NULL, tran))
            goto done;

        ent->name = strdup(names[i]);
        cat->nents++;

        if (bdb_get_table_sqlalias_tran(names[i], tran, &ent->alias) < 0) {
            free(csc2);
            goto done;
        }
        if (ent->alias == NULL) {
            const char *alias = timepart_views_sqlalias(cat->views, names[i]);
            if (alias)
                ent->alias = strdup(alias);
        }

        struct dbtable *live = get_live_dbtable_by_name(names[i]);
        if (live && strcasecmp(live->tablename, names[i]) == 0 && same_table(tran, live, ent->alias, csc2)) {
            free(csc2);
            dbs[i] = live;
            nlive++;
            continue;
        }

        changed = 1;
        ent->old = get_old_table(tran, names[i], ent->alias, csc2, csc2_vers);
        free(csc2);
        if (ent->old == NULL) {
            logmsg(LOGMSG_ERROR, "%s: cannot open table %s as of %u:%u\n", __func__, names[i],
                   clnt->modsnap_start_lsn_file, clnt->modsnap_start_lsn_offset);
            goto done;
        }
        dbs[i] = ent->old->db;
    }

    /* tables created after the start LSN */
    if (nlive != thedb->num_dbs)
        changed = 1;
    if (!same_views(cat->view_hash, thedb->view_hash))
        changed = 1;

    if (changed) {
        cat->master = create_master_entry_array(dbs, ntables, cat->view_hash, &cat->nmaster);
        if (cat->master == NULL)
            goto done;
    } else {
        clear_catalog(cat);
    }
    rc = 0;

done:
    if (tran)
        bdb_tran_abort(thedb->bdb_env, tran, &bdberr);
    for (int i = 0; names && i < ntables; i++)
        free(names[i]);
    free(names);
    free(dbnums);
    free(dbs);
    return rc;
}

void snap_catalog_prepare(struct sqlclntstate *clnt, uint64_t *id)
{
    struct snap_catalog *cat = clnt->dbtran.snap_catalog;
    int64_t gen;

    *id = 0;
    if (clnt->dbtran.mode != TRANLEVEL_SNAPISOL || !clnt->modsnap_in_progress)
        return;
    if (!sc_after_lsn(clnt->modsnap_start_lsn_file, clnt->modsnap_start_lsn_offset, &gen))
        return;

    if (cat && cat->sc_gen == gen && cat->dbopen_gen == bdb_get_dbopen_gen()) {
        *id = cat->nents ? cat->id : 0;
        return;
    }

    /* build without the old catalog, so lookups see the current tables; free
     * it after, so the old tables both use stay open */
    clnt->dbtran.snap_catalog = NULL;
    struct snap_catalog *newcat = calloc(1, sizeof(*newcat));
    if (newcat) {
        newcat->id = ATOMIC_ADD64(snap_seq, 1);
        newcat->sc_gen = gen;
        newcat->dbopen_gen = bdb_get_dbopen_gen();
        if (build_catalog(clnt, newcat)) {
            /* read the current tables until the next schema change: a table
             * changed since the start LSN fails the statement (see
             * check_snapshot_table()) */
            clear_catalog(newcat);
        }
    }
    snap_catalog_free(&cat);
    clnt->dbtran.snap_catalog = newcat;
    if (newcat && newcat->nents)
        *id = newcat->id;
}

int snap_catalog_copy_rootpages(struct sqlclntstate *clnt, struct sql_thread *thd)
{
    struct snap_catalog *cat = clnt->dbtran.snap_catalog;
    return get_copy_rootpages_custom(thd, cat->master, cat->nmaster);
}

timepart_views_t *snap_catalog_views(struct sqlclntstate *clnt)
{
    struct snap_catalog *cat = clnt->dbtran.snap_catalog;
    return cat && cat->nents ? cat->views : NULL;
}

int snap_catalog_find_table(const char *name, struct dbtable **db)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd == NULL || thd->clnt == NULL)
        return 0;
    struct snap_catalog *cat = thd->clnt->dbtran.snap_catalog;
    if (cat == NULL || cat->nents == 0)
        return 0;

    for (int i = 0; i < cat->nents; i++) {
        if (strcasecmp(cat->ents[i].name, name) == 0 ||
            (cat->ents[i].alias && strcasecmp(cat->ents[i].alias, name) == 0)) {
            *db = cat->ents[i].old ? cat->ents[i].old->db : get_live_dbtable_by_name(cat->ents[i].name);
            return 1;
        }
    }
    if (strcmp(name, COMDB2_STATIC_TABLE) == 0)
        return 0;
    *db = NULL;
    return 1;
}

int snap_catalog_find_view(const char *name, struct dbview **view)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (thd == NULL || thd->clnt == NULL)
        return 0;
    struct snap_catalog *cat = thd->clnt->dbtran.snap_catalog;
    if (cat == NULL || cat->nents == 0)
        return 0;
    *view = hash_find_readonly(cat->view_hash, &name);
    return 1;
}

int snap_catalog_is_old_table(const struct dbtable *db)
{
    return db->tag_tablename != NULL;
}
