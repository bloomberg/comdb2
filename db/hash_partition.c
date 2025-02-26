#include "hash_partition.h"
#include <plhash_glue.h>
#include "views.h"
#include "cson/cson.h"
#include "schemachange.h"
#include <regex.h>
#include "consistent_hash.h"
#include <string.h>
#include "fdb_fend.h"
struct hash_view {
    char *viewname;
    char *tblname;
    int num_keys;
    char **keynames;
    int num_partitions;
    char **partitions;
    int version;
    ch_hash_t *partition_hash;
};

int gbl_sharding_ddl_verbose = 0;
extern char gbl_dbname[MAX_DBNAME_LENGTH];
pthread_rwlock_t hash_partition_lk;
void dump_alias_info();
const char *hash_view_get_viewname(struct hash_view *view)
{
    return view->viewname;
}

const char *hash_view_get_tablename(struct hash_view *view)
{
    return view->tblname;
}
char **hash_view_get_keynames(struct hash_view *view)
{
    return view->keynames;
}
int hash_view_get_num_partitions(struct hash_view *view)
{
    return view->num_partitions;
}
int hash_view_get_num_keys(struct hash_view *view)
{
    return view->num_keys;
}

int hash_view_get_sqlite_view_version(struct hash_view *view)
{
    return view->version;
}

char** hash_view_get_partitions(struct hash_view *view) {
    return view->partitions;
}

static void free_hash_view(hash_view_t *mView)
{
    if (mView) {
        if (mView->viewname) {
            free(mView->viewname);
        }
        if (mView->tblname) {
            free(mView->tblname);
        }
        if (mView->keynames) {
            for (int i = 0; i < mView->num_keys; i++) {
                free(mView->keynames[i]);
            }
            free(mView->keynames);
        }
        if (mView->partitions) {
            for (int i=0; i< mView->num_partitions;i++) {
                free(mView->partitions[i]);
            }
            free(mView->partitions);
        }
        if (mView->partition_hash) {
            ch_hash_free(mView->partition_hash);
        }
        free(mView);
    }
}

hash_view_t *create_hash_view(const char *viewname, const char *tablename, uint32_t num_columns,
                            char **columns, uint32_t num_partitions,
                            char **partitions, struct errstat *err)
{
    hash_view_t *mView;

    mView = (hash_view_t *)calloc(1, sizeof(hash_view_t));

    if (!mView) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate view %s\n", __func__, viewname);
        goto oom;
    }

    logmsg(LOGMSG_USER, "CREATING A VIEW WITH NAME %s\n", viewname);
    mView->viewname = strdup(viewname);
    if (!mView->viewname) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate view name string %s\n", __func__, viewname);
        goto oom;
    }

    mView->tblname = strdup(tablename);
    if (!mView->tblname) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate table name string %s\n", __func__, tablename);
        goto oom;
    }
    mView->num_keys = num_columns;
    mView->keynames = (char **)malloc(sizeof(char *) * mView->num_keys);
    if (!mView->keynames) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate keynames\n", __func__);
        goto oom;
    }

    for (int i = 0; i < mView->num_keys; i++) {
        logmsg(LOGMSG_USER, "ADDING COLUMN %s\n", columns[i]);
        mView->keynames[i] = strdup(columns[i]);
        if (!mView->keynames[i]) {
            logmsg(LOGMSG_ERROR, "%s: Failed to allocate key name string %s\n", __func__, columns[i]);
            goto oom;
        }
    }
    mView->num_partitions = num_partitions;

    mView->partitions = (char **)calloc(1, sizeof(char *) * num_partitions);
    for (int i = 0; i < num_partitions; i++) {
        logmsg(LOGMSG_USER, "ADDING PARTITION %s\n", partitions[i]);
        mView->partitions[i] = strdup(partitions[i]);
        if (!mView->partitions[i]) {
            goto oom;
        }
    }

    ch_hash_t *ch = ch_hash_create(mView->num_partitions, ch_hash_sha);
    mView->partition_hash = ch;
    if (!mView->partition_hash) {
        logmsg(LOGMSG_ERROR, "Failed create consistent_hash\n");
        goto oom;
    }

    for(int i=0;i<mView->num_partitions;i++){
        if (ch_hash_add_node(ch, (uint8_t *)mView->partitions[i], strlen(mView->partitions[i]), ch->key_hashes[i]->hash_val)) {
        logmsg(LOGMSG_ERROR, "Failed to add node %s\n", mView->partitions[i]);
            goto oom;
                }
    }

    return mView;
oom:
    free_hash_view(mView);
    errstat_set_rcstrf(err, VIEW_ERR_MALLOC, "calloc oom");
    return NULL;
}

static int create_inmem_view(hash_t *hash_views, hash_view_t *view)
{
    Pthread_rwlock_wrlock(&hash_partition_lk);
    hash_add(hash_views, view);
    Pthread_rwlock_unlock(&hash_partition_lk);
    return VIEW_NOERR;
}

static int destroy_inmem_view(hash_t *hash_views, hash_view_t *view)
{
    if (!view) {
        logmsg(LOGMSG_ERROR, "Attempting to delete NULL view\n");
        return VIEW_ERR_NOTFOUND;
    }
    Pthread_rwlock_wrlock(&hash_partition_lk);
    struct hash_view *v = hash_find_readonly(hash_views, &view->viewname);
    int rc = VIEW_NOERR;
    if (!v) {
        rc = VIEW_ERR_NOTFOUND;
        goto done;
    }
    hash_del(hash_views, v);
    free_hash_view(v);
done:
    Pthread_rwlock_unlock(&hash_partition_lk);
    return rc;
}

static int find_inmem_view(hash_t *hash_views, const char *name, hash_view_t **oView)
{
    int rc = VIEW_ERR_EXIST;
    hash_view_t *view = NULL;
    Pthread_rwlock_rdlock(&hash_partition_lk);
    view = hash_find_readonly(hash_views, &name);
    if (!view) {
        rc = VIEW_ERR_NOTFOUND;
        goto done;
    }
    if (gbl_sharding_ddl_verbose) {
        logmsg(LOGMSG_USER, "FOUND VIEW %s\n", name);
    }
    if (oView) {
        if (gbl_sharding_ddl_verbose) {
            logmsg(LOGMSG_USER, "SETTING OUT VIEW TO FOUND VIEW\n");
        }
        *oView = view;
    }
done:
    Pthread_rwlock_unlock(&hash_partition_lk);
    return rc;
}

unsigned long long hash_partition_get_partition_version(const char *name)
{
    struct dbtable *db = get_dbtable_by_name(name);
    if (!db) {
        logmsg(LOGMSG_ERROR, "Could not find partition %s\n", name);
        return VIEW_ERR_NOTFOUND;
    }

    if (gbl_sharding_ddl_verbose) {
        logmsg(LOGMSG_USER, "RETURNING PARTITION VERSION %lld\n", db->tableversion);
    }
    return db->tableversion;
}

int is_hash_partition(const char *name)
{
    struct hash_view *v = NULL;
    hash_get_inmem_view(name, &v);
    return v != NULL;
}

/* check if table is part of a hash partition*/
int is_hash_partition_table(const char *tablename, hash_view_t **oView) {
    void *ent;
    unsigned int bkt;
    hash_view_t *view = NULL;
    Pthread_rwlock_rdlock(&hash_partition_lk);
    for (view = (hash_view_t *)hash_first(thedb->hash_partition_views, &ent, &bkt);view;
            view = (hash_view_t *)hash_next(thedb->hash_partition_views, &ent, &bkt)) {
        int n = hash_view_get_num_partitions(view);
        for(int i=0;i<n;i++){
            if (strcmp(tablename, view->partitions[i])==0) goto found;
        }
    }
found:
    *oView = view;
    Pthread_rwlock_unlock(&hash_partition_lk);
    return view!=NULL;
}

unsigned long long hash_view_get_version(const char *name)
{
    struct hash_view *v = NULL;
    if (find_inmem_view(thedb->hash_partition_views, name, &v)) {
        logmsg(LOGMSG_ERROR, "Could not find partition %s\n", name);
        return VIEW_ERR_NOTFOUND;
    }

    char **partitions = hash_view_get_partitions(v);
    return hash_partition_get_partition_version(partitions[0]);
}

int hash_get_inmem_view(const char *name, hash_view_t **oView)
{
    int rc;
    if (!name) {
        logmsg(LOGMSG_ERROR, "%s: Trying to retrieve nameless view!\n", __func__);
        return VIEW_ERR_NOTFOUND;
    }
    rc = find_inmem_view(thedb->hash_partition_views, name, oView);
    if (rc != VIEW_ERR_EXIST) {
        logmsg(LOGMSG_ERROR, "%s: failed to find in-memory view %s. rc: %d\n", __func__, name, rc);
    }
    return rc;
}

int hash_create_inmem_view(hash_view_t *view)
{
    int rc;
    if (!view) {
        return VIEW_ERR_NOTFOUND;
    }
    rc = create_inmem_view(thedb->hash_partition_views, view);
    if (rc != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to create in-memory view %s. rc: %d\n", __func__, view->viewname, rc);
    }
    return rc;
}

int hash_destroy_inmem_view(hash_view_t *view)
{
    int rc;
    if (!view) {
        return VIEW_ERR_NOTFOUND;
    }
    rc = destroy_inmem_view(thedb->hash_partition_views, view);
    if (rc != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to destroy in-memory view %s. rc: %d\n", __func__, view->viewname, rc);
    }
    return rc;
}
int hash_partition_llmeta_erase(void *tran, hash_view_t *view, struct errstat *err)
{
    int rc = 0;
    const char *view_name = hash_view_get_viewname(view);
    logmsg(LOGMSG_USER, "Erasing view %s\n", view_name);
    rc = hash_views_write_view(tran, view_name, NULL, 0);
    if (rc != VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "Failed to erase llmeta entry for partition %s. rc: %d\n", view_name, rc);
    }
    ++gbl_views_gen;
    view->version = gbl_views_gen;
    return rc;
}
int hash_partition_llmeta_write(void *tran, hash_view_t *view, struct errstat *err)
{
    /*
     * Get serialized view string
     * write to llmeta
     */
    char *view_str = NULL;
    int view_str_len;
    int rc;

    rc = hash_serialize_view(view, &view_str_len, &view_str);
    if (rc != VIEW_NOERR) {
        errstat_set_strf(err, "Failed to serialize view %s", view->viewname);
        errstat_set_rc(err, rc = VIEW_ERR_BUG);
        goto done;
    }

    logmsg(LOGMSG_USER, "WRITING THE VIEWS STRING : %s TO LLMETA\n", view_str);
    /* save the view */
    rc = hash_views_write_view(tran, view->viewname, view_str, 0);
    if (rc != VIEW_NOERR) {
        if (rc == VIEW_ERR_EXIST)
            errstat_set_rcstrf(err, VIEW_ERR_EXIST, "View %s already exists", view->viewname);
        else
            errstat_set_rcstrf(err, VIEW_ERR_LLMETA, "Failed to llmeta save view %s", view->viewname);
        goto done;
    }

    ++gbl_views_gen;
    view->version = gbl_views_gen;

done:
    if (view_str)
        free(view_str);
    return rc;
}

char *hash_views_read_all_views();
/*
 * Create hash map of all hash based partitions
 *
 */
hash_t *hash_create_all_views()
{
    hash_t *hash_views;
    char *views_str;
    int rc;
    struct hash_view *view = NULL;
    cson_value *rootVal = NULL;
    cson_object *rootObj = NULL;
    cson_object_iterator iter;
    cson_kvp *kvp = NULL;
    struct errstat err;
    hash_views = hash_init_strcaseptr(offsetof(struct hash_view, viewname));
    views_str = hash_views_read_all_views();

    if (!views_str) {
        logmsg(LOGMSG_ERROR, "Failed to read hash views from llmeta\n");
        goto done;
    }

    rc = cson_parse_string(&rootVal, views_str, strlen(views_str));
    if (rc) {
        logmsg(LOGMSG_ERROR, "error parsing cson string. rc: %d err: %s\n", rc, cson_rc_string(rc));
        goto done;
    }
    if (!cson_value_is_object(rootVal)) {
        logmsg(LOGMSG_ERROR, "error parsing cson: expected object type\n");
        goto done;
    }

    rootObj = cson_value_get_object(rootVal);

    if (!rootObj) {
        logmsg(LOGMSG_ERROR, "error parsing cson: couldn't retrieve object\n");
        goto done;
    }

    cson_object_iter_init(rootObj, &iter);
    kvp = cson_object_iter_next(&iter);
    while (kvp) {
        cson_value *val = cson_kvp_value(kvp);
        /*
         * Each cson_value above is a cson string representation
         * of a hash based partition. Validate that it's a string value
         * and deserialize into an in-mem representation.
         */
        if (!cson_value_is_string(val)) {
            logmsg(LOGMSG_ERROR, "error parsing cson: expected string type\n");
            goto done;
        }
        const char *view_str = cson_string_cstr(cson_value_get_string(val));
        view = hash_deserialize_view(view_str, &err);

        if (!view) {
            logmsg(LOGMSG_ERROR, "%s: failed to deserialize hash view %d %s\n", __func__, err.errval, err.errstr);
            goto done;
        }
        /* we've successfully deserialized a view.
         * now add it to the global hash of all hash-parititon based views */
        hash_add(hash_views, view);
        kvp = cson_object_iter_next(&iter);
    }
done:
    if (rootVal) {
        cson_value_free(rootVal);
    }

    if (views_str) {
        free(views_str);
    }
    return hash_views;
}

static int hash_update_partition_version(hash_view_t *view, void *tran)
{
    int num_partitions = hash_view_get_num_partitions(view);
    char **partitions = hash_view_get_partitions(view);
    for (int i=0;i<num_partitions;i++) {
        struct dbtable *db = get_dbtable_by_name(partitions[i]);
        if (db) {
            db->tableversion = table_version_select(db, tran);
        } else {
            logmsg(LOGMSG_ERROR, "UNABLE TO LOCATE partition %s\n", partitions[i]);
            return -1;
        }
    }
    return 0;
}

int hash_views_update_replicant(void *tran, const char *name)
{
    hash_t *hash_views = thedb->hash_partition_views;
    hash_view_t *view = NULL, *v = NULL;
    int rc = VIEW_NOERR;
    if (gbl_sharding_ddl_verbose) {
        logmsg(LOGMSG_USER, "++++++ Replicant updating views\n");
    }
    char *view_str = NULL;
    struct errstat xerr = {0};

    /* read the view str from updated llmeta */
    rc = hash_views_read_view(tran, name, &view_str);
    if (rc == VIEW_ERR_EXIST) {
        view = NULL;
        goto update_view_hash;
    } else if (rc != VIEW_NOERR || !view_str) {
        logmsg(LOGMSG_ERROR, "%s: Could not read metadata for view %s\n", __func__, name);
        goto done;
    }

    if (gbl_sharding_ddl_verbose) {
        logmsg(LOGMSG_USER, "The views string is %s\n", view_str);
    }
    /* create an in-mem view object */
    view = hash_deserialize_view(view_str, &xerr);
    if (!view) {
        logmsg(LOGMSG_ERROR, "%s: failed to deserialize hash view %d %s\n", __func__, xerr.errval, xerr.errstr);
        goto done;
    }
    hash_update_partition_version(view, tran);
update_view_hash:
    /* update global hash views hash.
     * - If a view with the given name exists, destroy it, create a new view and add to hash
     * - If a view with the given name does not exist, add to hash
     * - If view is NULL (not there in llmeta), destroy the view and remove from hash */

    if (!view) {
        logmsg(LOGMSG_USER, "The deserialized view is NULL. This is a delete case \n");
        /* It's okay to do this lockless. The subsequent destroy method
         * grabs a lock and does a find again */
        v = hash_find_readonly(hash_views, &name);
        if (!v) {
            logmsg(LOGMSG_ERROR, "Couldn't find view in llmeta or in-mem hash\n");
            goto done;
        }
        rc = hash_destroy_inmem_view(v);
        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "%s:%d Failed to destroy inmem view\n", __func__, __LINE__);
        }
    } else {
        if (gbl_sharding_ddl_verbose) {
            logmsg(LOGMSG_USER, "The deserialized view is NOT NULL. this is a view create/update case \n");
        }
        rc = hash_destroy_inmem_view(view);
        if (rc != VIEW_NOERR && rc != VIEW_ERR_NOTFOUND) {
            logmsg(LOGMSG_ERROR, "%s:%d Failed to destroy inmem view\n", __func__, __LINE__);
            goto done;
        }
        rc = hash_create_inmem_view(view);
        if (rc != VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "%s:%d Failed to create inmem view\n", __func__, __LINE__);
            goto done;
        }
    }

    ++gbl_views_gen;
    if (view) {
        view->version = gbl_views_gen;
    }
    return rc;
done:
    if (view_str) {
        free(view_str);
    }
    if (view) {
        free_hash_view(view);
    }
    return rc;
}
static int getDbHndl(cdb2_hndl_tp **hndl, const char *dbname) {
    int rc;
    int local = 0, lvl_override = 0;
    int flags = 0;
    char *tier = NULL;
    const enum mach_class lvl = get_fdb_class(&dbname, &local, &lvl_override);
    if (local) {
        tier = "localhost";
        flags |= CDB2_DIRECT_CPU;
    } else {
        tier = (char *)mach_class_class2name(lvl);
    }
    if (!tier) {
        logmsg(LOGMSG_ERROR, "Failed to get tier for remotedb %s\n", dbname);
        abort();
    }
    if (gbl_sharding_ddl_verbose) {
        logmsg(LOGMSG_USER, "GOT THE TIER AS %s\n", tier);
    }
    
    rc = cdb2_open(hndl, dbname, tier, flags);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s:%d Failed to connect to %s@%s (rc: %d)\n",
                       __func__, __LINE__, dbname, tier, rc);
        cdb2_close(*hndl);
    }
    return rc;
}

/* extract everything after "create table <tblname>"
 * and before "partitioned by ("*/
char *extractSchema(const char *insertQuery) {
    const char *start = strchr(insertQuery, '('); // Find the first '('
    const char *end = strchr(insertQuery, ')');   // Find the first ')'
    char *result = NULL;
    if (start != NULL && end != NULL && end > start) {
        size_t length = end - start - 1; // Calculate the length of the substring
        result = (char *)malloc(length + 1);         // Allocate memory for the result

        strncpy(result, start + 1, length); // Copy the substring between parentheses
        result[length] = '\0';              // Null-terminate the result

        if (gbl_sharding_ddl_verbose) {
            logmsg(LOGMSG_USER, "Extracted schema : %s\n", result);
        }
    } else {
        if (gbl_sharding_ddl_verbose) {
            logmsg(LOGMSG_USER, "No match found\n");
        }
    }
    return result;
}

/*Get the tablename from a create query*/
static void getTableName(const char *insertQuery, char *tableName) {
    if (strcasestr(insertQuery, "CREATE TABLE IF NOT EXISTS ")) {
        sscanf(insertQuery + 27, "%s", tableName); 
    } else if (strcasestr(insertQuery, "CREATE TABLE ")){
        sscanf(insertQuery + 13, "%s", tableName);
    }
}

/* 
 * Generate a "create table tableName(...)" statement to be run against 
 * remote databases
 * */
static char *getCreateTableStatement(const char *insertQuery, const char *tableName) {
    /* strip away CREATESHARDS and extract schema*/
    const char *schema = extractSchema(insertQuery);
    size_t createStatementLen = 26 + strlen(tableName) + strlen(schema) + 2 + 1; /* 26 -> "CREATE TABLE IF NOT EXISTS ", 2-> (, ) */
    char *createStatement = (char *)malloc(createStatementLen);
    strcpy(createStatement, "CREATE TABLE IF NOT EXISTS ");
    strcat(createStatement, tableName);
    strcat(createStatement, "(");
    strcat(createStatement, schema);
    strcat(createStatement, ")");
    if (gbl_sharding_ddl_verbose) {
        logmsg(LOGMSG_USER, "The create statement is %s\n", createStatement);
    }
    return createStatement;
}

static char *getDropStatement(const char *tableName) {
    size_t dropStatementLen = 21 + strlen(tableName) + 1;  /* 21-> "DROP TABLE IF EXISTS "*/
    char *dropStatement = (char *)malloc(dropStatementLen);
    strcpy(dropStatement, "DROP TABLE IF EXISTS ");
    strcat(dropStatement, tableName);
    if (gbl_sharding_ddl_verbose) {
        logmsg(LOGMSG_USER, "The drop statemetn is %s\n", dropStatement);
    }
    return dropStatement;
}

int deleteRemotePartitions(const char *viewName, char **tables, int startIdx) {
    cdb2_hndl_tp *hndl;
    int rc;
    int i;
    char remoteDbName[MAX_DBNAME_LENGTH],remoteTableName[MAXTABLELEN];
    char *deletePartition = getDropStatement(viewName);
    if (deletePartition == NULL) {
        logmsg(LOGMSG_ERROR, "Failed to generate drop statement for partition %s on database %s\n", viewName, remoteDbName);
        return -1;
    }
    for(i = startIdx; i >= 0; i--) {
        memset(remoteDbName, 0, MAX_DBNAME_LENGTH);
        memset(remoteTableName, 0, MAXTABLELEN);
        rc = sscanf(tables[i], "%[^.].%[^.]", remoteDbName, remoteTableName);
        if (strlen(remoteTableName)==0) {
            strncpy(remoteTableName, remoteDbName, sizeof(remoteTableName));
            strncpy(remoteDbName, gbl_dbname, sizeof(remoteDbName));
        }
        if (gbl_sharding_ddl_verbose) {
            logmsg(LOGMSG_USER, "The db is %s, the table is %s\n", remoteDbName, remoteTableName);
        }

        /*
         * Don't drop the local partition here.
         * It will happen as part of schemachange
         */
        if (strcmp(gbl_dbname, remoteDbName)) {
            rc = getDbHndl(&hndl, remoteDbName);
            if (rc) {
                logmsg(LOGMSG_ERROR, "Failed to get handle. rc: %d, err: %s\n", rc, cdb2_errstr(hndl));
                logmsg(LOGMSG_ERROR, "Failed to drop table partition %s on remote db %s\n", remoteDbName, remoteTableName);
                goto close_handle;
            }
            /* now delete the partition metadata */
            rc = cdb2_run_statement(hndl, deletePartition);
            if (rc) {
                logmsg(LOGMSG_ERROR, "Failed to drop partition . rc: %d, err: %s\n", rc, cdb2_errstr(hndl));
                goto close_handle;
            }
        }
    }
    free(deletePartition);
    return 0;
close_handle:
    cdb2_close(hndl);
    free(deletePartition);
    return -1;
}
/*
 */
int deleteRemoteTables(const char *viewName, char **tables, int startIdx) {
    cdb2_hndl_tp *hndl;
    int rc;
    int i;
    char remoteDbName[MAX_DBNAME_LENGTH],remoteTableName[MAXTABLELEN];
    for(i = startIdx; i >= 0; i--) {
        memset(remoteDbName, 0, MAX_DBNAME_LENGTH);
        memset(remoteTableName, 0, MAXTABLELEN);
        rc = sscanf(tables[i], "%[^.].%[^.]", remoteDbName, remoteTableName);
        if (strlen(remoteTableName)==0) {
            strncpy(remoteTableName, remoteDbName, sizeof(remoteTableName));
            strncpy(remoteDbName, gbl_dbname, sizeof(remoteDbName));
        }
        if (gbl_sharding_ddl_verbose) {
            logmsg(LOGMSG_USER, "The db is %s, the table is %s\n", remoteDbName, remoteTableName);
        }

        /* delete the tables locally as well
         */
        rc = getDbHndl(&hndl, remoteDbName);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to get handle. rc: %d, err: %s\n", rc, cdb2_errstr(hndl));
            logmsg(LOGMSG_ERROR, "Failed to drop table %s on remote db %s\n", remoteDbName, remoteTableName);
            goto close_handle;
        }
        char *dropStatement = getDropStatement(remoteTableName);
        if (!dropStatement) {
            logmsg(LOGMSG_ERROR, "Failed to generate drop Query\n");
            logmsg(LOGMSG_ERROR, "Failed to drop table %s on remote db %s\n", remoteDbName, remoteTableName);
            goto close_handle;
        } else {
            if (gbl_sharding_ddl_verbose) {
                logmsg(LOGMSG_USER, "The generated drop statement is %s\n", dropStatement);
            }
        }

        rc = cdb2_run_statement(hndl, dropStatement);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to drop table %s on database %s. rc: %d, err: %s\n", remoteTableName, remoteDbName, rc, cdb2_errstr(hndl));
            goto close_handle;
        }
        /* now delete the partition metadata */
        free(dropStatement);
        cdb2_close(hndl);
    }
    return 0;
close_handle:
    cdb2_close(hndl);
    return -1;
}

int createRemotePartitions(struct comdb2_partition *partition) {
    cdb2_hndl_tp *hndl;
    int rc;
    int num_partitions = partition->u.hash.num_partitions;
    int i;
    char remoteDbName[MAX_DBNAME_LENGTH],remoteTableName[MAXTABLELEN];
    for (i = 0; i < num_partitions; i++) {
        memset(remoteDbName, 0, MAX_DBNAME_LENGTH);
        memset(remoteTableName, 0, MAXTABLELEN);
        rc = sscanf(partition->u.hash.partitions[i], "%[^.].%[^.]", remoteDbName, remoteTableName);
        if (strlen(remoteTableName)==0) {
            strncpy(remoteTableName, remoteDbName, sizeof(remoteTableName));
            strncpy(remoteDbName, gbl_dbname, sizeof(remoteDbName));
        }

        if (gbl_sharding_ddl_verbose) {
            logmsg(LOGMSG_USER, "The db is %s, the table is %s\n", remoteDbName, remoteTableName);
        }

        /* don't create partition on locally
         * It'll be done as part of the current schemachange
         */
        if (strcmp(gbl_dbname, remoteDbName)) {
            rc = getDbHndl(&hndl, remoteDbName);
            if (rc) {
                goto cleanup_partitions;
            }
            /* Now setup catalog information on remote databases*/
            int length = strlen(partition->u.hash.createQuery);
            /* COPY EVERYTHING OTHER THAN CREATEPARTITIONS */
            char metadataQuery[length-16+1];
            rc = snprintf(metadataQuery, sizeof(metadataQuery), "%s", partition->u.hash.createQuery);
            if (gbl_sharding_ddl_verbose) {
                logmsg(LOGMSG_USER, "THE METADATA QUERY IS %s\n", metadataQuery);
            }
            rc = cdb2_run_statement(hndl, metadataQuery);
            if (rc) {
                logmsg(LOGMSG_ERROR, "Failed to setup partition metatdata on database %s. rc: %d, err: %s\n", remoteDbName, rc, cdb2_errstr(hndl));
                goto cleanup_partitions;
            }
            cdb2_close(hndl);
        }
    }
    return 0;
cleanup_partitions:
    cdb2_close(hndl);
    /* TODO: drop partitions created of the shards above*/
    return -1;
}


int createRemoteTables(struct comdb2_partition *partition) {
    cdb2_hndl_tp *hndl;
    int rc;
    int num_partitions = partition->u.hash.num_partitions;
    int i;
    char remoteDbName[MAX_DBNAME_LENGTH],remoteTableName[MAXTABLELEN];
    for (i = 0; i < num_partitions; i++) {
        memset(remoteDbName, 0, MAX_DBNAME_LENGTH);
        memset(remoteTableName, 0, MAXTABLELEN);
        rc = sscanf(partition->u.hash.partitions[i], "%[^.].%[^.]", remoteDbName, remoteTableName);
        if (strlen(remoteTableName)==0) {
            strncpy(remoteTableName, remoteDbName, sizeof(remoteTableName));
            strncpy(remoteDbName, gbl_dbname, sizeof(remoteDbName));
        }

        if (gbl_sharding_ddl_verbose) {
            logmsg(LOGMSG_USER, "The db is %s, the table is %s\n", remoteDbName, remoteTableName);
        }

        rc = getDbHndl(&hndl, remoteDbName);
        if (rc) {
            goto cleanup_tables;
        }
        char *createStatement = getCreateTableStatement(partition->u.hash.createQuery, remoteTableName);
        if (!createStatement) {
            logmsg(LOGMSG_ERROR, "Failed to generate createQuery\n");
        } else {
            if (gbl_sharding_ddl_verbose) {
                logmsg(LOGMSG_USER, "The generated create statement is %s\n", createStatement);
            }
        }

        rc = cdb2_run_statement(hndl, createStatement);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to create table %s on database %s. rc: %d, err: %s\n", remoteTableName, remoteDbName, rc, cdb2_errstr(hndl));
            goto cleanup_tables;
        }

        cdb2_close(hndl);
    }
    /*truncate the CREATETABLES phrase from the create statement */
    int len = strlen(partition->u.hash.createQuery);
    /* chop off ' CREATETABLES'*/
    partition->u.hash.createQuery[len-13] = '\0';
    return 0;
cleanup_tables:
    /* close most recent handle*/
    cdb2_close(hndl);
    char table[MAXTABLELEN] = {0};
    getTableName(partition->u.hash.createQuery, table);
    if (strlen(table)==0) {
        logmsg(LOGMSG_ERROR, "Failed to extract tablename. Not cleaning up remote databases!\n");
        return -1;
    }
    deleteRemoteTables(table,(char **)partition->u.hash.partitions, i);
    return -1;
}

int remove_alias(const char *);
void deleteLocalAliases(struct comdb2_partition *partition, int startIdx) {
    int i, rc;
    cdb2_hndl_tp *hndl;
    char *savePtr = NULL, *remoteDbName = NULL, *remoteTableName = NULL, *p = NULL;
    /* get handle to local db*/
    rc = getDbHndl(&hndl, gbl_dbname);
    if (rc) {
        goto fatal;
    }
    for(i = startIdx; i >= 0; i--) {
        p = strdup(partition->u.hash.partitions[i]);
        remoteDbName = strtok_r(p,".", &savePtr);
        remoteTableName = strtok_r(NULL, ".", &savePtr);
        if (remoteTableName == NULL) {
            remoteTableName = remoteDbName;
            remoteDbName = gbl_dbname;
        }

        if (gbl_sharding_ddl_verbose) {
            logmsg(LOGMSG_USER, "The db is %s, the table is %s\n", remoteDbName, remoteTableName);
        }

        char localAlias[MAXPARTITIONLEN+1]; /* remoteDbName_remoteTableName */
        char deleteAliasStatement[MAXPARTITIONLEN*2+3+10+1]; /* 3->spaces 8->'PUT ALIAS' 1 */
        rc = snprintf(localAlias, sizeof(localAlias), "%s_%s", remoteDbName, remoteTableName);
        if (!rc) {
            logmsg(LOGMSG_ERROR, "Failed to generate local alias name. rc: %d %s\n", rc, localAlias);
            goto fatal;
        } else {
            logmsg(LOGMSG_ERROR, "The generated alias is %s\n", localAlias);
        }
        rc = snprintf(deleteAliasStatement,sizeof(deleteAliasStatement), "PUT ALIAS %s ''", localAlias);
        // drop the alias
        if (!rc) {
            logmsg(LOGMSG_ERROR, "Failed to generate delete alias query\n");
            goto fatal;
        } else {
            if (gbl_sharding_ddl_verbose) {
                logmsg(LOGMSG_USER, "The generated delete statement for Alias is %s\n", deleteAliasStatement);
            }
        }

        rc = cdb2_run_statement(hndl, deleteAliasStatement);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to delete alias %s on database %s. rc: %d, err: %s\n", remoteTableName, remoteDbName, rc, cdb2_errstr(hndl));
            goto fatal;
        }
        free(p);
    }
    return;
fatal:
    cdb2_close(hndl);
    logmsg(LOGMSG_FATAL, "Failed to drop alias while undoing create partition failure!! Giving up..\n");
}

int createLocalAliases(struct comdb2_partition *partition) {
    cdb2_hndl_tp *hndl;
    int rc;
    int num_partitions = partition->u.hash.num_partitions;
    int i=0;
    char *savePtr = NULL, *remoteDbName = NULL, *remoteTableName = NULL, *p = NULL;
    /* get handle to local db*/
    rc = getDbHndl(&hndl, gbl_dbname);
    if (rc) {
        goto cleanup_aliases;
    }
    for (i = 0; i < num_partitions; i++) {
        p = strdup(partition->u.hash.partitions[i]);
        remoteDbName = strtok_r(p,".", &savePtr);
        remoteTableName = strtok_r(NULL, ".", &savePtr);
        if (remoteTableName == NULL) {
            remoteTableName = remoteDbName;
            remoteDbName = gbl_dbname;
        }

        logmsg(LOGMSG_USER, "The db is %s, the table is %s\n", remoteDbName, remoteTableName);

        char localAlias[MAXPARTITIONLEN+1]; /* remoteDbName_remoteTableName */
        char createAliasStatement[MAXPARTITIONLEN*2+3+10+1]; /* 3->spaces 8->'PUT ALIAS' */
        rc = snprintf(localAlias, sizeof(localAlias), "%s_%s", remoteDbName, remoteTableName);
        if (!rc) {
            logmsg(LOGMSG_ERROR, "Failed to generate local alias name. rc: %d %s\n", rc, localAlias);
            goto cleanup_aliases;
        } else {
            if (gbl_sharding_ddl_verbose) {
                logmsg(LOGMSG_USER, "The generated alias is %s\n", localAlias);
            }
        }
        rc = snprintf(createAliasStatement,sizeof(createAliasStatement), "PUT ALIAS %s '%s.%s'", localAlias, remoteDbName, remoteTableName);
        if (!rc) {
            logmsg(LOGMSG_ERROR, "Failed to generate create alias query\n");
            goto cleanup_aliases;
        } else {
            if (gbl_sharding_ddl_verbose) {
                logmsg(LOGMSG_USER, "The generated create statement for Alias is %s\n", createAliasStatement);
            }
        }

        rc = cdb2_run_statement(hndl, createAliasStatement);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Failed to create alias %s on database %s. rc: %d, err: %s\n", remoteTableName, remoteDbName, rc, cdb2_errstr(hndl));
            goto cleanup_aliases;
        }
        free(p);
    }
    // dump_alias_info();
    cdb2_close(hndl);
    return 0;
cleanup_aliases:
    /* close most recent handle*/
    cdb2_close(hndl);
    free(p);
    deleteLocalAliases(partition, i);
    return -1;
}

static int hash_views_collect(void *obj, void *arg) {
    struct systable_hashpartitions *recs = (struct systable_hashpartitions *)arg;
    hash_view_t *view = (hash_view_t *)obj;
    if (!view) {
        logmsg(LOGMSG_ERROR, "VIEW IS NULL\n");
        return 0;
    }
    struct systable_hashpartitions *elem = NULL;
    ch_hash_t *ch = view->partition_hash;
    int64_t prevMax = 0;
    char *dta = NULL;
    int64_t firstRecno = recs->recno;
    for(int i=0;i<ch->num_keyhashes;i++){
        elem = &recs[recs->recno++];
        elem->name = strdup((char *)view->viewname);
        dta = (char *)ch->key_hashes[i]->node->data;
        elem->shardname = strdup(dta);
        elem->minKey = prevMax+1;
        elem->maxKey = ch->key_hashes[i]->hash_val;
        prevMax = elem->maxKey;
    }

    /* set the min key value for the first shard
     * as prevMax + 1. (circular range)
     */
    elem = &recs[firstRecno];
    elem->minKey = prevMax+1;
    return 0;
}
static int hash_views_calculate_size(void *obj, void *arg) {
    int *nrecs = (int *)arg;
    hash_view_t *view = (hash_view_t *)obj;
    *nrecs = *nrecs + view->num_partitions;
    return 0;
}
int hash_systable_collect(void **data, int *nrecords) {
    hash_t *views = NULL;
    struct systable_hashpartitions *recs = NULL;
    int nrecs = 0, rc = 0;
    Pthread_rwlock_rdlock(&hash_partition_lk);
    if (thedb->hash_partition_views == NULL) {
        goto done;
    }
    views = thedb->hash_partition_views;
    hash_for(views, hash_views_calculate_size, &nrecs);
    recs = calloc(nrecs , sizeof(struct systable_hashpartitions));
    if (!recs) {
        rc = -1;
        goto done;
    }
    hash_for(views, hash_views_collect, recs); 
done:
    Pthread_rwlock_unlock(&hash_partition_lk);
    *nrecords = nrecs;
    *data = recs;
    return rc;
}
void hash_systable_free(void *data, int nrecords) {
    struct systable_hashpartitions *recs = (struct systable_hashpartitions *)data;
    for (int i=0; i<nrecords; i++) {
        if (recs[i].name) free(recs[i].name);
        if (recs[i].shardname) free(recs[i].shardname);
    }
    // free(recs);
}
