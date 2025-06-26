#include <plhash_glue.h>
#include "views.h"
#include "cson/cson.h"
#include "schemachange.h"
#include <regex.h>
#include "consistent_hash.h"
#include <string.h>
#include "fdb_fend.h"
#include "gen_shard.h"

#define LLMETA_GENERIC_SHARD "gen_shard"
int gbl_gen_shard_verbose = 0;
const char *cson_extract_str(cson_object *cson_obj, const char *param,
                                     struct errstat *err);
int cson_extract_int(cson_object *cson_obj, const char *param,
                             struct errstat *err);
cson_array *cson_extract_array(cson_object *cson_obj, const char *param,
                                       struct errstat *err);
int views_sqlite_del_view(const char *view_name, sqlite3 *db,
                           struct errstat *err);
char *describe_row(const char *tblname, const char *prefix,
                           enum views_trigger_op op_type, struct errstat *err);



int gen_shard_serialize_shard(const char *tablename, uint32_t numdbs, char **dbnames, uint32_t numcols,
									char **columns, char **shardnames, uint32_t *outLen, char **out)
{
	cson_value *rootVal = NULL, *arrVal = NULL;
	cson_object *rootObj = NULL;
	int rc;
	rootVal = cson_value_new_object();
	rootObj = cson_value_get_object(rootVal);

	rc = cson_object_set(rootObj, "TABLENAME", cson_value_new_string(tablename, strlen(tablename)));
	if (rc) {
		goto err;
	}
	rc = cson_object_set(rootObj, "NUMDBS", cson_value_new_integer(numdbs));
	if (rc) {
		goto err;
	}
	arrVal = cson_value_new_array();
	for (int i = 0; i < numdbs; i++) {
		rc = cson_array_append(cson_value_get_array(arrVal), cson_value_new_string(dbnames[i], strlen(dbnames[i])));
		if (rc) {
			goto err;
		}
	}
	rc = cson_object_set(rootObj, "DBNAMES", arrVal);
	if (rc) {
		goto err;
	}

	rc = cson_object_set(rootObj, "NUMCOLS", cson_value_new_integer(numcols));
	if (rc) {
		goto err;
	}

	arrVal = cson_value_new_array();
	for (int i = 0; i < numcols; i++) {
		rc = cson_array_append(cson_value_get_array(arrVal), cson_value_new_string(columns[i], strlen(columns[i])));
		if (rc) {
			goto err;
		}
	}
	rc = cson_object_set(rootObj, "COLUMNS", arrVal);
	if (rc) {
		goto err;
	}

	arrVal = cson_value_new_array();
	for (int i = 0; i < numdbs; i++) {
		rc = cson_array_append(cson_value_get_array(arrVal), cson_value_new_string(shardnames[i], strlen(shardnames[i])));
		if (rc) {
			goto err;
		}
	}

	rc = cson_object_set(rootObj, "SHARDNAMES", arrVal);
	if (rc) {
		goto err;
	}
	cson_buffer buf;
	rc = cson_output_buffer(rootVal, &buf);
	if (rc != 0) {
		logmsg(LOGMSG_ERROR, "%s cson_output_buffer error. rc: %d\n", __func__, rc);
		goto err;
	} else {
		*out = strndup((char *)buf.mem, buf.used);
		*outLen = strlen(*out);
        if (gbl_gen_shard_verbose) {
            logmsg(LOGMSG_USER, "The serialized CSON string is %s\n", *out);
        }
	}
	cson_value_free(rootVal);
	return 0;

err:
	return -1;
}


int gen_shard_add(tran_type *tran, struct dbtable *tbl,
                  char *tablename, uint32_t numdbs, char **dbnames, char **shardnames,
                  uint32_t numcols, char **columns, struct errstat *err)
{
    char *serializedStr = NULL;
	uint32_t serializedStrLen = 0;
	int rc = 0, bdberr = 0;

    tbl->partition.genshard_name = strdup(tablename);
    if (!tbl->partition.genshard_name) {
        errstat_set_rcstrf(err, rc = -1, "malloc %s:%d", __func__, __LINE__);
        goto done;
    }
    tbl->partition.numdbs = numdbs;
    tbl->partition.dbnames = (char **)calloc(tbl->partition.numdbs, sizeof(char*));
    if (!tbl->partition.dbnames) {
        errstat_set_rcstrf(err, rc = -1, "malloc %s:%d", __func__, __LINE__);
        goto done;
    }
    for (int i = 0; i < tbl->partition.numdbs; i++) {
        tbl->partition.dbnames[i] = strdup(dbnames[i]);
        if (!tbl->partition.dbnames[i]) {
            errstat_set_rcstrf(err, rc = -1, "malloc %s:%d", __func__, __LINE__);
            goto done;
        }
    }
    tbl->partition.shardnames = (char **)calloc(tbl->partition.numdbs, sizeof(char*));
    if (!tbl->partition.shardnames) {
        errstat_set_rcstrf(err, rc = -1, "malloc %s:%d", __func__, __LINE__);
        goto done;
    }
    for (int i = 0; i < tbl->partition.numdbs; i++) {
        tbl->partition.shardnames[i] = strdup(shardnames[i]);
        if (!tbl->partition.shardnames[i]) {
            errstat_set_rcstrf(err, rc = -1, "malloc %s:%d", __func__, __LINE__);
            goto done;
        }
    }
    tbl->partition.numcols = numcols;
    tbl->partition.columns = (char **)calloc(tbl->partition.numcols, sizeof(char*));
    if (!tbl->partition.columns) {
        errstat_set_rcstrf(err, rc = -1, "malloc %s:%d", __func__, __LINE__);
        goto done;
    }
    for (int i = 0; i < tbl->partition.numcols; i++) {
        tbl->partition.columns[i] = strdup(columns[i]);
        if (!tbl->partition.columns[i]) {
            errstat_set_rcstrf(err, rc = -1, "malloc %s:%d", __func__, __LINE__);
            goto done;
        }
    }

    rc = gen_shard_serialize_shard(tablename, numdbs, dbnames, numcols, columns, shardnames,
            &serializedStrLen, &serializedStr);
    if (rc) {
		logmsg(LOGMSG_ERROR, "Failed to serialize partition. %d\n", rc);
		goto done;
	}

    if (gbl_gen_shard_verbose) {
        logmsg(LOGMSG_USER, "THE SERIALIZED STRING IS  %s\n", serializedStr);
        logmsg(LOGMSG_USER, "WRITING TO LLMETA FOR GENERIC SHARD %s\n", tablename);
    }
    rc = bdb_set_genshard(tran, tablename, serializedStr, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "FAILED TO SET LLMETA FOR SHARDED TABLE %s rc %d bdberr %d\n",
               tablename, rc, bdberr);
    }
done:
	if (serializedStr){
		free(serializedStr);
	}
    if (rc) {
        gen_shard_rem_inmem_tbl(tbl);
    }
	return rc;
}

int gen_shard_rem(tran_type *tran, char *tablename, struct errstat *err)
{
    struct dbtable *tbl = get_dbtable_by_name(tablename);
    int rc = 0, bdberr = 0;

    assert(tbl);

    rc = bdb_set_genshard(tran, tablename, NULL, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "FAILED TO REMOVE SHARD INFO FOR TABLE %s rc %d bdberr %d\n",
               tablename, rc, bdberr);
        goto done;
    }

    gen_shard_rem_inmem_tbl(tbl);

done:
    return rc;
}

int gen_shard_deserialize_shard(char **genshard_name, uint32_t *numdbs, char ***dbnames, char ***shardnames,
                                uint32_t *numcols, char ***columns, char *serializedStr)
{
    cson_object *rootObj = NULL;
    cson_value *rootVal = NULL, *arrVal = NULL;
    cson_array *dbs_arr = NULL, *cols_arr = NULL, *shards_arr = NULL;
    char *tablename = NULL;
    char **dbs = NULL, **cols = NULL, **shards = NULL;
    char *err_str = NULL;
    int num_dbs = 0, num_cols = 0;
    int rc;

    struct errstat err;
    /* parse string */
    rc = cson_parse_string(&rootVal, serializedStr, strlen(serializedStr));
    if (rc) {
        logmsg(LOGMSG_ERROR, "Parsing JSON error rc=%d err:%s\n", rc, cson_rc_string(rc));
        goto error;
    }

    rc = cson_value_is_object(rootVal);
    rootObj = cson_value_get_object(rootVal);

    tablename = (char *)cson_extract_str(rootObj, "TABLENAME", &err);
    if (!tablename) {
        err_str = "INVALID CSON. Couldn't find 'TABLENAME' key";
        goto error;
    }

    num_dbs = cson_extract_int(rootObj, "NUMDBS", &err);
    if (num_dbs < 0) {
        err_str = "INVALID CSON. couldn't find 'NUMDBS' key";
        goto error;
    }

    dbs_arr = cson_extract_array(rootObj, "DBNAMES", &err);
    if (!dbs_arr) {
        err_str = "INVALID CSON. couldn't find 'DBNAMES' key";
        goto error;
    }

    dbs = (char**)malloc(sizeof(char*) * num_dbs);
    if (!dbs) {
        err_str = "OOM. Couldn't allocate dbnames";
        goto error;
    }
    for (int i = 0; i < num_dbs; i++) {
        arrVal = cson_array_get(dbs_arr, i);
        if (!cson_value_is_string(arrVal)) {
            err_str = "INVALID CSON. Array element is not a string";
            goto error;
        }
        dbs[i] = strdup(cson_value_get_cstr(arrVal));
    }

    num_cols = cson_extract_int(rootObj, "NUMCOLS", &err);
    if (num_cols < 0) {
        err_str = "INVALID CSON. couldn't find 'NUMCOLS' key";
        goto error;
    }

    cols_arr = cson_extract_array(rootObj, "COLUMNS", &err);
    if (!cols_arr) {
        err_str = "INVALID CSON. couldn't find 'COLUMNS' key";
        goto error;
    }

    cols = (char**)malloc(sizeof(char*) * num_cols);
    if (!cols) {
        err_str = "OOM. Couldn't allocate columns";
        goto error;
    }
    for (int i = 0; i < num_cols; i++) {
        arrVal = cson_array_get(cols_arr, i);
        if (!cson_value_is_string(arrVal)) {
            err_str = "INVALID CSON. Array element is not a string";
            goto error;
        }
        cols[i] = strdup(cson_value_get_cstr(arrVal));
    }

    shards_arr = cson_extract_array(rootObj, "SHARDNAMES", &err);
    if (!shards_arr) {
        err_str = "INVALID CSON. couldn't find 'SHARDNAMES' key";
        goto error;
    }
    shards = (char**)malloc(sizeof(char*) * num_dbs);
    if (!shards) {
        err_str = "OOM. Couldn't allocate shards";
        goto error;
    }
    for (int i = 0; i < num_dbs; i++) {
        arrVal = cson_array_get(shards_arr, i);
        if (!cson_value_is_string(arrVal)) {
            err_str = "INVALID CSON. Array element is not a string";
            goto error;
        }
        shards[i] = strdup(cson_value_get_cstr(arrVal));
    }
    *genshard_name = strdup(tablename);
    *numdbs = num_dbs;
    *dbnames = dbs;
    *numcols = num_cols;
    *columns = cols;
    *shardnames = shards;
    return 0;

error:
    if (err_str) {
        logmsg(LOGMSG_ERROR, "%s\n", err_str);
    }

    if (dbs) {
        for(int i=0;i<num_dbs;i++){
            free(dbs[i]);
        }
        free(dbs);
    }

    if (cols) {
        for(int i=0;i<num_cols;i++) {
            free(cols[i]);
        }
        free(cols);
    }

    if (shards) {
        for(int i=0;i<num_dbs;i++){
            free(shards[i]);
        }
        free(shards);
    }
    if (rootVal) {
        cson_value_free(rootVal);
    }
    return -1;
}

int gen_shard_add_inmem_tbl(void *tran, struct dbtable *tbl, const char *name)
{
    char *serializedStr = NULL;
    uint32_t numdbs = 0, numcols = 0;
    char **dbnames = NULL, **columns = NULL, **shardnames = NULL, *genshard_name = NULL;
    int rc = 0, bdberr = 0, size = 0;

    rc = bdb_get_genshard(tran, name, &serializedStr, &size, &bdberr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Failed to read from llmeta for table %s\n", name);
        goto done;
    }

    rc = gen_shard_deserialize_shard(&genshard_name,&numdbs, &dbnames, &shardnames, &numcols, &columns, serializedStr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Failed to deserialized llmeta str for table %s\n", name);
        goto done;
    }

    /*update the table object*/
    tbl->partition.genshard_name = genshard_name;
    tbl->partition.numdbs = numdbs;
    tbl->partition.dbnames = dbnames;
    tbl->partition.numcols = numcols;
    tbl->partition.columns = columns;
    tbl->partition.shardnames = shardnames;
done:
    if (serializedStr) {
        free(serializedStr);
    }
    return rc;
}

void gen_shard_rem_inmem_tbl(struct dbtable *tbl)
{
    int i;
    assert(tbl);

    free(tbl->partition.genshard_name);
    tbl->partition.genshard_name = NULL;

    for(i=0; i < tbl->partition.numdbs; i++) {
        free(tbl->partition.dbnames[i]);
        free(tbl->partition.shardnames[i]);
    }
    free(tbl->partition.dbnames);
    tbl->partition.dbnames = NULL;
    free(tbl->partition.shardnames);
    tbl->partition.shardnames = NULL;
    tbl->partition.numdbs = 0;

    for(i=0; i < tbl->partition.numcols; i++){
        free(tbl->partition.columns[i]);
    }
    free(tbl->partition.columns);
    tbl->partition.columns = NULL;
    tbl->partition.numcols = 0;
}

char * _create_view_query(struct dbtable *tbl, sqlite3 *db, struct errstat *err)
{
    char *select_str = NULL;
    char *cols_str = NULL;
    char *tmp_str = NULL;
    char *ret_str = NULL;
    int numshards = tbl->partition.numdbs;
    const char *viewname = tbl->partition.genshard_name;
    char **dbnames = tbl->partition.dbnames;
    char **shardnames = tbl->partition.shardnames;
    int i;
    cols_str = sqlite3_mprintf("rowid as __hidden__rowid, ");
    if (!cols_str) {
        goto malloc;
    }

    cols_str = describe_row(tbl->tablename, cols_str, VIEWS_TRIGGER_QUERY, err);
    if (!cols_str) {
        /* preserve error, if any */
        if (err->errval != VIEW_NOERR)
            return NULL;
        goto malloc;
    } else {
        if (gbl_gen_shard_verbose) {
            logmsg(LOGMSG_USER, "GOT cols_str as %s\n", cols_str);
        }
    }

    select_str = sqlite3_mprintf("");
    i = 0;
    logmsg(LOGMSG_USER, "num shards is : %d\n", numshards);
    for(;i<numshards;i++){
        tmp_str = sqlite3_mprintf("%s%sSELECT %s FROM %s.'\%s'", select_str, (i > 0) ? " UNION ALL " : "", cols_str,
                                  dbnames[i], shardnames[i]);
        sqlite3_free(select_str);
        if (!tmp_str) {
            sqlite3_free(cols_str);
            goto malloc;
        }
        select_str = tmp_str;
    }

    ret_str = sqlite3_mprintf("CREATE VIEW %w AS %s", viewname, select_str);
    if (!ret_str) {
        sqlite3_free(select_str);
        sqlite3_free(cols_str);
        goto malloc;
    }

    sqlite3_free(select_str);
    sqlite3_free(cols_str);

    if (gbl_gen_shard_verbose) {
        logmsg(LOGMSG_USER, "THE GENERATED VIEW QUERY IS %s\n", ret_str);
    }

    return ret_str;

malloc:
    err->errval = VIEW_ERR_MALLOC;
    snprintf(err->errstr, sizeof(err->errstr), "View %s out of memory\n", viewname);
    return NULL;
}

int _run_sql(sqlite3 *db, char *stmt, struct errstat *err)
{
    char *errstr = NULL;
    int rc;

    /* create the view */
    rc = sqlite3_exec(db, stmt, NULL, NULL, &errstr);
    if (rc != SQLITE_OK) {
        err->errval = VIEW_ERR_BUG;
        snprintf(err->errstr, sizeof(err->errstr), "Sqlite error \"%s\"", errstr);
        /* can't control sqlite errors */
        err->errstr[sizeof(err->errstr) - 1] = '\0';

        logmsg(LOGMSG_USER, "%s: sqlite error \"%s\" sql \"%s\"\n", __func__, errstr, stmt);

        if (errstr)
            sqlite3_free(errstr);
        return err->errval;
    }

    return VIEW_NOERR;
}

int _add_view(struct dbtable *tbl, sqlite3 *db, struct errstat *err) 
{
    char *stmt_str;
    int rc;

    /* create the statement */
    stmt_str = _create_view_query(tbl, db, err);
    if (!stmt_str) {
        return err->errval;
    }

    rc = _run_sql(db, stmt_str, err);

    if (gbl_gen_shard_verbose) {
        logmsg(LOGMSG_USER, "+++++++++++sql: %s, rc: %d\n", stmt_str, rc);
    }

    /* free the statement */
    sqlite3_free(stmt_str);

    if (rc != VIEW_NOERR) {
        return err->errval;
    }
    return VIEW_NOERR;
}

int gen_shard_update_sqlite(sqlite3 *db, struct errstat *err)
{
    Table *tab;
    int rc;
    for (int tbl_idx = 0; tbl_idx < thedb->num_dbs; ++tbl_idx) {
        struct dbtable *tbl = thedb->dbs[tbl_idx];
        if (tbl->partition.genshard_name) {
            logmsg(LOGMSG_USER, "TRYING ADD VIEW FOR TABLE %s PART OF GENSHARD %s\n", tbl->tablename,
                   tbl->partition.genshard_name);
            /* this table is a component shard of a genshard table*/
            tab = sqlite3FindTableCheckOnly(db, tbl->partition.genshard_name, NULL);
            if (tab) {
                /* found view, is it the same version ? */
                if (tbl->tableversion != tab->version) {
                    /* older version, destroy current view */
                    rc = views_sqlite_del_view(tbl->partition.genshard_name, db, err);
                    if (rc != VIEW_NOERR) {
                        logmsg(LOGMSG_ERROR, "%s: failed to remove old view\n", __func__);
                        goto done;
                    }
                } else {
                    /* up to date, nothing to do */
                    continue;
                }
            }
            rc = _add_view(tbl, db, err);
            if (rc != VIEW_NOERR) {
                goto done;
            }
        }
    }
    rc = VIEW_NOERR;
done:
    return rc;
}

int is_gen_shard(const char *tablename) {
    struct dbtable *db = get_dbtable_by_name(tablename);
    if (db && strcmp(db->partition.genshard_name, tablename)==0) {
        return 1;
    }
    return 0;
}
