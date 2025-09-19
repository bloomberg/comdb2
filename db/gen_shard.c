#include <plhash_glue.h>
#include "views.h"
#include "cson/cson.h"
#include "schemachange.h"
#include <regex.h>
#include "consistent_hash.h"
#include <string.h>
#include "fdb_fend.h"

#define LLMETA_GENERIC_SHARD "gen_shard"
int gbl_gen_shard_verbose = 0;
const char *cson_extract_str(cson_object *cson_obj, const char *param,
                                     struct errstat *err);
int cson_extract_int(cson_object *cson_obj, const char *param,
                             struct errstat *err);
cson_array *cson_extract_array(cson_object *cson_obj, const char *param,
                                       struct errstat *err);
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

int gen_shard_llmeta_write_serialized_str(tran_type *tran, const char *tablename, const char *str) {
	int rc;
	if (str){
		rc = bdb_set_table_parameter(tran, LLMETA_GENERIC_SHARD, tablename, str);
	} else {
		/* this is a partition drop */
		rc = bdb_clear_table_parameter(tran, LLMETA_GENERIC_SHARD, tablename);
	}

	if (rc) {
		logmsg(LOGMSG_ERROR, "FAILED TO WRITE SHARD STRING TO LLMETA. RC: %d\n", rc);
	}
	return rc;
}

int gen_shard_llmeta_remove(tran_type *tran, char *tablename, struct errstat *err) {
    int rc = 0;
    rc = gen_shard_llmeta_write_serialized_str(tran, tablename, NULL);
    if (rc) {
        logmsg(LOGMSG_ERROR, "FAILED TO ERASE LLMETA ENTRY FOR SHARDED TABLE %s. rc: %d\n", tablename, rc);
    }
    return rc;
}

int gen_shard_llmeta_add(tran_type *tran, char *tablename, uint32_t numdbs, char **dbnames, 
		uint32_t numcols, char **columns, char **shardnames, struct errstat *err) {
	char *serializedStr = NULL;
	uint32_t serializedStrLen = 0;
	int rc = 0;

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
	rc = gen_shard_llmeta_write_serialized_str(tran, tablename, serializedStr);
done:
	if (serializedStr){
		free(serializedStr);
	}
	return rc;
}

int gen_shard_llmeta_read(void *tran, const char *name, char **pstr)
{
    int rc;

    *pstr = NULL;

    rc = bdb_get_table_parameter_tran(LLMETA_GENERIC_SHARD, name, pstr, tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "bdb_get_table_parameter_tran failed with err: %d\n", rc);
    }
    return rc;
}

int gen_shard_deserialize_shard(uint32_t *numdbs, char ***dbnames, uint32_t *numcols, char ***columns, char ***shardnames, char *serializedStr) {
    cson_object *rootObj = NULL;
    cson_value *rootVal = NULL, *arrVal = NULL;
    cson_array *dbs_arr = NULL, *cols_arr = NULL, *shards_arr = NULL;
    const char *tablename = NULL;
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

    tablename = cson_extract_str(rootObj, "TABLENAME", &err);
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
            if (dbs[i])
                free(dbs[i]);
        }
        free(dbs);
    }

    if (cols) {
        for(int i=0;i<num_cols;i++) {
            if (cols[i])
                free(cols[i]);
        }
        free(cols);
    }

    if (shards) {
        for(int i=0;i<num_dbs;i++){
            if (shards[i])
                free(shards[i]);
        }
        free(shards);
    }
    if (rootVal) {
        cson_value_free(rootVal);
    }
    return -1;

}

int gen_shard_update_inmem_db(void *tran, struct dbtable *db, const char *name) {
    char *serializedStr = NULL;
    uint32_t numdbs = 0, numcols = 0;
    char **dbnames = NULL, **columns = NULL, **shardnames = NULL;
    int rc = 0;
    rc = gen_shard_llmeta_read(tran, name, &serializedStr); 
    if (rc) {
        logmsg(LOGMSG_ERROR, "Failed to read from llmeta for table %s\n", name);
        goto done;
    }

    rc = gen_shard_deserialize_shard(&numdbs, &dbnames, &numcols, &columns, &shardnames, serializedStr);
    if (rc) {
        logmsg(LOGMSG_ERROR, "Failed to deserialized llmeta str for table %s\n", name);
        goto done;
    }

    /*update the table object*/
    db->numdbs = numdbs;
    db->dbnames = dbnames;
    db->numcols = numcols;
    db->columns = columns;
    db->shardnames = shardnames;
done:
    if (serializedStr) {
        free(serializedStr);
    }
    return rc;
}


