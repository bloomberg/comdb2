/**
 * Use llmeta operation to persist the time partition views
 *
 * The serialization format is cson
 *
 * FORMAT
 * key = "timeparts"
 *
 * data = [ "aname",
 *          "anothername",
 *          ....
 *        ]
 *
 *  key = "timepartview"+"aname"
 *  data =
 *       {
 *          "NAME": "aname",
 *          "PERIOD": "daily"|"weekly"|"monthly"|"yearly|manual",
 *          "RETENTION" : n,  #here n is 4
 *          "SOURCE_ID" : "uuid string",
 *          "ROLLOUT" : "adddrop|truncate",
 *          "TABLES":
 *             [
 *                {
 *                   "TABLENAME" : "ashardname1",
 *                   "LOW"       : INT_MIN,
 *                   "HIGH"      : lim1
 *                },
 *                {
 *                   "TABLENAME" : "ashardname2",
 *                   "LOW"       : lim1,
 *                   "HIGH"      : lim2
 *                },
 *                {
 *                   "TABLENAME" : "ashardname3",
 *                   "LOW"       : lim2,
 *                   "HIGH"      : lim3
 *                },
 *                {
 *                   "TABLENAME" : "ashardname4",
 *                   "LOW"       : lim3,
 *                   "HIGH"      : INT_MAX
 *                }
 *             ]
 *       }
 *
 */
#include <stdio.h>
#include <stdarg.h>
#include <ctype.h>
#include <ctype.h>
#include <uuid/uuid.h>

#include "comdb2.h"
#include "cson.h"
#include "views.h"
#include "comdb2uuid.h"
#include "bdb_access.h"

static char *_concat(char *str, int *len, const char *fmt, ...);

static const char *_cson_extract_str(cson_object *cson_obj, const char *param,
                                     struct errstat *err);
static timepart_view_t *partition_deserialize_cson_value(cson_value *cson_view,
                                                         struct errstat *err);
static timepart_views_t *_create_all_views(const char *views_str);

/* this is used in the "ADDDROP" time partitions */
char *build_createcmd_json(char **out, int *len, const char *name,
                           const char *tablename, uint32_t period,
                           int retention, uint64_t starttime)
{
    char time[256];
    const char *period_str = period_to_name(period);
    convert_to_start_string(period, starttime, time, sizeof(time));
    *out =
        _concat(NULL, len,
                " {\n"
                "  \"COMMAND\"       : \"CREATE\",\n"
                "  \"TYPE\"      : \"TIME BASED\",\n"
                "  \"NAME\"  : \"%s\",\n"
                "  \"SHARD0NAME\"    : \"%s\",\n"
                "  \"PERIOD\"    : \"%s\",\n"
                "  \"RETENTION\" : %d,\n"
                "  \"%s : \"%s\"\n"
                "}",
                name, tablename, period_str, retention,
                IS_TIMEPARTITION(period) ? "STARTTIME\"" : "START\"    ", time);
    return *out;
}

char *build_dropcmd_json(char **out, int *len, const char *name)
{
    *out = _concat(NULL, len, " {\n"
                              "  \"COMMAND\"       : \"DESTROY\",\n"
                              "  \"TYPE\"      : \"TIME BASED\",\n"
                              "  \"NAME\"  : \"%s\"\n"
                              "}",
                   name);
    return *out;
}

/**
 * Serialize an in-memory view into a CSON string
 *
 * NOTE: this appends to *out string;  Please set it to NULL
 * for new strings
 */
int timepart_serialize_view(timepart_view_t *view, int *plen, char **out,
                            int user_friendly)
{
    timepart_shard_t *shard;
    char *str;
    int len;
    int j;
    char now[256];
    uuidstr_t us;

    /* NOTE:
       You will notice this is generated ad-hoc, while deserializing uses CSON
       functions, which is
       more portable (ignores additional fields!).
       I do that just so I make sure I understand json */

    /* concatenation */
    str = *out;
    len = *plen;

    str = _concat(str, &len,
                  " {\n"
                  "  \"NAME\"      : \"%s\",\n"
                  "  \"PERIOD\"    : \"%s\",\n"
                  "  \"RETENTION\" : %d,\n"
                  "  \"%s : \"%s\",\n"
                  "  \"SHARD0NAME\": \"%s\",\n"
                  "  \"SOURCE_ID\" : \"%s\",\n",
                  view->name, period_to_name(view->period), view->retention,
                  (IS_TIMEPARTITION(view->period)) ? "STARTTIME\""
                                                   : "START\"    ",
                  convert_to_start_string(view->period, view->starttime, now,
                                          sizeof(now)),
                  view->shard0name, comdb2uuidstr(view->source_id, us));

    if (!str)
        return VIEW_ERR_MALLOC;

    if (view->rolltype == TIMEPART_ROLLOUT_TRUNCATE) {
        str = _concat(str, &len, "  \"ROLLOUT\"   : \"TRUNCATE\",\n");
        if (!str)
            return VIEW_ERR_MALLOC;
    }

    str = _concat(str, &len,
                  "  \"TABLES\"    :\n"
                  "  [\n");
    if (!str)
        return VIEW_ERR_MALLOC;

    for (j = 0; j < view->nshards; j++) {
        char lowstr[256];
        char highstr[256];

        shard = &view->shards[j];

        if (!user_friendly) {
            snprintf(lowstr, sizeof(lowstr), "%d", shard->low);
            snprintf(highstr, sizeof(highstr), "%d", shard->high);
        } else {
            convert_to_start_string(view->period, shard->low, lowstr,
                                    sizeof(lowstr));
            convert_to_start_string(view->period, shard->high, highstr,
                                    sizeof(highstr));
        }

        str = _concat(str, &len, "  {\n"
                                 "   \"TABLENAME\"    : \"%s\",\n"
                                 "   \"LOW\"          : %s,\n"
                                 "   \"HIGH\"         : %s\n"
                                 "  }%s\n",
                      shard->tblname, lowstr, highstr,
                      (j == view->nshards - 1) ? "" : ",");
        if (!str)
            return VIEW_ERR_MALLOC;
    }
    str = _concat(str, &len, "  ]\n"
                             " }\n");
    if (!str)
        return VIEW_ERR_MALLOC;

    *out = str;
    *plen = len;

    return VIEW_NOERR;
}

/**
 * Serialize in-memory view repository into a CSON string
 *
 */
int timepart_serialize(timepart_views_t *views, char **out, int user_friendly)
{
    char *str;
    int len;
    int i;
    int rc = VIEW_NOERR;

    *out = NULL;

    rdlock_schema_lk(); /* prevent race with partitions sc */
    Pthread_rwlock_wrlock(&views_lk);

    if (views->nviews == 0) {
        str = _concat(NULL, &len, "[]\n");
        goto done;
    }

    /* manual way to generate a cson */
    len = 0;
    str = NULL;

    str = _concat(str, &len, "[\n");
    if (!str) {
        rc = VIEW_ERR_MALLOC;
        goto done;
    }

    for (i = 0; i < views->nviews; i++) {
        rc =
            timepart_serialize_view(views->views[i], &len, &str, user_friendly);
        if (rc != VIEW_NOERR) {
            if (str) {
                free(str);
                str = NULL;
            }
            goto done;
        }

        str = _concat(str, &len, "]%s", (i == views->nviews - 1) ? "" : ",\n");
        if (!str) {
            rc = VIEW_ERR_MALLOC;
            goto done;
        }
    }

    rc = VIEW_NOERR;

done:
    Pthread_rwlock_unlock(&views_lk);
    unlock_schema_lk();

    *out = str;

    return rc;
}

/**
 * Deserialize a CSON string into a in-memory view
 *
 */
timepart_view_t *timepart_deserialize_view(const char *str, struct errstat *err)
{
    timepart_view_t *view = NULL;
    cson_value *cson_view = NULL;
    int rc = VIEW_NOERR;

    if (!str) {
        snprintf(err->errstr, sizeof(err->errstr), "Missing string");
        err->errval = VIEW_ERR_BUG;
        goto done;
    }

    /* parse string */
    rc = cson_parse_string(&cson_view, str, strlen(str));
    if (rc) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Parsing JSON error rc=%d err:%s\n", rc, cson_rc_string(rc));
        err->errval = VIEW_ERR_PARAM;
        goto done;
    }

    view = partition_deserialize_cson_value(cson_view, err);
    if (!view) {
        goto done;
    }

done:
    return view;
}

/**
 * Deserialize a CSON string into a in-memory view repository
 *
 */
timepart_views_t *timepart_deserialize(const char *str, struct errstat *err)
{
    timepart_views_t *views = NULL;
    cson_value *cson = NULL;
    cson_value *cson_view = NULL;
    int rc;
    cson_array *view_arr = NULL;
    int i;
    int len;

    if (!str) {
        snprintf(err->errstr, sizeof(err->errstr), "Missing string");
        rc = err->errval = VIEW_ERR_BUG;
        return NULL;
    }

    /* parse string */
    rc = cson_parse_string(&cson, str, strlen(str));
    if (rc) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Parsing JSON error rc=%d err:%s\n", rc, cson_rc_string(rc));
        rc = err->errval = VIEW_ERR_PARAM;
        return NULL;
    }

    if (!cson_value_is_array(cson)) {
        logmsg(LOGMSG_ERROR,
               "Wrong JSON format, we expect an array of views\n");
        goto error;
    }

    /* create in-memory view repository */
    views = (timepart_views_t *)calloc(1, sizeof(timepart_views_t));
    if (!views) {
    oom:
        snprintf(err->errstr, sizeof(err->errstr), "Malloc OOM");
        rc = err->errval = VIEW_ERR_MALLOC;
        goto error;
    }

    view_arr = cson_value_get_array(cson);
    len = cson_array_length_get(view_arr);
    if (len == 0) {
        logmsg(LOGMSG_ERROR, "Wrong JSON format, empty array of views\n");
        goto error;
    }

    views->views = (timepart_view_t **)calloc(len, sizeof(timepart_view_t *));
    if (!views->views) {
        goto oom;
    }

    for (i = 0; i < len; i++) {
        cson_view = cson_array_get(view_arr, i);
        if (!cson_view) {
            snprintf(err->errstr, sizeof(err->errstr),
                     "VIEWS string has no arrays of views");
            rc = err->errval = VIEW_ERR_PARAM;
            goto error;
        }

        views->views[i] = partition_deserialize_cson_value(cson_view, err);
        if (!views->views[i]) {
            goto error;
        }
    }

    cson_value_free(cson);

    return views;

error:
    /* destroy views*/
    if (views) {
        timepart_free_views_unlocked(views);
        views = NULL;
    }
    cson_value_free(cson);

    return NULL;
}

/**
 * Process all views from llmeta
 *
 */
timepart_views_t *views_create_all_views(void)
{
    char *views_str;
    timepart_views_t *ret;

    views_str = views_read_all_views();
    if (!views_str) {
        ret = (timepart_views_t *)calloc(1, sizeof(timepart_views_t));
        if (ret) {
            ret->preemptive_rolltime = bdb_attr_get(
                thedb->bdb_attr, BDB_ATTR_VIEWS_DFT_PREEMPT_ROLL_SECS);
            ret->rollout_delete_lag = bdb_attr_get(
                thedb->bdb_attr, BDB_ATTR_VIEWS_DFT_ROLL_DELETE_LAG_SECS);
        }
        return ret;
    }

    return _create_all_views(views_str);
}

/* parse a user-provided json create command and update the in-memory views */
static int _views_do_partition_create(void *tran, timepart_views_t *views,
                                      const char *name, cson_value *cson,
                                      const char *cmd, struct errstat *err)
{
    timepart_view_t *view = NULL;
    cson_object *cson_obj;
    const char *first_shard;
    const char *type;
    struct dbtable *db;
    int rc;
    char *err_partname;
    int err_shardidx;

    cson_obj = cson_value_get_object(cson);

    /* check type */
    type = _cson_extract_str(cson_obj, "TYPE", err);
    if (!type) {
        goto error;
    }
    if (strcasecmp(type, "TIME BASED")) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Invalid view type for \"%s\"", name);
        err->errval = VIEW_ERR_PARAM;
        goto error;
    }

    /* extract the time partition schedule */
    view = partition_deserialize_cson_value(cson, err);
    if (!view) {
        goto error;
    }

    /* make sure names match */
    if (strcmp(name, view->name)) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Mismatching view name for \"%s\"", view->name);
        err->errval = VIEW_ERR_PARAM;
        goto error;
    }

    if (view->nshards != 1) {
        /* NOTE: we do not support aggregating existing tables in shards;
           only one must be provided */
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr),
                 "Invalid view type for \"%s\"", cmd);
        goto error;
    }

    /* make sure the name is not overlapping a table name (this breaks in sqlite
     */
    db = get_dbtable_by_name(view->name);
    if (db) {
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr),
                 "Partition name %s matches an existing table", view->name);
        goto error;
    }

    first_shard = view->shards[0].tblname;

    /*
       check if the table exists !
 TODO: add support for alias to work with remote tables
     */
    db = get_dbtable_by_name(first_shard);
    if (!db) {
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr), "Table %s does not exist",
                 first_shard);
        goto error;
    }
    if (db->periods[PERIOD_SYSTEM].enable) {
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr), "Table %s is temporal",
                 first_shard);
        goto error;
    }

    /* reverse constraints from the time partition to other table not supported */
    if (db->n_rev_constraints > 0) {
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr), "Table %s has constraints",
                 first_shard);
        goto error;
    }
    
    check_columns_null_and_dbstore(view->name, db);

    /* check to see if the name exists either as a table, or part of a
       different partition */
    rc = comdb2_partition_check_name_reuse(first_shard, &err_partname, 
                                           &err_shardidx);
    if(rc) {
        if (rc != VIEW_ERR_EXIST)
            abort();

        if(err_shardidx == -1) {
            snprintf(err->errstr, sizeof(err->errstr), 
                     "Partition name \"%s\" matches seed shard in partition \"%s\"",
                     first_shard, err_partname);
        } else {
            snprintf(err->errstr, sizeof(err->errstr), 
                     "Partition name \"%s\" matches shard %d in partition \"%s\"",
                     first_shard, err_shardidx, err_partname);
        }
        err->errval = VIEW_ERR_PARAM;
        free(err_partname);
        goto error;
    }

    /* make sure we can generate new file names */
    rc = _view_check_sharding(view, err);
    if (rc != VIEW_NOERR) {
        goto error;
    }

    /* assign a uuid */
    comdb2uuid(view->source_id);

    /* time to add the view */
    rc = timepart_add_view(NULL /*tran -- I want this commit independent!!! */,
                           views, view, err);
    if (rc != VIEW_NOERR) {
        goto error;
    }

    err->errval = VIEW_NOERR;
    err->errstr[0] = '\0';

    return VIEW_NOERR;

error:
    if (view) {
        timepart_free_view(view);
    }
    return err->errval;
}

/* parse a user-provided json destroy command and update the in-memory views */
static int _views_do_partition_destroy(void *tran, timepart_views_t *views,
                                       const char *name, cson_value *cson,
                                       const char *cmd, struct errstat *err)
{
    cson_object *cson_obj;
    const char *viewname;
    int rc;

    cson_obj = cson_value_get_object(cson);

    viewname = _cson_extract_str(cson_obj, "NAME", err);
    if (!viewname) {
        goto error;
    }

    /* make sure names match */
    if (strcmp(name, viewname)) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Mismatching view name in:\n\"%s\"\n", cmd);
        err->errval = VIEW_ERR_PARAM;
        goto error;
    }

    /* time to delete the view */
    rc = timepart_del_view(NULL /*tran -- I want this commit independent!!! */,
                           views, viewname);
    if (rc != VIEW_NOERR) {
        err->errval = rc;
        snprintf(err->errstr, sizeof(err->errstr), "Failed to destroy view %s",
                 viewname);
        goto error;
    }

    err->errval = VIEW_NOERR;
    err->errstr[0] = '\0';

error:
    return err->errval;
}

static int timepart_get_access_type(bdb_state_type *bdb_state, void *tran,
                                    char *tab, char *user, int access_type,
                                    int *bdberr)
{
    int rc = 0;

    switch (access_type) {
    case ACCESS_READ:
        rc = bdb_tbl_access_read_get(bdb_state, tran, tab, user, bdberr);
        break;
    case ACCESS_WRITE:
        rc = bdb_tbl_access_write_get(bdb_state, tran, tab, user, bdberr);
        break;
    case ACCESS_DDL:
        rc = bdb_tbl_op_access_get(bdb_state, tran, 0, tab, user, bdberr);
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s:%d invalid access type '%d'\n", __func__,
               __LINE__, access_type);
        return 1;
    }

    if ((rc != 0) && (*bdberr != BDBERR_FETCH_DTA)) {
        logmsg(LOGMSG_ERROR,
               "%s:%d failed to set user access information (rc: %d, bdberr: "
               "%d)\n",
               __func__, __LINE__, rc, *bdberr);
    }
    return rc;
}

static int timepart_set_access_type(bdb_state_type *bdb_state, void *tran,
                                    char *tab, char *user, int access_type,
                                    int *bdberr)
{
    int rc = 0;

    switch (access_type) {
    case ACCESS_READ:
        rc = bdb_tbl_access_read_set(bdb_state, tran, tab, user, bdberr);
        break;
    case ACCESS_WRITE:
        rc = bdb_tbl_access_write_set(bdb_state, tran, tab, user, bdberr);
        break;
    case ACCESS_DDL:
        rc = bdb_tbl_op_access_set(bdb_state, tran, 0, tab, user, bdberr);
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s:%d invalid access type '%d'\n", __func__,
               __LINE__, access_type);
        return 1;
    }

    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "%s:%d failed to set user access information (rc: %d, bdberr: "
               "%d)\n",
               __func__, __LINE__, rc, *bdberr);
    }
    return rc;
}

static int timepart_delete_access_type(bdb_state_type *bdb_state, void *tran,
                                       char *tab, char *user, int access_type,
                                       int *bdberr)
{
    int rc = 0;

    switch (access_type) {
    case ACCESS_READ:
        rc = bdb_tbl_access_read_delete(bdb_state, tran, tab, user, bdberr);
        break;
    case ACCESS_WRITE:
        rc = bdb_tbl_access_write_delete(bdb_state, tran, tab, user, bdberr);
        break;
    case ACCESS_DDL:
        rc = bdb_tbl_op_access_delete(bdb_state, tran, 0, tab, user, bdberr);
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s:%d invalid access type '%d'\n", __func__,
               __LINE__, access_type);
        return 1;
    }

    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "%s:%d failed to delete user access information (rc: %d, "
               "bdberr: %d)\n",
               __func__, __LINE__, rc, *bdberr);
    }
    return rc;
}

/* Copy the current 'src' table permission for the specified user to the 'dst'
 * table. */
static int timepart_copy_access_type(bdb_state_type *bdb_state, void *tran,
                                     char *dst, char *src, char *user,
                                     int access_type)
{
    int rc = 0;
    int bdberr;

    rc = timepart_get_access_type(bdb_state, tran, src, user, access_type,
                                  &bdberr);
    if (rc) {
        if (bdberr == BDBERR_FETCH_DTA) {
            return 0;
        }
    } else {
        rc = timepart_delete_access_type(bdb_state, tran, dst, user,
                                         access_type, &bdberr);
        if (!rc) {
            rc = timepart_set_access_type(bdb_state, tran, dst, user,
                                          access_type, &bdberr);
        }
    }

    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "error setting user access information (rc: %d, bdberr: %d)\n",
               rc, bdberr);
        return rc;
    }
    return rc;
}

/* Copy the current 'src' table permissions for the specified user to the
 * newly created 'dst' table. */
int timepart_copy_access(bdb_state_type *bdb_state, void *tran, char *dst,
                         char *src, int acquire_schema_lk)
{
    char **users;
    int nUsers;
    int rc = 0;

    logmsg(LOGMSG_INFO, "setting access permissions for time partition %s\n",
           dst);

    if (acquire_schema_lk) {
        wrlock_schema_lk();
    }

    /* Retrieve all access permissions for all existing users for the dst
     * table. */
    bdb_user_get_all_tran(tran, &users, &nUsers);

    for (int i = 0; i < nUsers; ++i) {
        // read access
        if ((rc = timepart_copy_access_type(bdb_state, tran, dst, src, users[i],
                                            ACCESS_READ)))
            goto err;

        // write access
        if ((rc = timepart_copy_access_type(bdb_state, tran, dst, src, users[i],
                                            ACCESS_WRITE)))
            goto err;

        // DDL access
        if ((rc = timepart_copy_access_type(bdb_state, tran, dst, src, users[i],
                                            ACCESS_DDL)))
            goto err;
    }

err:
    if (acquire_schema_lk) {
        unlock_schema_lk();
    }

    for (int i = 0; i < nUsers; ++i) {
        free(users[i]);
    }

    return rc;
}

static int timepart_delete_access(bdb_state_type *bdb_state, void *tran,
                                  const char *name, int acquire_schema_lk)
{
    char **users;
    int nUsers;
    int rc = 0;
    int bdberr;

    logmsg(LOGMSG_INFO, "deleting access permissions for time partition %s\n",
           name);

    if (acquire_schema_lk) {
        wrlock_schema_lk();
    }

    /* Delete all access permissions for all existing users for the time
       partitions. */
    bdb_user_get_all_tran(tran, &users, &nUsers);

    for (int i = 0; i < nUsers; ++i) {
        // read access
        if ((rc = bdb_tbl_access_read_delete(bdb_state, tran, name, users[i],
                                             &bdberr)))
            goto err;

        // write access
        if ((rc = bdb_tbl_access_write_delete(bdb_state, tran, name, users[i],
                                              &bdberr)))
            goto err;

        // DDL access
        if ((rc = bdb_tbl_op_access_delete(bdb_state, tran, 0, name, users[i],
                                           &bdberr)))
            goto err;
    }

    if (acquire_schema_lk) {
        unlock_schema_lk();
    }

    for (int i = 0; i < nUsers; ++i) {
        free(users[i]);
    }

err:
    if (rc != 0) {
        logmsg(LOGMSG_ERROR,
               "error deleting user access information (rc: %d, bdberr: %d)\n",
               rc, bdberr);
    }

    return rc;
}

static int timepart_copy_shard_names(char *view_name, char ***shard_names,
                                     int *nshards)
{
    timepart_views_t *views;
    timepart_view_t *view;
    int rc = 0;
    char **shards;

    views = thedb->timepart_views;

    Pthread_rwlock_wrlock(&views_lk);
    for (int i = 0; i < views->nviews; i++) {
        view = views->views[i];
        if (!strcasecmp(view_name, view->name)) {
            *nshards = view->nshards;
            if (view->nshards == 0) {
                break;
            }
            shards = (char **)malloc(view->nshards * sizeof(char *));
            if (!shards) {
                logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__,
                       __LINE__);
                rc = 1;
                goto done;
            }
            for (int j = 0; j < view->nshards; j++) {
                shards[j] = strdup(view->shards[j].tblname);
                if (!shards[j]) {
                    logmsg(LOGMSG_ERROR, "%s:%d out-of-memory\n", __func__,
                           __LINE__);
                    for (int k = 0; k < j; k++) {
                        free(shards[k]);
                    }
                    free(shards);
                    rc = 1;
                    goto done;
                }
            }
            *shard_names = shards;
            break;
        }
    }
done:
    Pthread_rwlock_unlock(&views_lk);
    return rc;
}

int timepart_shards_grant_access(bdb_state_type *bdb_state, void *tran,
                                 char *name, char *user, int access_type)
{
    int rc = 0;
    int bdberr;
    char **shard_names = 0;
    int nshards = 0;

    rc = timepart_copy_shard_names(name, &shard_names, &nshards);
    if (rc) {
        return 1;
    }

    wrlock_schema_lk();
    for (int i = 0; i < nshards; i++) {
        rc = timepart_delete_access_type(bdb_state, tran, shard_names[i], user,
                                         access_type, &bdberr);
        if (!rc) {
            rc = timepart_set_access_type(bdb_state, tran, shard_names[i], user,
                                          access_type, &bdberr);
        }
        if (rc) {
            break;
        }
    }
    unlock_schema_lk();

    for (int i = 0; i < nshards; i++) {
        if (shard_names[i]) {
            free(shard_names[i]);
        }
    }
    free(shard_names);

    return rc;
}

int timepart_shards_revoke_access(bdb_state_type *bdb_state, void *tran,
                                  char *name, char *user, int access_type)
{
    int rc = 0;
    int bdberr;
    char **shard_names = 0;
    int nshards = 0;

    rc = timepart_copy_shard_names(name, &shard_names, &nshards);
    if (rc) {
        return 1;
    }

    wrlock_schema_lk();
    for (int i = 0; i < nshards; i++) {
        rc = timepart_delete_access_type(bdb_state, tran, shard_names[i], user,
                                         access_type, &bdberr);
        if (rc) {
            break;
        }
    }
    unlock_schema_lk();

    for (int i = 0; i < nshards; i++) {
        if (shard_names[i]) {
            free(shard_names[i]);
        }
    }
    free(shard_names);

    return rc;
}

/**
 * Process a cson command
 *
 * Format:
 * {
 *    "COMMAND"   : "CREATE|DESTROY|DISPLAY",
 *    "VIEWNAME"  : "a_name",
 *    "TYPE"      : "TIME BASED",
 *    "PERIOD     : "DAILY|WEEKLY|MONTHLY|YEARLY|MANUAL",
 *    "RETENTION" : n,
 *    "TABLE0"    : "an_existing_table_name" // THIS IS SHARD0NAME
 * }
 *
 *
 */
int views_do_partition(void *tran, timepart_views_t *views, const char *name,
                       const char *cmd, struct errstat *err)
{
    cson_value *cson_cmd;
    cson_object *cson_obj;
    const char *op;
    int rc;

    /* string to conversion */
    rc = cson_parse_string(&cson_cmd, cmd, strlen(cmd));
    if (rc) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Invalid JSON string \"%s\":\n\"%s\"\n", cson_rc_string(rc),
                 cmd);
        err->errval = VIEW_ERR_PARAM;
        return err->errval;
    }
    if (!cson_value_is_object(cson_cmd)) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Invalid JSON string expected object\n");
        err->errval = VIEW_ERR_PARAM;
        return err->errval;
    }
    cson_obj = cson_value_get_object(cson_cmd);

    op = _cson_extract_str(cson_obj, "COMMAND", err);
    if (!op) {
        goto error;
    }

    if (!strcasecmp(op, "CREATE")) {
        /* Time partition inherits shard0's permissions. */
        const char *shard0 = _cson_extract_str(cson_obj, "SHARD0NAME", err);
        if (!shard0) {
            rc = err->errval;
            goto error;
        } else if (timepart_copy_access(thedb->bdb_env, NULL, (char *)name,
                                        (char *)shard0,
                                        1 /* acquire schema_lk */) != 0) {
            logmsg(LOGMSG_ERROR,
                   "failed to set table permissions for time partition\n");
            rc = 1;
            goto error;
        }

        rc = _views_do_partition_create(tran, views, name, cson_cmd, cmd, err);
        if (rc != VIEW_NOERR) {
            goto error;
        }
    } else if (!strcasecmp(op, "DESTROY")) {
        rc = _views_do_partition_destroy(tran, views, name, cson_cmd, cmd, err);
        if (rc != VIEW_NOERR) {
            goto error;
        }

        /* Delete access permissions related to this time partition from llmeta.
         */
        timepart_delete_access(thedb->bdb_env, NULL, name,
                               1 /* acquire schema_lk */);
    } else {
        err->errval = VIEW_ERR_UNIMPLEMENTED;
        logmsg(LOGMSG_ERROR, "Unrecognized command \"%s\" in:\n\"%s\"\n", op,
               cmd);
        goto error;
    }

#if 0
   /* time to change the change persistent */
    char *latest_views_str;
   rc = timepart_serialize(views, &latest_views_str, 0);
   if (rc != VIEW_NOERR || !latest_views_str)
   {
      err->errval = rc;
      snprintf(err->errstr, sizeof(err->errstr), "Failed to serialize updated views\n");
      goto error;
   }

   rc = views_write(latest_views_str);
   if(rc)
   {
      err->errval = rc;
      snprintf(err->errstr, sizeof(err->errstr), "Failed to write updated views\n");
      goto error;
   }

   /* DONE! at this point, schema change will trigger a replicated event everywhere */
#endif

    err->errval = VIEW_NOERR;
    err->errstr[0] = '\0';

error:

    cson_free_value(cson_cmd);
    return err->errval;
}

static char *_concat(char *str, int *plen, const char *fmt, ...)
{
#define CHUNK 1024

    va_list args;
    int idx;
    int crtlen;
    int increm;
    int len;

    if (!str) {
        len = CHUNK;
        str = malloc(len);
        if (!str)
            return NULL;
        idx = 0;
    } else {
        len = *plen;
        idx = strlen(str);
    }

    va_start(args, fmt);
    crtlen = vsnprintf(str + idx, len - idx, fmt, args);
    va_end(args);
    crtlen++; /* include 0*/

    if (crtlen > len - idx) {
        increm = crtlen - (len - idx);
        if (increm < CHUNK)
            increm = CHUNK;

        str = realloc(str, len + increm);
        if (!str) {
            return NULL;
        }

        len += increm;
        *plen = len;

        va_start(args, fmt);
        vsnprintf(str + idx, len - idx, fmt, args);
        va_end(args);
    } else {
        *plen = len;
    }

    return str;
}

const char DAILY_STR[] = "daily";
const char WEEKLY_STR[] = "weekly";
const char MONTHLY_STR[] = "monthly";
const char YEARLY_STR[] = "yearly";
const char TEST2MIN_STR[] = "test2min";
const char MANUAL_STR[] = "manual";
const char INVALID_STR[] = "";

const char *period_to_name(enum view_partition_period period)
{
    switch (period) {
    case VIEW_PARTITION_DAILY:
        return DAILY_STR;
    case VIEW_PARTITION_WEEKLY:
        return WEEKLY_STR;
    case VIEW_PARTITION_MONTHLY:
        return MONTHLY_STR;
    case VIEW_PARTITION_YEARLY:
        return YEARLY_STR;
    case VIEW_PARTITION_TEST2MIN:
        return TEST2MIN_STR;
    case VIEW_PARTITION_MANUAL:
        return MANUAL_STR;
    default:
        break;
    }
    return INVALID_STR;
}

enum view_partition_period name_to_period(const char *str)
{
    if (!strcasecmp(str, DAILY_STR))
        return VIEW_PARTITION_DAILY;
    if (!strcasecmp(str, WEEKLY_STR))
        return VIEW_PARTITION_WEEKLY;
    if (!strcasecmp(str, MONTHLY_STR))
        return VIEW_PARTITION_MONTHLY;
    if (!strcasecmp(str, YEARLY_STR))
        return VIEW_PARTITION_YEARLY;
    if (!strcasecmp(str, TEST2MIN_STR))
        return VIEW_PARTITION_TEST2MIN;
    if (!strcasecmp(str, MANUAL_STR))
        return VIEW_PARTITION_MANUAL;

    return VIEW_PARTITION_INVALID;
}

static cson_value *_cson_it_get_value(cson_object_iterator *it,
                                      const char *field, int itr)
{
    const char *key;
    cson_kvp *kv;

    kv = cson_object_iter_next(it);
    if (!kv) {
        logmsg(LOGMSG_ERROR, "Wrong JSON format, no %s for %d view\n", field,
               itr);
        return NULL;
    }

    key = cson_string_cstr(cson_kvp_key(kv));
    if (strcasecmp(key, field)) {
        logmsg(LOGMSG_ERROR, "Wrong JSON format, no %s for %d view\n", field,
               itr);
        return NULL;
    }

    return cson_kvp_value(kv);
}

/**
 * We bring all cson to same format for comparison purposes
 * Normalizing is:
 * remove all newlines
 * preserve all strings as such
 * all isspace() sequences are replaced with ONE space
 */
char *normalize_string(const char *str)
{
    char *out;
    int in_string;
    int len;
    int i;
    int j;

    len = strlen(str);

    out = calloc(1, len);
    if (!out)
        return NULL;

    in_string = 0;
    j = 0;

    for (i = 0; i < len; i++) {
        /* catch all isspace() chars that are not part of a string */
        if (!in_string && isspace(str[i])) {
            while ((i < len) && isspace(str[i])) {
                i++;
            }
            out[j++] = ' ';
            if (i < len) {
                goto nospace;
            }

            continue;
        } else {
        nospace:
            if (str[i] == '"') {
                if (in_string) {
                    in_string = 0;
                } else {
                    in_string = 1;
                }
            }
            out[j++] = str[i];
        }
    }

    return out;
}

static const char *_cson_extract_str(cson_object *cson_obj, const char *param,
                                     struct errstat *err)
{
    cson_value *param_val;
    const char *ret_str;

    param_val = cson_object_get(cson_obj, param);
    if (!param_val) {
        /* try lower case */
        char *lower_param = strdup(param);
        int i;
        for (i = 0; i < strlen(param); i++) {
            lower_param[i] = tolower(param[i]);
        }
        param_val = cson_object_get(cson_obj, lower_param);
        free(lower_param);
        if (!param_val) {
            err->errval = VIEW_ERR_PARAM;
            snprintf(err->errstr, sizeof(err->errstr),
                     "Missing JSON \"%s\" field", param);
            return NULL;
        }
    }

    if (!cson_value_is_string(param_val)) {
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr),
                 "JSON \"%s\" field not string", param);
        return NULL;
    }
    ret_str = cson_string_cstr(cson_value_get_string(param_val));

    return ret_str;
}

static int _cson_extract_int(cson_object *cson_obj, const char *param,
                             struct errstat *err)
{
    cson_value *param_val;
    const char *ret_str;
    int ret_int;

    param_val = cson_object_get(cson_obj, param);
    if (!param_val) {
        /* try lower case */
        char *lower_param = strdup(param);
        int i;
        for (i = 0; i < strlen(param); i++) {
            lower_param[i] = tolower(param[i]);
        }
        param_val = cson_object_get(cson_obj, lower_param);
        if (!param_val) {
            err->errval = VIEW_ERR_PARAM;
            snprintf(err->errstr, sizeof(err->errstr),
                     "Missing JSON \"%s\" field", param);
            ret_int = -1;
        }
    }

    if (cson_value_is_string(param_val)) {
        ret_str = cson_string_cstr(cson_value_get_string(param_val));
        ret_int = atoi(ret_str);
    } else if (cson_value_is_integer(param_val)) {
        ret_int = cson_value_get_integer(param_val);
    } else {
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr),
                 "JSON \"%s\" field not string or integer", param);
        ret_int = -1;
    }

    return ret_int;
}

static cson_array *_cson_extract_array(cson_object *cson_obj, const char *param,
                                       struct errstat *err)
{
    cson_value *param_val;

    param_val = cson_object_get(cson_obj, param);
    if (!param_val) {
        /* try lower case */
        char *lower_param = strdup(param);
        int i;
        for (i = 0; i < strlen(param); i++) {
            lower_param[i] = tolower(param[i]);
        }
        param_val = cson_object_get(cson_obj, lower_param);
        if (!param_val) {
            err->errval = VIEW_ERR_PARAM;
            snprintf(err->errstr, sizeof(err->errstr),
                     "Missing JSON \"%s\" field", param);
            return NULL;
        }
    }

    if (!cson_value_is_array(param_val)) {
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr),
                 "JSON \"%s\" field not string or integer", param);
        return NULL;
    }

    return cson_value_get_array(param_val);
}

static int _cson_extract_start_string(cson_object *cson_obj, const char *param,
                                      enum view_partition_period period,
                                      struct errstat *err)
{
    cson_value *param_val;
    int ret_int = -1;

    param_val = cson_object_get(cson_obj, param);
    if (!param_val) {
        /* try lower case */
        char *lower_param = strdup(param);
        int i;
        for (i = 0; i < strlen(param); i++) {
            lower_param[i] = tolower(param[i]);
        }
        param_val = cson_object_get(cson_obj, lower_param);
        if (!param_val) {
            err->errval = VIEW_ERR_PARAM;
            snprintf(err->errstr, sizeof(err->errstr),
                     "Missing JSON \"%s\" field", param);
            return -1;
        }
    }

    /* epoch time? */
    if (cson_value_is_integer(param_val)) {
        ret_int = cson_value_get_integer(param_val);
    }
    /* Olson time */
    else if (cson_value_is_string(param_val)) {
        ret_int = convert_from_start_string(
            period, cson_string_cstr(cson_value_get_string(param_val)));
    } else {
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr),
                 "JSON \"%s\" field not string or integer", param);
        return -1;
    }

    return ret_int;
}

/* this will extract all the fields and populate a provided view */
static timepart_view_t *partition_deserialize_cson_value(cson_value *cson_view,
                                                         struct errstat *err)
{
    timepart_view_t *view = NULL;
    const char *name, *tmp_str;
    int period, retention;
    long long starttime;
    uuid_t source_id;
    enum TIMEPART_ROLLOUT_TYPE rolltype;

    cson_array *tbl_arr = NULL;
    cson_object *obj;
    cson_value *val;
    cson_object *obj_arr;
    int j;
    const char *errs = NULL;

    if (!cson_value_is_object(cson_view) ||
        (obj = cson_value_get_object(cson_view)) == NULL) {
        errs = "Expect an object for a view";
        goto error;
    }

    /* NAME */
    tmp_str = _cson_extract_str(obj, "NAME", err);
    if (!tmp_str) {
        goto error;
    }
    name = tmp_str;

    /* PERIOD */
    tmp_str = _cson_extract_str(obj, "PERIOD", err);
    if (!tmp_str) {
        goto error;
    }
    period = name_to_period(tmp_str);
    if (period == VIEW_PARTITION_INVALID) {
        errs = "Wrong JSON format, PERIOD value invalid";
        goto error;
    }

    /* RETENTION */
    retention = _cson_extract_int(obj, "RETENTION", err);
    if (retention < 0) {
        goto error;
    }

    tmp_str = (IS_TIMEPARTITION(period)) ? "STARTTIME" : "START";
    /* check for starttime */
    starttime = _cson_extract_start_string(obj, tmp_str, period, err);

    tmp_str = _cson_extract_str(obj, "SOURCE_ID", err);
    if(!tmp_str)
        comdb2uuid_clear(source_id);
    else if (uuid_parse(tmp_str, source_id)) {
        errs = "Wrong JSON format, SOURCE_ID value invalid uuid";
        goto error;
        }

        tmp_str = _cson_extract_str(obj, "ROLLOUT", err);
        if (tmp_str) {
            if (strncasecmp(tmp_str, "truncate", sizeof("truncate") + 1)) {
                errs = "Wrong ROLLOUT value";
                goto error;
            }
            rolltype = TIMEPART_ROLLOUT_TRUNCATE;
        } else {
            /* rollout is optional, it is only required to opt-in the new
             * truncate based rollout
             */
            bzero(err, sizeof(*err));
            rolltype = TIMEPART_ROLLOUT_ADDDROP;
        }

        view = timepart_new_partition(name, period, retention, starttime,
                                      &source_id, rolltype, NULL, err);
        if (!view)
            goto error;

        /* TABLES */
        tbl_arr = _cson_extract_array(obj, "TABLES", err);
        if (!tbl_arr) {
            /* initial creation doesn't require a table section */
            bzero(err, sizeof(*err));
            goto look_for_shard0;
    }

    view->nshards = cson_array_length_get(tbl_arr);
    if (view->nshards <= 0) {
        errs = "Wrong JSON format, TABLES has no shards";
        goto error;
    }

    view->shards =
        (timepart_shard_t *)calloc(view->nshards, sizeof(timepart_shard_t));
    if (!view->shards) {
        goto oom;
    }
    for (j = 0; j < view->nshards; j++) {
        val = cson_array_get(tbl_arr, j);
        if (!cson_value_is_object(val) ||
            ((obj_arr = cson_value_get_object(val)) == NULL)) {
            errs = "Wrong JSON format, TABLES entry not object";
            goto error;
        }

        /* TBLNAME */
        tmp_str = _cson_extract_str(obj_arr, "TABLENAME", err);
        if (!tmp_str) {
            goto error;
        }
        view->shards[j].tblname = strdup(tmp_str);
        if (!view->shards[j].tblname) {
            goto oom;
        }

        /* LOW */
        view->shards[j].low = _cson_extract_int(obj_arr, "LOW", err);
        if (view->rolltype == TIMEPART_ROLLOUT_ADDDROP &&
            (j == view->nshards - 1) && view->shards[j].low != INT_MIN) {
            errstat_set_rcstrf(
                err, VIEW_ERR_PARAM,
                "Wrong JSON format, TABLES entry %d has wrong integer for LOW",
                j);
            goto error;
        }

        /* HIGH */
        view->shards[j].high = _cson_extract_int(obj_arr, "HIGH", err);
        if (view->rolltype == TIMEPART_ROLLOUT_ADDDROP && (j == 0) &&
            view->shards[j].high != INT_MAX) {
            errstat_set_rcstrf(
                err, VIEW_ERR_PARAM,
                "Wrong JSON format, TABLES entry %d has wrong integer for HIGH",
                j);
            goto error;
        }
    }

look_for_shard0:
    /* SHARD0NAME */
    tmp_str = _cson_extract_str(obj, "SHARD0NAME", err);
    if (!tmp_str) {
        if (view->nshards != 1) {
            /* this catches cases when both TABLES and SHARD0NAME are missing!
             */
            goto error;
        } else {
            /* on creation shard0name is implicit */
            tmp_str = view->shards[0].tblname;
        }
    }
    view->shard0name = strdup(tmp_str);
    if (!view->shard0name) {
        goto oom;
    }

    /* if there was no tables, preinit a one shard array */
    if (view->nshards == 0) {
        assert(view->shard0name);
        view->nshards = 1;
        view->shards =
            (timepart_shard_t *)calloc(view->nshards, sizeof(timepart_shard_t));
        if (!view->shards) {
            goto oom;
        }

        view->shards[0].tblname = strdup(view->shard0name);
        if (!view->shards[0].tblname) {
            goto oom;
        }
        view->shards[0].low = INT_MIN;
        view->shards[0].high = INT_MAX;
    }

    return view;

error:
    if (errs) {
        errstat_set_rcstrf(err, VIEW_ERR_PARAM, errs);
    }
    /* error "err", if any prep-ed by called functionality */
    if (view) {
        timepart_free_view(view);
    }
    return NULL;

oom:
    errstat_set_rcstrf(err, VIEW_ERR_MALLOC, "%s Malloc OOM", __func__);
    goto error;
}


char *convert_epoch_to_time_string(int epoch, char *buf, int buflen)
{
    server_datetime_t dt = {0};
    int outlen = 0;
    int isnull;
    int rc;
    struct field_conv_opts_tz outopts = {0};
    struct sql_thread *thd =pthread_getspecific(query_info_key);

    /* types.c expects big-endian, or flag for LENDIAN */
    epoch = htonl(epoch);
    rc = CLIENT_INT_to_SERVER_DATETIME(&epoch, sizeof(epoch), 0, NULL, NULL,
                                       &dt, sizeof(dt), &outlen, NULL, NULL);
    if (rc || outlen != sizeof(dt)) {
        return "Error epoch to server datetime";
    }

    isnull = outlen = 0;
    if(!thd || !thd->clnt)
        strcpy(outopts.tzname, "UTC");
    else
        strcpy(outopts.tzname, thd->clnt->tzname);
    outopts.flags |= FLD_CONV_TZONE;
    rc = SERVER_DATETIME_to_CLIENT_CSTR(
        &dt, sizeof(dt), NULL, NULL, buf, buflen, &isnull, &outlen,
        (const struct field_conv_opts *)&outopts, NULL);
    if (rc) {
        return "Error server datetime to client string";
    }

    return buf;
}

int convert_time_string_to_epoch(const char *time_str)
{
    /* convert this to a datetime and pass it along */
    server_datetime_t sdt;
    struct field_conv_opts_tz convopts = {0};
    int outdtsz;
    int ret = 0;
    int isnull = 0;
    struct sql_thread *thd =pthread_getspecific(query_info_key);

    if(!thd || !thd->clnt)
        strcpy(convopts.tzname, "UTC");
    else
        strcpy(convopts.tzname, thd->clnt->tzname);
    convopts.flags = FLD_CONV_TZONE;

    if (CLIENT_CSTR_to_SERVER_DATETIME(time_str, strlen(time_str) + 1, 0,
                                       (struct field_conv_opts *)&convopts,
                                       NULL, &sdt, sizeof(sdt), &outdtsz, NULL,
                                       NULL)) {
        logmsg(LOGMSG_ERROR,
               "Error client time string to server datetime \"%s\"\n",
               time_str);
        return -1;
    }

    if (SERVER_DATETIME_to_CLIENT_INT(&sdt, sizeof(sdt), NULL, NULL, &ret,
                                      sizeof(ret), &isnull, &outdtsz, NULL,
                                      NULL)) {
        logmsg(LOGMSG_ERROR, "Error server datetime to client int\n");
        return -1;
    } else {
        ret = ntohl(ret);
    }

    return ret;
}

char *convert_to_start_string(enum view_partition_period period, int value,
                              char *buf, int buflen)
{
    if (IS_TIMEPARTITION(period))
        return convert_epoch_to_time_string(value, buf, buflen);
    if (period == VIEW_PARTITION_MANUAL) {
        snprintf(buf, buflen, "%d", value);
        return buf;
    }
    abort();
}

int convert_from_start_string(enum view_partition_period period,
                              const char *str)
{
    if (IS_TIMEPARTITION(period))
        return convert_time_string_to_epoch(str);
    if (period == VIEW_PARTITION_MANUAL)
        return atoi(str);

    abort();
}

/* convert a views configuration string in a timepart_views_t struct */
static timepart_views_t *_create_all_views(const char *views_str)
{
    timepart_views_t *views;
    timepart_view_t *view;
    cson_value *cson = NULL;
    cson_kvp *kvp;
    cson_object *obj;
    cson_object_iterator it;
    int rc;
    struct errstat xerr = {0};

    rc = cson_parse_string(&cson, views_str, strlen(views_str));
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: views incorrect llmeta format", __func__);
        return NULL;
    }

    if (!cson_value_is_object(cson) ||
        (obj = cson_value_get_object(cson)) == NULL) {
        logmsg(LOGMSG_ERROR, "%s: views format expect object", __func__);
        return NULL;
    }

    views = (timepart_views_t *)calloc(1, sizeof(timepart_views_t));
    if (!views) {
        rc = VIEW_ERR_MALLOC;
        goto done;
    }

    views->preemptive_rolltime =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_VIEWS_DFT_PREEMPT_ROLL_SECS);
    views->rollout_delete_lag =
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_VIEWS_DFT_ROLL_DELETE_LAG_SECS);
    logmsg(LOGMSG_INFO, "Partition rollout preemption=%dsecs lag=%dsecs\n",
           views->preemptive_rolltime, views->rollout_delete_lag);

    cson_object_iter_init(obj, &it);

    while ((kvp = cson_object_iter_next(&it))) {
        cson_string const *ckey = cson_kvp_key(kvp);
        cson_value *v = cson_kvp_value(kvp);
        const char *str;

        /* skip timepart_views, old format */
        if (strcmp(cson_string_cstr(ckey), "timepart_views") == 0) {
            continue;
        }

        if (!cson_value_is_string(v) ||
            ((str = cson_string_cstr(cson_value_get_string(v))) == NULL)) {
            logmsg(LOGMSG_ERROR, "view %s malformed", cson_string_cstr(ckey));
            rc = VIEW_ERR_GENERIC;
            goto done;
        }

        views->views = (timepart_view_t **)realloc(
            views->views, (views->nviews + 1) * sizeof(timepart_view_t *));
        if (!views->views) {
            rc = VIEW_ERR_MALLOC;
            goto done;
        }
        view = timepart_deserialize_view(str, &xerr);
        if (!view) {
            rc = xerr.errval;
            goto done;
        }

        rc = _view_register_shards(views, view, &xerr);
        if (rc) {
            logmsg(LOGMSG_ERROR, "Time partition error rc=%d [%s]\n",
                   xerr.errval, xerr.errstr);
            timepart_free_view(view);
            view = NULL;
            continue;
        }
        if (view->rolltype == TIMEPART_ROLLOUT_TRUNCATE) {
            _view_find_current_shard(view);
        }

        /* make sure view names are the same */
        if (strcmp(view->name, cson_string_cstr(ckey))) {
            logmsg(LOGMSG_ERROR, "%s: incorrect view format for key %s\n",
                   __func__, cson_string_cstr(ckey));
            rc = VIEW_ERR_BUG;
            goto done;
        }

        views->views[views->nviews] = view;
        views->nviews++;
    }

    rc = VIEW_NOERR;

done:
    if (rc != VIEW_NOERR) {
        if (views) {
            timepart_free_views(views);
#if 0
            TODO: reinstate this abort() when code is fixed
         free(views);
         views = NULL;
#else
            bzero(views, sizeof(*views));
#endif
        }
        logmsg(LOGMSG_ERROR,
               "Error creating views from llmeta rc=%d errstr=\"%s\"\n",
               xerr.errval, xerr.errstr);
    }
    cson_value_free(cson);

    return views;
}

char *_read_config(const char *filename)
{
    FILE *fd = NULL;
    char *config = NULL;
    char line[512];
    int len = 0;

    fd = fopen(filename, "r");
    if (!fd) {
        logmsg(LOGMSG_ERROR, "%s: cannot open file %s\n", __func__, filename);
        return NULL;
    }

    while (fgets(line, sizeof(line), fd)) {
        config = _concat(config, &len, "%s", line);
        if (!config) {
            logmsg(LOGMSG_ERROR, "%s: cannot create json string\n", __func__);
            goto done;
        }
    }

done:
    fclose(fd);

    return config;
}

/**
 * Create timepartition llmeta entries based on file configuration
 *
 */
int timepart_apply_file(const char *filename)
{
    timepart_views_t *views;
    int rc = 0;
    char *config = NULL;
    int i;
    struct errstat err = {0};

    config = _read_config(filename);
    if (!config)
        return VIEW_ERR_GENERIC;

    views = _create_all_views(config);
    if (!views) {
        logmsg(LOGMSG_ERROR, "%s: cannot deserialize json string\n", __func__);
        rc = VIEW_ERR_GENERIC;
        goto done;
    }

    Pthread_rwlock_wrlock(&views_lk);

    for (i = 0; i < views->nviews; i++) {
        rc = partition_llmeta_write(NULL, views->views[i], 1, &err);
        if (rc != VIEW_NOERR) {
            Pthread_rwlock_unlock(&views_lk);
            goto done;
        }
    }

    Pthread_rwlock_unlock(&views_lk);

done:
    if (views)
        timepart_free_views(views);
    free(config);

    return rc;
}

#ifdef VIEWS_PERSIST_TEST

const char *tests[2] = {"[\n"
                        " {\n"
                        "  \"NAME\"        :  \"testview\",\n"
                        "  \"PERIOD\"      :  \"daily\",\n"
                        "  \"RETENTION\"   :  3,\n"
                        "  \"TABLES\"      :  \n"
                        "   [\n"
                        "    {\n"
                        "     \"TABLENAME\":  \"t\",\n"
                        "     \"LOW\"      :  -2147483648,\n"
                        "     \"HIGH\"     :  2147483647\n"
                        "    }\n"
                        "   ]\n"
                        " }\n"
                        "]",
                        "[\n"
                        " {\n"
                        "  \"NAME\"        :  \"testview\",\n"
                        "  \"PERIOD\"      :  \"daily\",\n"
                        "  \"RETENTION\"   :  3,\n"
                        "  \"TABLES\"      :  \n"
                        "   [\n"
                        "    {\n"
                        "     \"TABLENAME\":  \"t\",\n"
                        "     \"LOW\"      :  -2147483648,\n"
                        "     \"HIGH\"     :  100\n"
                        "    },\n"
                        "    {\n"
                        "     \"TABLENAME\":  \"t2\",\n"
                        "     \"LOW\"      :  100,\n"
                        "     \"HIGH\"     :  2147483647\n"
                        "    }\n"
                        "   ]\n"
                        " }\n"
                        "]"

};

int main(int argc, char **argv)
{
    timepart_views_t *repo;
    char *str = NULL;
    int i;
    int rc;
    int orc = 0;

    for (i = 0; i < sizeof(tests) / sizeof(tests[0]); i++) {
        /* test deserializing a string */
        repo = timepart_deserialize(tests[i]);
        if (!repo) {
            logmsg(LOGMSG_ERROR,
                   "FAILURE in test %d deserializing string:\n"
                   "\"%s\"\n",
                   i + 1, tests[i]);
            orc = -1;
            continue;
        }

        /* test serializing a string */
        rc = timepart_serialize(repo, &str, 0);
        if (rc || !str) {
            logmsg(LOGMSG_ERROR,
                   "FAILURE in test %d, serializing repo created from str:\n"
                   "\"%s\"\n",
                   i + 1, tests[i]);
            orc = -1;
        }
        if (str) {
            char *norm_str = normalize_string(str);
            char *norm_test = normalize_string(tests[i]);

            if (strcmp(norm_str, norm_test)) {
                logmsg(LOGMSG_ERROR,
                       "FAILURE in test %d, strings differs, from str:\n"
                       "\"%s\"\n"
                       "to str:\n"
                       "\"%s\"\n",
                       i + 1, tests[i], (str) ? str : "NULL");
                orc = -1;
            }
            free(str);
            free(norm_str);
            free(norm_test);
            str = NULL;
        }

        timepart_free_views(repo);
        free(repo);
    }
    if (!orc)
        printf("SUCCESS in testing %d strings\n", i);

    return orc;
}

#endif

