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
 *          "PERIOD": "daily"|"weekly"|"monthly"|"yearly",
 *          "RETENTION" : n,  #here n is 4
 *          "SOURCE_ID" : "uuid string", 
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
#include "cson_amalgamation_core.h"
#include "views.h"
#include "comdb2uuid.h"

static char *_concat(char *str, int *len, const char *fmt, ...);

static const char *_cson_extract_str(cson_object *cson_obj, const char *param,
                                     struct errstat *err);
static int timepart_deserialize_cson_value(cson_value *cson_view,
                                           timepart_view_t *view,
                                           struct errstat *err);
static void _view_adjust_start_time(timepart_view_t *view, int now);

char *build_createcmd_json(char **out, int *len, const char *name,
                           const char *tablename, uint32_t period,
                           int retention, uint64_t starttime)
{
    char time[256];
    const char *period_str = period_to_name(period);
    convert_epoch_to_time_string(starttime, time, sizeof(time));
    *out = _concat(NULL, len, " {\n"
                              "  \"COMMAND\"       : \"CREATE\",\n"
                              "  \"TYPE\"      : \"TIME BASED\",\n"
                              "  \"NAME\"  : \"%s\",\n"
                              "  \"SHARD0NAME\"    : \"%s\",\n"
                              "  \"PERIOD\"    : \"%s\",\n"
                              "  \"RETENTION\" : %d,\n"
                              "  \"STARTTIME\" : \"%s\"\n"
                              "}",
                   name, tablename, period_str, retention, time);
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

    str =
        _concat(str, &len, " {\n"
                           "  \"NAME\"      : \"%s\",\n"
                           "  \"PERIOD\"    : \"%s\",\n"
                           "  \"RETENTION\" : %d,\n"
                           "  \"STARTTIME\" : \"%s\",\n"
                           "  \"SHARD0NAME\": \"%s\",\n"
                           "  \"SOURCE_ID\" : \"%s\",\n"
                           "  \"TABLES\"    :\n"
                           "  [\n",
                view->name, period_to_name(view->period), view->retention,
                convert_epoch_to_time_string(view->starttime, now, sizeof(now)),
                view->shard0name, comdb2uuidstr(view->source_id, us));
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
            convert_epoch_to_time_string(shard->low, lowstr, sizeof(lowstr));
            convert_epoch_to_time_string(shard->high, highstr, sizeof(highstr));
        }

        str = _concat(str, &len, "  {\n"
                                 "   \"TABLENAME\"    : \"%s\",\n"
                                 "   \"LOW\"          : %s,\n"
                                 "   \"HIGH\"         : %s\n"
                                 "  }%s\n",
                      shard->tblname, lowstr, highstr,
                      (j == view->nshards - 1) ? "" : ",");
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
    int rc;

    *out = NULL;

    pthread_mutex_lock(&views_mtx);

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
    pthread_mutex_unlock(&views_mtx);

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
    rc = cson_parse_string(&cson_view, str, strlen(str), NULL, NULL);
    if (rc) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Parsing JSON error rc=%d err:%s\n", rc, cson_rc_string(rc));
        err->errval = VIEW_ERR_PARAM;
        goto done;
    }

    view = (timepart_view_t *)calloc(1, sizeof(timepart_view_t));
    if (!view) {
        snprintf(err->errstr, sizeof(err->errstr), "Malloc OOM");
        err->errval = VIEW_ERR_MALLOC;
        goto done;
    }

    rc = timepart_deserialize_cson_value(cson_view, view, err);
    if (rc != VIEW_NOERR) {
        if (view) {
            timepart_free_view(view);
            view = NULL;
        }
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
    timepart_views_t *views;
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
    rc = cson_parse_string(&cson, str, strlen(str), NULL, NULL);
    if (rc) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Parsing JSON error rc=%d err:%s\n", rc, cson_rc_string(rc));
        rc = err->errval = VIEW_ERR_PARAM;
        return NULL;
    }

    if (!cson_value_is_array(cson)) {
        fprintf(stderr, "Wrong JSON format, we expect an array of views\n");
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
        fprintf(stderr, "Wrong JSON format, empty array of views\n");
        goto error;
    }

    views->views = (timepart_view_t **)calloc(len, sizeof(timepart_view_t *));
    if (!views->views) {
        goto oom;
    }

    for (i = 0; i < len; i++) {
        views->views[i] = (timepart_view_t *)calloc(1, sizeof(timepart_view_t));
        if (!views->views[i]) {
            goto oom;
        }

        cson_view = cson_array_get(view_arr, i);
        if (!cson_view) {
            snprintf(err->errstr, sizeof(err->errstr),
                     "VIEWS string has no arrays of views");
            rc = err->errval = VIEW_ERR_PARAM;
            goto error;
        }

        rc = timepart_deserialize_cson_value(cson_view, views->views[i], err);
        if (rc != VIEW_NOERR) {
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
    timepart_views_t *views;
    timepart_view_t *view;
    char *views_str;
    cson_value *cson = NULL;
    cson_kvp *kvp;
    cson_object *obj;
    cson_object_iterator it;
    int rc;
    struct errstat xerr = {0};

    views_str = views_read_all_views();

    if (!views_str) {
        return (timepart_views_t *)calloc(1, sizeof(timepart_views_t));
    }

    rc = cson_parse_string(&cson, views_str, strlen(views_str), NULL, NULL);
    if (rc) {
        fprintf(stderr, "%s: views incorrect llmeta format", __func__);
        return NULL;
    }

    if (!cson_value_is_object(cson) ||
        (obj = cson_value_get_object(cson)) == NULL) {
        fprintf(stderr, "%s: views format expect object", __func__);
        return NULL;
    }

    views = (timepart_views_t *)calloc(1, sizeof(timepart_views_t));
    if (!views) {
        rc = VIEW_ERR_MALLOC;
        goto done;
    }

    views->preemptive_rolltime = VIEWS_DFT_PREEMPT_ROLL_SECS;
    views->rollout_delete_lag = VIEWS_DFT_ROLL_DELETE_LAG_SECS;
    fprintf(stderr, "Partition rollout preemption=%dsecs lag=%dsecs\n",
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
            fprintf(stderr, "view %s malformed", cson_string_cstr(ckey));
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

        rc = views_validate_view(views, view, &xerr);
        if(rc) {
            fprintf(stderr, "Time partition error rc=%d [%s]\n", 
                    xerr.errval, xerr.errstr);
            timepart_free_view(view);
            view=NULL;
            continue;
        }

        /* make sure view names are the same */
        if (strcmp(view->name, cson_string_cstr(ckey))) {
            fprintf(stderr, "%s: incorrect view format for key %s\n", __func__,
                    ckey);
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
        fprintf(stderr,
                "Error creating views from llmeta rc=%d errstr=\"%s\"\n",
                xerr.errval, xerr.errstr);
    }
    cson_value_free(cson);

    return views;
}

/* parse a user-provided json create command and update the in-memory views */
static int _views_do_partition_create(void *tran, timepart_views_t *views,
                                      const char *name, cson_value *cson,
                                      const char *cmd, struct errstat *err)
{
    timepart_view_t *view;
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

    view = (timepart_view_t *)calloc(1, sizeof(timepart_view_t));
    if (!view) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Invalid view type for \"%s\"", name);
        err->errval = VIEW_ERR_PARAM;
        goto error;
    }

    /* extract the time partition schedule */
    rc = timepart_deserialize_cson_value(cson, view, err);
    if (rc != VIEW_NOERR) {
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

    /* time to add the view */
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

/**
 * Process a cson command
 *
 * Format:
 * {
 *    "COMMAND"   : "CREATE|DESTROY|DISPLAY",
 *    "VIEWNAME"  : "a_name",
 *    "TYPE"      : "TIME BASED",
 *    "PERIOD     : "DAILY|WEEKLY|MONTHLY|YEARLY",
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
    char *latest_views_str;

    /* string to conversion */
    rc = cson_parse_string(&cson_cmd, cmd, strlen(cmd), NULL, NULL);
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
        rc = _views_do_partition_create(tran, views, name, cson_cmd, cmd, err);
        if (rc != VIEW_NOERR) {
            goto error;
        }
    } else if (!strcasecmp(op, "DESTROY")) {
        rc = _views_do_partition_destroy(tran, views, name, cson_cmd, cmd, err);
        if (rc != VIEW_NOERR) {
            goto error;
        }
    } else {
        err->errval = VIEW_ERR_UNIMPLEMENTED;
        fprintf(stderr, "Unrecognized command \"%s\" in:\n\"%s\"\n", op, cmd);
        goto error;
    }

#if 0
   /* time to change the change persistent */
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
const char INVALID_STR[] = "";

const char *period_to_name(enum view_timepart_period period)
{
    switch (period) {
    case VIEW_TIMEPART_DAILY:
        return DAILY_STR;
    case VIEW_TIMEPART_WEEKLY:
        return WEEKLY_STR;
    case VIEW_TIMEPART_MONTHLY:
        return MONTHLY_STR;
    case VIEW_TIMEPART_YEARLY:
        return YEARLY_STR;
    case VIEW_TIMEPART_TEST2MIN:
        return TEST2MIN_STR;
    }
    return INVALID_STR;
}

enum view_timepart_period name_to_period(const char *str)
{
    if (!strcasecmp(str, DAILY_STR))
        return VIEW_TIMEPART_DAILY;
    if (!strcasecmp(str, WEEKLY_STR))
        return VIEW_TIMEPART_WEEKLY;
    if (!strcasecmp(str, MONTHLY_STR))
        return VIEW_TIMEPART_MONTHLY;
    if (!strcasecmp(str, YEARLY_STR))
        return VIEW_TIMEPART_YEARLY;
    if (!strcasecmp(str, TEST2MIN_STR))
        return VIEW_TIMEPART_TEST2MIN;

    return VIEW_TIMEPART_INVALID;
}

static cson_value *_cson_it_get_value(cson_object_iterator *it,
                                      const char *field, int itr)
{
    const char *key;
    cson_kvp *kv;

    kv = cson_object_iter_next(it);
    if (!kv) {
        fprintf(stderr, "Wrong JSON format, no %s for %d view\n", field, itr);
        return NULL;
    }

    key = cson_string_cstr(cson_kvp_key(kv));
    if (strcasecmp(key, field)) {
        fprintf(stderr, "Wrong JSON format, no %s for %d view\n", field, itr);
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

#if 0
   fprintf(stderr, "Converted:\n%s\nto:\n%s\n",str, out);
#endif

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
    cson_array *array;

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

static int _cson_extract_datetime_string(cson_object *cson_obj,
                                         const char *param, struct errstat *err)
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
        ret_int = convert_time_string_to_epoch(
            cson_string_cstr(cson_value_get_string(param_val)));
    } else {
        err->errval = VIEW_ERR_PARAM;
        snprintf(err->errstr, sizeof(err->errstr),
                 "JSON \"%s\" field not string or integer", param);
        return -1;
    }

    return ret_int;
}

/* this will extract all the fields and populate a provided view */
static int timepart_deserialize_cson_value(cson_value *cson_view,
                                           timepart_view_t *view,
                                           struct errstat *err)
{
    cson_object *obj;
    cson_value *val;
    const char *tmp_str;

    cson_array *tbl_arr = NULL;
    cson_object *obj_arr;
    int j;
    int rc = VIEW_NOERR;
    int now;

    if (!cson_value_is_object(cson_view) ||
        (obj = cson_value_get_object(cson_view)) == NULL) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Expect an object for a view");
        rc = err->errval = VIEW_ERR_PARAM;
        goto error;
    }

    /* NAME */
    tmp_str = _cson_extract_str(obj, "NAME", err);
    if (!tmp_str) {
        rc = err->errval;
        goto error;
    }
    view->name = strdup(tmp_str);
    if (!view->name) {
        goto oom;
    }

    /* PERIOD */
    tmp_str = _cson_extract_str(obj, "PERIOD", err);
    if (!tmp_str) {
        rc = err->errval;
        goto error;
    }
    view->period = name_to_period(tmp_str);
    if (view->period == VIEW_TIMEPART_INVALID) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Wrong JSON format, PERIOD value invalid");
        rc = err->errval = VIEW_ERR_PARAM;
        goto error;
    }

    /* RETENTION */
    view->retention = _cson_extract_int(obj, "RETENTION", err);
    if (view->retention < 0) {
        rc = err->errval;
        goto error;
    }

    /* check for starttime */
    view->starttime = _cson_extract_datetime_string(obj, "STARTTIME", err);
    _view_adjust_start_time(view, time_epoch());

    tmp_str = _cson_extract_str(obj, "SOURCE_ID", err);
    if(!tmp_str)
        comdb2uuid_clear(view->source_id);
    else
        if(uuid_parse(tmp_str, view->source_id)) {
            snprintf(err->errstr, sizeof(err->errstr),
                    "Wrong JSON format, SOURCE_ID value invalid uuid");
            rc = err->errval = VIEW_ERR_PARAM;
            goto error;
        }

    /* TABLES */
    tbl_arr = _cson_extract_array(obj, "TABLES", err);
    if (!tbl_arr) {
        /* initial creation doesn't require a table section */
        bzero(err, sizeof(*err));
        goto look_for_shard0;
    }

    view->nshards = cson_array_length_get(tbl_arr);
    if (view->nshards <= 0) {
        snprintf(err->errstr, sizeof(err->errstr),
                 "Wrong JSON format, TABLES has no shards");
        rc = err->errval = VIEW_ERR_PARAM;
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
            snprintf(err->errstr, sizeof(err->errstr),
                     "Wrong JSON format, TABLES entry not object");
            rc = err->errval = VIEW_ERR_PARAM;
            goto error;
        }

        /* TBLNAME */
        tmp_str = _cson_extract_str(obj_arr, "TABLENAME", err);
        if (!tmp_str) {
            rc = err->errval;
            goto error;
        }

        view->shards[j].tblname = strdup(tmp_str);
        if (!view->shards[j].tblname) {
            goto oom;
        }

        /* LOW */
        view->shards[j].low = _cson_extract_int(obj_arr, "LOW", err);
        if ((j == view->nshards - 1) && view->shards[j].low != INT_MIN) {
            snprintf(
                err->errstr, sizeof(err->errstr),
                "Wrong JSON format, TABLES entry %d has wrong integer for LOW",
                j);
            rc = err->errval = VIEW_ERR_PARAM;
            goto error;
        }

        /* HIGH */
        view->shards[j].high = _cson_extract_int(obj_arr, "HIGH", err);
        if ((j == 0) && view->shards[j].high != INT_MAX) {
            snprintf(
                err->errstr, sizeof(err->errstr),
                "Wrong JSON format, TABLES entry %d has wrong integer for HIGH",
                j);
            rc = err->errval = VIEW_ERR_PARAM;
            goto error;
        }
    }

look_for_shard0:
    /* SHARD0NAME */
    tmp_str = _cson_extract_str(obj, "SHARD0NAME", err);
    if (!tmp_str) {
        if (view->nshards != 1) {
            /* this catches cases when both TABLES and SHARD0NAME are missing !
             */
            rc = err->errval;
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

    rc = VIEW_NOERR;

error:
    /* error "err", if any prep-ed by called functionality */

    return rc;

oom:
    snprintf(err->errstr, sizeof(err->errstr), "%s Malloc OOM", __func__);
    err->errval = VIEW_ERR_MALLOC;
    return VIEW_ERR_MALLOC;
}

static void _view_adjust_start_time(timepart_view_t *view, int now)
{
#if 0
   Current generic cron handles past times as well, no need to adjust
#define MIN_PREAMBLE_TIME_SECS 600

   if(view->starttime < now)
   {
      if(view->starttime < 0)
      {
         fprintf(stderr, "Warning: creating view has no start time, starting from now=%d\n",
               now);
      }
      else
      {
         fprintf(stderr, "Warning: starttime before \"now\", starting from now=%d\n",
               now);
      }
      view->starttime = now;
   }
   else
   {
      if(view->starttime < now+MIN_PREAMBLE_TIME_SECS)
      {
         if(view->period == VIEW_TIMEPART_TEST2MIN)
         {
            /* try to run the rollout phase-1 60 seconds before startime */
            view->starttime = now + view->period/2;
         }
         else
         {
            fprintf(stderr, 
                  "Warning: starttime too early, needs 10 min prep time, setting startime to now + 10min\n");
            view->starttime = now+MIN_PREAMBLE_TIME_SECS;
         }
      }
   }
#endif
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
    if(!thd || !thd->sqlclntstate)
	    strcpy(outopts.tzname, "UTC");
    else
	    strcpy(outopts.tzname, thd->sqlclntstate->tzname);
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

    if(!thd || !thd->sqlclntstate)
	    strcpy(convopts.tzname, "UTC");
    else
	    strcpy(convopts.tzname, thd->sqlclntstate->tzname);
    convopts.flags = FLD_CONV_TZONE;

    if (CLIENT_CSTR_to_SERVER_DATETIME(time_str, strlen(time_str) + 1, 0,
                                       (struct field_conv_opts *)&convopts,
                                       NULL, &sdt, sizeof(sdt), &outdtsz, NULL,
                                       NULL)) {
        fprintf(stderr, "Error client time string to server datetime \"%s\"\n",
                time_str);
        return -1;
    }

    if (SERVER_DATETIME_to_CLIENT_INT(&sdt, sizeof(sdt), NULL, NULL, &ret,
                                      sizeof(ret), &isnull, &outdtsz, NULL,
                                      NULL)) {
        fprintf(stderr, "Error server datetime to client int\n");
        return -1;
    } else {
        ret = ntohl(ret);
    }

    return ret;
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
            fprintf(stderr, "FAILURE in test %d deserializing string:\n"
                            "\"%s\"\n",
                    i + 1, tests[i]);
            orc = -1;
            continue;
        }

        /* test serializing a string */
        rc = timepart_serialize(repo, &str, 0);
        if (rc || !str) {
            fprintf(stderr,
                    "FAILURE in test %d, serializing repo created from str:\n"
                    "\"%s\"\n",
                    i + 1, tests[i]);
            orc = -1;
        }
        if (str) {
            char *norm_str = normalize_string(str);
            char *norm_test = normalize_string(tests[i]);

            if (strcmp(norm_str, norm_test)) {
                fprintf(stderr,
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

