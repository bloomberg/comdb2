/**
 * Middle layer between a CSON representation and llmeta
 *
 */

#include "bdb_api.h"
#include "views.h"

#define LLMETA_PARAM_NAME "timepart_views"
#define LLMETA_TABLE_NAME "sys_views"
#define LLMETA_MOD_VIEWS_TABLE "mod_views"
/**
 *  Write a CSON representation overriding the current llmeta
 *
 *  NOTE: writing a NULL or 0 length string deletes existing entry if any
 *
 */
int views_write(const char *str)
{
    int rc;

    if (str) {
        /* this is an upsert */
        rc = bdb_set_table_parameter(NULL, LLMETA_TABLE_NAME, LLMETA_PARAM_NAME,
                                     str);
    } else {
        /* this is a delete */
        /* TODO: llmeta reports -1 on a notfound delete...
           fix this to be differentiated from other errors */
        rc = bdb_clear_table_parameter(NULL, LLMETA_TABLE_NAME,
                                       LLMETA_PARAM_NAME);
    }

    if (rc)
        return VIEW_ERR_LLMETA;

    return VIEW_NOERR;
}

/**
 * Read a CSON representation from llmeta
 *
 */
int views_read(char **pstr)
{
    int rc;

    *pstr = NULL;

    rc = bdb_get_table_parameter(LLMETA_TABLE_NAME, LLMETA_PARAM_NAME, pstr);
    if (rc == 1) {
        return VIEW_ERR_EXIST;
    } else if (rc) {
        return VIEW_ERR_LLMETA;
    }

    return VIEW_NOERR;
}

/**
 *  Write a CSON representation of a view
 *  The view is internally saved as a parameter "viewname" for the table
 *  "sys_views".
 *  "override" set to 1 allows the function to behave like an upsert (if
 *  old view exists, it is overwritten).  Otherwise, an error is returned.
 *
 *  NOTE: writing a NULL or 0 length string deletes existing entry if any
 */
int views_write_view(void *tran, const char *viewname, const char *str,
                     int override)
{
    int rc;

    if (str) {
        /* check if the table exists, and if it does, return error */
        if (!override) {
            char *oldstr = NULL;
            rc = views_read_view(tran, viewname, &oldstr);
            if (rc == VIEW_NOERR) {
                logmsg(LOGMSG_ERROR,
                       "View \"%s\" already exists, old string \"%s\"\n",
                       viewname, oldstr);
                free(oldstr);
                return VIEW_ERR_EXIST;
            }
        }
        /* this is an upsert */
        rc = bdb_set_table_parameter(tran, LLMETA_TABLE_NAME, viewname, str);
    } else {
        /* this is a delete */
        /* TODO: llmeta reports -1 on a notfound delete...
           fix this to be differentiated from other errors */
        rc = bdb_clear_table_parameter(tran, LLMETA_TABLE_NAME, viewname);
    }
    rc = (rc) ? VIEW_ERR_LLMETA : VIEW_NOERR;

    return rc;
}

/**
 * Read a view CSON representation from llmeta
 *
 */
int views_read_view(void *tran, const char *name, char **pstr)
{
    int rc;

    *pstr = NULL;

    rc = bdb_get_table_parameter_tran(LLMETA_TABLE_NAME, name, pstr, tran);
    if (rc == 1) {
        return VIEW_ERR_EXIST;
    } else if (rc) {
        return VIEW_ERR_LLMETA;
    }

    return VIEW_NOERR;
}

/**
 * Read the llmeta cson representation of all the views
 *
 */
char *views_read_all_views(void)
{
    char *blob = NULL;
    int blob_len = 0;
    int rc;

    rc =
        bdb_get_table_csonparameters(NULL, LLMETA_TABLE_NAME, &blob, &blob_len);
    if (rc) {
        return NULL;
    }

    return blob;
}

/**
 * Read a view CSON representation from llmeta
 *
 */
int mod_views_read_view(void *tran, const char *name, char **pstr)
{
    int rc;

    *pstr = NULL;

    rc = bdb_get_table_parameter_tran(LLMETA_MOD_VIEWS_TABLE, name, pstr, tran);
    if (rc == 1) {
        return VIEW_ERR_EXIST;
    } else if (rc) {
        return VIEW_ERR_LLMETA;
    }

    return VIEW_NOERR;
}
/**
 *  Write a CSON representation of a view of mod based shard partitions
 *  The view is internally saved as a parameter "viewname" for the table
 *  "mod_views".
 */
int mod_views_write_view(void *tran, const char *viewname, const char *str, int override)
{
    int rc;

    if (str) {
        char *oldstr = NULL;
        rc = mod_views_read_view(tran, viewname, &oldstr);
        if (rc == VIEW_NOERR) {
            logmsg(LOGMSG_ERROR, "View \"%s\" already exists, old string \"%s\"\n", viewname, oldstr);
            free(oldstr);
            return VIEW_ERR_EXIST;
        }
        rc = bdb_set_table_parameter(tran, LLMETA_MOD_VIEWS_TABLE, viewname, str);
    } else {
        /* this is a delete */
        rc = bdb_clear_table_parameter(tran, LLMETA_MOD_VIEWS_TABLE, viewname);
    }
    rc = (rc) ? VIEW_ERR_LLMETA : VIEW_NOERR;

    return rc;
}

char *mod_views_read_all_views(void)
{
    char *blob = NULL;
    int blob_len = 0;
    int rc;

    rc = bdb_get_table_csonparameters(NULL, LLMETA_MOD_VIEWS_TABLE, &blob, &blob_len);
    if (rc) {
        return NULL;
    }

    return blob;
}
