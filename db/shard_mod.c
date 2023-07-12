#include "shard_mod.h"
#include <plhash_glue.h>
#include "views.h"
#include "cson/cson.h"
struct mod_shard {
    int mod_val;
    char *dbname;

};

struct mod_view {
    char *viewname;
    char *tblname;
    /* TODO: make separate key type */
    char *keyname; 
    int num_shards;
    hash_t *shards;
};

static int free_mod_shard(void *obj, void *unused) {
    struct mod_shard *shard = (struct mod_shard *)obj;
    if (shard) {
        if (shard->dbname) {
            free(shard->dbname);
        }
        free(shard);
    }
    return 0;
}

static mod_shard_t *create_mod_shard(int key, char *name) 
{
    mod_shard_t *mShard;
    mShard = (mod_shard_t *)calloc(1, sizeof(mod_shard_t));

    if (!mShard) {
        goto oom;
    }
    mShard->mod_val = key;
    mShard->dbname = strdup(name);
    if (!mShard->dbname) {
        goto oom;
    }

    return mShard;

oom:
    logmsg(LOGMSG_ERROR,"%s : Failed to allocate memory for shard with key: %d and dbname: %s\n", __func__, key, name);
    if (mShard) {
        if (mShard->dbname) {
            free(mShard->dbname);
        }
        free(mShard);
    }

    return NULL;
}

const char *mod_view_get_viewname(struct mod_view *view) {
    return view->viewname;
}

const char *mod_view_get_tablename(struct mod_view *view) {
    return view->tblname;
}
const char *mod_view_get_keyname(struct mod_view *view) {
    return view->keyname;
}
int mod_view_get_num_shards(struct mod_view *view) {
    return view->num_shards;
}

hash_t *mod_view_get_shards(struct mod_view *view) {
    return view->shards;
}
int mod_shard_get_mod_val(struct mod_shard *shard) {
    return shard->mod_val;
}

const char *mod_shard_get_dbname(struct mod_shard *shard) {
    return shard->dbname;
}

static void free_inmem_view(mod_view_t *mView) {
    if (mView) {
        if (mView->viewname) {
            free(mView->viewname);
        }
        if (mView->tblname) {
            free(mView->tblname);
        }
        if (mView->keyname) {
            free(mView->keyname);
        }

        if (mView->shards) {
            hash_for(mView->shards, free_mod_shard, NULL);
            hash_clear(mView->shards);
            hash_free(mView->shards);
        }
        free(mView);
    }
}

mod_view_t *create_mod_view(const char *viewname, const char *tablename, const char *keyname, uint32_t num_shards, uint32_t keys[], char shards[][MAX_DBNAME_LENGTH], struct errstat *err) 
{
    mod_view_t *mView;

    mView = (mod_view_t *)calloc(1, sizeof(mod_view_t));

    if (!mView) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate view %s\n",__func__, viewname);
        goto oom;
    }

    mView->viewname = strdup(viewname);
    if (!mView->viewname) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate view name string %s\n",__func__, viewname);
        goto oom;
    }

    mView->tblname = strdup(tablename);
    if (!mView->tblname) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate table name string %s\n",__func__, tablename);
        goto oom;
    }
    mView->keyname = strdup(keyname);
    if (!mView->keyname) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate key name string %s\n",__func__, keyname);
        goto oom;
    }
    mView->num_shards = num_shards;

    mView->shards = hash_init_o(offsetof(struct mod_shard, mod_val), sizeof(int));
    mod_shard_t *tmp = NULL;
    for (int i=0;i<num_shards;i++) {
        tmp = create_mod_shard(keys[i], shards[i]);
        if (!tmp) {
            goto oom;
        }
        hash_add(mView->shards, tmp);
    }

    return mView;
oom:
    free_inmem_view(mView);
    errstat_set_rcstrf(err, VIEW_ERR_MALLOC, "calloc oom");
    return NULL;
}

static int create_inmem_view(hash_t *mod_views, mod_view_t *view) {
    hash_add(mod_views, view);
    return VIEW_NOERR;
}


static int destroy_inmem_view(hash_t *mod_views, mod_view_t *view) {
    struct mod_view *v = hash_find(mod_views, view->viewname);

    if (!v) {
        return VIEW_ERR_NOTFOUND;
    }
    hash_del(mod_views, v);
    return VIEW_NOERR;
}

int mod_create_inmem_view(mod_view_t *view) {
    int rc;
    rc = create_inmem_view(thedb->mod_shard_views, view);
    if (rc!=VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to create in-memory view %s. rc: %d\n",
                __func__, view->viewname, rc);
    }
    return rc;
}

int mod_destroy_inmem_view(mod_view_t *view) {
    int rc;
    rc = destroy_inmem_view(thedb->mod_shard_views, view);
    if (rc!=VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to destroy in-memory view %s. rc: %d\n",
                __func__, view->viewname , rc);
    }
    return rc;
}

int mod_shard_llmeta_write(void *tran, mod_view_t *view, struct errstat *err) {
    /* 
     * Get serialized view string 
     * write to llmeta
     */
    char *view_str = NULL;
    int view_str_len;
    int rc;

    rc = mod_serialize_view(view, &view_str_len, &view_str);
    if (rc!=VIEW_NOERR) {
        errstat_set_strf(err, "Failed to serialize view %s", view->viewname);
        errstat_set_rc(err, rc = VIEW_ERR_BUG);
        goto done;
    }

    logmsg(LOGMSG_USER, "%s\n", view_str);
    /* save the view */
    rc = mod_views_write_view(tran, view->viewname, view_str, 0);
    if (rc != VIEW_NOERR) {
        if (rc == VIEW_ERR_EXIST)
            errstat_set_rcstrf(err, VIEW_ERR_EXIST,
                               "View %s already exists", view->viewname);
        else
            errstat_set_rcstrf(err, VIEW_ERR_LLMETA,
                               "Failed to llmeta save view %s", view->viewname);
        goto done;
    }

done:
    if (view_str)
        free(view_str);
    return rc;
}

char *mod_views_read_all_views();
/*
 * Create hash map of all mod based shards
 *
 */
hash_t *mod_create_all_views()
{
    hash_t *mod_views;
    char *views_str;
    int rc;
    struct mod_view *view = NULL;
    cson_value *rootVal = NULL;
    cson_object *rootObj = NULL;
    cson_object_iterator iter;
    cson_kvp *kvp = NULL;
    struct errstat err;
    mod_views = hash_init_strcaseptr(offsetof(struct mod_view, viewname));
    views_str = mod_views_read_all_views();
    if (!views_str) {
        return mod_views;
    }
    /* populate global hash of all mod based table partitions */
    rc = cson_parse_string(&rootVal, views_str, strlen(views_str));
    if (rc) {
        logmsg(LOGMSG_ERROR, "error parsing cson string. rc: %d err: %s\n",
                rc, cson_rc_string(rc));
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
        cson_string *key = cson_kvp_key(kvp);
        logmsg(LOGMSG_USER, "THE CSON KEY IS : %s\n", cson_string_cstr(key));
        cson_value *val = cson_kvp_value(kvp);
        /*
         * Each cson_value above is a cson string representation
         * of a mod based partition. Validate that it's a string value
         * and deserialize into an in-mem representation.
         */
        if (!cson_value_is_string(val)) {
            logmsg(LOGMSG_ERROR, "error parsing cson: expected object type\n");
            goto done;
        }
        const char *view_str = cson_string_cstr(cson_value_get_string(val));
        view = mod_deserialize_view(view_str, &err);

        if(!view) {
            logmsg(LOGMSG_ERROR, "%s: failed to deserialize mod view %d %s\n", __func__,
                    err.errval, err.errstr);
            goto done;
        }
        /* we've successfully deserialized a view. 
         * now add it to the global hash of all mod-parititon based views */
        hash_add(mod_views, view);
        kvp = cson_object_iter_next(&iter);
    }
done:
    if (rootVal) {
        cson_value_free(rootVal);
    }
    return mod_views;
}


int mod_views_update_replicant(void *tran, const char *name)
{
    hash_t *mod_views = thedb->mod_shard_views;
    mod_view_t *view = NULL, *v = NULL;
    int rc = VIEW_NOERR;
    return rc;
    logmsg(LOGMSG_USER, "Replicant updating views\n");
    char *view_str = NULL;
    struct errstat xerr = {0};

    /* read the view str from updated llmeta */
    rc = mod_views_read_view(tran, name, &view_str);
    if (rc == VIEW_ERR_EXIST) {
        view = NULL;
        goto update_view_hash;
    } else if (rc != VIEW_NOERR || !view_str) {
        logmsg(LOGMSG_ERROR, "%s: Could not read metadata for view %s\n", __func__, name);
        goto done;
    }

    /* create an in-mem view object */
    view = mod_deserialize_view(view_str, &xerr); 
    if (!view) {
        logmsg(LOGMSG_ERROR, "%s: failed to deserialize mod view %d %s\n", __func__,
                xerr.errval, xerr.errstr);
        goto done;
    }
update_view_hash:
    /* update global mod views hash.
     * - If a view with the given name exists, destroy it, create a new view and add to hash
     * - If a view with the given name does not exist, add to hash
     * - If view is NULL (not there in llmeta), destroy the view and remove from hash */
    
    v = hash_find(mod_views, name);

    if (v) {
       hash_del(mod_views, v);
       free_inmem_view(v);
       if (view) {
           hash_add(mod_views, view);
       }
    } else {
        assert(view != NULL);
        hash_add(mod_views, view);
    }

    rc = VIEW_NOERR;
done:
    if(view_str) {
        free(view_str);
    }

    return rc;
}
