#include "shard_mod.h"
#include <plhash_glue.h>
#include "views.h"
struct mod_shard {
    int mod_val;
    char *dbname;

};

struct mod_view {
    char *name;
    /* TODO: make separate key type */
    char *keyname; 
    int num_shards;
    hash_t *shards;
    uuid_t id;
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

mod_shard_t *create_mod_shard(int key, char *name) 
{
    mod_shard_t *mShard;
    mShard = (mod_shard_t *)calloc(1, sizeof(mod_shard_t));

    if (!mShard) {
        logmsg(LOGMSG_USER,"%s : Failed to allocate memory for shard with key: %d and dbname: %s\n", __func__, key, name);
        return NULL;
    }
    mShard->mod_val = key;
    mShard->dbname = strdup(name);
    if (!mShard->dbname) {
        logmsg(LOGMSG_USER,"%s : Failed to allocate memory for name of shard with key: %d and dbname: %s\n", __func__, key, name);
        return NULL;
    }

    return mShard;
}

const char *mod_view_get_name(struct mod_view *shard) {
    return shard->name;
}

mod_view_t *create_mod_view(char *name, char *keyname, uint32_t num_shards, uint32_t keys[], char shards[][MAX_DBNAME_LENGTH], struct errstat *err) 
{
    mod_view_t *mView;

    mView = (mod_view_t *)calloc(1, sizeof(mod_view_t));

    if (!mView) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate view %s\n",__func__, name);
        goto oom;
    }

    mView->name = strdup(name);
    if (!mView->name) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate view name string %s\n",__func__, name);
        goto oom;
    }

    mView->keyname = strdup(keyname);
    if (!mView->keyname) {
        logmsg(LOGMSG_ERROR, "%s: Failed to allocate key name string %s\n",__func__, name);
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

    comdb2uuid(mView->id);
    return mView;
oom:
    if (mView) {
        if (mView->name) {
            free(mView->name);
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
    errstat_set_rcstrf(err, VIEW_ERR_MALLOC, "calloc oom");
    return NULL;
}

static int create_inmem_view(hash_t *mod_views, mod_view_t *view) {
    hash_add(mod_views, view);
    return VIEW_NOERR;
}

static int destroy_inmem_view(hash_t *mod_views, mod_view_t *view) {
    struct mod_view *v = hash_find(mod_views, view->name);

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
                __func__, view->name, rc);
    }
    return rc;
}

int mod_destroy_inmem_view(mod_view_t *view) {
    int rc;
    rc = destroy_inmem_view(thedb->mod_shard_views, view);
    if (rc!=VIEW_NOERR) {
        logmsg(LOGMSG_ERROR, "%s: failed to destroy in-memory view %s. rc: %d\n",
                __func__, view->name , rc);
    }
    return rc;
}
