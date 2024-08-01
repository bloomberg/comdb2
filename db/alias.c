#include <stdlib.h>
#include <string.h>
#include <limits.h>
#include <string.h>
#include <pthread.h>
#include <memory_sync.h>

#include "comdb2.h"
#include "sql.h"
#include "schemachange.h"
#include "bdb_schemachange.h"
#include "str0.h"
#include "logmsg.h"
#include "cron.h"
#include "cron_systable.h"
#include "timepart_systable.h"
#include "logical_cron.h"
#include "sc_util.h"
#include "bdb_int.h"
#include "util.h"

static hash_t *alias_tbl_hash = NULL;
static pthread_mutex_t alias_tbl_hash_lk = PTHREAD_MUTEX_INITIALIZER;
struct table_alias {
    char *alias;
    char *tablename;
};

void load_aliases_from_llmeta() {
    if (!alias_tbl_hash) {
        alias_tbl_hash = hash_init_strptr(0);
    }
    llmeta_collect_tablename_alias();
}

static int free_alias(void *obj, void *arg)
{
    struct table_alias *t = (struct table_alias *)obj;
    if (t != NULL) {
        free(t->alias);
        free(t->tablename);
        free(t);
    }
    return 0;
}

void clear_aliases() {
    Pthread_mutex_lock(&alias_tbl_hash_lk);
    hash_for(alias_tbl_hash, free_alias, NULL);
    hash_clear(alias_tbl_hash);
    Pthread_mutex_unlock(&alias_tbl_hash_lk);
}

void reload_aliases() {
    clear_aliases();
    load_aliases_from_llmeta();
}

char *get_tablename(const char *alias) {
    struct table_alias *t = NULL;
    char *result = NULL;
    Pthread_mutex_lock(&alias_tbl_hash_lk);
    t = hash_find(alias_tbl_hash, &alias);
    if (t) {
        result = strdup(t->tablename);
    }
    Pthread_mutex_unlock(&alias_tbl_hash_lk);
    return result;
}

int add_alias(const char *alias, const char *tablename){
    struct table_alias *t = calloc(1, sizeof(struct table_alias));
    if (!t) {
        logmsg(LOGMSG_ERROR, "%s:%d : OOM\n", __func__, __LINE__);
        return -1;
    }
    logmsg(LOGMSG_USER, "Adding ALIAS %s for table %s\n", alias, tablename);
    t->alias = strdup(alias);
    t->tablename = strdup(tablename);
    Pthread_mutex_lock(&alias_tbl_hash_lk);
    if (hash_find(alias_tbl_hash, &alias)) {
        logmsg(LOGMSG_ERROR, "Alias %s already exists\n", alias);
        Pthread_mutex_unlock(&alias_tbl_hash_lk);
        return -1;
    }
    hash_add(alias_tbl_hash, t);
    Pthread_mutex_unlock(&alias_tbl_hash_lk);
    return 0;
}

void remove_alias(const char *alias) {
    struct table_alias *t = NULL;
    Pthread_mutex_lock(&alias_tbl_hash_lk);
    t = hash_find(alias_tbl_hash, &alias);
    if(!t) {
        logmsg(LOGMSG_WARN, "Couldn't find alias %s\n", alias);
    } else {
        hash_del(alias_tbl_hash, t);
    }
    Pthread_mutex_unlock(&alias_tbl_hash_lk);
}

char *get_alias(const char *tablename){
    struct table_alias *t = NULL;
    void *ent;
    unsigned int bkt;
    char *result = NULL;
    Pthread_mutex_lock(&alias_tbl_hash_lk);
    for (t = (struct table_alias *)hash_first(alias_tbl_hash, &ent, &bkt); t;
         t = (struct table_alias *)hash_next(alias_tbl_hash, &ent, &bkt)) {
        if (strcmp(t->tablename, tablename)==0) {
            result = strdup(t->alias);
            break;
        }
    }
    Pthread_mutex_unlock(&alias_tbl_hash_lk);
    return result;
}


static int print_alias_info(void *obj, void *arg)
{
    struct table_alias *t = (struct table_alias *)obj;
    if (t != NULL) {
        logmsg(LOGMSG_USER, "\"%s\" -> \"%s\"\n", t->alias, t->tablename);
    }
    return 0;
}
void dump_alias_info() {
    Pthread_mutex_lock(&alias_tbl_hash_lk);
    hash_for(alias_tbl_hash, print_alias_info, NULL);
    Pthread_mutex_unlock(&alias_tbl_hash_lk);
}


