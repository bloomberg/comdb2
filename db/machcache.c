/*
   Copyright 2024 Bloomberg Finance L.P.

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

#include <pthread.h>
#include <plhash_glue.h>
#include <string.h>
#include <sys_wrap.h>
#include <cdb2api.h>
#include <machcache.h>

struct class_machs {
    const char *dbname;
    const char *class;
    int count;
    char **machs;
    time_t last_updated;
};

static pthread_mutex_t class_machs_mutex = PTHREAD_MUTEX_INITIALIZER;
static hash_t *class_machs_hash = NULL;

enum { PRIME = 8388013 };
static inline unsigned int hash_two_strptr(const char *s1, const char *s2)
{
    unsigned hash = 0;
    const char *key1 = s1;
    for (; *key1; key1++)
        hash = ((hash % PRIME) << 8) + (*key1);
    const char *key2 = s2;
    for (; *key2; key2++)
        hash = ((hash % PRIME) << 8) + (*key2);
    return hash;
}

static inline unsigned int class_machs_hash_func(const void *key, int len)
{
    struct class_machs *cm = (struct class_machs *)key;
    return hash_two_strptr(cm->dbname, cm->class);
}

static int class_machs_cmp(const void *key1, const void *key2, size_t len)
{
    struct class_machs *cm1 = (struct class_machs *)key1;
    struct class_machs *cm2 = (struct class_machs *)key2;
    int cmp;
    if ((cmp = strcmp(cm1->dbname, cm2->dbname)) != 0) {
        return cmp;
    }
    return strcmp(cm1->class, cm2->class);
}

/* name / class of comdb2db */
static char *comdb2dbclass = NULL;
static char *comdb2dbname = NULL;

void class_machs_init()
{
    class_machs_hash = hash_init_user((hashfunc_t *)class_machs_hash_func, (cmpfunc_t *)class_machs_cmp, 0, 0);
    cdb2_get_comdb2db(&comdb2dbname, &comdb2dbclass);
}

int gbl_class_machs_refresh = 300;

const char *query = "select m.name from machines as m, clusters as c, databases as d"
                    "  where c.name=@dbname and c.cluster_name=@class and "
                    "        m.cluster=c.cluster_machs and d.name=@dbname";

static void class_machs_refresh(const char *dbname, const char *class)
{
    cdb2_hndl_tp *comdb2db;
    int rc = cdb2_open(&comdb2db, comdb2dbname, comdb2dbclass, 0);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: cdb2_open failed %d %s\n", __func__, rc, cdb2_errstr(comdb2db));
        return;
    }

    rc = cdb2_bind_param(comdb2db, "dbname", CDB2_CSTRING, dbname, strlen(dbname));
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: cdb2_bind_param failed %d %s\n", __func__, rc, cdb2_errstr(comdb2db));
        cdb2_close(comdb2db);
        return;
    }

    rc = cdb2_bind_param(comdb2db, "class", CDB2_CSTRING, class, strlen(class));
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: cdb2_bind_param failed %d %s\n", __func__, rc, cdb2_errstr(comdb2db));
        cdb2_close(comdb2db);
        return;
    }

    rc = cdb2_run_statement(comdb2db, query);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: cdb2_run_statement failed %d %s\n", __func__, rc, cdb2_errstr(comdb2db));
        cdb2_close(comdb2db);
        return;
    }
    int count = 0;
    char **machs = NULL;
    while ((rc = cdb2_next_record(comdb2db)) == CDB2_OK) {
        machs = realloc(machs, sizeof(char *) * (count + 1));
        machs[count] = strdup(cdb2_column_value(comdb2db, 0));
        count++;
    }
    cdb2_close(comdb2db);

    struct class_machs fnd = {.dbname = dbname, .class = class}, *cm;
    Pthread_mutex_lock(&class_machs_mutex);
    if ((cm = hash_find(class_machs_hash, &fnd)) == NULL) {
        cm = malloc(sizeof(struct class_machs));
        cm->dbname = strdup(dbname);
        cm->class = strdup(class);
        cm->count = count;
        cm->machs = machs;
        cm->last_updated = time(NULL);
        hash_add(class_machs_hash, cm);
    } else {
        for (int i = 0; i < cm->count; i++) {
            free(cm->machs[i]);
        }
        free(cm->machs);
        cm->count = count;
        cm->machs = machs;
        cm->last_updated = time(NULL);
    }
    Pthread_mutex_unlock(&class_machs_mutex);
}

int class_machs(const char *dbname, const char *class, int *count, char ***machs)
{
    struct class_machs fnd = {.dbname = dbname, .class = class}, *cm;
    int now = time(NULL);
    Pthread_mutex_lock(&class_machs_mutex);

    if ((cm = hash_find(class_machs_hash, &fnd)) == NULL || (now - cm->last_updated) > gbl_class_machs_refresh) {
        Pthread_mutex_unlock(&class_machs_mutex);
        class_machs_refresh(dbname, class);
        Pthread_mutex_lock(&class_machs_mutex);
        cm = hash_find(class_machs_hash, &fnd);
    }

    if (cm && cm->count > 0) {
        char **machs_cpy = malloc(sizeof(char *) * cm->count);
        for (int i = 0; i < cm->count; i++) {
            machs_cpy[i] = strdup(cm->machs[i]);
        }
        Pthread_mutex_unlock(&class_machs_mutex);
        *count = cm->count;
        *machs = machs_cpy;
        return 0;
    }
    Pthread_mutex_unlock(&class_machs_mutex);
    return -1;
}

int class_mach_add(const char *dbname, const char *class, const char *mach)
{
    struct class_machs fnd = {.dbname = dbname, .class = class}, *cm;
    int now = time(NULL);
    Pthread_mutex_lock(&class_machs_mutex);
    if ((cm = hash_find(class_machs_hash, &fnd)) == NULL) {
        cm = calloc(sizeof(struct class_machs), 1);
        cm->dbname = strdup(dbname);
        cm->class = strdup(class);
        hash_add(class_machs_hash, cm);
    }
    for (int i = 0; i < cm->count; i++) {
        if (strcmp(cm->machs[i], mach) == 0) {
            Pthread_mutex_unlock(&class_machs_mutex);
            return 0;
        }
    }
    cm->machs = realloc(cm->machs, sizeof(char *) * (cm->count + 1));
    cm->machs[cm->count++] = strdup(mach);
    cm->last_updated = now;
    Pthread_mutex_unlock(&class_machs_mutex);
    return 0;
}

int print_mach(void *obj, void *unused)
{
    struct class_machs *cm = (struct class_machs *)obj;
    for (int i = 0; i < cm->count; i++) {
        logmsg(LOGMSG_USER, "%s %s %s\n", cm->dbname, cm->class, cm->machs[i]);
    }
    return 0;
}

void class_machs_dump(void)
{
    Pthread_mutex_lock(&class_machs_mutex);
    hash_for(class_machs_hash, print_mach, NULL);
    Pthread_mutex_unlock(&class_machs_mutex);
}
