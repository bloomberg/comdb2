/*
   Copyright 2015 Bloomberg Finance L.P.

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
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdint.h>

#include <dirent.h>

#include "cdb2api.h"
#include <clienthost.h>
#include "intern_strings.h"
#include "list.h"
#include "cdb2_constants.h"
#include "util.h"
#include "epochlib.h"
#include "machclass.h"
#include "logmsg.h"
#include "sys_wrap.h"
#include <plhash_glue.h>

static int machine_is_up_default(const char *host, int *isdrtest);
static int machine_status_init(void);
static int machine_class_default(const char *host);
static int machine_my_class_default(void);
static int machine_dc_default(const char *host);
static int machine_num_default(const char *host);
static int machine_cluster_default(const char *host, const char **cluster);
static int machine_my_cluster_default(const char **cluster);
static int machine_cluster_machs_default(const char *cluster, int *count, const char ***machs);
static int machine_add_cluster_default(const char *host, const char *cluster);

static int (*machine_is_up_cb)(const char *host, int *drtest) = machine_is_up_default;
static int (*machine_status_init_cb)(void) = machine_status_init;
static int (*machine_class_cb)(const char *host) = machine_class_default;
static int (*machine_my_class_cb)(void) = machine_my_class_default;
static int (*machine_dc_cb)(const char *host) = machine_dc_default;
static int (*machine_num_cb)(const char *host) = machine_num_default;
static int (*machine_cluster_cb)(const char *, const char **) = machine_cluster_default;
static int (*machine_my_cluster_cb)(const char **) = machine_my_cluster_default;
static int (*machine_cluster_machs_cb)(const char *cluster, int *count,
                                       const char ***machs) = machine_cluster_machs_default;
static int (*machine_add_cluster_cb)(const char *host, const char *cluster) = machine_add_cluster_default;

static int inited = 0;
static pthread_once_t once = PTHREAD_ONCE_INIT;
static void do_once(void)
{
    inited = 1;
    machine_status_init_cb();
}

static void init_once(void)
{
    pthread_once(&once, do_once);
}

void register_rtcpu_callbacks(int (*a)(const char *, int *), int (*b)(void), int (*c)(const char *), int (*d)(void),
                              int (*e)(const char *), int (*f)(const char *), int (*g)(const char *, const char **),
                              int (*h)(const char **), int (*i)(const char *, int *, const char ***),
                              int (*j)(const char *, const char *))
{

    if (inited) {
        logmsg(LOGMSG_WARN, "rtcpu already initialized\n");
        return;
    }

    machine_is_up_cb = a;
    machine_status_init_cb = b;
    machine_class_cb = c;
    machine_my_class_cb = d;
    machine_dc_cb = e;
    machine_num_cb = f;
    machine_cluster_cb = g;
    machine_my_cluster_cb = h;
    machine_cluster_machs_cb = i;
    machine_add_cluster_cb = j;
}

int machine_is_up(const char *host, int *drtest)
{
    init_once();

    if (!isinterned(host))
        abort();

    return machine_is_up_cb(host, drtest);
}

int machine_class(const char *host)
{
    return machine_class_cb(host);
}

int machine_my_class(void)
{
    return machine_my_class_cb();
}

int machine_cluster(const char *host, const char **cluster)
{
    return machine_cluster_cb(host, cluster);
}

int machine_my_cluster(const char **cluster)
{
    return machine_my_cluster_cb(cluster);
}

int machine_cluster_machs(const char *cluster, int *count, const char ***machs)
{
    return machine_cluster_machs_cb(cluster, count, machs);
}

int machine_add_cluster(const char *host, const char *cluster)
{
    return machine_add_cluster_cb(host, cluster);
}

int machine_dc(const char *host)
{
    return machine_dc_cb(host);
}

int machine_num(const char *host)
{
    return machine_num_cb(host);
}

static hash_t *fake_drtest_hash = NULL;
static pthread_mutex_t fake_drtest_lk = PTHREAD_MUTEX_INITIALIZER;

void add_fake_drtest(const char *host)
{
    Pthread_mutex_lock(&fake_drtest_lk);
    if (!fake_drtest_hash) {
        fake_drtest_hash = hash_init_str(0);
    }
    hash_add(fake_drtest_hash, strdup(host));
    Pthread_mutex_unlock(&fake_drtest_lk);
}

void del_fake_drtest(const char *host)
{
    Pthread_mutex_lock(&fake_drtest_lk);
    char *h;
    if (fake_drtest_hash && (h = hash_find(fake_drtest_hash, host))) {
        hash_del(fake_drtest_hash, h);
    }
    Pthread_mutex_unlock(&fake_drtest_lk);
}

static int print_fake_drtest_hash(void *obj, void *arg)
{
    logmsg(LOGMSG_USER, "%s\n", (char *)obj);
    return 0;
}

void dump_fake_drtest(void)
{
    if (!fake_drtest_hash) {
        logmsg(LOGMSG_USER, "No fake drtest hosts\n");
        return;
    }
    Pthread_mutex_lock(&fake_drtest_lk);
    if (fake_drtest_hash) {
        hash_for(fake_drtest_hash, print_fake_drtest_hash, NULL);
    }
    Pthread_mutex_unlock(&fake_drtest_lk);
}

static int is_fake_drtest(const char *host)
{
    int rc = 0;
    if (!fake_drtest_hash) {
        return 0;
    }
    Pthread_mutex_lock(&fake_drtest_lk);
    if (fake_drtest_hash && hash_find(fake_drtest_hash, host) != NULL) {
        rc = 1;
    }
    Pthread_mutex_unlock(&fake_drtest_lk);
    return rc;
}

static int machine_is_up_default(const char *host, int *drtest)
{
    int is_fake = is_fake_drtest(host);

    if (is_fake) {
        logmsg(LOGMSG_WARN, "%s fakedr returning machine-down for host: %s\n", __func__, host);
    }

    if (drtest) {
        *drtest = is_fake;
    }
    return is_fake ? 0 : 1;
}

static int machine_status_init(void)
{
    return 0;
}

extern char *gbl_myhostname;

/* Token implementation allows testing in rr */
static hash_t *mach_class_hash = NULL;
struct mach_class_entry {
    int class;
    char host[1];
};

static pthread_mutex_t machine_class_lk = PTHREAD_MUTEX_INITIALIZER;

int machine_class_add(const char *host, int class)
{
    Pthread_mutex_lock(&machine_class_lk);
    if (mach_class_hash == NULL) {
        mach_class_hash = hash_init_str(offsetof(struct mach_class_entry, host));
    }
    struct mach_class_entry *fnd = hash_find(mach_class_hash, host);
    if (!fnd) {
        fnd = calloc(offsetof(struct mach_class_entry, host) + strlen(host) + 1, 1);
        strcpy(fnd->host, host);
        hash_add(mach_class_hash, fnd);
    }
    fnd->class = class;

    Pthread_mutex_unlock(&machine_class_lk);
    return 0;
}

/* pthread_once? */
static int machine_class_default(const char *host)
{
    static enum mach_class my_class = CLASS_UNKNOWN;

    Pthread_mutex_lock(&machine_class_lk);

    if (mach_class_hash == NULL) {
        mach_class_hash = hash_init_str(offsetof(struct mach_class_entry, host));
    }
    struct mach_class_entry *fnd = hash_find(mach_class_hash, host);
    if (fnd) {
        int rtn = fnd->class;
        Pthread_mutex_unlock(&machine_class_lk);
        return rtn;
    }

    if (strcmp(host, gbl_myhostname) != 0) {
        Pthread_mutex_unlock(&machine_class_lk);
        return CLASS_PROD;
    }

    if (my_class == CLASS_UNKNOWN) {

        char *envclass;
        envclass = getenv("COMDB2_CLASS");
        if (envclass) {
            my_class = mach_class_name2class(envclass);
            if (my_class == CLASS_UNKNOWN)
                logmsg(LOGMSG_ERROR,
                       "envclass set to \"%s\", don't recognize it\n",
                       envclass);
        }
        /* Error if can't find class? */
        if (my_class == CLASS_UNKNOWN) {
            logmsg(LOGMSG_DEBUG, "Can't find class -- assigning PROD\n");
            my_class = CLASS_PROD;
        }
        struct mach_class_entry *m = calloc(offsetof(struct mach_class_entry, host) + strlen(gbl_myhostname) + 1, 1);
        strcpy(m->host, gbl_myhostname);
        m->class = my_class;
        hash_add(mach_class_hash, m);
    }

    Pthread_mutex_unlock(&machine_class_lk);
    return my_class;
}

static int machine_my_class_default(void)
{
    return machine_class_default(gbl_myhostname);
}

static int resolve_dc(const char *host)
{
    int dc = 99; /* Default to something, 0 would make us do the same lookup. */
    return dc;
}

static int machine_dc_default(const char *host)
{
    struct interned_string *host_interned = intern_ptr(host);
    struct clienthost *c = retrieve_clienthost(host_interned);

    if (c->machine_dc == 0)
        c->machine_dc = resolve_dc(host);
    return c->machine_dc;
}

static int machine_num_default(const char *host)
{
    struct interned_string *host_interned = intern_ptr(host);
    return host_interned->ix;
}

static int machine_cluster_default(const char *host, const char **cluster)
{
    return mach_cluster(host, cluster);
}

static int machine_my_cluster_default(const char **cluster)
{
    return machine_cluster_default(gbl_myhostname, cluster);
}

static int machine_cluster_machs_default(const char *cluster, int *count, const char ***machs)
{
    return mach_cluster_machs(cluster, count, machs);
}

static int machine_add_cluster_default(const char *host, const char *cluster)
{
    return mach_addcluster(host, cluster);
}
