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

static int machine_is_up_default(const char *host);
static int machine_status_init(void);
static int machine_class_default(const char *host);
static int machine_my_class_default(void);
static int machine_dc_default(const char *host);
static int machine_num_default(const char *host);
static int machine_cluster_default(const char *host, const char **cluster);
static int machine_my_cluster_default(const char **cluster);
static int machine_cluster_machs_default(const char *cluster, int *count, const char ***machs);
static int machine_add_cluster_default(const char *host, const char *cluster);

static int (*machine_is_up_cb)(const char *host) = machine_is_up_default;
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

void register_rtcpu_callbacks(int (*a)(const char *), int (*b)(void), int (*c)(const char *), int (*d)(void),
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

int machine_is_up(const char *host)
{
    init_once();

    if (!isinterned(host))
        abort();

    return machine_is_up_cb(host);
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

static int machine_is_up_default(const char *host)
{
    return 1;
}

static int machine_status_init(void)
{
    return 0;
}

extern char *gbl_myhostname;

/* pthread_once? */
static int machine_class_default(const char *host)
{
    static enum mach_class my_class = CLASS_UNKNOWN;
    static pthread_mutex_t mtx = PTHREAD_MUTEX_INITIALIZER;

    Pthread_mutex_lock(&mtx);

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
    }

    Pthread_mutex_unlock(&mtx);

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
