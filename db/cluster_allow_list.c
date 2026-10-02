/*
   Copyright 2026 Bloomberg Finance L.P.

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

#include <stdlib.h>
#include <string.h>
#include <pthread.h>

#include <plhash_glue.h>

#include "bb_oscompat.h"
#include "comdb2.h"
#include "cluster_allow_list.h"
#include "clienthost.h"
#include "epochlib.h"
#include "intern_strings.h"
#include "logmsg.h"
#include "rtcpu.h"
#include "sys_wrap.h"

/* How long we trust a cached group-membership verdict.  A membership change
 * outside of us takes at most this long to take effect. */
#define GROUP_CACHE_TTL 60

static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;
static hash_t *allowed_hosts = NULL;  /* objects are strdup'd host names */
static hash_t *allowed_groups = NULL; /* objects are strdup'd group names */

/* Bumped whenever the group set changes.  A cached verdict from an older
 * generation is stale.  Starts at 1 so that a freshly calloc'd clienthost
 * never matches. */
static int group_generation = 1;

static void init_hashes_lk(void)
{
    if (!allowed_hosts)
        allowed_hosts = hash_init_str(0);
    if (!allowed_groups)
        allowed_groups = hash_init_str(0);
}

static int add_lk(hash_t *h, const char *name)
{
    if (hash_find(h, name))
        return 0;
    hash_add(h, strdup(name));
    return 1;
}

static int del_lk(hash_t *h, const char *name)
{
    char *found = hash_find(h, name);
    if (!found)
        return 0;
    hash_del(h, found);
    free(found);
    return 1;
}

/* We key on the resolved name, so that an operator can name a host any way the
 * machine-name plugin understands.  Small integer machine identifiers are the
 * reason this matters: "allow cluster with 12345" must land on the same entry
 * as the name the net layer reports.
 *
 * comdb2_gethostbyname() turns the number into the bare machine name.  Do not
 * use comdb2_getcanonicalname() here; that appends the domain, which is not
 * the name the rest of the database uses.
 *
 * Resolving asks the operating system or the plugin, so cache the answer on
 * the clienthost.  The answer does not change while we run.  Interned strings
 * live forever, which keeps the cached pointer valid. */
static const char *resolved_name(const char *host)
{
    struct clienthost *c = retrieve_clienthost(intern_ptr(host));

    if (!c->resolved_host) {
        char *name = (char *)host;
        /* This leaves the name alone when it cannot resolve it. */
        (void)comdb2_gethostbyname(&name, NULL);
        c->resolved_host = intern(name ? name : host);
    }
    return c->resolved_host;
}

void cluster_allow_list_init(void)
{
    if (gbl_myhostname)
        cluster_allow_list_add_host(gbl_myhostname);
    for (int ii = 0; thedb && ii < thedb->nsiblings; ii++)
        cluster_allow_list_add_host(thedb->sibling_hostname[ii]);
}

int cluster_allow_list_add_host(const char *host)
{
    if (!host)
        return 0;
    const char *name = resolved_name(host);
    Pthread_mutex_lock(&lk);
    init_hashes_lk();
    int added = add_lk(allowed_hosts, name);
    Pthread_mutex_unlock(&lk);
    return added;
}

int cluster_allow_list_del_host(const char *host)
{
    if (!host)
        return 0;
    const char *name = resolved_name(host);
    Pthread_mutex_lock(&lk);
    init_hashes_lk();
    int removed = del_lk(allowed_hosts, name);
    Pthread_mutex_unlock(&lk);
    return removed;
}

int cluster_allow_list_add_group(const char *group)
{
    if (!group)
        return 0;
    Pthread_mutex_lock(&lk);
    init_hashes_lk();
    int added = add_lk(allowed_groups, group);
    if (added)
        group_generation++;
    Pthread_mutex_unlock(&lk);
    return added;
}

int cluster_allow_list_del_group(const char *group)
{
    if (!group)
        return 0;
    Pthread_mutex_lock(&lk);
    init_hashes_lk();
    int removed = del_lk(allowed_groups, group);
    if (removed)
        group_generation++;
    Pthread_mutex_unlock(&lk);
    return removed;
}

struct name_collector {
    char **names;
    int count;
    int max;
};

static int collect_name(void *obj, void *arg)
{
    struct name_collector *c = arg;
    if (c->count < c->max)
        c->names[c->count++] = (char *)obj;
    return 0;
}

/* Copy the group names out from under the lock.  Resolving a group asks the
 * machine registry, which may be slow, so we do not want to do it while we
 * hold the lock. */
static int copy_groups(char ***names_out)
{
    Pthread_mutex_lock(&lk);
    init_hashes_lk();
    int count = hash_get_num_entries(allowed_groups);
    if (count == 0) {
        Pthread_mutex_unlock(&lk);
        *names_out = NULL;
        return 0;
    }
    struct name_collector c = {.names = malloc(sizeof(char *) * count), .count = 0, .max = count};
    hash_for(allowed_groups, collect_name, &c);
    for (int ii = 0; ii < c.count; ii++)
        c.names[ii] = strdup(c.names[ii]);
    Pthread_mutex_unlock(&lk);

    *names_out = c.names;
    return c.count;
}

static void free_names(char **names, int count)
{
    for (int ii = 0; ii < count; ii++)
        free(names[ii]);
    free(names);
}

/* A group is a machine cluster.  machine_cluster_machs() lists the hosts in
 * one: machinfo.c answers from the machine registry, and without that plugin
 * the built-in registry that the lrl "machine_cluster" directive feeds answers
 * instead.  An unknown group has no members.
 *
 * host is already resolved.  Compare the plain member name first, then put
 * the member through the same resolution, so that the two sides agree however
 * the group named its machines. */
static int host_in_group(const char *host, const char *group)
{
    const char **machs = NULL;
    int count = 0;

    if (machine_cluster_machs(group, &count, &machs) != 0)
        return 0;

    for (int ii = 0; ii < count; ii++) {
        if (!machs[ii])
            continue;
        if (strcmp(machs[ii], host) == 0)
            return 1;
        if (strcmp(resolved_name(machs[ii]), host) == 0)
            return 1;
    }
    return 0;
}

static int in_any_allowed_group(const char *host)
{
    char **groups;
    int count = copy_groups(&groups);
    int member = 0;

    for (int ii = 0; ii < count && !member; ii++)
        member = host_in_group(host, groups[ii]);

    free_names(groups, count);
    return member;
}

int cluster_allow_list_permits(const char *host)
{
    if (!host)
        return 0;

    /* We always cluster with ourselves. */
    if (gbl_myhostname && strcmp(host, gbl_myhostname) == 0)
        return 1;

    /* Look up by the same name we store under. */
    const char *name = resolved_name(host);

    Pthread_mutex_lock(&lk);
    init_hashes_lk();
    int found = (hash_find(allowed_hosts, name) != NULL);
    int ngroups = hash_get_num_entries(allowed_groups);
    int generation = group_generation;
    Pthread_mutex_unlock(&lk);

    if (found)
        return 1;
    if (ngroups == 0)
        return 0;

    /* Fall back to the groups.  This asks a plugin, so cache the answer. */
    struct clienthost *c = retrieve_clienthost(intern_ptr(name));
    int now = comdb2_time_epoch();
    if (c->cluster_group_gen == generation && c->cluster_group_time != 0 &&
        (now - c->cluster_group_time) < GROUP_CACHE_TTL) {
        return c->cluster_group_ok;
    }

    int member = in_any_allowed_group(name);
    c->cluster_group_ok = member;
    c->cluster_group_gen = generation;
    c->cluster_group_time = now;
    return member;
}

static int cmp_names(const void *a, const void *b)
{
    return strcmp(*(const char *const *)a, *(const char *const *)b);
}

static void dump_hash(hash_t *h, const char *what)
{
    int count = hash_get_num_entries(h);
    if (count == 0) {
        logmsg(LOGMSG_USER, "  no %ss\n", what);
        return;
    }
    struct name_collector c = {.names = malloc(sizeof(char *) * count), .count = 0, .max = count};
    hash_for(h, collect_name, &c);
    qsort(c.names, c.count, sizeof(char *), cmp_names);
    for (int ii = 0; ii < c.count; ii++)
        logmsg(LOGMSG_USER, "  %s %s\n", what, c.names[ii]);
    free(c.names);
}

void cluster_allow_list_dump(void)
{
    Pthread_mutex_lock(&lk);
    init_hashes_lk();
    logmsg(LOGMSG_USER, "Cluster allow list (%s)\n",
           gbl_enforce_cluster_allow_list ? "enforced" : "not enforced, warn only");
    dump_hash(allowed_hosts, "host");
    dump_hash(allowed_groups, "group");
    Pthread_mutex_unlock(&lk);
}
