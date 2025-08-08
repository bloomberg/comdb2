/*
   Copyright 2020 Bloomberg Finance L.P.

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

#include <arpa/inet.h>
#include <netdb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/types.h>

#include <hostname_support.h>
#include <errno.h>

static int get_hostname_by_addr(struct sockaddr_in *addr, char *host, socklen_t hostlen)
{
    socklen_t addrlen = sizeof(*addr);
    if (getnameinfo((struct sockaddr *)addr, addrlen, host, hostlen, NULL, 0, 0)) {
        if (!inet_ntop(addr->sin_family, &addr->sin_addr, host, hostlen)) {
            return -1;
        }
    }
    return 0;
}

#ifndef DISABLE_HOSTADDR_CACHE
#include <pthread.h>
#include <intern_strings.h>
#include <sys_wrap.h>
#include <plhash_glue.h>

static hash_t *hs;
static pthread_mutex_t lk = PTHREAD_MUTEX_INITIALIZER;

struct peer_entry {
    struct in_addr addr;
    char *name;
};

static struct peer_entry *new_peer_entry(struct in_addr addr, char *name)
{
    struct peer_entry *entry = malloc(sizeof(struct peer_entry));
    entry->addr = addr;
    entry->name = intern(name);
    return entry;
}

static int free_peer_entry(void *obj, void *arg)
{
    free(obj);
    return 0;
}

static char *find_peer_hash(struct in_addr addr)
{
    Pthread_mutex_lock(&lk);
    struct peer_entry *entry = hash_find(hs, &addr);
    Pthread_mutex_unlock(&lk);
    return entry ? entry->name : NULL;
}

static char *add_peer_hash(struct in_addr addr, char *name)
{
    struct peer_entry *entry = new_peer_entry(addr, name);
    Pthread_mutex_lock(&lk);
    hash_add(hs, entry);
    Pthread_mutex_unlock(&lk);
    return entry->name;
}

void init_peer_hash(void)
{
    hs = hash_init_o(offsetof(struct peer_entry, addr), sizeof(struct in_addr));
}

void cleanup_peer_hash(void)
{
    Pthread_mutex_lock(&lk);
    hash_for(hs, free_peer_entry, NULL);
    hash_free(hs);
    Pthread_mutex_unlock(&lk);
}

#endif /* ifndef DISABLE_HOSTADDR_CACHE */

char *get_cached_hostname_by_addr(struct sockaddr_in *saddr) {
    char host[NI_MAXHOST];
# ifdef DISABLE_HOSTADDR_CACHE
    if (get_hostname_by_addr(saddr, host, NI_MAXHOST)) return NULL;
    return strdup(host);
# else
    char *name = find_peer_hash(saddr->sin_addr);
    if (name) return name;
    if (get_hostname_by_addr(saddr, host, NI_MAXHOST)) return NULL;
    return add_peer_hash(saddr->sin_addr, host);
# endif
}

char *get_hostname_by_fileno(int fd)
{
    struct sockaddr_in saddr;
    socklen_t len = sizeof(saddr);
    if (getpeername(fd, (struct sockaddr *)&saddr, &len)) return NULL;
    return get_cached_hostname_by_addr(&saddr);
}

// sets getpeername's errno (0 on success)
char *get_hostname_by_fileno_err(int fd, int *err)
{
    struct sockaddr_in saddr;
    socklen_t len = sizeof(saddr);
    if (err) *err = 0;
    if (getpeername(fd, (struct sockaddr *)&saddr, &len)) {
        if (err) *err = errno;
        return NULL;
    }
    return get_cached_hostname_by_addr(&saddr);
}

int get_hostname_by_fileno_v2(int fd, char *out, size_t sz)
{
    struct sockaddr_in saddr;
    socklen_t len = sizeof(saddr);
    if (getpeername(fd, (struct sockaddr *)&saddr, &len)) return -1;
# ifdef DISABLE_HOSTADDR_CACHE
    return get_hostname_by_addr(&saddr, out, sz);
# else
    char *name = find_peer_hash(saddr.sin_addr);
    if (name) return snprintf(out, sz, "%s", name) >= sz;
    if (get_hostname_by_addr(&saddr, out, sz)) return -1;
    add_peer_hash(saddr.sin_addr, out);
    return 0;
# endif
}
