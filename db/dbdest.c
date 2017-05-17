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

#include "plhash.h"
#include "bb_oscompat.h"
#include "segstring.h"
#include "logmsg.h"

struct dbdests {
    int dbnum;
    int numhosts;
    char **hosts;
};

static hash_t *hosts_for_dbnum;
static pthread_mutex_t dbdestlk = PTHREAD_MUTEX_INITIALIZER;

void dbdest_init(void)
{
    hosts_for_dbnum = hash_init_o(offsetof(struct dbdests, dbnum), sizeof(int));
}

static int handle_dbdest_locked(char *line, int llen)
{
    int toff = 0;
    int tlen;
    int dbnum;
    char *tok;
    struct dbdests *dests;
    struct hostent *h;

    tok = segtok(line, llen, &toff, &tlen);
    if (tlen == 0) {
        logmsg(LOGMSG_ERROR, "dbdest: expected db number\n");
        return 1;
    }
    dbnum = toknum(tok, tlen);
    if (dbnum <= 0) {
        logmsg(LOGMSG_ERROR, "dbdest: invalid db number\n");
        return 1;
    }

    tok = segtok(line, llen, &toff, &tlen);
    if (tlen == 0) {
        logmsg(LOGMSG_ERROR, "dbdest: expected a list of hosts\n");
        return 1;
    }

    dests = hash_find(hosts_for_dbnum, &dbnum);
    if (dests == NULL) {
        dests = malloc(sizeof(struct dbdests));
        if (dests == NULL) {
            logmsg(LOGMSG_ERROR, "dbdest: can't allocate memory\n");
            return -1;
        }
        dests->numhosts = 0;
        dests->hosts = NULL;
        dests->dbnum = dbnum;
        hash_add(hosts_for_dbnum, dests);
    }
    while (tlen) {
        int i;
        char *host;

        for (int i = 0; i < dests->numhosts; i++) {
            /* already have, don't add duplicate */
            if (tokcmp(tok, tlen, dests->hosts[i]) == 0)
                goto next;
        }

        host = tokdup(tok, tlen);
        if (host == NULL) {
            logmsg(LOGMSG_ERROR, "dbdest: can't allocate memory for hostname\n");
            return -1;
        }
        h = bb_gethostbyname(host);
        if (h == NULL) {
            logmsg(LOGMSG_ERROR, "dbdest: can't resolve hostname %s\n", host);
            free(host);
            goto next;
        }
        void *p;
        p = realloc(dests->hosts, sizeof(char *) * (dests->numhosts + 1));
        if (p == NULL) {
            logmsg(LOGMSG_ERROR, "dbdest: can't allocate memory for host list\n");
            free(host);
            return -1;
        }
        dests->hosts = p;
        dests->hosts[dests->numhosts++] = host;

    next:
        tok = segtok(line, llen, &toff, &tlen);
    }

    return 0;
}

int dbdest_get_destinations(int dbnum, int *nhosts, char ***hosts)
{
    struct dbdests *dests;
    char **h;
    pthread_mutex_lock(&dbdestlk);
    dests = hash_find(hosts_for_dbnum, &dbnum);
    if (dests == NULL) {
        pthread_mutex_unlock(&dbdestlk);
        return -1;
    }
    h = malloc(sizeof(char *) * dests->numhosts);
    if (h == NULL) {
        pthread_mutex_unlock(&dbdestlk);
        return -1;
    }
    for (int i = 0; i < dests->numhosts; i++) {
        h[i] = strdup(dests->hosts[i]);
        if (h[i] == NULL) {
            pthread_mutex_unlock(&dbdestlk);
            free(h);
            return -1;
        }
    }
    *hosts = h;
    *nhosts = dests->numhosts;
    pthread_mutex_unlock(&dbdestlk);
    return 0;
}

static int printdests(void *obj, void *arg)
{
    struct dbdests *dests = (struct dbdests *)obj;
    logmsg(LOGMSG_USER, "%d ", dests->dbnum);
    for (int i = 0; i < dests->numhosts; i++)
        logmsg(LOGMSG_USER, "%s ", dests->hosts[i]);
    if (dests->numhosts == 0)
        logmsg(LOGMSG_USER, "no valid hosts ");
    logmsg(LOGMSG_USER, "\n");
    return 0;
}

int dbdest_mtrap(char *line, int llen, int toff)
{
    int tlen;
    char *tok;

    tok = segtok(line, llen, &toff, &tlen);
    if (tlen == 0)
        return -1;
    if (tokcmp(tok, tlen, "stat") == 0) {
        pthread_mutex_lock(&dbdestlk);
        hash_for(hosts_for_dbnum, printdests, NULL);
        pthread_mutex_unlock(&dbdestlk);
    }
    return 0;
}

int handle_dbdest(char *line, int llen)
{
    int rc;
    pthread_mutex_lock(&dbdestlk);
    rc = handle_dbdest_locked(line, llen);
    pthread_mutex_unlock(&dbdestlk);
    return rc;
}
