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

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include <pthread.h>

#include <pool.h>

#include <segstr.h>

#include "comdb2.h"
#include "history.h"

static void hist_print_header(history *h);

extern pthread_attr_t sndatt;
int init_history(history *h, size_t size)
{
    int rc = 0;
    pthread_t stat_thread_tid;
    h->size = size;
    h->head = h->tail = 0;
    h->hist = calloc(size, sizeof(history_request *));
    h->pool = pool_setalloc_init(sizeof(history_request), size, malloc, free);
    h->total = 0;
    Pthread_mutex_init(&h->lock, NULL);
    return (int)(h->hist && h->pool && !rc);
}

history_request *hist_get_event(history *h)
{
    history_request *req;
    Pthread_mutex_lock(&h->lock);
    req = pool_getzblk(h->pool);
    pthread_mutex_unlock(&h->lock);
    return req;
}

/* Add event to history. */
void hist_add_event(history *h, history_request *req)
{
    void *sv1, *sv2;
    sv1 = req->fullrq;
    sv2 = req->fullrsp;
    Pthread_mutex_lock(&h->lock);
    h->hist[h->tail] = req;
    h->tail = (h->tail + 1) % h->size;
    if (h->tail == h->head) {
        pool_relablk(h->pool, h->hist[h->head]);
        h->head = (h->head + 1) % h->size;
    }
    h->total++;
    pthread_mutex_unlock(&h->lock);
    if (h->wholereq) {
        if (sv1)
            free(sv1);
        if (sv2)
            free(sv2);
    }
}

/* mod(-11, 10) = 9, not -1 */
static int mod(int i, int j)
{
    int m = i % j;
    if (m < 0)
        return j + m;
    return m;
}

void hist_dumpreq(int opcode, char *buf);
void hist_dumpresp(int opcode, char *buf);

void hist_print_event(history *h, history_request *req)
{
    if (req == NULL) {
        printf("bug in history\n");
        return;
    }
    if (!h->wholereq)
        printf("%5d %4d %6d %10s %5d %08x%08x%08x %08x%08x%08x\n", req->frompid,
               req->fromnode, req->master, req2a(req->opcode), req->rcode,
               req->rq[0], req->rq[1], req->rq[2], req->rsp[0], req->rsp[1],
               req->rsp[2]);
    else {
        printf("%5d %4d %6d %10s %5d\n", req->frompid, req->fromnode,
               req->master, req2a(req->opcode), req->rcode);
#if 0
        hist_dumpreq(req->opcode, req->rq);
        hist_dumpresp(req->opcode, req->rsp);
#endif
    }
}

static void hist_print_header(history *h)
{
    if (!h->wholereq)
        printf("%5s %4s %4s %10s %5s %24s %24s\n", "pid", "node", "master",
               "opcode", "rcode", "req", "rsp");
    else
        printf("%5s %4s %4s %10s %5s\n", "pid", "node", "master", "opcode",
               "rcode");
}

void hist_dump(history *h, unsigned int skip, unsigned int count)
{
    int i;
    int cnt = 0;

    if (h->total == 0 || skip + count > h->size)
        return;

    for (i = mod(h->tail - skip - 1, h->size); cnt != count;
         i = mod(i - 1, h->size), cnt++) {
        if ((cnt + 1) % 10 == 1)
            hist_print_header(h);
        hist_print_event(h, h->hist[i]);
        if (i == h->head)
            break;
    }
}

#define search_hist(A)                                                         \
    static void search_history_##A(history *h, int minval, int maxval,         \
                                   int count, int skip)                        \
    {                                                                          \
        int i;                                                                 \
        history_request *req;                                                  \
        int cnt = 0;                                                           \
        int need_header = 1;                                                   \
        for (i = mod(h->tail - skip - 1, h->size); cnt < count;                \
             i = mod(i - 1, h->size)) {                                        \
            if ((((cnt + 1) % 10) == 1) && need_header == 1) {                 \
                hist_print_header(h);                                          \
                need_header = 0;                                               \
            }                                                                  \
            req = h->hist[i];                                                  \
            if (minval == -1 && maxval != -1 && req->A <= maxval) {            \
                cnt++;                                                         \
                need_header = 1;                                               \
                hist_print_event(h, req);                                      \
            }                                                                  \
            if (minval != -1 && maxval == -1 && req->A >= minval) {            \
                need_header = 1;                                               \
                cnt++;                                                         \
                hist_print_event(h, req);                                      \
            }                                                                  \
            if (minval != -1 && maxval != -1 && req->A >= minval &&            \
                req->A <= maxval) {                                            \
                need_header = 1;                                               \
                cnt++;                                                         \
                hist_print_event(h, req);                                      \
            }                                                                  \
            if (i == h->head)                                                  \
                break;                                                         \
        }                                                                      \
    }

search_hist(fromnode) search_hist(frompid) search_hist(master)
    search_hist(opcode) search_hist(rcode)

        void search_history(history *list, char *msg)
{
    int pos = 0, ltok = 0;
    char *tok;
    void (*searchfunc)(history *, int, int, int, int);
    int minval, maxval;
    int skip = 0, count = 20;
    int tlen = 0;

    if (list->total == 0)
        return;

    tok = segtok(msg, 56, &pos, &ltok);
    if (tok == NULL || ltok == 0)
        goto usage;
    tlen += ltok;
    if (isdigit(*tok)) {
        count = strtol(tok, NULL, 10);
        tok = segtok(msg, 56 - tlen, &pos, &ltok);
        if (tok == NULL || ltok == 0)
            goto usage;
        tlen += ltok;
        if (isdigit(*tok)) {
            skip = strtol(tok, NULL, 10);
            tok = segtok(msg, 56 - tlen, &pos, &ltok);
            if (tok == NULL || ltok == 0)
                goto usage;
            tlen += ltok;
        }
    }

    if (ltok == 0 || tok == NULL)
        goto usage;
    /* what are we searching for? */
    if (!strncasecmp(tok, "fromnode", ltok))
        searchfunc = search_history_frompid;
    if (!strncasecmp(tok, "frompid", ltok))
        searchfunc = search_history_frompid;
    if (!strncasecmp(tok, "master", ltok))
        searchfunc = search_history_frompid;
    if (!strncasecmp(tok, "opcode", ltok))
        searchfunc = search_history_frompid;
    if (!strncasecmp(tok, "rcode", ltok))
        searchfunc = search_history_frompid;
    else
        goto usage;

    tok = segtok(msg, 56 - tlen, &pos, &ltok);
    if (ltok == 0 || tok == NULL)
        goto usage;
    tlen += ltok;
    if (tok[0] == '+') {
        /* search with maximum value */
        minval = -1;
        maxval = strtol(tok + 1, NULL, 10);
        searchfunc(list, minval, maxval, count, skip);
        return;
    } else if (tok[0] == '~') {
        /* search with minimum value */
        maxval = -1;
        minval = strtol(tok + 1, NULL, 10);
        searchfunc(list, minval, maxval, count, skip);
        return;
    } else if (isdigit(*tok) || *tok == '-') {
        /* search for given value */
        minval = maxval = strtol(tok, NULL, 10);
        tok = segtok(msg, 56 - tlen, &pos, &ltok);
        if (ltok != 0 && tok != NULL && (isdigit(*tok) || *tok == '-'))
            /* search for range of values */
            maxval = strtol(tok, NULL, 10);
        searchfunc(list, minval, maxval, count, skip);
        return;
    }

usage:
    printf("Usage: shst [count] [skip] type min max        (values between min "
           "and max)\n"
           "       shst [count] [skip] type +min           (values >= min)\n"
           "       shst [count] [skip] type ~max           (values <= max)\n"
           "       shst [count] [skip] type value          (values == value)\n"
           "valid types are: fromnode frompid master opcode rcode\n");
    return;
}

int hist_is_empty(history *hist) { return hist->total == 0; }

int hist_num_entries(history *h)
{
    if (h->total > h->size)
        return h->size;
    return h->total;
}
