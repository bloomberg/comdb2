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

/* module to generate quanitze report, a la dtrace */
/* 9/2007 */

#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <alloca.h>

#include "quantize.h"
#include "ctrace.h"
#include "logmsg.h"

struct quantize {
    int step;
    int qmax;
    int qnum;
    int *cnts;
    char *units_name;
    long long sumval;
    int numvals;
    int maxval;
    int minval;
};

/* allocate new quantize struct */
struct quantize *quantize_new(int step, int qmax, char *units_name)
{
    struct quantize *q = (struct quantize *)malloc(sizeof(*q));
    if (!q) return NULL;
    q->step = step;
    q->qmax = qmax;
    q->qnum = qmax / step;
    q->cnts = 0;
    q->units_name = strdup(units_name);
    q->sumval = 0;
    q->numvals = 0;
    q->maxval = 0;
    q->minval = 0;
    return q;
}

/* consolidate counts from source in to dest, and reset source */
int quantize_consolidate(struct quantize *dest, struct quantize *source)
{
    int i;
    if (dest->qnum != source->qnum || dest->step != source->step) {
        /* different sizes not handled */
        return -1;
    }
    if (source->cnts == 0)
        return 0; /* nothing to merge */
    if (dest->cnts == 0)
        dest->cnts = (int *)calloc(dest->qnum + 1, sizeof(int));
    for (i = 0; i <= dest->qnum; i++) {
        dest->cnts[i] += source->cnts[i];
        source->cnts[i] = 0; /* reset source */
    }
    return 0;
}

/* release resources */
void quantize_free(struct quantize *q)
{
    if (q) {
        if (q->units_name) {
            free(q->units_name);
            q->units_name = NULL;
        }
        if (q->cnts) {
            free(q->cnts);
            q->cnts = NULL;
        }
        free(q);
        q = NULL;
    }
}

/* count a value */
void quantize(struct quantize *q, int val)
{
    unsigned int bkt = ((val + q->step - 1) / q->step);
    int *cnt, rc;
    if (bkt > q->qnum)
        bkt = q->qnum;
    cnt = q->cnts;
    if (cnt == 0)
        cnt = q->cnts = (int *)calloc(q->qnum + 1, sizeof(int));
    cnt[bkt]++;
    /* for keeping average */
    if (val < 0) {
        logmsg(LOGMSG_ERROR, "*** quantize: ERR BAD VALUE PASSED IN %d\n", val);
        return;
    }
    q->sumval += val;
    q->numvals++;
    if (val < q->minval)
        q->minval = val;
    if (val > q->maxval)
        q->maxval = val;
}

int quantize_ctrace(struct quantize *q, char *title)
{
    int i, *cnt = q->cnts, zerosinarow;
    int maxcnt, linelength;
    static char *line =
        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*****";
    if (q->numvals == 0)
        return 0;
    ctrace("%s\n", title);
    if (cnt == 0) {
        int sz = sizeof(int) * (q->qnum + 1);
        cnt = (int *)alloca(sz);
        memset(cnt, 0, sz);
    }
    for (maxcnt = 0, i = 0; i <= q->qnum; i++)
        if (maxcnt < cnt[i])
            maxcnt = cnt[i];
    ctrace("<=%.8s  -- avg %-8lld -- min %-8d -- max %-8d -- count\n",
           q->units_name, (long long)(q->sumval / q->numvals), q->minval,
           q->maxval);
    zerosinarow = 0;
    for (i = 0; i <= q->qnum; i++) {
        if (maxcnt == 0)
            linelength = 0;
        else
            linelength = (double)cnt[i] * (double)50 / (double)maxcnt;
        if (cnt[i] == 0) {
            zerosinarow++;
            continue;
        }
        if (zerosinarow > 1)
            ctrace(" ...\n");
        zerosinarow = 0;
        if (i == q->qnum)
            ctrace("         >|%-50.*s %-8d\n", linelength, line, cnt[i]);
        else
            ctrace("    %6d|%-50.*s %-8d\n", i * q->step, linelength, line,
                   cnt[i]);
    }
    return 0;
}

/* dump distribution table */
int quantize_dump(struct quantize *q, FILE *ff)
{
    int i, *cnt = q->cnts, zerosinarow;
    int maxcnt, linelength;
    static char *line =
        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@*****";
    if (q->numvals == 0)
        return 0;
    if (cnt == 0) {
        int sz = sizeof(int) * (q->qnum + 1);
        cnt = (int *)alloca(sz);
        memset(cnt, 0, sz);
    }
    for (maxcnt = 0, i = 0; i <= q->qnum; i++)
        if (maxcnt < cnt[i])
            maxcnt = cnt[i];
    logmsg(LOGMSG_USER, "<=%.4s  -- avg %-8lld -- min %-8d -- max %-8d -- count\n",
            q->units_name, (long long)(q->sumval / q->numvals), q->minval,
            q->maxval);
    zerosinarow = 0;
    for (i = 0; i <= q->qnum; i++) {
        if (maxcnt == 0)
            linelength = 0;
        else
            linelength = (double)cnt[i] * (double)50 / (double)maxcnt;
        if (cnt[i] == 0) {
            zerosinarow++;
            continue;
        }
        if (zerosinarow > 1)
            logmsg(LOGMSG_USER, " ...\n");
        zerosinarow = 0;
        if (i == q->qnum)
            logmsg(LOGMSG_USER, "     >");
        else
            logmsg(LOGMSG_USER, "%6d", i * q->step);
        logmsg(LOGMSG_USER, "|%-50.*s %-8d\n", linelength, line, cnt[i]);
    }
    return 0;
}

#include <plhash.h>

struct qobj {
    int numvals;
    long long sumval;
    int minval, maxval;
    /* object key follows */
};

struct qcount {
    hash_t *h_objs;
    int objlen;
};

/*
typedef void (*qcount_callback_func)(void *callbackarg, void *obj, int numvals,
long long sumval, int minval, int maxval);
*/

/* allocate new counting struct, to track counts by object */
struct qcount *qcount_new(int objlen)
{
    struct qcount *q = malloc(sizeof(*q));
    hash_t *h;
    if (q == 0) {
        logmsgperror("qcount_new:failed malloc qcount");
        return 0;
    }
    h = hash_init_o(sizeof(struct qobj), objlen);
    if (h == 0) {
        logmsg(LOGMSG_ERROR, "qcount_new:failed hash init %d\n", objlen);
        free(q);
        return 0;
    }
    q->objlen = objlen;
    q->h_objs = h;
    return q;
}

/* add value to count for an object */
void qcount_count(struct qcount *q, void *obj, int val)
{
    struct qobj *o = (struct qobj *)hash_find(q->h_objs, obj);
    if (o == 0) {
        /* allocate new object */
        int size = sizeof(struct qobj) + q->objlen;
        o = malloc(size);
        if (o == 0) {
            logmsg(LOGMSG_ERROR, "qcount_count:failed alloc new obj, size %d\n",
                    size);
            return;
        }
        o->numvals = 0;
        o->sumval = 0;
        o->minval = 0;
        o->maxval = 0;
        memcpy(o + 1, obj, q->objlen);
        hash_add(q->h_objs, o);
    }
    if (val < 0) {
        logmsg(LOGMSG_ERROR, "*** qcount_count: ERR BAD VALUE PASSED IN %d\n", val);
        return;
    }
    o->sumval += val;
    if (o->sumval < 0) {
        logmsg(LOGMSG_ERROR, "*** qcount_count: ERR WRAPPED SUMVAL??\n");
        o->sumval = 0; /*fix it */
    }
    o->numvals++;
    if (val < o->minval)
        o->minval = val;
    if (val > o->maxval)
        o->maxval = val;
}

/* how many objects are tracked */
int qcount_numobjs(struct qcount *q) { return hash_get_num_entries(q->h_objs); }

struct qfceargs {
    struct qcount *q;
    qcount_callback_func *func;
    void *arg;
};

/* iterate thru hashtable */
static int qcfe(void *obj, void *arg)
{
    struct qfceargs *a = (struct qfceargs *)arg;
    struct qobj *o = (struct qobj *)obj;
    (*a->func)(a->arg, o + 1, o->numvals, o->sumval, o->minval, o->maxval);
    return 0;
}

/*walk each object and pass various counts */
void qcount_foreach(struct qcount *q, qcount_callback_func *callback,
                    void *callbackarg)
{
    struct qfceargs a;
    a.q = q;
    a.func = callback;
    a.arg = callbackarg;
    hash_for(q->h_objs, qcfe, &a);
}

void quantize_clear(struct quantize *q)
{
    int i;
    if (q->cnts == NULL)
        return;
    for (i = 0; i < q->qnum; i++)
        q->cnts[i] = 0;
    q->numvals = 0;
}
