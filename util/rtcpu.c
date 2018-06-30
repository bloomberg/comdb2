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
#include "nodemap.h"
#include "intern_strings.h"
#include "list.h"
#include "cdb2_constants.h"
#include "util.h"
#include "epochlib.h"
#include "nodemap.h"
#include "machclass.h"
#include "logmsg.h"

static int machine_is_up_default(const char *host);
static void machine_mark_down_default(char *host);
static int machine_status_init(void);
static int machine_class_default(const char *host);
static int machine_dc_default(const char *host);
static int machine_num_default(const char *host);

static int (*machine_is_up_cb)(const char *host) = machine_is_up_default;
static int (*machine_status_init_cb)(void) = machine_status_init;
static int (*machine_class_cb)(const char *host) = machine_class_default;
static int (*machine_dc_cb)(const char *host) = machine_dc_default;
static int (*machine_num_cb)(const char *host) = machine_num_default;

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

void register_rtcpu_callbacks(int (*a)(const char *), int (*b)(void),
                              int (*c)(const char *), int (*d)(const char *),
                              int (*e)(const char *))
{

    if (inited) {
        logmsg(LOGMSG_WARN, "rtcpu already initialized\n");
        return;
    }

    machine_is_up_cb = a;
    machine_status_init_cb = b;
    machine_class_cb = c;
    machine_dc_cb = d;
    machine_num_cb = e;
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

static enum mach_class my_class = CLASS_UNKNOWN;

extern char *gbl_mynode;

/* pthread_once? */
static int machine_class_default(const char *host)
{
    if (my_class == CLASS_UNKNOWN) {
        char *envclass;
        cdb2_hndl_tp *db = NULL;

        envclass = getenv("COMDB2_CLASS");
        if (envclass) {
            if (strcmp(envclass, "dev") == 0 || strcmp(envclass, "test") == 0)
                my_class = CLASS_TEST;
            else if (strcmp(envclass, "alpha") == 0)
                my_class = CLASS_ALPHA;
            else if (strcmp(envclass, "beta") == 0)
                my_class = CLASS_BETA;
            else if (strcmp(envclass, "prod") == 0)
                my_class = CLASS_PROD;
            else
                logmsg(LOGMSG_ERROR,
                       "envclass set to \"%s\", don't recognize it\n",
                       envclass);
        } else {
            /* Try comdb2db */
            char *sql = "select class from machines where name=@name";
            char *envclass;
            int rc;

            cdb2_init_ssl(0, 0);
            rc = cdb2_open(&db, "comdb2db", "default", 0);
            if (rc) {
                logmsg(LOGMSG_INFO, "%s(%s) open rc %d %s!\n", __func__, host,
                       rc, cdb2_errstr(db));
                goto done;
            }
            rc = cdb2_bind_param(db, "name", CDB2_CSTRING, gbl_mynode,
                                 strlen(gbl_mynode));
            if (rc) {
                logmsg(LOGMSG_ERROR, "%s(%s) bind rc %d %s!\n", __func__, host,
                       rc, cdb2_errstr(db));
                goto done;
            }
            rc = cdb2_run_statement(db, sql);
            if (rc) {
                logmsg(LOGMSG_ERROR, "run rc %d %s, while trying to discover "
                                     "current machine class\n",
                       rc, sql);
                goto done;
            }
            rc = cdb2_next_record(db);
            if (rc && rc != CDB2_OK_DONE) {
                logmsg(LOGMSG_ERROR, "next rc %d %s, while trying to discover "
                                     "current machine class\n",
                       rc, sql);
                goto done;
            }
            envclass = cdb2_column_value(db, 0);
            if (strcmp(envclass, "dev") == 0 || strcmp(envclass, "test") == 0)
                my_class = CLASS_TEST;
            else if (strcmp(envclass, "alpha") == 0)
                my_class = CLASS_ALPHA;
            else if (strcmp(envclass, "beta") == 0)
                my_class = CLASS_BETA;
            else if (strcmp(envclass, "prod") == 0)
                my_class = CLASS_PROD;
            do {
                rc = cdb2_next_record(db);
            } while (rc == CDB2_OK);
            /* Shouldn't happen, so warn - but not an error. */
            if (rc != CDB2_OK_DONE)
                logmsg(LOGMSG_ERROR,
                       "consume rc %d %s, while trying to discover "
                       "current machine class\n",
                       rc, sql);
        }
    done:
        /* Error if can't find class? */
        if (my_class == CLASS_UNKNOWN)
            my_class = CLASS_PROD;
        if (db)
            cdb2_close(db);
    }
    return my_class;
}

static int machine_dcs[MAXNODES];

static int resolve_dc(const char *host)
{
    int rc;
    char *dcstr;
    int dc = 99; /* Default to something, 0 would make us do the same lookup. */
    int types[] = {CDB2_CSTRING};
    cdb2_hndl_tp *db = NULL;

    cdb2_init_ssl(0, 0);
    rc = cdb2_open(&db, "comdb2db", "default", 0);
    if (rc) {
        logmsg(LOGMSG_INFO, "%s(%s) open rc %d %s!\n", __func__, host,
               rc, cdb2_errstr(db));
        goto done;
    }
    rc = cdb2_bind_param(db, "name", CDB2_CSTRING, host, strlen(host));
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s(%s) bind rc %d %s!\n", __func__, host, rc,
               cdb2_errstr(db));
        goto done;
    }
    rc = cdb2_run_statement_typed(db,
                                  "select room from machines where name=@name",
                                  sizeof(types) / sizeof(types), types);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s(%s) run rc %d %s!\n", __func__, host, rc,
               cdb2_errstr(db));
        goto done;
    }
    rc = cdb2_next_record(db);
    if (rc && rc != CDB2_OK_DONE) {
        logmsg(LOGMSG_ERROR, "%s(%s) next rc %d %s!\n", __func__, host, rc,
               cdb2_errstr(db));
        goto done;
    }
    if (rc == 0) {
        dcstr = (char *)cdb2_column_value(db, 0);
        if (dcstr) {
            if (strcmp(dcstr, "NY") == 0)
                dc = 1;
            else if (strcmp(dcstr, "NJ") == 0)
                dc = 5;
            else if (strcmp(dcstr, "ORG") == 0)
                dc = 6;
        }

        rc = cdb2_next_record(db);
        if (rc != CDB2_OK_DONE) {
            logmsg(LOGMSG_ERROR, "%s(%s) next (last) rc %d %s\n!", __func__,
                   host, rc, cdb2_errstr(db));
            goto done;
        }
    }

done:
    if (db != NULL)
        cdb2_close(db);
    // printf("%s->%d\n",  host, dc);
    return dc;
}

static int machine_dc_default(const char *host)
{
    int ix;
    ix = nodeix(host);
    if (machine_dcs[ix] == 0)
        machine_dcs[ix] = resolve_dc(host);
    return machine_dcs[ix];
}

static int machine_num_default(const char *host)
{
    return nodeix(host);
}
