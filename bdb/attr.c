/*
   Copyright 2015, 2017 Bloomberg Finance L.P.

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

#include <assert.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>
#include <stddef.h>

#include <db.h>
// #include <peutil.h> /* for time_epoch() */

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"

#include <plbitlib.h> /* for bset/btst */
#include <logmsg.h>

static int log_region_sz_update(void *context, void *value)
{
    comdb2_tunable *tunable = (comdb2_tunable *)context;
    /* Log region size is specified in KBs. */
    *(int *)tunable->var = *(int *)value * 1024;
    return 0;
}

static void bdb_attr_set_int(bdb_state_type *bdb_state, bdb_attr_type *bdb_attr,
                             int attr, int value)
{
    /* Overrides for special cases */
    switch (attr) {
    case BDB_ATTR_BLOBSTRIPE:
        if (value)
            bdb_attr->blobstripe = bdb_attr->dtastripe;
        else
            bdb_attr->blobstripe = 0;
        return;

    case BDB_ATTR_COMMITDELAY:
        /* set delay */
        bdb_attr->commitdelay = value;

        /* if max is set, cap at max */
        if ((bdb_attr->commitdelaymax > 0) &&
            (bdb_attr->commitdelay > bdb_attr->commitdelaymax))
            bdb_attr->commitdelay = bdb_attr->commitdelaymax;
        return;

    case BDB_ATTR_REP_WORKERS:
        bdb_attr->rep_workers = value;
        if (bdb_state && bdb_state->dbenv)
            bdb_state->dbenv->set_num_recovery_worker_threads(bdb_state->dbenv,
                                                              value);
        return;

    case BDB_ATTR_REP_PROCESSORS:
        bdb_attr->rep_processors = value;
        if (bdb_state && bdb_state->dbenv)
            bdb_state->dbenv->set_num_recovery_processor_threads(
                bdb_state->dbenv, value);
        return;

    case BDB_ATTR_REP_MEMSIZE:
        bdb_attr->rep_memsize = value;
        if (bdb_state && bdb_state->dbenv)
            bdb_state->dbenv->set_recovery_memsize(bdb_state->dbenv, value);

    case BDB_ATTR_PAGE_EXTENT_SIZE:
        bdb_attr->page_extent_size = value;
        if (bdb_state && bdb_state->dbenv)
            bdb_state->dbenv->set_page_extent_size(bdb_state->dbenv, value);
        return;

    case BDB_ATTR_TRACK_REPLICATION_TIMES:
        bdb_attr->track_replication_times = value;
        if (bdb_state && value == 0)
            bdb_disable_replication_time_tracking(bdb_state);
        break;
    }

#define DEF_ATTR(NAME, name, type, dflt, desc)                                 \
    case BDB_ATTR_##NAME:                                                      \
        bdb_attr->name = value;                                                \
        break;
#define DEF_ATTR_2(NAME, name, type, dflt, desc, flags, verify_fn, update_fn)  \
    case BDB_ATTR_##NAME: bdb_attr->name = value; break;

    switch (attr) {
#include "attr.h"
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown attribute %d\n", __func__, attr);
        break;
    }
#undef DEF_ATTR
#undef DEF_ATTR_2

    if (attr == BDB_ATTR_REPLIMIT) {
        if (bdb_state) {
            if (bdb_state->dbenv) {
                int rc;
                const char *fname;
#if defined(BERKDB_4_5) || defined(BERKDB_46)
                rc = bdb_state->dbenv->rep_set_limit(bdb_state->dbenv, 0,
                                                     bdb_attr->replimit);
#else
                rc = bdb_state->dbenv->set_rep_limit(bdb_state->dbenv, 0,
                                                     bdb_attr->replimit);
#endif
                if (rc)
                    logmsg(LOGMSG_ERROR, "%s:set_rep_limit: %d %s\n", __func__,
                           rc, bdb_strerror(rc));
                else
                    logmsg(
                        LOGMSG_USER,
                        "dbenv->set_rep_limit called with new rep limit %d\n",
                        bdb_attr->replimit);
            }
        } else
            logmsg(
                LOGMSG_ERROR,
                "%s: BDB_ATTR_REPLIMIT changed but environment not updated\n",
                __func__);

    } else if (attr == BDB_ATTR_REPMETHODMAXSLEEP) {
        /* echo the attribute change into the berkdb layer
         * (gbl_rep_method_max_sleep_cnt is defined in rep/rep_method.c) */
        extern int gbl_rep_method_max_sleep_cnt;
        gbl_rep_method_max_sleep_cnt = bdb_attr->repmethodmaxsleep;
    }
}

void bdb_attr_set(bdb_attr_type *bdb_attr, int attr, int value)
{
    bdb_attr_set_int(NULL, bdb_attr, attr, value);
}

int bdb_attr_set_by_name(bdb_state_type *bdb_handle, bdb_attr_type *bdb_attr,
                         const char *attrname, int value)
{
#define DEF_ATTR(NAME, name, type, dflt, desc)                                 \
    else if (strcasecmp(attrname, #NAME) == 0)                                 \
    {                                                                          \
        bdb_attr_set_int(bdb_handle, bdb_attr, BDB_ATTR_##NAME, value);        \
    }
#define DEF_ATTR_2(NAME, name, type, dflt, desc, flags, verify_fn, update_fn)  \
    else if (strcasecmp(attrname, #NAME) == 0)                                 \
    {                                                                          \
        bdb_attr_set_int(bdb_handle, bdb_attr, BDB_ATTR_##NAME, value);        \
    }

    if (!attrname) {
        logmsg(LOGMSG_ERROR, "%s: attrname is NULL\n", __func__);
        return -1;
    }
#include "attr.h"
    else {
        logmsg(LOGMSG_ERROR, "%s: unknown attribute %s\n", __func__, attrname);
        return -1;
    }
#undef DEF_ATTR
#undef DEF_ATTR_2
    return 0;
}

int bdb_attr_get(bdb_attr_type *bdb_attr, int attr)
{
#define DEF_ATTR(NAME, name, type, dflt, desc)                                 \
    case BDB_ATTR_##NAME:                                                      \
        return bdb_attr->name;
#define DEF_ATTR_2(NAME, name, type, dflt, desc, flags, verify_fn, update_fn)  \
    case BDB_ATTR_##NAME: return bdb_attr->name;
    switch (attr) {
#include "attr.h"
    default:
        logmsg(LOGMSG_ERROR, "%s: unknown attribute %d\n", __func__, attr);
        return -1;
    }
#undef DEF_ATTR
#undef DEF_ATTR_2
}

static const char *bdb_attr_units(int type)
{
    switch (type) {
    case BDB_ATTRTYPE_SECS: return " secs";
    case BDB_ATTRTYPE_MSECS: return " msecs";
    case BDB_ATTRTYPE_USECS: return " usecs";
    case BDB_ATTRTYPE_BYTES: return " bytes";
    case BDB_ATTRTYPE_KBYTES: return " kbytes";
    case BDB_ATTRTYPE_MBYTES: return " megabytes";
    case BDB_ATTRTYPE_BOOLEAN: return " (boolean)";
    case BDB_ATTRTYPE_QUANTITY: return "";
    case BDB_ATTRTYPE_PERCENT: return "%";
    default: return " (unknown type?!)";
    }
}

void bdb_attr_dump(FILE *fh, const bdb_attr_type *bdb_attr)
{
#define DEF_ATTR(NAME, name, type, dflt, desc)                                 \
    logmsg(LOGMSG_USER, "%-20s = %d%s\n", #NAME, bdb_attr->name,               \
           bdb_attr_units(BDB_ATTRTYPE_##type));
#define DEF_ATTR_2(NAME, name, type, dflt, desc, flags, verify_fn, update_fn)  \
    logmsg(LOGMSG_USER, "%-20s = %d%s\n", #NAME, bdb_attr->name,               \
           bdb_attr_units(BDB_ATTRTYPE_##type));
#include "attr.h"
#undef DEF_ATTR
#undef DEF_ATTR_2
}

static inline comdb2_tunable_type bdb_to_tunable_type(int type)
{
    switch (type) {
    case BDB_ATTRTYPE_SECS:
    case BDB_ATTRTYPE_MSECS:
    case BDB_ATTRTYPE_USECS:
    case BDB_ATTRTYPE_BYTES:
    case BDB_ATTRTYPE_KBYTES:
    case BDB_ATTRTYPE_MBYTES:
    case BDB_ATTRTYPE_QUANTITY:
    case BDB_ATTRTYPE_PERCENT: return TUNABLE_INTEGER;
    case BDB_ATTRTYPE_BOOLEAN: return TUNABLE_BOOLEAN;
    default: assert(0);
    }
}

static inline int bdb_to_tunable_flag(int type)
{
    switch (type) {
    case BDB_ATTRTYPE_SECS:
    case BDB_ATTRTYPE_MSECS:
    case BDB_ATTRTYPE_USECS:
    case BDB_ATTRTYPE_BYTES:
    case BDB_ATTRTYPE_KBYTES:
    case BDB_ATTRTYPE_MBYTES:
    case BDB_ATTRTYPE_QUANTITY:
    case BDB_ATTRTYPE_PERCENT: return 0;
    case BDB_ATTRTYPE_BOOLEAN: return NOARG;
    default: assert(0);
    }
}

void *bdb_attr_create(void)
{
    bdb_attr_type *bdb_attr;

    if (!(bdb_attr = mymalloc(sizeof(bdb_attr_type)))) {
        logmsg(LOGMSG_ERROR, "%s:%d Out-of-memory", __FILE__, __LINE__);
        return NULL;
    }
    memset(bdb_attr, 0, sizeof(bdb_attr_type));

#define DEF_ATTR(NAME, name, type, dflt, desc)                                 \
    bdb_attr->name = (dflt);                                                   \
    REGISTER_TUNABLE(#NAME, desc, bdb_to_tunable_type(BDB_ATTRTYPE_##type),    \
                     &bdb_attr->name,                                          \
                     bdb_to_tunable_flag(BDB_ATTRTYPE_##type), NULL, NULL,     \
                     NULL, NULL);
#define DEF_ATTR_2(NAME, name, type, dflt, desc, flags, verify_fn, update_fn)  \
    bdb_attr->name = (dflt);                                                   \
    REGISTER_TUNABLE(#NAME, desc, bdb_to_tunable_type(BDB_ATTRTYPE_##type),    \
                     &bdb_attr->name,                                          \
                     bdb_to_tunable_flag(BDB_ATTRTYPE_##type) | flags, NULL,   \
                     verify_fn, update_fn, NULL);

#include "attr.h"
#undef DEF_ATTR
#undef DEF_ATTR_2

    REGISTER_TUNABLE("disable_pageorder_recsz_check",
                     "If set, allow page-order table scans even for larger "
                     "record sizes where they don't necessarily lead to "
                     "improvement.",
                     TUNABLE_BOOLEAN, &bdb_attr->disable_pageorder_recsz_chk,
                     NOARG, NULL, NULL, NULL, NULL);
    REGISTER_TUNABLE("enable_pageorder_recsz_check",
                     "Disables 'disable_pageorder_recsz_chk'", TUNABLE_BOOLEAN,
                     &bdb_attr->disable_pageorder_recsz_chk,
                     INVERSE_VALUE | NOARG, NULL, NULL, NULL, NULL);
    REGISTER_TUNABLE("nochecksums", "Disables 'checksums'", TUNABLE_BOOLEAN,
                     &bdb_attr->checksums, INVERSE_VALUE | NOARG, NULL, NULL,
                     NULL, NULL);

    /* echo our default attribute setting into the berkdb library */
    bdb_attr_set(bdb_attr, BDB_ATTR_REPMETHODMAXSLEEP,
                 bdb_attr->repmethodmaxsleep);
    listc_init(&bdb_attr->deferred_berkdb_options,
               offsetof(struct deferred_berkdb_option, lnk));

    return bdb_attr;
}
