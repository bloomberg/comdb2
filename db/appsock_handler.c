/*
   Copyright 2015, 2018, Bloomberg Finance L.P.

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

/*
 * Application socket handler,
 * For dump/reload and alternate interfaces
 *
 * We now use a thread pool for appsock.  The threads have a small (64KB)
 * stack by default allowing us to maintain lots of connections.  Be aware
 * of this when doing appsock stuff!
 */
#include <pthread.h>

#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "limit_fortify.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <time.h>

#include <segstr.h>
#include <machpthread.h>
#include <plink.h>
#include <tcputil.h>
#include <lockmacro.h>

#include <memory_sync.h>

#include <sbuf2.h>
#include <bdb_api.h>

#include "util.h"
#include "socket_interfaces.h"
#include "comdb2.h"
#include "thdpool.h"
#include "analyze.h"

#include <alloca.h>

#include "fdb_comm.h"

#include <bdb_api.h>
#include <bdb_schemachange.h>
#include <logmsg.h>

#include "comdb2_plugin.h"
#include "comdb2_appsock.h"
#include "plhash.h"
#include "comdb2_atomic.h"

#ifdef DEBUG
// was crashing because of the small stack size when debug was on
#define GBL_APPSOCK_THDPOOL_STCKSZ 512 * 1024
#else
#define GBL_APPSOCK_THDPOOL_STCKSZ 192 * 1024
#endif

extern struct dbenv *thedb; /* handles 1 db for now */
extern int gbl_use_appsock_as_sqlthread;
extern comdb2_plugin_t *gbl_plugins;

struct appsock_thd_state {
    struct thr_handle *thr_self;
};

struct thdpool *gbl_appsock_thdpool = NULL;
char appsock_unknown_old[] = "-1 #unknown command\n";
char appsock_unknown[] = "Error: -1 #unknown command\n";
char appsock_supported[] = "supported\n";
int active_appsock_conns = 0;

/* HASH of all registered appsock handlers (one handler per appsock type) */
hash_t *gbl_appsock_hash;

static unsigned long long total_appsock_conns = 0;
static unsigned long long num_bad_toks = 0;
static unsigned long long total_toks = 0;
static unsigned long long total_appsock_rejections = 0;
static pthread_mutex_t appsock_conn_lk = PTHREAD_MUTEX_INITIALIZER;
static size_t num_commands = 0;

static void appsock_thd_start(struct thdpool *pool, void *thddata);
static void appsock_thd_end(struct thdpool *pool, void *thddata);

void close_appsock(SBUF2 *sb)
{
    net_end_appsock(sb);
    LOCK(&appsock_conn_lk)
    {
        active_appsock_conns--;
    }
    UNLOCK(&appsock_conn_lk);
}

int appsock_init(void)
{
    /* Initialize the appsock handler hash. */
    gbl_appsock_hash =
        hash_init_user((hashfunc_t *)strhashfunc, (cmpfunc_t *)strcmpfunc,
                       offsetof(comdb2_appsock_t, name), 0);
    logmsg(LOGMSG_DEBUG, "appsock handler hash initialized\n");

    gbl_appsock_thdpool =
        thdpool_create("appsockpool", sizeof(struct appsock_thd_state));

    if (gbl_exit_on_pthread_create_fail)
        thdpool_set_exit(gbl_appsock_thdpool);

    /* Nice small stack so we can handle lots of connections */
    thdpool_set_stack_size(gbl_appsock_thdpool, GBL_APPSOCK_THDPOOL_STCKSZ);
    thdpool_set_init_fn(gbl_appsock_thdpool, appsock_thd_start);
    thdpool_set_delt_fn(gbl_appsock_thdpool, appsock_thd_end);
    thdpool_set_minthds(gbl_appsock_thdpool, 1);
    thdpool_set_linger(gbl_appsock_thdpool, 10);

    thdpool_set_mem_size(gbl_appsock_thdpool, 4 * 1024);

    return 0;
}

void appsock_quick_stat(void)
{
    logmsg(LOGMSG_USER, "num appsock connections %llu\n", total_appsock_conns);
    logmsg(LOGMSG_USER, "num active appsock connections %d\n",
           active_appsock_conns);
    logmsg(LOGMSG_USER, "num appsock commands    %llu\n", total_toks);
}

void appsock_stat(void)
{
    comdb2_appsock_t *rec;
    unsigned int exec_count;
    unsigned int bkt;
    void *ent;

    appsock_quick_stat();
    logmsg(LOGMSG_USER, "bad appsock commands    %llu\n", num_bad_toks);
    logmsg(LOGMSG_USER, "rejected appsock conns  %llu\n",
           total_appsock_rejections);

    for (rec = hash_first(gbl_appsock_hash, &ent, &bkt); rec;
         rec = hash_next(gbl_appsock_hash, &ent, &bkt)) {
        exec_count = ATOMIC_LOAD(rec->exec_count);
        if (exec_count > 0) {
            logmsg(LOGMSG_USER, "  num %-16s  %u\n", rec->name, exec_count);
        }
    }
}

void appsock_get_dbinfo2_stats(uint32_t *n_appsock, uint32_t *n_sql)
{
    comdb2_appsock_t *rec;
    unsigned int exec_count;
    unsigned int bkt;
    void *ent;

    *n_appsock = total_appsock_conns;
    exec_count = 0;

    /*
      Iterate through the list of registered appsock handlers
      and find the number of executions of SQL appsock handlers.
    */
    for (rec = hash_first(gbl_appsock_hash, &ent, &bkt); rec;
         rec = hash_next(gbl_appsock_hash, &ent, &bkt)) {
        if (rec->flags & APPSOCK_FLAG_IS_SQL) {
            exec_count += ATOMIC_LOAD(rec->exec_count);
        }
    }
    *n_sql = exec_count;
}

#if 0
static void dumprrns(struct dbtable *tbl, SBUF2 *sb)
{
    char key[MAXKEYLEN];
    char fndkey[MAXKEYLEN];
    char fnddta[MAXLRL];
    int fndrrn, fndlen, ii, rc, rrn;
    static char *hexchars = "0123456789ABCDEF";
    unsigned char u;
    struct ireq liq = {0};
    unsigned long long genid;

    memset(key, 0, sizeof(key));
    liq.usedb = tbl;
    rc = ix_find(&liq, 0, key, 1, fndkey, &fndrrn, &genid, fnddta, &fndlen,
                 sizeof(fnddta));
    while (rc >= 0 && rc <= 2) /*got a key*/
    {
        sbuf2printf(sb, "%d rrn %d len %d ", rc, fndrrn, fndlen);
        for (ii = 0; ii < fndlen; ii++) {
            u = fnddta[ii];
            sbuf2putc(sb, hexchars[(u >> 4) & 0xf]);
            sbuf2putc(sb, hexchars[u & 0xf]);
        }
        sbuf2putc(sb, '\n');
        memcpy(key, fndkey, MAXKEYLEN);
        rrn = fndrrn;
        rc = ix_next(&liq, 0, key, 1, key, rrn, genid, fndkey, &fndrrn, &genid,
                     fnddta, &fndlen, sizeof(fnddta), 0);
    }
    sbuf2printf(sb, "%d #done\n", rc);
}

struct loadrrn_cmd {
    int op;
    int parm;
};

enum { LOAD_ADD_RECORD, LOAD_GET_STATUS };
static int loadrrns(struct dbtable *tbl, SBUF2 *sb, char *tag)
{
    int len;
    char *buf;
    int rc, rrc;
    int recno = 0;
    int op;
    int status;
    char *dta;
    unsigned char nullbits[MAXNULLBITS] = {0};

    len = get_size_of_schema_by_name(tbl->tablename, tag);
    if (len == 0)
        return -1;
    buf = malloc(len);
    dta = malloc(getdatsize(tbl));

    do {
        rrc = sbuf2fread((char *)&op, sizeof(int), 1, sb);
        if (rrc != 1) {
            rc = -1;
            break;
        }

        switch (op) {
        case LOAD_ADD_RECORD:
            rrc = sbuf2fread(buf, len, 1, sb);
            if (rrc != 1) {
                rc = -1;
                break;
            }

            rc = ctag_to_stag_buf(tbl->tablename, tag, buf, len, nullbits,
                                  ".ONDISK", dta, 0, NULL);
            if (rc != -1) {
                rc = load_record(tbl, buf);
                recno++;
            }
            break;

        case LOAD_GET_STATUS:
            rrc = sbuf2fwrite((char *)&rc, sizeof(int), 1, sb);
            if (rrc != 1) {
                rc = -1;
                break;
            }
            break;
        }
    } while (rrc == 1);

    free(buf);
    return rc;
}

/* callback for converting records to given tag */
static int fstdump_callback(void *rec, size_t reclen, void *clientrec,
                            size_t clientreclen, struct dbtable *tbl,
                            const char *tag, const char *tzname, uint8_t ver,
                            int conv_flags)
{
    unsigned char nulls[MAXNULLBITS];
    int rc = 0;
    if (tbl->dbtype == DBTYPE_TAGGED_TABLE) {
        int len = reclen;
        if (ver < tbl->version) {
            void *newrec = alloca(tbl->lrl);
            memcpy(newrec, rec, reclen);
            rec = newrec;
        }
        vtag_to_ondisk(tbl, rec, &len, ver, 0);
        rc = stag_to_ctag_buf_tz(tbl->tablename, ".ONDISK", rec, len, tag,
                                 clientrec, nulls, conv_flags, NULL, NULL,
                                 tzname);
    } else {
        memcpy(clientrec, rec, clientreclen);
    }
    if (rc < 0)
        return -1;
    else {
        /* reverse psychology?  calling code used to treat -1 as good,
           everything
           else as a failure */
        return 0;
    }
}

struct fstdmp_t {
    int rc;
    bdb_state_type *bdb_state;
    SBUF2 *sb;
    size_t sendrecsz;
    bdb_fstdumpdta_callback_t convert_callback;
    int callback_flags;
    void *userptr;
    void *userptr2;
    int timeoutms;
    int safe_mode;
    int *bdberr;
    const char *tzname;
    int get_genids;
};

static void *fstdump_hndlr(void *arg_)
{
    struct fstdmp_t *arg = arg_;
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);
    arg->rc = bdb_fstdumpdta_sendsz(
        arg->bdb_state, arg->sb, arg->sendrecsz, arg->convert_callback,
        arg->callback_flags, arg->userptr, arg->userptr2, arg->timeoutms,
        arg->safe_mode, arg->bdberr, arg->tzname, arg->get_genids);
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);
    return NULL;
}
#endif

static void *thd_appsock_int(SBUF2 *sb, int *keepsocket,
                             struct thr_handle *thr_self)
{
    comdb2_appsock_t *appsock;
    comdb2_appsock_arg_t arg;
    struct dbtable *tab;
    char line[128];
    char command[128];
    char *ptr;
    int rc, ltok, st;
    char *tok;

    *keepsocket = 0; /* Safety */

    sbuf2settimeout(sb, IOTIMEOUTMS, IOTIMEOUTMS);

    tab = thedb->dbs[0];

    while (1) {
        thrman_where(thr_self, NULL);

        /* Read a line until and including '\n' */
        rc = sbuf2gets(line, sizeof(line), sb);

        if (rc <= 0)
            break;

        st = 0;

        printf(">>>> thd_apsock_int(): '%s'\n", line);

        tok = segtok(line, rc, &st, &ltok);
        if (ltok == 0)
            continue;
        if (tok[0] == '#')
            continue;

        memcpy(command, tok, ltok);
        command[ltok] = 0;
        ptr = command;

        appsock = hash_find_readonly(gbl_appsock_hash, &ptr);
        if (!appsock) {
            /* No handler found for the received appsock request. */
            logmsg(LOGMSG_ERROR, "appsock '%s' not supported\n", ptr);
            sbuf2printf(sb, appsock_unknown);
            sbuf2flush(sb);
            num_bad_toks++;
            continue;
        }

        total_toks++;

        /* Prepare the argument to be passed to the handler. */
        arg.thr_self = thr_self;
        arg.dbenv = thedb;
        arg.tab = tab;
        arg.sb = sb;
        arg.cmdline = line;
        arg.keepsocket = keepsocket;

        thrman_where(thr_self, appsock->name);

        /* Increment the execution count. */
        ATOMIC_ADD(appsock->exec_count, 1);

        /* Invoke the handler. */
        rc = appsock->appsock_handler(&arg);
        if (rc != APPSOCK_RETURN_CONT)
            break;
    }

    thrman_where(thr_self, NULL);

    return 0;
}

static void appsock_thd_start(struct thdpool *pool, void *thddata)
{
    struct appsock_thd_state *state = thddata;
    state->thr_self = thrman_register(THRTYPE_APPSOCK_POOL);
    if (!gbl_use_appsock_as_sqlthread)
        backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDWR);
}

static void appsock_thd_end(struct thdpool *pool, void *thddata)
{
    struct appsock_thd_state *state = thddata;
    if (!gbl_use_appsock_as_sqlthread)
        backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDWR);
}

static void appsock_work(struct thdpool *pool, void *work, void *thddata)
{
    struct appsock_thd_state *state = thddata;
    SBUF2 *sb = work;
    int keepsocket = 0;

    thrman_setfd(state->thr_self, sbuf2fileno(sb));
    thd_appsock_int(sb, &keepsocket, state->thr_self);
    thrman_setfd(state->thr_self, -1);
    thrman_where(state->thr_self, NULL);
    if (keepsocket == 0)
        close_appsock(sb);

    if (thrman_get_type(state->thr_self) != THRTYPE_APPSOCK_POOL)
        thrman_change_type(state->thr_self, THRTYPE_APPSOCK_POOL);
}

static void appsock_work_pp(struct thdpool *pool, void *work, void *thddata,
                            int op)
{
    SBUF2 *sb = (SBUF2 *)work;

    switch (op) {
    case THD_RUN:
        appsock_work(pool, work, thddata);
        break;

    case THD_FREE:
        close_appsock(sb);
        break;

    default:
        abort();
    }
}

int gbl_appsock_connection_warn_threshold = 80;

void dump_appsock_threads(void)
{
    thdpool_print_stats(stderr, gbl_appsock_thdpool);
    thrman_dump();
}

void appsock_handler_start(struct dbenv *dbenv, SBUF2 *sb)
{
    /*START HANDLER THREAD*/
    int rc;
    pthread_t tid;
    static int last_thread_dump_time = 0;
    static int last_thread_dump_warn_time = 0;
    int nconns = 0;
    int maxconns;
    time_t now;

    now = time(NULL);

    maxconns = thdpool_get_maxthds(gbl_appsock_thdpool);
    nconns = thdpool_get_nthds(gbl_appsock_thdpool);
    if (nconns < maxconns &&
        nconns > (double)maxconns *
                     ((double)gbl_appsock_connection_warn_threshold / 100)) {
        if ((now - last_thread_dump_warn_time) > 10) {
            logmsg(LOGMSG_WARN,
                   "Warning: reached %d%% of max concurrent connections "
                   "(%d/%d):\n",
                   gbl_appsock_connection_warn_threshold, nconns, maxconns);
            last_thread_dump_warn_time = now;
            dump_appsock_threads();
        }
    }

    /* reject requests if we're not up, going down, or not interested */
    if (dbenv->stopped || gbl_exit || !gbl_ready) {
        total_appsock_rejections++;
        net_end_appsock(sb);
        return;
    }

    if (active_appsock_conns >=
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_APPSOCKSLIMIT)) {
        static int lastprint = 0;
        int now = time_epoch();

        if ((now - lastprint) > 0) {
            int max = bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXAPPSOCKSLIMIT);

            logmsg(
                LOGMSG_WARN,
                "%s: Maximum appsock connection limit approaching (%d/%d).\n",
                __func__, active_appsock_conns, max);
            lastprint = now;

            if ((now - last_thread_dump_time) > 10) {
                last_thread_dump_time = now;
                dump_appsock_threads();
            }
        }
    }

    if (thdpool_enqueue(gbl_appsock_thdpool, appsock_work_pp, sb, 0, NULL) !=
        0) {
        total_appsock_rejections++;
        if ((now - last_thread_dump_time) > 10) {
            logmsg(LOGMSG_WARN, "Too many concurrent SQL connections:\n");
            last_thread_dump_time = now;
            dump_appsock_threads();
        }

        logmsg(LOGMSG_ERROR, "%s:thdpool_enqueue error\n", __func__);
        net_end_appsock(sb);
        return;
    }

    total_appsock_conns++;
    LOCK(&appsock_conn_lk)
    {
        active_appsock_conns++;
    }
    UNLOCK(&appsock_conn_lk);
    if (active_appsock_conns >
        bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXSOCKCACHED)) {
        logmsg(LOGMSG_WARN,
               "TOO many active socket connections. current limit %d\n",
               bdb_attr_get(thedb->bdb_attr, BDB_ATTR_MAXSOCKCACHED));
    }
}

int set_rowlocks(void *trans, int enable)
{
    int rc, bdberr, rlstate;
    if (enable) {
        rlstate = LLMETA_ROWLOCKS_ENABLED;
    } else {
        rlstate = LLMETA_ROWLOCKS_DISABLED;
    }

    if ((rc = bdb_set_rowlocks_state(NULL, rlstate, &bdberr)) != 0) {
        logmsg(LOGMSG_ERROR, "Error setting rowlocks state, rc=%d, bdberr=%d\n",
               rc, bdberr);
        return -1;
    }

    if (gbl_rowlocks) {
        gbl_sql_tranlevel_preserved = gbl_sql_tranlevel_default;
        gbl_sql_tranlevel_default = SQL_TDEF_SNAPISOL;
    } else {
        gbl_sql_tranlevel_default = gbl_sql_tranlevel_preserved;
    }

    return 0;
}
