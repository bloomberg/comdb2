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
#include <errno.h>
#include <string.h>
#include <strings.h>
#include <stddef.h>

#include <fcntl.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <unistd.h>

#include <tcputil.h>

#include <sbuf2.h>

#include "comdb2.h"
#include "sql.h"
#include "sqlinterfaces.h"
#include "dbglog.h"
#include <alloca.h>
#include "osqlcomm.h"

#include <socket_pool.h>

#include "flibc.h"
#include <cdb2_constants.h>
#include <cdb2api.h>
#include <logmsg.h>

/* dbglog + //DBSTATS support.  We have lots of interaces to
 sql, so here's
   a quick summary.

   In all instances, the client supplies a 8-byte cookie received from fastseed
 along with the
   request to enable dbglog.  When enabled, the db creates a file in its tmpdbs
 directory.  All
   stats collected during the run go there.  The client can make a further
 request to retrieve
   the information logged.  Comdb2 deletes the log file when it's requested.

 * Reads:
   Before each statement, the client sends a FSQL_SET_SQL_DEBUG request.  The
 payload of
   the request is the string "set debug dbglog 0000000000000000"
   where 000... is replaced by a fastseed value.  This is the cookie for this
 request.
   FSQL_SET_SQL_DEBUG has no reply.  After each statement, the client sends a
 FSQL_GRAB_DBGLOG
   request to retrieve the data for this statement.

   The other modes refer to writes only, reads work in an identical way
 * COMDB2:
   The 'read' part of the query, if any, is handled by the 'Reads' comment
 above.  The write
   portion injects a new BLOCK2_DBGLOG_COOKIE block opcode.  This sets the
 cookie.  After the
   write is done, client makes a new socket connection to the master and makes a
 "grabdbglog"
   appsock request which sends back the dbglog file.

 * BLOCKSQL
   Same as comdb2 mode - BLOCK2_DBGLOG_COOKIE blockop sets the cookie, makes
 appsock connection
   and "grabdbglog" request to read data.  The node that gets the offloaded read
 request also
   creates a debug file.  The master makes grabdbglog request to the offloaded
 node to fetch the
   data and put it in its own file before processing blockop requests.

 * SOCKSQL, READ COMMITTED, SNAPSHOT ISOLATION, SERIALIZABLE
   Same FSQL_SET_SQL_DEBUG request made as for reads.  After a query completes
 and write blocksops
   are sent to the master, replicant makes a "grabdbglog" to the master to add
 the write information
   to its own debug file.  Client makes FSQL_GRAB_DBGLOG request after each
 statement.

   Yes, the double hops in some cases aren't cheap but this only happens when
 //DBSTATS is enabled.
 */

/* Create dbglog file in the right place given the cookie. */
SBUF2 *open_dbglog_file(unsigned long long cookie)
{
    char fname[512];
    SBUF2 *sb;
    int fd;

    get_full_filename(fname, sizeof(fname), DIR_TMP, "dbglog.%d.%016llx",
                      gbl_mynode, cookie);
    fd = open(fname, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd == -1)
        return NULL;
    sb = sbuf2open(fd, 0);
    if (sb == NULL) {
        close(fd);
        return NULL;
    }
    return sb;
}

/* Called when a query is done, while all the cursors are still open.  Traverses
   the list of cursors and saves the query path and cost. */
int record_query_cost(struct sql_thread *thd, struct sqlclntstate *clnt)
{
    double cost;
    struct client_query_path_component *stats;
    int i;
    struct client_query_stats *query_info;
    struct query_path_component *c;
    int max_components;
    int sz;

    if (clnt->query_stats) {
        free(clnt->query_stats);
        clnt->query_stats = NULL;
    }

    if (!thd)
        return -1;

    max_components = listc_size(&thd->query_stats);
    sz = offsetof(struct client_query_stats, path_stats) +
         sizeof(struct client_query_path_component) * max_components;
    query_info = calloc(1, sz);
    clnt->query_stats = query_info;
    stats = query_info->path_stats;

    query_info->nlocks = -1;
    query_info->n_write_ios = -1;
    query_info->n_read_ios = -1;
    query_info->cost = query_cost(thd);
    query_info->n_components = max_components;
    query_info->n_rows =
        0; /* client computes from #records read, this is only
             useful for writes where this information doesn't come
             back */
    query_info->queryid = clnt->queryid;
    memset(query_info->reserved, 0xff, sizeof(query_info->reserved));

    i = 0;
    LISTC_FOR_EACH(&thd->query_stats, c, lnk)
    {
        if (i >= max_components) {
            free(clnt->query_stats);
            clnt->query_stats = 0;
            return -1;
        }

        if (c->nfind == 0 && c->nnext == 0 && c->nwrite == 0) {
            query_info->n_components--;
            continue;
        }

        stats[i].nfind = c->nfind;
        stats[i].nnext = c->nnext;
        stats[i].nwrite = c->nwrite;
        stats[i].ix = c->ix;
        stats[i].table[0] = 0;
        if (c->u.db) {
            if (c->remote) {
                snprintf(stats[i].table, sizeof(stats[i].table), "%s.%s",
                         fdb_table_entry_dbname(c->u.fdb),
                         fdb_table_entry_tblname(c->u.fdb));
            } else {
                strncpy(stats[i].table, c->u.db->dbname,
                        sizeof(stats[i].table));
            }
            stats[i].table[sizeof(stats[i].table) - 1] = 0;
        }

        i++;
    }
    return 0;
}

/* Determine n_components from a network-ordered buffer - this is used to
   calculate the total amount of memory for this client_query_stats request */
static inline int query_stats_n_components_get(const uint8_t *p_buf_stats)
{
    int *n_components =
        (int *)p_buf_stats + offsetof(struct client_query_stats, n_components);
    return ntohl(*n_components);
}

/* Write this client query stats request to a dbglog file.  The request is
   already packed in big endian format (it's from the wire), so just add a
   header & write */
void dump_client_query_stats_packed(SBUF2 *sb,
                                    const uint8_t *p_buf_client_query_stats)
{
    struct dbglog_hdr hdr;
    int n_components, sz;
    uint8_t hdrbuf[DBGLOG_HDR_LEN];

    /* grab endianized number of components */
    n_components = query_stats_n_components_get(p_buf_client_query_stats);

    /* calculate the total size */
    sz = offsetof(struct client_query_stats, path_stats) +
         sizeof(struct client_query_path_component) * n_components;

    /* create header */
    hdr.type = DBGLOG_QUERYSTATS;
    hdr.len = sz;

    /* pack header */
    if (!(dbglog_hdr_put(&hdr, hdrbuf, hdrbuf + DBGLOG_HDR_LEN))) {
        logmsg(LOGMSG_ERROR, "%s line %d: error packing client response\n", __func__,
                __LINE__);
        return;
    }

    /* write header */
    sbuf2fwrite((char *)&hdrbuf, DBGLOG_HDR_LEN, 1, sb);

    /* write query_stats buffer */
    sbuf2fwrite((char *)p_buf_client_query_stats, sz, 1, sb);

    /* flush */
    sbuf2flush(sb);
}

/* Endianize & write query stats to dbglog file */
void dump_client_query_stats(SBUF2 *sb, struct client_query_stats *st)
{
    int sz, used_malloc = 0;
    struct dbglog_hdr hdr;
    uint8_t hdrbuf[DBGLOG_HDR_LEN];
    uint8_t *buf, *p_buf, *p_buf_end;

    sz = offsetof(struct client_query_stats, path_stats) +
         sizeof(struct client_query_path_component) * st->n_components;

    /* pack dbglog header */
    hdr.type = DBGLOG_QUERYSTATS;
    hdr.len = sz;

    if (!(dbglog_hdr_put(&hdr, hdrbuf, hdrbuf + DBGLOG_HDR_LEN))) {
        logmsg(LOGMSG_ERROR, "%s line %d: error packing client response\n", __func__,
                __LINE__);
        return;
    }

    /* use alloca if I can */
    if (sz > 4096) {
        buf = malloc(sz);
        used_malloc = 1;
    } else {
        buf = alloca(sz);
    }

    /* set pointers */
    p_buf = buf;
    p_buf_end = (p_buf + sz);

    /* endianize the query stats structure */
    if (!(client_query_stats_put(st, p_buf, p_buf_end))) {
        logmsg(LOGMSG_ERROR, "%s line %d: error packing client response\n", __func__,
                __LINE__);
        if (used_malloc)
            free(buf);
        return;
    }

    /* write everything out */
    sbuf2fwrite((char *)&hdrbuf, DBGLOG_HDR_LEN, 1, sb);
    sbuf2fwrite((char *)buf, sz, 1, sb);
    sbuf2flush(sb);

    if (used_malloc)
        free(buf);
}

#define RETURN_DBGLOG_ERROR(errstr)                                            \
    {                                                                          \
        int errstr_len = strlen(errstr) + 1;                                   \
        int irc;                                                               \
        rsp.rcode = -1;                                                        \
        rsp.followlen = errstr_len;                                            \
        if (!(fsqlresp_put(&rsp, p_fsqlresp, p_fsqlresp_end))) {               \
            logmsg(LOGMSG_ERROR,                                               \
                   "%s line %d: error writing fsqlresp header\n", __func__,    \
                   __LINE__);                                                  \
            rc = -1;                                                           \
            goto done;                                                         \
        }                                                                      \
        irc =                                                                  \
            sbuf2fwrite((char *)&fsqlrespbuf, sizeof(struct fsqlresp), 1, sb); \
        if (irc == 1) sbuf2fwrite((char *)errstr, errstr_len, 1, sb);          \
        sbuf2flush(sb);                                                        \
        rc = -1;                                                               \
        goto done;                                                             \
    }

/* Dump the dbglog file to an sbuf (socket to user).  This can be read through
   an sql or appsock
   interface. To avoid duplicating code response always goes out with as a
   fsql_write_response-type
   response. If we are writing from an sql thread, we need to synchronize with
   the appsock thread
   which may be trying to write heartbeats to the same connection. */
int grab_dbglog_file(SBUF2 *sb, unsigned long long cookie,
                     struct sqlclntstate *clnt)
{
    char line[128];
    char fname[512];
    char *buf =
        NULL; /* appsock threads have a small stack, so this is malloced */
    int rc = 0;
    int fd = -1;
    struct stat st;
    SBUF2 *dbgfile = NULL;
    int sz;
    int bufsz = 4096;
    int i;
    int lim;
    struct fsqlresp rsp;
    uint8_t fsqlrespbuf[FSQLRESP_LEN];
    uint8_t *p_fsqlresp = fsqlrespbuf;
    uint8_t *p_fsqlresp_end = (p_fsqlresp + FSQLRESP_LEN);

    rsp.response = FSQL_QUERY_STATS;
    rsp.flags = 0;
    rsp.rcode = 0;
    rsp.parm = 0;
    rsp.followlen = 0;

    if (clnt)
        pthread_mutex_lock(&clnt->write_lock);

    get_full_filename(fname, sizeof(fname), DIR_TMP, "dbglog.%d.%016llx",
                      gbl_mynode, cookie);
    fd = open(fname, O_RDONLY);
    if (fd == -1) {
        RETURN_DBGLOG_ERROR("can't open dbglog file");
    }
    dbgfile = sbuf2open(fd, 0);
    if (dbgfile == NULL) {
        RETURN_DBGLOG_ERROR("can't open sbuf for dbglog file");
    }

    rc = fstat(fd, &st);
    if (rc) {
        RETURN_DBGLOG_ERROR("can't open dbglog file size");
    }
    sz = st.st_size;
    rsp.followlen = sz;
    buf = malloc(bufsz);

    /* endianize the fsql response-header */
    if (!(fsqlresp_put(&rsp, p_fsqlresp, p_fsqlresp_end))) {
        logmsg(LOGMSG_ERROR, "%s line %d: error writing fsqlresp header\n", __func__,
                __LINE__);
        rc = -1;
        goto done;
    }

    /* write endianized header */
    rc = sbuf2fwrite((char *)&fsqlrespbuf, sizeof(struct fsqlresp), 1, sb);
    if (rc != 1) {
        rc = -1;
        goto done;
    }
    sbuf2flush(sb);

    /* log will be endianized already */
    for (i = 0; i < (sz / bufsz + ((sz % bufsz) ? 1 : 0)); i++) {
        if (i == sz / bufsz)
            lim = sz % bufsz;
        else
            lim = bufsz;

        /* read from dbglog */
        rc = sbuf2fread(buf, lim, 1, dbgfile);
        if (rc != 1) {
            rc = -1;
            goto done;
        }

        /* write */
        rc = sbuf2fwrite(buf, lim, 1, sb);
        if (rc != 1) {
            rc = -1;
            goto done;
        }
    }
    rc = 0;
    sbuf2flush(sb);

done:
    if (clnt)
        pthread_mutex_unlock(&clnt->write_lock);
    if (dbgfile) {
        sbuf2close(dbgfile);
        unlink(fname);
    } else {
        if (fd != -1)
            close(fd);
    }
    if (buf)
        free(buf);

    return rc;
}

int dbglog_init_write_counters(struct ireq *iq)
{
    if (!iq->nwrites)
        iq->nwrites = malloc(sizeof(int) * thedb->num_dbs);
    bzero(iq->nwrites, sizeof(int) * thedb->num_dbs);
    return 0;
}

void dbglog_record_db_write(struct ireq *iq, char *optype)
{
    /* save cost here */
    iq->cost += (1 + iq->usedb->nix + iq->usedb->numblobs) * 100;

    if (!iq->dbglog_file)
        return;
    if (iq->usedb == NULL)
        return;
    if (iq->usedb->dbs_idx == -1)
        return;
    iq->nwrites[iq->usedb->dbs_idx]++;
}

void dbglog_dump_write_stats(struct ireq *iq)
{
    int i;
    int cnt = 0;
    struct client_query_stats *st = NULL;
    double cost = 0;
    int ix = 0;

    for (i = 0; i < thedb->num_dbs; i++) {
        if (iq->nwrites[i]) {
            cnt++;
            cost += iq->nwrites[i] * 100.0 *
                    (thedb->dbs[i]->nix + thedb->dbs[i]->numblobs + 1);
        }
    }
    if (cnt == 0)
        return;

    st = calloc(1, offsetof(struct client_query_stats, path_stats) +
                       sizeof(struct client_query_path_component) * cnt);
    st->nlocks = st->n_write_ios = st->n_read_ios = -1;
    st->n_components = cnt;
    st->cost = cost;
    st->queryid = iq->queryid;

    /* TODO: Should record every index? Every index gets written. */
    for (i = 0; i < thedb->num_dbs; i++) {
        if (iq->nwrites[i]) {
            st->path_stats[ix].nwrite = iq->nwrites[i];
            st->path_stats[ix].ix = -1;
            strcpy(st->path_stats[ix].table, thedb->dbs[i]->dbname);
            ix++;
            st->n_rows += iq->nwrites[i];
        }
    }
    dump_client_query_stats(iq->dbglog_file, st);
    free(st);
}

/* These are private... */
/* SOCKPOOL CLIENT APIS */
int cdb2_socket_pool_get(const char *typestr, int dbnum,
                         int *port); /* returns the fd.*/
void cdb2_socket_pool_donate_ext(const char *typestr, int fd, int ttl,
                                 int dbnum, int flags, void *destructor,
                                 void *voidargs);

int gbl_use_sockpool_for_debug_logs = 1;
void append_debug_logs_from_master(SBUF2 *oursb,
                                   unsigned long long master_dbglog_cookie)
{
    int rc;
    int cpu = 0, port = 0;
    const char *err;
    SBUF2 *sb = NULL;
    struct host_node_info nodes[REPMAX];
    int nnodes;
    int i;
    int fd = -1;
    unsigned long long flipped_cookie;
    struct in_addr addr;
    struct fsqlreq req;
    struct fsqlresp dbgrsp = {0};
    void *debugbuf = NULL;
    int fd_from_sockpool = 0;

    /* stack-space for fsql request buffer & pointers */
    uint8_t fsqlreqbuf[FSQLREQ_LEN];
    uint8_t *p_fsqlreq = fsqlreqbuf;
    uint8_t *p_fsqlreq_end = (p_fsqlreq + FSQLREQ_LEN);

    /* stack-space for fsql response buffer & pointers */
    uint8_t fsqlrespbuf[FSQLRESP_LEN];
    uint8_t *p_fsqlresp = fsqlrespbuf;
    uint8_t *p_fsqlresp_end = (p_fsqlresp + FSQLRESP_LEN);
    int donated = 0;
    char typestr[64];
    int use_sockpool;

    /* we need the port to connect to on the master */
    nnodes = net_get_nodes_info(thedb->handle_sibling, REPMAX, nodes);

    /* no network information? */
    if (nnodes <= 0)
        return;

    /* look for master */
    for (i = 0; i < nnodes; i++) {
        if (nodes[i].host == thedb->master)
            break;
    }

    /* not found? return */
    if (i == nnodes)
        return;

    use_sockpool = gbl_use_sockpool_for_debug_logs;

    if (use_sockpool) {
        snprintf(typestr, sizeof(typestr), "comdb2/%s/sql/%s", thedb->envname,
                 thedb->master);
        fd = cdb2_socket_pool_get(typestr, thedb->dbnum, &port);
        if (fd != -1)
            fd_from_sockpool = 1;
    }
    if (port == 0)
        port = nodes[i].port;
    if (port == 0)
        return;

    if (fd == -1)
        fd = tcpconnecth(thedb->master, port, 0);
    if (fd == -1)
        return;

    sb = sbuf2open(fd, 0);
    if (sb == NULL) {
        close(fd);
        goto done;
    }

    if (!fd_from_sockpool) {
        rc = sbuf2printf(sb, "fastsql %s\n", thedb->envname);
        if (rc <= 0)
            goto done;
    }

    /* send request to get data */
    req.request = FSQL_GRAB_DBGLOG;
    req.flags = 0;
    req.parm = 0;
    req.followlen = sizeof(uint64_t);

    /* endianize fsql request */
    if (!(fsqlreq_put(&req, p_fsqlreq, p_fsqlreq_end))) {
        logmsg(LOGMSG_ERROR, "%s: error packing fsqlreq buffer\n", __func__);
        goto done;
    }

    rc = sbuf2fwrite((char *)fsqlreqbuf, FSQLREQ_LEN, 1, sb);
    if (rc != 1)
        goto done;

    /* debuglog cookie is a fastseed- we want it to look the same everywhere */
    flipped_cookie = flibc_htonll(master_dbglog_cookie);

    rc = sbuf2fwrite((char *)&flipped_cookie, sizeof(flipped_cookie), 1, sb);
    if (rc != 1)
        goto done;
    rc = sbuf2flush(sb);
    if (rc < 0)
        goto done;

again:
    rc = sbuf2fread((char *)&fsqlrespbuf, FSQLRESP_LEN, 1, sb);
    if (rc != 1)
        goto done;

    /* convert response to host order */
    if (!(fsqlresp_get(&dbgrsp, p_fsqlresp, p_fsqlresp_end))) {
        logmsg(LOGMSG_ERROR, "%s line %d: error reading fsqlresp\n", __func__,
                __LINE__);
        rc = -1;
        goto done;
    }

    /* a heartbeat can sneak into the response stream - skip it */
    if (dbgrsp.response == FSQL_HEARTBEAT)
        goto again;

    debugbuf = malloc(dbgrsp.followlen);
    rc = sbuf2fread((char *)debugbuf, dbgrsp.followlen, 1, sb);
    if (rc != 1)
        goto done;

    if (dbgrsp.rcode == 0) {
        rc = sbuf2fwrite(debugbuf, dbgrsp.followlen, 1, oursb);
        if (rc != 1)
            goto done;
    }

    if (use_sockpool && fd != -1) {
        cdb2_socket_pool_donate_ext(typestr, fd, 10, thedb->dbnum, 0, NULL,
                                    NULL);
        donated = 1;
    }

done:
    if (sb)
        sbuf2free(sb);
    if (debugbuf)
        free(debugbuf);
    if (fd != -1 && !donated)
        close(fd);
}
