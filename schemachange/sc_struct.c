/*
   Copyright 2015, 2022 Bloomberg Finance L.P.

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

#include "schemachange.h"
#include "sc_struct.h"
#include "logmsg.h"
#include "sc_csc2.h"
#include "sc_schema.h"
#include "macc_glue.h"
#include "schemachange.pb-c.h"
#include "str0.h"
#include "sc_version.h"

/************ SCHEMACHANGE TO BUF UTILITY FUNCTIONS
 * *****************************/

struct schema_change_type *init_schemachange_type(struct schema_change_type *sc)
{
    sc->tran = NULL;
    sc->sb = NULL;
    sc->newcsc2 = NULL;
    sc->nothrevent = 0;
    sc->onstack = 0;
    sc->live = 0;
    sc->use_plan = 0;
    /* default values: no change */
    sc->headers = -1;
    sc->compress = -1;
    sc->compress_blobs = -1;
    sc->ip_updates = -1;
    sc->instant_sc = -1;
    sc->persistent_seq = -1;
    sc->dbnum = -1; /* -1 = not changing, anything else = set value */
    sc->source_node[0] = 0;
    sc->timepartition_name = NULL;
    sc->partition.type = PARTITION_NONE;
    listc_init(&sc->dests, offsetof(struct dest, lnk));
    Pthread_mutex_init(&sc->mtx, NULL);
    Pthread_mutex_init(&sc->livesc_mtx, NULL);
    Pthread_mutex_init(&sc->mtxStart, NULL);
    Pthread_cond_init(&sc->condStart, NULL);
    return sc;
}

struct schema_change_type *new_schemachange_type()
{
    struct schema_change_type *sc =
        calloc(1, sizeof(struct schema_change_type));
    if (sc != NULL)
        sc = init_schemachange_type(sc);

    return sc;
}

void cleanup_strptr(char **schemabuf)
{
    if (*schemabuf) free(*schemabuf);
    *schemabuf = NULL;
}

static void free_dests(struct schema_change_type *s)
{
    for (; s->dests.count > 0;) {
        struct dest *d = listc_rbl(&s->dests);
        free(d->dest);
        free(d);
    }
}

void free_schema_change_type(struct schema_change_type *s)
{
    if (!s)
        return;
    free(s->newcsc2);
    free(s->sc_convert_done);

    free_dests(s);
    Pthread_mutex_destroy(&s->mtx);
    Pthread_mutex_destroy(&s->livesc_mtx);

    Pthread_cond_destroy(&s->condStart);
    Pthread_mutex_destroy(&s->mtxStart);

    /*if (s->partition.type == PARTITION_ADD_COL_HASH) {
        char *str = s->partition.u.hash.viewname;
        if (str) free(str);
        for(int i=0;i<s->partition.u.hash.num_columns;i++){
            str = s->partition.u.hash.columns[i];
            if (str) free(str);
        }
        if (s->partition.u.hash.columns) {
            free(s->partition.u.hash.columns);
        }
        for(int i=0;i<s->partition.u.hash.num_partitions;i++){
            str = s->partition.u.hash.partitions[i];
            if (str) free(str);
        }
        if (s->partition.u.hash.partitions) {
            free(s->partition.u.hash.partitions);
        }
    }*/

    if (s->sb && s->must_close_sb) {
        close_appsock(s->sb);
        s->sb = NULL;
    }
    if (!s->onstack) {
        free(s);
    }

}

static size_t dests_field_packed_size(struct schema_change_type *s)
{

    size_t len = sizeof(s->dests.count);
    int i;
    struct dest *d;
    for (i = 0, d = s->dests.top; d; d = d->lnk.next, i++) {
        len += sizeof(int);
        len += strlen(d->dest);
    }
    return len;
}

static size_t _partition_packed_size(struct comdb2_partition *p)
{
    size_t shardNamesSize = 0;
    size_t columnNamesSize = 0;
    switch (p->type) {
    case PARTITION_NONE:
    case PARTITION_REMOVE:
    case PARTITION_REMOVE_COL_HASH:
        return sizeof(p->type);
    case PARTITION_ADD_TIMED:
    case PARTITION_ADD_MANUAL:
        return sizeof(p->type) + sizeof(p->u.tpt.period) +
               sizeof(p->u.tpt.retention) + sizeof(p->u.tpt.start);
    case PARTITION_MERGE:
        return sizeof(p->type) + sizeof(p->u.mergetable.tablename) +
               sizeof(p->u.mergetable.version);
	case PARTITION_ADD_COL_HASH:
        for (int i = 0; i < p->u.hash.num_partitions; i++) {
            logmsg(LOGMSG_USER, "length of %s is %ld\n", p->u.hash.partitions[i], strlen(p->u.hash.partitions[i]));
            shardNamesSize += sizeof(size_t) + strlen(p->u.hash.partitions[i]);
        }

        for (int i = 0; i < p->u.hash.num_columns; i++) {
            logmsg(LOGMSG_USER, "length of %s is %ld\n", p->u.hash.columns[i], strlen(p->u.hash.columns[i]));
            columnNamesSize += sizeof(size_t) + strlen(p->u.hash.columns[i]);
        }
        return sizeof(p->type) + sizeof(size_t) + strlen(p->u.hash.viewname) + sizeof(p->u.hash.num_partitions) + 
            sizeof(p->u.hash.num_columns) + shardNamesSize + columnNamesSize;
    default:
        logmsg(LOGMSG_ERROR, "Unimplemented partition type %d\n", p->type);
        abort();
    }
}

size_t schemachange_packed_size(struct schema_change_type *s)
{
    s->tablename_len = strlen(s->tablename) + 1;
    s->fname_len = strlen(s->fname) + 1;
    s->aname_len = strlen(s->aname) + 1;
    s->spname_len = strlen(s->spname) + 1;
    s->newcsc2_len = (s->newcsc2) ? strlen(s->newcsc2) + 1 : 0;

    s->packed_len =
        sizeof(s->kind) + sizeof(s->rqid) + sizeof(s->uuid) +
        sizeof(s->tablename_len) + s->tablename_len + sizeof(s->fname_len) +
        s->fname_len + sizeof(s->aname_len) + s->aname_len +
        sizeof(s->avgitemsz) + sizeof(s->newdtastripe) + sizeof(s->blobstripe) +
        sizeof(s->live) + sizeof(s->newcsc2_len) + s->newcsc2_len +
        sizeof(s->scanmode) + sizeof(s->delay_commit) +
        sizeof(s->force_rebuild) + sizeof(s->force_dta_rebuild) +
        sizeof(s->force_blob_rebuild) + sizeof(s->force) + sizeof(s->headers) +
        sizeof(s->header_change) + sizeof(s->compress) +
        sizeof(s->compress_blobs) + sizeof(s->persistent_seq) +
        sizeof(s->ip_updates) + sizeof(s->instant_sc) + sizeof(s->preempted) +
        sizeof(s->use_plan) + sizeof(s->commit_sleep) +
        sizeof(s->convert_sleep) + sizeof(s->same_schema) + sizeof(s->dbnum) +
        sizeof(s->flg) + sizeof(s->rebuild_index) +
        sizeof(s->index_to_rebuild) + sizeof(s->source_node) +
        dests_field_packed_size(s) + sizeof(s->spname_len) + s->spname_len +
        sizeof(s->lua_func_flags) + sizeof(s->newtable) +
        sizeof(s->usedbtablevers) + sizeof(s->qdb_file_ver) +
        _partition_packed_size(&s->partition);

    return s->packed_len;
}

static void *buf_put_dests(struct schema_change_type *s, void *p_buf,
                           void *p_buf_end)
{
    int len;
    struct dest *d;
    int i;

    p_buf = buf_put(&s->dests.count, sizeof(s->dests.count), p_buf, p_buf_end);

    for (i = 0, d = s->dests.top; d; d = d->lnk.next, i++) {
        len = strlen(d->dest);
        p_buf = buf_put(&len, sizeof(len), p_buf, p_buf_end);
        if (len) {
            p_buf = buf_no_net_put(d->dest, len, p_buf, p_buf_end);
        }
    }

    return p_buf;
}

int gbl_sc_protobuf = 1;
int gbl_sc_current_version = SC_VERSION;

int pack_schema_change_protobuf(struct schema_change_type *s, void **packed_sc, size_t *sz)
{
    int32_t sc_version = gbl_sc_current_version;
    CDB2Schemachange sc = CDB2__SCHEMACHANGE__INIT;

    s->tablename_len = strlen(s->tablename) + 1;
    s->fname_len = strlen(s->fname) + 1;
    s->aname_len = strlen(s->aname) + 1;
    s->spname_len = strlen(s->spname) + 1;
    s->newcsc2_len = (s->newcsc2) ? strlen(s->newcsc2) + 1 : 0;
    s->sc_version = sc.version = sc_version;

    sc.kind = s->kind;
    sc.rqid = s->rqid;
    sc.uuid.data = s->uuid;
    sc.uuid.len = sizeof(s->uuid);
    sc.tablename = s->tablename;
    sc.fname = s->fname;
    sc.aname = s->aname;
    sc.avgitemsz = s->avgitemsz;
    sc.newdtastripe = s->newdtastripe;
    sc.blobstripe = s->blobstripe;
    sc.live = s->live;
    sc.newcsc2.data = (uint8_t *)s->newcsc2;
    sc.newcsc2.len = s->newcsc2_len;
    sc.scanmode = s->scanmode;
    sc.force_rebuild = s->force_rebuild;
    sc.force_dta_rebuild = s->force_dta_rebuild;
    sc.force_blob_rebuild = s->force_blob_rebuild;
    sc.force = s->force;
    sc.headers = s->headers;
    sc.header_change = s->header_change;
    sc.compress = s->compress;
    sc.compress_blobs = s->compress_blobs;
    sc.persistent_seq = s->persistent_seq;
    sc.ip_updates = s->ip_updates;
    sc.instant_sc = s->instant_sc;
    sc.preempted = s->preempted;
    sc.use_plan = s->use_plan;
    sc.commit_sleep = s->commit_sleep;
    sc.convert_sleep = s->convert_sleep;
    sc.same_schema = s->same_schema;
    sc.dbnum = s->dbnum;
    sc.flg = s->flg;
    sc.rebuild_index = s->rebuild_index;
    sc.index_to_rebuild = s->index_to_rebuild;
    sc.source_node = s->source_node;

    sc.dests = malloc(sizeof(char *) * s->dests.count);
    if (!sc.dests) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        return -1;
    }
    struct dest *d;
    int i;

    for (i = 0, d = s->dests.top; d; d = d->lnk.next, i++) {
        sc.dests[i] = d->dest;
    }
    sc.n_dests = s->dests.count;
    sc.spname = s->spname;
    sc.lua_func_flags = s->lua_func_flags;
    sc.newtable = s->newtable;
    sc.usedtablevers = s->usedbtablevers;
    sc.qdb_file_ver = s->qdb_file_ver;
    sc.partition_type = s->partition.type;
    switch (s->partition.type) {
        case PARTITION_ADD_TIMED:
        case PARTITION_ADD_MANUAL: {
            sc.has_tpperiod = 1;
            sc.has_tpretention = 1;
            sc.has_tpstart = 1;
            sc.tpperiod = s->partition.u.tpt.period;
            sc.tpretention = s->partition.u.tpt.retention;
            sc.tpstart = s->partition.u.tpt.start;
            break;
        }
        case PARTITION_MERGE: {
            sc.has_tpmergeversion = 1;
            sc.tpmergetable = s->partition.u.mergetable.tablename;
            sc.tpmergeversion = s->partition.u.mergetable.version;
            break;
        }
        case PARTITION_ADD_COL_HASH: {
           sc.has_hashnumpartitions = 1;
           sc.has_hashnumcolumns = 1;
           sc.hashviewname = s->partition.u.hash.viewname;
           sc.hashnumpartitions = s->partition.u.hash.num_partitions;
           sc.n_hashpartitions = s->partition.u.hash.num_partitions;
           sc.hashnumcolumns = s->partition.u.hash.num_columns;
           sc.n_hashcolumns = s->partition.u.hash.num_columns;
           sc.hashcolumns = malloc(sizeof(char *) * s->partition.u.hash.num_columns);
           if (!sc.hashcolumns) {
               logmsg(LOGMSG_ERROR, "%s:%d, malloc failed\n", __func__, __LINE__);
               return -1;
           }
           for (int i=0;i<s->partition.u.hash.num_columns;i++) {
               sc.hashcolumns[i] = s->partition.u.hash.columns[i];
           }
           sc.hashpartitions = malloc(sizeof(char *) * s->partition.u.hash.num_partitions);
           if (!sc.hashpartitions) {
               logmsg(LOGMSG_ERROR, "%s:%d, malloc failed\n", __func__, __LINE__);
               return -1;
           }
           for (int i=0;i<s->partition.u.hash.num_partitions;i++) {
               sc.hashpartitions[i] = s->partition.u.hash.partitions[i];
           }
           sc.hashcreatequery = s->partition.u.hash.createQuery;
           break;
        }
    }
    /* if (sc_version > 3) {
     *    sc.has_optional = 1;
     *    sc.optional = 123;
     * }
     */

    size_t psize = cdb2__schemachange__get_packed_size(&sc);
    if (psize == 0) {
        logmsg(LOGMSG_ERROR, "%s: packed size is invalid\n", __func__);
        free(sc.dests);
        return -1;
    }

    s->packed_len = *sz = (psize + sizeof(uint32_t) + sizeof(uint32_t));
    unsigned char *buf = malloc(*sz);
    if (!buf) {
        logmsg(LOGMSG_ERROR, "%s: malloc failed\n", __func__);
        free(sc.dests);
        return -1;
    }
    int32_t *ibuf = (int32_t *)buf;

    /* -1 (word 0) -> protobuf, word 1 -> size */
    ibuf[0] = htonl(-1);
    ibuf[1] = htonl(psize);
    cdb2__schemachange__pack(&sc, (unsigned char *)&ibuf[2]);
    *packed_sc = buf;
    free(sc.dests);
    return 0;
}

int unpack_schema_change_protobuf(struct schema_change_type *s, void *packed_sc, size_t *plen)
{
    int32_t *ibuf = (int32_t *)packed_sc;
    if (ntohl(ibuf[0]) != -1) {
        logmsg(LOGMSG_ERROR, "%s: invalid packed_sc\n", __func__);
        return -1;
    }
    size_t len = ntohl(ibuf[1]);
    *plen = len + sizeof(uint32_t) + sizeof(uint32_t);
    CDB2Schemachange *sc = cdb2__schemachange__unpack(NULL, len, (const unsigned char *)&ibuf[2]);
    if (!sc) {
        logmsg(LOGMSG_ERROR, "%s: error unpacking sc\n", __func__);
        return -1;
    }

    if (sc->version < SC_MIN_VERSION || sc->version > SC_VERSION) {
        logmsg(LOGMSG_ERROR, "%s: invalid version %d: SC_MIN_VERSION=%d SC_VERSION=%d\n", __func__, sc->version,
               SC_MIN_VERSION, SC_VERSION);
        cdb2__schemachange__free_unpacked(sc, NULL);
        return -1;
    }

    s->sc_version = sc->version;
    s->kind = sc->kind;
    s->rqid = sc->rqid;
    if (sc->uuid.len > 0) {
        memcpy(s->uuid, sc->uuid.data, sizeof(s->uuid));
    } else {
        bzero(s->uuid, sizeof(s->uuid));
    }
    strncpy0(s->tablename, sc->tablename, sizeof(s->tablename));
    s->tablename_len = strlen(s->tablename) + 1;
    strncpy0(s->fname, sc->fname, sizeof(s->fname));
    s->fname_len = strlen(s->fname) + 1;
    strncpy0(s->aname, sc->aname, sizeof(s->aname));
    s->aname_len = strlen(s->aname) + 1;
    s->avgitemsz = sc->avgitemsz;
    s->newdtastripe = sc->newdtastripe;
    s->blobstripe = sc->blobstripe;
    s->live = sc->live;
    if (sc->newcsc2.len) {
        s->newcsc2_len = sc->newcsc2.len;
        s->newcsc2 = malloc(s->newcsc2_len);
        memcpy(s->newcsc2, sc->newcsc2.data, s->newcsc2_len);
    } else {
        s->newcsc2_len = 0;
        s->newcsc2 = NULL;
    }
    s->scanmode = sc->scanmode;
    s->force_rebuild = sc->force_rebuild;
    s->force_dta_rebuild = sc->force_dta_rebuild;
    s->force_blob_rebuild = sc->force_blob_rebuild;
    s->force = sc->force;
    s->headers = sc->headers;
    s->header_change = sc->header_change;
    s->compress = sc->compress;
    s->compress_blobs = sc->compress_blobs;
    s->persistent_seq = sc->persistent_seq;
    s->ip_updates = sc->ip_updates;
    s->instant_sc = sc->instant_sc;
    s->preempted = sc->preempted;
    s->use_plan = sc->use_plan;
    s->commit_sleep = sc->commit_sleep;
    s->convert_sleep = sc->convert_sleep;
    s->same_schema = sc->same_schema;
    s->dbnum = sc->dbnum;
    s->flg = sc->flg;
    s->rebuild_index = sc->rebuild_index;
    s->index_to_rebuild = sc->index_to_rebuild;
    strncpy(s->source_node, sc->source_node, sizeof(s->source_node));
    listc_init(&s->dests, offsetof(struct dest, lnk));

    for (int i = 0; i < sc->n_dests; i++) {
        struct dest *d = malloc(sizeof(struct dest));
        // dest:method:xyz -- drop 'dest:' pfx
        char pfx[] = "dest:";
        char *p = strncmp(sc->dests[i], pfx, strlen(pfx)) == 0 ? sc->dests[i] + strlen(pfx) : sc->dests[i];
        d->dest = strdup(p);
        listc_abl(&s->dests, d);
    }

    strncpy(s->spname, sc->spname, sizeof(s->spname));
    s->spname[sizeof(s->spname) - 1] = '\0';
    s->spname_len = strlen(s->spname) + 1;
    s->lua_func_flags = sc->lua_func_flags;
    strncpy(s->newtable, sc->newtable, sizeof(s->newtable));
    s->newtable[sizeof(s->newtable) - 1] = '\0';
    s->usedbtablevers = sc->usedtablevers;
    s->qdb_file_ver = sc->qdb_file_ver;
    s->partition.type = sc->partition_type;
    switch (sc->partition_type) {
        case PARTITION_ADD_TIMED:
        case PARTITION_ADD_MANUAL: {
            s->partition.u.tpt.period = sc->tpperiod;
            s->partition.u.tpt.retention = sc->tpretention;
            s->partition.u.tpt.start = sc->tpstart;
            break;
        }
        case PARTITION_MERGE: {
            strncpy(s->partition.u.mergetable.tablename, sc->tpmergetable, sizeof(s->partition.u.mergetable.tablename));
            s->partition.u.mergetable.tablename[sizeof(s->partition.u.mergetable.tablename) - 1] = '\0';
            s->partition.u.mergetable.version = sc->tpmergeversion;
            break;
        }
        case PARTITION_ADD_COL_HASH: {
            s->partition.u.hash.viewname = strdup(sc->hashviewname);
            s->partition.u.hash.num_partitions = sc->hashnumpartitions;
            s->partition.u.hash.num_columns = sc->hashnumcolumns;
            s->partition.u.hash.partitions = (char **)malloc(sizeof(char *) * s->partition.u.hash.num_partitions);
            s->partition.u.hash.columns = (char **)malloc(sizeof(char *) * s->partition.u.hash.num_columns);
            for (int i=0;i < sc->n_hashcolumns; i++) {
                s->partition.u.hash.columns[i] = strdup(sc->hashcolumns[i]);
            }

            for(int i=0; i<sc->n_hashpartitions;i++) {
                s->partition.u.hash.partitions[i] = strdup(sc->hashpartitions[i]);
            }
            s->partition.u.hash.createQuery = strdup(sc->hashcreatequery);
            break;
        }
    }

    return 0;
}

void *buf_get_schemachange_protobuf(struct schema_change_type *s, void *p_buf, void *p_buf_end)
{
    size_t plen = 0;
    int rc = unpack_schema_change_protobuf(s, p_buf, &plen);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s: failed to unpack schema change, rc=%d\n", __func__, rc);
        return NULL;
    }
    p_buf += plen;
    if (p_buf > p_buf_end) {
        logmsg(LOGMSG_ERROR, "%s: advanced %ld bytes past end of buffer\n",
            __func__, (uintptr_t) p_buf - (uintptr_t) p_buf_end);
        return NULL;
    }
    return p_buf;
}

void *buf_put_schemachange(struct schema_change_type *s, void *p_buf, void *p_buf_end)
{
    size_t len = 0;
    if (p_buf >= p_buf_end) return NULL;

    p_buf = buf_put(&s->kind, sizeof(s->kind), p_buf, p_buf_end);

    p_buf = buf_put(&s->rqid, sizeof(s->rqid), p_buf, p_buf_end);

    p_buf = buf_no_net_put(&s->uuid, sizeof(s->uuid), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->tablename_len, sizeof(s->tablename_len), p_buf, p_buf_end);

    p_buf = buf_no_net_put(s->tablename, s->tablename_len, p_buf, p_buf_end);

    p_buf = buf_put(&s->fname_len, sizeof(s->fname_len), p_buf, p_buf_end);

    p_buf = buf_no_net_put(s->fname, s->fname_len, p_buf, p_buf_end);

    p_buf = buf_put(&s->aname_len, sizeof(s->aname_len), p_buf, p_buf_end);

    p_buf = buf_no_net_put(s->aname, s->aname_len, p_buf, p_buf_end);

    p_buf = buf_put(&s->avgitemsz, sizeof(s->avgitemsz), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->newdtastripe, sizeof(s->newdtastripe), p_buf, p_buf_end);

    p_buf = buf_put(&s->blobstripe, sizeof(s->blobstripe), p_buf, p_buf_end);

    p_buf = buf_put(&s->live, sizeof(s->live), p_buf, p_buf_end);

    p_buf = buf_put(&s->newcsc2_len, sizeof(s->newcsc2_len), p_buf, p_buf_end);

    if (s->newcsc2_len) {
        p_buf = buf_no_net_put(s->newcsc2, s->newcsc2_len, p_buf, p_buf_end);
    }

    p_buf = buf_put(&s->scanmode, sizeof(s->scanmode), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->delay_commit, sizeof(s->delay_commit), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->force_rebuild, sizeof(s->force_rebuild), p_buf, p_buf_end);

    p_buf = buf_put(&s->force_dta_rebuild, sizeof(s->force_dta_rebuild), p_buf,
                    p_buf_end);

    p_buf = buf_put(&s->force_blob_rebuild, sizeof(s->force_blob_rebuild),
                    p_buf, p_buf_end);

    p_buf = buf_put(&s->force, sizeof(s->force), p_buf, p_buf_end);

    p_buf = buf_put(&s->headers, sizeof(s->headers), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->header_change, sizeof(s->header_change), p_buf, p_buf_end);

    p_buf = buf_put(&s->compress, sizeof(s->compress), p_buf, p_buf_end);

    p_buf = buf_put(&s->compress_blobs, sizeof(s->compress_blobs), p_buf,
                    p_buf_end);

    p_buf = buf_put(&s->persistent_seq, sizeof(s->persistent_seq), p_buf,
                    p_buf_end);

    p_buf = buf_put(&s->ip_updates, sizeof(s->ip_updates), p_buf, p_buf_end);

    p_buf = buf_put(&s->instant_sc, sizeof(s->instant_sc), p_buf, p_buf_end);

    p_buf = buf_put(&s->preempted, sizeof(s->preempted), p_buf, p_buf_end);

    p_buf = buf_put(&s->use_plan, sizeof(s->use_plan), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->commit_sleep, sizeof(s->commit_sleep), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->convert_sleep, sizeof(s->convert_sleep), p_buf, p_buf_end);

    p_buf = buf_put(&s->same_schema, sizeof(s->same_schema), p_buf, p_buf_end);

    p_buf = buf_put(&s->dbnum, sizeof(s->dbnum), p_buf, p_buf_end);

    p_buf = buf_put(&s->flg, sizeof(s->flg), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->rebuild_index, sizeof(s->rebuild_index), p_buf, p_buf_end);

    p_buf = buf_put(&s->index_to_rebuild, sizeof(s->index_to_rebuild), p_buf,
                    p_buf_end);

    p_buf = buf_put(&s->source_node, sizeof(s->source_node), p_buf, p_buf_end);

    p_buf = buf_put_dests(s, p_buf, p_buf_end);

    p_buf = buf_put(&s->spname_len, sizeof(s->spname_len), p_buf, p_buf_end);
    p_buf = buf_no_net_put(s->spname, s->spname_len, p_buf, p_buf_end);
    p_buf = buf_put(&s->lua_func_flags, sizeof(s->lua_func_flags), p_buf, p_buf_end);

    p_buf = buf_no_net_put(s->newtable, sizeof(s->newtable), p_buf, p_buf_end);
    p_buf = buf_put(&s->usedbtablevers, sizeof(s->usedbtablevers), p_buf,
                    p_buf_end);

    p_buf = buf_put(&s->qdb_file_ver, sizeof(s->qdb_file_ver), p_buf, p_buf_end);

    p_buf = buf_put(&s->partition.type, sizeof(s->partition.type), p_buf,
                    p_buf_end);
    switch (s->partition.type) {
    case PARTITION_ADD_TIMED:
    case PARTITION_ADD_MANUAL: {
        p_buf = buf_put(&s->partition.u.tpt.period,
                        sizeof(s->partition.u.tpt.period), p_buf, p_buf_end);
        p_buf = buf_put(&s->partition.u.tpt.retention,
                        sizeof(s->partition.u.tpt.retention), p_buf, p_buf_end);
        p_buf = buf_put(&s->partition.u.tpt.start,
                        sizeof(s->partition.u.tpt.start), p_buf, p_buf_end);
        break;
    }
    case PARTITION_MERGE: {
        p_buf = buf_no_net_put(s->partition.u.mergetable.tablename,
                        sizeof(s->partition.u.mergetable.tablename), p_buf, p_buf_end);
        p_buf = buf_put(&s->partition.u.mergetable.version,
                        sizeof(s->partition.u.mergetable.version), p_buf, p_buf_end);
        break;
    }
	case PARTITION_ADD_COL_HASH: {
        len = strlen(s->partition.u.hash.viewname);
        p_buf = buf_put(&len, sizeof(len), p_buf, p_buf_end);
        p_buf = buf_no_net_put(s->partition.u.hash.viewname, strlen(s->partition.u.hash.viewname), p_buf, p_buf_end);
        p_buf = buf_put(&s->partition.u.hash.num_columns, sizeof(s->partition.u.hash.num_columns), p_buf, p_buf_end);
        for (int i = 0; i < s->partition.u.hash.num_columns; i++) {
            len = strlen(s->partition.u.hash.columns[i]);
            p_buf = buf_put(&len, sizeof(len), p_buf, p_buf_end);
            p_buf =
                buf_no_net_put(s->partition.u.hash.columns[i], strlen(s->partition.u.hash.columns[i]), p_buf, p_buf_end);
        }
        p_buf = buf_put(&s->partition.u.hash.num_partitions, sizeof(s->partition.u.hash.num_partitions), p_buf, p_buf_end);
        for (int i = 0; i < s->partition.u.hash.num_partitions; i++) {
            len = strlen(s->partition.u.hash.partitions[i]);
            p_buf = buf_put(&len, sizeof(len), p_buf, p_buf_end);
            p_buf =
                buf_no_net_put(s->partition.u.hash.partitions[i], strlen(s->partition.u.hash.partitions[i]), p_buf, p_buf_end);
        }
        break;
    }
    }

    return p_buf;
}

static const void *buf_get_dests(struct schema_change_type *s,
                                 const void *p_buf, void *p_buf_end)
{
    listc_init(&s->dests, offsetof(struct dest, lnk));

    int count = 0;
    p_buf = (uint8_t *)buf_get(&count, sizeof(count), p_buf, p_buf_end);

    for (int i = 0; i < count; i++) {
        int w_len = 0, len;
        int no_pfx = 0;
        p_buf = (uint8_t *)buf_get(&w_len, sizeof(w_len), p_buf, p_buf_end);
        char pfx[] = "dest:"; // dest:method:xyz -- drop 'dest:' pfx
        len = w_len;
        if (w_len > strlen(pfx)) {
            p_buf = (void *)buf_no_net_get(pfx, strlen(pfx), p_buf, p_buf_end);
            if (strncmp(pfx, "dest:", 5) != 0) {
                /* "dest:" was dropped already */
                no_pfx = 1;
            }
            len = w_len - strlen(pfx);
        }
        if (len > 0) {
            struct dest *d = malloc(sizeof(struct dest));
            char *pdest;
            if (no_pfx) {
                d->dest = malloc(w_len + 1);
                strcpy(d->dest, pfx);
                pdest = d->dest + strlen(pfx);
                d->dest[w_len] = '\0';
            } else {
                pdest = d->dest = malloc(len + 1);
                d->dest[len] = '\0';
            }
            p_buf = (void *)buf_no_net_get(pdest, len, p_buf, p_buf_end);
            listc_abl(&s->dests, d);
        } else {
            free_dests(s);
            return NULL;
        }
    }
    return p_buf;
}

void *buf_get_schemachange_v1(struct schema_change_type *s, void *p_buf,
                              void *p_buf_end)
{
    int type = 0,          fastinit = 0,   addonly = 0,    fulluprecs = 0,
        partialuprecs = 0, alteronly = 0,  is_trigger = 0, drop_table = 0,
        addsp = 0,         delsp = 0,      defaultsp = 0,  is_sfunc = 0,
        is_afunc = 0,      rename = 0;

    if (p_buf >= p_buf_end) return NULL;

    p_buf = (uint8_t *)buf_get(&s->rqid, sizeof(s->rqid), p_buf, p_buf_end);

    p_buf =
        (uint8_t *)buf_no_net_get(&s->uuid, sizeof(s->uuid), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&type, sizeof(type), p_buf, p_buf_end); /* s->type */

    p_buf = (uint8_t *)buf_get(&s->tablename_len, sizeof(s->tablename_len),
                               p_buf, p_buf_end);
    if (s->tablename_len != strlen((const char *)p_buf) + 1 ||
        s->tablename_len > sizeof(s->tablename)) {
        s->tablename_len = -1;
        return NULL;
    }
    p_buf = (uint8_t *)buf_no_net_get(s->tablename, s->tablename_len, p_buf,
                                      p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->fname_len, sizeof(s->fname_len), p_buf,
                               p_buf_end);
    if (s->fname_len != strlen((const char *)p_buf) + 1 ||
        s->fname_len > sizeof(s->fname)) {
        s->fname_len = -1;
        return NULL;
    }
    p_buf = (uint8_t *)buf_no_net_get(s->fname, s->fname_len, p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->aname_len, sizeof(s->aname_len), p_buf,
                               p_buf_end);
    if (s->aname_len != strlen((const char *)p_buf) + 1 ||
        s->aname_len > sizeof(s->aname)) {
        s->aname_len = -1;
        return NULL;
    }

    p_buf = (uint8_t *)buf_get(s->aname, s->aname_len, p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->avgitemsz, sizeof(s->avgitemsz), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&fastinit, sizeof(fastinit), p_buf, p_buf_end); /* s->fastinit */

    p_buf = (uint8_t *)buf_get(&s->newdtastripe, sizeof(s->newdtastripe), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->blobstripe, sizeof(s->blobstripe), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->live, sizeof(s->live), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&addonly, sizeof(addonly), p_buf, p_buf_end); /* s->addonly */
    p_buf = (uint8_t *)buf_get(&fulluprecs, sizeof(fulluprecs), p_buf, p_buf_end); /* s->fulluprecs */
    p_buf = (uint8_t *)buf_get(&partialuprecs, sizeof(partialuprecs), p_buf, p_buf_end); /* s->partialuprecs */
    p_buf = (uint8_t *)buf_get(&alteronly, sizeof(alteronly), p_buf, p_buf_end); /* s->alteronly */
    p_buf = (uint8_t *)buf_get(&is_trigger, sizeof(is_trigger), p_buf, p_buf_end); /* s->is_trigger */

    p_buf = (uint8_t *)buf_get(&s->newcsc2_len, sizeof(s->newcsc2_len), p_buf,
                               p_buf_end);

    if (s->newcsc2_len) {
        if (s->newcsc2_len != strlen((const char *)p_buf) + 1) {
            s->newcsc2_len = 0;
            return NULL;
        }

        s->newcsc2 = (char *)malloc(s->newcsc2_len);
        if (!s->newcsc2) return NULL;

        p_buf = (uint8_t *)buf_no_net_get(s->newcsc2, s->newcsc2_len, p_buf,
                                          p_buf_end);
    } else
        s->newcsc2 = NULL;

    p_buf =
        (uint8_t *)buf_get(&s->scanmode, sizeof(s->scanmode), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->delay_commit, sizeof(s->delay_commit), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->force_rebuild, sizeof(s->force_rebuild),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->force_dta_rebuild,
                               sizeof(s->force_dta_rebuild), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->force_blob_rebuild,
                               sizeof(s->force_blob_rebuild), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->force, sizeof(s->force), p_buf, p_buf_end);

    p_buf =
        (uint8_t *)buf_get(&s->headers, sizeof(s->headers), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->header_change, sizeof(s->header_change),
                               p_buf, p_buf_end);

    p_buf =
        (uint8_t *)buf_get(&s->compress, sizeof(s->compress), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->compress_blobs, sizeof(s->compress_blobs),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->ip_updates, sizeof(s->ip_updates), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->instant_sc, sizeof(s->instant_sc), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->preempted, sizeof(s->preempted), p_buf,
                               p_buf_end);

    p_buf =
        (uint8_t *)buf_get(&s->use_plan, sizeof(s->use_plan), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->commit_sleep, sizeof(s->commit_sleep), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->convert_sleep, sizeof(s->convert_sleep),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->same_schema, sizeof(s->same_schema), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->dbnum, sizeof(s->dbnum), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->flg, sizeof(s->flg), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->rebuild_index, sizeof(s->rebuild_index),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->index_to_rebuild,
                               sizeof(s->index_to_rebuild), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&drop_table, sizeof(drop_table), p_buf, p_buf_end); /* s->drop_table */

    p_buf =
        (uint8_t *)buf_get(&s->source_node, sizeof(s->source_node), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get_dests(s, p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->spname_len, sizeof(s->spname_len), p_buf,
                               p_buf_end);
    p_buf =
        (uint8_t *)buf_no_net_get(s->spname, s->spname_len, p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&addsp, sizeof(addsp), p_buf, p_buf_end); /* s->addsp */
    p_buf = (uint8_t *)buf_get(&delsp, sizeof(delsp), p_buf, p_buf_end); /* s->delsp */
    p_buf = (uint8_t *)buf_get(&defaultsp, sizeof(defaultsp), p_buf, p_buf_end); /* s->defaultsp */
    p_buf = (uint8_t *)buf_get(&is_sfunc, sizeof(is_sfunc), p_buf, p_buf_end); /* s->is_sfunc */
    p_buf = (uint8_t *)buf_get(&is_afunc, sizeof(is_afunc), p_buf, p_buf_end); /* s->is_afunc */
    p_buf = (uint8_t *)buf_get(&rename, sizeof(rename), p_buf, p_buf_end); /* s->rename */

    p_buf = (uint8_t *)buf_no_net_get(s->newtable, sizeof(s->newtable), p_buf,
                                      p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->usedbtablevers, sizeof(s->usedbtablevers),
                               p_buf, p_buf_end);

    if (fastinit && drop_table)
        s->kind = SC_DROPTABLE;
    else if (fastinit)
        s->kind = SC_TRUNCATETABLE;
    else if (alteronly)
        s->kind = SC_ALTERTABLE;
    else if (addonly)
        s->kind = SC_ADDTABLE;
    else if (rename)
        s->kind = SC_RENAMETABLE;
    else if (fulluprecs)
        s->kind = SC_FULLUPRECS;
    else if (partialuprecs)
        s->kind = SC_PARTIALUPRECS;
    else if (is_trigger && addonly)
        s->kind = SC_ADD_TRIGGER;
    else if (is_trigger && drop_table)
        s->kind = SC_DEL_TRIGGER;
    else if (drop_table)
        s->kind = SC_DROPTABLE;
    else if (addsp)
        s->kind = SC_ADDSP;
    else if (delsp)
        s->kind = SC_DELSP;
    else if (defaultsp)
        s->kind = SC_DEFAULTSP;

    s->sc_version = 1;

    return p_buf;
}

void *buf_get_schemachange_v2(struct schema_change_type *s,
                              void *p_buf, void *p_buf_end)
{
    size_t len = 0;
    if (p_buf >= p_buf_end) return NULL;

    p_buf = (uint8_t *)buf_get(&s->kind, sizeof(s->kind), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->rqid, sizeof(s->rqid), p_buf, p_buf_end);

    p_buf =
        (uint8_t *)buf_no_net_get(&s->uuid, sizeof(s->uuid), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->tablename_len, sizeof(s->tablename_len),
                               p_buf, p_buf_end);
    if (s->tablename_len != strlen((const char *)p_buf) + 1 ||
        s->tablename_len > sizeof(s->tablename)) {
        s->tablename_len = -1;
        return NULL;
    }
    p_buf = (uint8_t *)buf_no_net_get(s->tablename, s->tablename_len, p_buf,
                                      p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->fname_len, sizeof(s->fname_len), p_buf,
                               p_buf_end);
    if (s->fname_len != strlen((const char *)p_buf) + 1 ||
        s->fname_len > sizeof(s->fname)) {
        s->fname_len = -1;
        return NULL;
    }
    p_buf = (uint8_t *)buf_no_net_get(s->fname, s->fname_len, p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->aname_len, sizeof(s->aname_len), p_buf,
                               p_buf_end);
    if (s->aname_len != strlen((const char *)p_buf) + 1 ||
        s->aname_len > sizeof(s->aname)) {
        s->aname_len = -1;
        return NULL;
    }

    p_buf = (uint8_t *)buf_get(s->aname, s->aname_len, p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->avgitemsz, sizeof(s->avgitemsz), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->newdtastripe, sizeof(s->newdtastripe), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->blobstripe, sizeof(s->blobstripe), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->live, sizeof(s->live), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->newcsc2_len, sizeof(s->newcsc2_len), p_buf,
                               p_buf_end);

    if (s->newcsc2_len) {
        if (s->newcsc2_len != strlen((const char *)p_buf) + 1) {
            s->newcsc2_len = 0;
            return NULL;
        }

        s->newcsc2 = (char *)malloc(s->newcsc2_len);
        if (!s->newcsc2) return NULL;

        p_buf = (uint8_t *)buf_no_net_get(s->newcsc2, s->newcsc2_len, p_buf,
                                          p_buf_end);
    } else
        s->newcsc2 = NULL;

    p_buf =
        (uint8_t *)buf_get(&s->scanmode, sizeof(s->scanmode), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->delay_commit, sizeof(s->delay_commit), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->force_rebuild, sizeof(s->force_rebuild),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->force_dta_rebuild,
                               sizeof(s->force_dta_rebuild), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->force_blob_rebuild,
                               sizeof(s->force_blob_rebuild), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->force, sizeof(s->force), p_buf, p_buf_end);

    p_buf =
        (uint8_t *)buf_get(&s->headers, sizeof(s->headers), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->header_change, sizeof(s->header_change),
                               p_buf, p_buf_end);

    p_buf =
        (uint8_t *)buf_get(&s->compress, sizeof(s->compress), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->compress_blobs, sizeof(s->compress_blobs),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->persistent_seq, sizeof(s->persistent_seq),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->ip_updates, sizeof(s->ip_updates), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->instant_sc, sizeof(s->instant_sc), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->preempted, sizeof(s->preempted), p_buf,
                               p_buf_end);

    p_buf =
        (uint8_t *)buf_get(&s->use_plan, sizeof(s->use_plan), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->commit_sleep, sizeof(s->commit_sleep), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->convert_sleep, sizeof(s->convert_sleep),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->same_schema, sizeof(s->same_schema), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->dbnum, sizeof(s->dbnum), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->flg, sizeof(s->flg), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->rebuild_index, sizeof(s->rebuild_index),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->index_to_rebuild,
                               sizeof(s->index_to_rebuild), p_buf, p_buf_end);

    p_buf =
        (uint8_t *)buf_get(&s->source_node, sizeof(s->source_node), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get_dests(s, p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->spname_len, sizeof(s->spname_len), p_buf,
                               p_buf_end);
    p_buf =
        (uint8_t *)buf_no_net_get(s->spname, s->spname_len, p_buf, p_buf_end);
    p_buf =
        (uint8_t *)buf_get(&s->lua_func_flags, sizeof(s->lua_func_flags), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_no_net_get(s->newtable, sizeof(s->newtable), p_buf,
                                      p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->usedbtablevers, sizeof(s->usedbtablevers),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->qdb_file_ver, sizeof(s->qdb_file_ver),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->partition.type, sizeof(s->partition.type),
                               p_buf, p_buf_end);
    switch (s->partition.type) {
    case PARTITION_ADD_TIMED:
    case PARTITION_ADD_MANUAL: {
        p_buf = (uint8_t *)buf_get(&s->partition.u.tpt.period, sizeof(s->partition.u.tpt.period), p_buf, p_buf_end);
        p_buf =
            (uint8_t *)buf_get(&s->partition.u.tpt.retention, sizeof(s->partition.u.tpt.retention), p_buf, p_buf_end);
        p_buf = (uint8_t *)buf_get(&s->partition.u.tpt.start, sizeof(s->partition.u.tpt.start), p_buf, p_buf_end);
        break;
    }
    case PARTITION_MERGE: {
        p_buf = (uint8_t *)buf_no_net_get(s->partition.u.mergetable.tablename,
                                          sizeof(s->partition.u.mergetable.tablename), p_buf, p_buf_end);
        p_buf = (uint8_t *)buf_get(&s->partition.u.mergetable.version, sizeof(s->partition.u.mergetable.version), p_buf,
                                   p_buf_end);
        break;
    }
    case PARTITION_ADD_COL_HASH: {
        p_buf = (uint8_t *)buf_get(&len, sizeof(len), p_buf, p_buf_end);
        logmsg(LOGMSG_USER, "GOT LEN AS %ld for viewname\n", len);
        s->partition.u.hash.viewname = (char *)malloc(sizeof(char) * len + 1);
        p_buf = (uint8_t *)buf_no_net_get(s->partition.u.hash.viewname, len, p_buf,
                                          p_buf_end);
            logmsg(LOGMSG_USER, "GOT VIEWNAME AS %s \n", s->partition.u.hash.viewname);
            s->partition.u.hash.viewname[len]='\0';
        p_buf = (uint8_t *)buf_get(&s->partition.u.hash.num_columns, sizeof(s->partition.u.hash.num_columns), p_buf,
                                   p_buf_end);
        s->partition.u.hash.columns = (char **)malloc(sizeof(char *) * s->partition.u.hash.num_columns);
        for (int i = 0; i < s->partition.u.hash.num_columns; i++) {
            p_buf = (uint8_t *)buf_get(&len, sizeof(len), p_buf, p_buf_end);
            logmsg(LOGMSG_USER, "GOT LEN AS %ld for column %d\n", len, i);
            s->partition.u.hash.columns[i] = (char *)malloc(sizeof(char) * len + 1);
            p_buf = (uint8_t *)buf_no_net_get(s->partition.u.hash.columns[i], len,
                                              p_buf, p_buf_end);
            s->partition.u.hash.columns[i][len]='\0';
            logmsg(LOGMSG_USER, "GOT COLUMN AS %s \n", s->partition.u.hash.columns[i]);
        }
        p_buf =
            (uint8_t *)buf_get(&s->partition.u.hash.num_partitions, sizeof(s->partition.u.hash.num_partitions), p_buf, p_buf_end);
        s->partition.u.hash.partitions = (char **)malloc(sizeof(char *) * s->partition.u.hash.num_partitions);
        for (int i = 0; i < s->partition.u.hash.num_partitions; i++) {
            p_buf = (uint8_t *)buf_get(&len, sizeof(len), p_buf, p_buf_end);
            logmsg(LOGMSG_USER, "GOT LEN AS %ld for partition %d\n", len, i);
            s->partition.u.hash.partitions[i] = (char *)malloc(sizeof(char) * len + 1);
            p_buf = (uint8_t *)buf_no_net_get(s->partition.u.hash.partitions[i], len, p_buf,
                                              p_buf_end);
            s->partition.u.hash.partitions[i][len]='\0';
            logmsg(LOGMSG_USER, "GOT PARTITION AS %s \n", s->partition.u.hash.partitions[i]);
        }
        break;
    }
    }

    s->sc_version = 2;

    return p_buf;
}

#ifdef DEBUG_PROTOBUF_SC
void compare_scs(struct schema_change_type *s1, struct schema_change_type *s2)
{
    int diff_cnt = 0;
    if (s1->kind != s2->kind) {
        logmsg(LOGMSG_ERROR, "kind %d != %d\n", s1->kind, s2->kind);
        diff_cnt++;
    }
    if (s1->rqid != s2->rqid) {
        logmsg(LOGMSG_ERROR, "rqid %lld != %lld\n", s1->rqid, s2->rqid);
        diff_cnt++;
    }
    if (memcmp(&s1->uuid, &s2->uuid, sizeof(uuid_t)) != 0) {
        logmsg(LOGMSG_ERROR, "uuid mismatch\n");
        diff_cnt++;
    }
    if (s1->tablename_len != s2->tablename_len) {
        logmsg(LOGMSG_ERROR, "tablename_len %ld != %ld\n", s1->tablename_len, s2->tablename_len);
        diff_cnt++;
    }
    if (memcmp(s1->tablename, s2->tablename, s1->tablename_len) != 0) {
        logmsg(LOGMSG_ERROR, "tablename mismatch\n");
        diff_cnt++;
    }
    if (s1->fname_len != s2->fname_len) {
        logmsg(LOGMSG_ERROR, "fname_len %ld != %ld\n", s1->fname_len, s2->fname_len);
        diff_cnt++;
    }
    if (memcmp(s1->fname, s2->fname, s1->fname_len) != 0) {
        logmsg(LOGMSG_ERROR, "fname mismatch\n");
        diff_cnt++;
    }
    if (s1->aname_len != s2->aname_len) {
        logmsg(LOGMSG_ERROR, "aname_len %ld != %ld\n", s1->aname_len, s2->aname_len);
        diff_cnt++;
    }
    if (memcmp(s1->aname, s2->aname, s1->aname_len) != 0) {
        logmsg(LOGMSG_ERROR, "aname mismatch\n");
        diff_cnt++;
    }
    if (s1->avgitemsz != s2->avgitemsz) {
        logmsg(LOGMSG_ERROR, "avgitemsz %d != %d\n", s1->avgitemsz, s2->avgitemsz);
        diff_cnt++;
    }
    if (s1->newdtastripe != s2->newdtastripe) {
        logmsg(LOGMSG_ERROR, "newdtastripe %d != %d\n", s1->newdtastripe, s2->newdtastripe);
        diff_cnt++;
    }
    if (s1->blobstripe != s2->blobstripe) {
        logmsg(LOGMSG_ERROR, "blobstripe %d != %d\n", s1->blobstripe, s2->blobstripe);
        diff_cnt++;
    }
    if (s1->live != s2->live) {
        logmsg(LOGMSG_ERROR, "live %d != %d\n", s1->live, s2->live);
        diff_cnt++;
    }
    if (s1->newcsc2_len != s2->newcsc2_len) {
        logmsg(LOGMSG_ERROR, "newcsc2_len %ld != %ld\n", s1->newcsc2_len, s2->newcsc2_len);
        diff_cnt++;
    }
    if (memcmp(s1->newcsc2, s2->newcsc2, s1->newcsc2_len) != 0) {
        logmsg(LOGMSG_ERROR, "newcsc2 mismatch\n");
        diff_cnt++;
    }
    if (s1->scanmode != s2->scanmode) {
        logmsg(LOGMSG_ERROR, "scanmode %d != %d\n", s1->scanmode, s2->scanmode);
        diff_cnt++;
    }
    if (s1->delay_commit != s2->delay_commit) {
        logmsg(LOGMSG_ERROR, "delay_commit %d != %d\n", s1->delay_commit, s2->delay_commit);
        diff_cnt++;
    }
    if (s1->force_rebuild != s2->force_rebuild) {
        logmsg(LOGMSG_ERROR, "force_rebuild %d != %d\n", s1->force_rebuild, s2->force_rebuild);
        diff_cnt++;
    }
    if (s1->force_dta_rebuild != s2->force_dta_rebuild) {
        logmsg(LOGMSG_ERROR, "force_dta_rebuild %d != %d\n", s1->force_dta_rebuild, s2->force_dta_rebuild);
        diff_cnt++;
    }
    if (s1->force_blob_rebuild != s2->force_blob_rebuild) {
        logmsg(LOGMSG_ERROR, "force_blob_rebuild %d != %d\n", s1->force_blob_rebuild, s2->force_blob_rebuild);
        diff_cnt++;
    }
    if (s1->force != s2->force) {
        logmsg(LOGMSG_ERROR, "force %d != %d\n", s1->force, s2->force);
        diff_cnt++;
    }
    if (s1->headers != s2->headers) {
        logmsg(LOGMSG_ERROR, "headers %d != %d\n", s1->headers, s2->headers);
        diff_cnt++;
    }
    if (s1->header_change != s2->header_change) {
        logmsg(LOGMSG_ERROR, "header_change %d != %d\n", s1->header_change, s2->header_change);
        diff_cnt++;
    }
    if (s1->compress != s2->compress) {
        logmsg(LOGMSG_ERROR, "compress %d != %d\n", s1->compress, s2->compress);
        diff_cnt++;
    }
    if (s1->compress_blobs != s2->compress_blobs) {
        logmsg(LOGMSG_ERROR, "compress_blobs %d != %d\n", s1->compress_blobs, s2->compress_blobs);
        diff_cnt++;
    }
    if (s1->persistent_seq != s2->persistent_seq) {
        logmsg(LOGMSG_ERROR, "persistent_seq %d != %d\n", s1->persistent_seq, s2->persistent_seq);
        diff_cnt++;
    }
    if (s1->ip_updates != s2->ip_updates) {
        logmsg(LOGMSG_ERROR, "ip_updates %d != %d\n", s1->ip_updates, s2->ip_updates);
        diff_cnt++;
    }
    if (s1->instant_sc != s2->instant_sc) {
        logmsg(LOGMSG_ERROR, "instant_sc %d != %d\n", s1->instant_sc, s2->instant_sc);
        diff_cnt++;
    }
    if (s1->preempted != s2->preempted) {
        logmsg(LOGMSG_ERROR, "preempted %d != %d\n", s1->preempted, s2->preempted);
        diff_cnt++;
    }
    if (s1->use_plan != s2->use_plan) {
        logmsg(LOGMSG_ERROR, "use_plan %d != %d\n", s1->use_plan, s2->use_plan);
        diff_cnt++;
    }
    if (s1->commit_sleep != s2->commit_sleep) {
        logmsg(LOGMSG_ERROR, "commit_sleep %d != %d\n", s1->commit_sleep, s2->commit_sleep);
        diff_cnt++;
    }
    if (s1->convert_sleep != s2->convert_sleep) {
        logmsg(LOGMSG_ERROR, "convert_sleep %d != %d\n", s1->convert_sleep, s2->convert_sleep);
        diff_cnt++;
    }
    if (s1->same_schema != s2->same_schema) {
        logmsg(LOGMSG_ERROR, "same_schema %d != %d\n", s1->same_schema, s2->same_schema);
        diff_cnt++;
    }
    if (s1->dbnum != s2->dbnum) {
        logmsg(LOGMSG_ERROR, "dbnum %d != %d\n", s1->dbnum, s2->dbnum);
        diff_cnt++;
    }
    if (s1->flg != s2->flg) {
        logmsg(LOGMSG_ERROR, "flg %d != %d\n", s1->flg, s2->flg);
        diff_cnt++;
    }
    if (s1->rebuild_index != s2->rebuild_index) {
        logmsg(LOGMSG_ERROR, "rebuild_index %d != %d\n", s1->rebuild_index, s2->rebuild_index);
        diff_cnt++;
    }
    if (s1->index_to_rebuild != s2->index_to_rebuild) {
        logmsg(LOGMSG_ERROR, "index_to_rebuild %d != %d\n", s1->index_to_rebuild, s2->index_to_rebuild);
        diff_cnt++;
    }
    if (s1->dests.count != s2->dests.count) {
        logmsg(LOGMSG_ERROR, "dests.count %d != %d\n", s1->dests.count, s2->dests.count);
        diff_cnt++;
    }
    if (s1->spname_len != s2->spname_len) {
        logmsg(LOGMSG_ERROR, "spname_len %ld != %ld\n", s1->spname_len, s2->spname_len);
        diff_cnt++;
    }
    if (memcmp(s1->spname, s2->spname, s1->spname_len) != 0) {
        logmsg(LOGMSG_ERROR, "spname mismatch\n");
        diff_cnt++;
    }
    if (s1->lua_func_flags != s2->lua_func_flags) {
        logmsg(LOGMSG_ERROR, "lua_func_flags %d != %d\n", s1->lua_func_flags, s2->lua_func_flags);
        diff_cnt++;
    }
    if (memcmp(s1->newtable, s2->newtable, sizeof(s1->newtable)) != 0) {
        logmsg(LOGMSG_ERROR, "newtable mismatch\n");
        diff_cnt++;
    }
    if (s1->usedbtablevers != s2->usedbtablevers) {
        logmsg(LOGMSG_ERROR, "usedbtablevers %d != %d\n", s1->usedbtablevers, s2->usedbtablevers);
        diff_cnt++;
    }
    if (s1->qdb_file_ver != s2->qdb_file_ver) {
        logmsg(LOGMSG_ERROR, "qdb_file_ver %lld != %lld\n", s1->qdb_file_ver, s2->qdb_file_ver);
        diff_cnt++;
    }
    if (s1->partition.type != s2->partition.type) {
        logmsg(LOGMSG_ERROR, "partition.type %d != %d\n", s1->partition.type, s2->partition.type);
        diff_cnt++;
    }
    if (diff_cnt > 0) {
        logmsg(LOGMSG_ERROR, "schema_change_type mismatch - %d differences\n", diff_cnt);
    }
}
#endif

/*********************************************************************************/

/* Packs a schema_change_type struct into an opaque binary buffer so that it can
 * be stored in the low level meta table and the schema change can be resumed by
 * a different master if necessary.
 * Returns 0 if successful or <0 if failed.
 * packed is set to a pointer to the packed data and is owned by callee if this
 * function succeeds */
int pack_schema_change_type(struct schema_change_type *s, void **packed, size_t *packed_len)
{

    int is_protobuf = gbl_sc_protobuf;

    if (is_protobuf) {
        int rc = pack_schema_change_protobuf(s, packed, packed_len);
        if (rc) {
            logmsg(LOGMSG_ERROR, "pack_schema_change_type: failed to pack schema change, rc %d\n", rc);
            *packed_len = 0;
        }
#ifdef DEBUG_PROTOBUF_SC
        else {
            size_t len = *packed_len;
            struct schema_change_type ck = {0};
            unpack_schema_change_protobuf(&ck, *packed, &len);
            compare_scs(s, &ck);
        }
#endif
        return rc;
    }

    /* compute the length of our buffer */
    *packed_len = schemachange_packed_size(s);

    /* grab memory for our buffer */
    *packed = malloc(*packed_len);
    if (!*packed) {
        logmsg(LOGMSG_ERROR, "pack_schema_change_type: ran out of memory\n");
        *packed_len = 0;
        return -1;
    }

    /* get the beginning */
    uint8_t *p_buf = (uint8_t *)(*packed);

    /* get the end */
    uint8_t *p_buf_end = (p_buf + *packed_len);

    p_buf = buf_put_schemachange(s, p_buf, p_buf_end);
    if (p_buf != (uint8_t *)((char *)(*packed)) + *packed_len) {
        logmsg(LOGMSG_ERROR, "pack_schema_change_type: size of data written did not"
                             " equal precomputed size, this should not happen\n");
        free(*packed);
        *packed = NULL;
        *packed_len = 0;
        return -1;
    }

    return 0; /* success */
}

/* Unpacks an opaque buffer from the low level meta table into a
 * schema_change_type struct and the schema change can be resumed by a different
 * master if necessary.
 * Returns 0 if successful or <0 if failed.
 * If successfull, spoints to a pointer to a newly malloc'd schema_change_type
 * struct that has been populated with the unpacked values, the caller owns this
 * value.
 */
int unpack_schema_change_type(struct schema_change_type *s, void *packed,
                              size_t packed_len)
{
    /* get the beginning */
    uint8_t *p_buf = (uint8_t *)packed, *p_buf_end = p_buf + packed_len;

    /* unpack all the data */
    p_buf = buf_get_schemachange(s, p_buf, p_buf_end);

    if (p_buf == NULL) {

        if (s->tablename_len == -1) { // is set to -1 on error
            logmsg(
                LOGMSG_ERROR,
                "unpack_schema_change_type: length of table in packed"
                " data doesn't match specified length or it is longer then the "
                "array in schema_change_type\n");
            return -1;
        }

        if (s->fname_len == -1) { // is set to -1 on error
            logmsg(
                LOGMSG_ERROR,
                "unpack_schema_change_type: length of fname in packed"
                " data doesn't match specified length or it is longer then the "
                "array in schema_change_type\n");
            return -1;
        }
        if (s->aname_len == -1) { // is set to -1 on error
            logmsg(
                LOGMSG_ERROR,
                "unpack_schema_change_type: length of aname in packed"
                " data doesn't match specified length or it is longer then the "
                "array in schema_change_type\n");
            return -1;
        }
    }

    if (s->newcsc2 && s->newcsc2_len == 0) { // is set to 0 on error
        logmsg(LOGMSG_ERROR, "unpack_schema_change_type: length of newcsc2 in "
                             "packed data doesn't match specified length\n");
        return -1;
    } else if (!s->newcsc2 && s->newcsc2_len > 0) {
        logmsg(LOGMSG_ERROR, "unpack_schema_change_type: ran out of memory\n");
        return -1;
    }

    if (s->flg & SC_MASK_FLG) {
        logmsg(LOGMSG_ERROR, "%s failed: can't resume schemachange - "
                             "it was initiated using newer version of comdb2\n"
                             "please resubmit schemachange.",
               __func__);
        return -1;
    }

    if (s->flg & SC_IDXRBLD) {
        p_buf = (uint8_t *)buf_get(&s->rebuild_index, sizeof(s->rebuild_index),
                                   p_buf, p_buf_end);
        p_buf =
            (uint8_t *)buf_get(&s->index_to_rebuild,
                               sizeof(s->index_to_rebuild), p_buf, p_buf_end);
    }

    return 0; /* success */
}

void print_schemachange_info(struct schema_change_type *s, struct dbtable *db,
                             struct dbtable *newdb)
{
    char *info;
    int olddb_compress;
    int olddb_compress_blobs;
    int olddb_inplace_updates;

    if (newdb->odh && !db->odh)
        info = ">Table will be odh enabled.\n";
    else if (!newdb->odh && db->odh)
        info = ">Table will not support odh.\n";
    else if (newdb->odh && db->odh)
        info = ">Table is already odh enabled.\n";
    else
        info = ">Table does not support odh.\n";

    if (s->dryrun)
        sbuf2printf(s->sb, info);
    else
        sc_printf(s, info + 1);

    if (get_db_compress(db, &olddb_compress)) olddb_compress = 0;
    if (s->compress && !olddb_compress)
        info = ">Table records will be compressed.\n";
    else if (!s->compress && olddb_compress)
        info = ">Table records will be decompressed.\n";
    else if (s->compress && olddb_compress)
        info = ">Table records are compressed.\n";
    else
        info = ">Table records are not compressed.\n";

    if (s->dryrun)
        sbuf2printf(s->sb, info);
    else
        sc_printf(s, info + 1);

    if (get_db_compress_blobs(db, &olddb_compress_blobs))
        olddb_compress_blobs = 0;
    if (s->compress_blobs && !olddb_compress_blobs)
        info = ">Table blobs will be compressed.\n";
    else if (!s->compress_blobs && olddb_compress_blobs)
        info = ">Table blobs will be decompressed.\n";
    else if (s->compress_blobs && olddb_compress_blobs)
        info = ">Table blobs are compressed.\n";
    else
        info = ">Table blobs are not compressed.\n";

    if (s->dryrun)
        sbuf2printf(s->sb, info);
    else
        sc_printf(s, info + 1);

    if (get_db_inplace_updates(db, &olddb_inplace_updates))
        olddb_inplace_updates = 0;
    if (s->ip_updates && !olddb_inplace_updates)
        info = ">Table will support in-place updates.\n";
    else if (!s->ip_updates && olddb_inplace_updates)
        info = ">Table will not support in-place updates.\n";
    else if (s->ip_updates && olddb_inplace_updates)
        info = ">Table already supports in-place updates.\n";
    else
        info = ">Table does not support in-place updates.\n";

    if (s->dryrun)
        sbuf2printf(s->sb, info);
    else
        sc_printf(s, info + 1);

    if (newdb->instant_schema_change && !db->instant_schema_change)
        info = ">Table will support instant schema change.\n";
    else if (!newdb->instant_schema_change && db->instant_schema_change)
        info = ">Table will not support instant schema change.\n";
    else if (newdb->instant_schema_change && db->instant_schema_change)
        info = ">Table already supports instant schema change.\n";
    else
        info = ">Table does not support instant schema change.\n";

    if (s->dryrun)
        sbuf2printf(s->sb, info);
    else
        sc_printf(s, info + 1);

    if (IS_FASTINIT(s))
        sc_printf(s, "fastinit starting on table %s\n", s->tablename);

    switch (s->scanmode) {
    case SCAN_INDEX:
        sc_printf(s, "Schema change running in index scan mode\n");
        break;
    case SCAN_DUMP:
        sc_printf(s, "Schema change running in bulk data dump mode\n");
        break;
    case SCAN_PARALLEL:
        sc_printf(s, "%s schema change running in parallel scan mode\n",
                  (s->live ? "Live" : "Readonly"));
        break;
    case SCAN_PAGEORDER:
        sc_printf(s, "%s schema change running in pageorder scan mode\n",
                  (s->live ? "Live" : "Readonly"));
        break;
    case SCAN_STRIPES:
        sc_printf(s, "Schema change running in stripes scan mode\n");
        break;
    case SCAN_OLDCODE:
        sc_printf(s, "Schema change running in oldcode mode\n");
        break;
    }
}

void set_schemachange_options_tran(struct schema_change_type *s, struct dbtable *db, struct scinfo *scinfo,
                                   tran_type *tran)
{
    int rc;

    /* Get properties from meta */
    rc = get_db_odh_tran(db, &scinfo->olddb_odh, tran);
    if (rc)
        scinfo->olddb_odh = 0;

    rc = get_db_compress_tran(db, &scinfo->olddb_compress, tran);
    if (rc)
        scinfo->olddb_compress = 0;

    rc = get_db_compress_blobs_tran(db, &scinfo->olddb_compress_blobs, tran);
    if (rc)
        scinfo->olddb_compress_blobs = 0;

    rc = get_db_inplace_updates_tran(db, &scinfo->olddb_inplace_updates, tran);
    if (rc)
        scinfo->olddb_inplace_updates = 0;

    rc = get_db_instant_schema_change_tran(db, &scinfo->olddb_instant_sc, tran);
    if (rc)
        scinfo->olddb_instant_sc = 0;

    /* Set schema_change_type properties */
    if (s->headers == -1)
        s->headers = db->odh;

    if (s->compress == -1)
        s->compress = scinfo->olddb_compress;

    if (s->compress_blobs == -1)
        s->compress_blobs = scinfo->olddb_compress_blobs;

    if (s->ip_updates == -1)
        s->ip_updates = scinfo->olddb_inplace_updates;

    if (s->instant_sc == -1)
        s->instant_sc = scinfo->olddb_instant_sc;
}

void set_schemachange_options(struct schema_change_type *s, struct dbtable *db, struct scinfo *scinfo)
{
    set_schemachange_options_tran(s, db, scinfo, NULL);
}

/* helper function to reload csc2 schema */
static int reload_csc2_schema(struct dbtable *db, tran_type *tran, const char *csc2, char *table)
{
    int bdberr;
    void *old_bdb_handle, *new_bdb_handle;
    struct dbtable *newdb;
    int changed = 0;
    int rc;

    struct errstat err = {0};
    newdb = create_new_dbtable(thedb, table, (char *)csc2, db->dbnum, 1, 1, 0, &err);

    if (newdb == NULL) {
        /* shouldn't happen */
        logmsg(LOGMSG_ERROR, "%s (%s:%d)\n", err.errstr, __FILE__, __LINE__);
        backout_schemas(table);
        return 1;
    }

    newdb->dbnum = db->dbnum;
    newdb->meta = db->meta;
    newdb->dtastripe = gbl_dtastripe;

    changed = ondisk_schema_changed(table, newdb, NULL, NULL);
    /* let this fly, which will be ok for fastinit;
       master will catch early non-fastinit cases */
    if (changed < 0 && !DBPAD_OR_DBSTORE_ERR(changed)) {
        if (changed == -2) {
            logmsg(LOGMSG_ERROR, "Error reloading schema!\n");
        }
        /* shouldn't happen */
        backout_schemas(table);
        return 1;
    }

    old_bdb_handle = db->handle;

    logmsg(LOGMSG_DEBUG, "%s isopen %d\n", db->tablename,
           bdb_isopen(db->handle));

    /* the master doesn't tell the replicants to close the db
     * ahead of time */
    rc = bdb_close_only_sc(old_bdb_handle, tran, &bdberr);
    if (rc || bdberr != BDBERR_NOERROR) {
        logmsg(LOGMSG_ERROR, "Error closing old db: %s\n", db->tablename);
        return 1;
    }

    /* reopen db */
    newdb->handle = bdb_open_more_tran(
        table, thedb->basedir, newdb->lrl, newdb->nix,
        (short *)newdb->ix_keylen, newdb->ix_dupes, newdb->ix_recnums,
        newdb->ix_datacopy, newdb->ix_datacopylen, newdb->ix_collattr, newdb->ix_nullsallowed,
        newdb->numblobs + 1, thedb->bdb_env, tran, 0, &bdberr);
    logmsg(LOGMSG_DEBUG, "reload_schema handle %p bdberr %d\n", newdb->handle,
           bdberr);
    if (bdberr != 0 || newdb->handle == NULL)
        return 1;

    new_bdb_handle = newdb->handle;

    rc = bdb_get_csc2_highest(tran, table, &newdb->schema_version, &bdberr);
    if (rc) {
        logmsg(LOGMSG_FATAL, "bdb_get_csc2_highest() failed! PANIC!!\n");
        abort();
    }

    set_odh_options_tran(newdb, tran);
    transfer_db_settings(db, newdb);
    restore_constraint_pointers(db, newdb);

    /* create new csc2 file and modify lrl to reflect that (takes
     * llmeta into account and does the right thing ) */
    rc = write_csc2_file(db, csc2);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "Failed to write table .csc2 file\n");
        return -1;
    }

    free_db_and_replace(db, newdb);
    fix_constraint_pointers(db, newdb);

    rc = bdb_free_and_replace(old_bdb_handle, new_bdb_handle, &bdberr);
    if (rc)
        logmsg(LOGMSG_ERROR, "%s:%d bdb_free rc %d %d\n", __FILE__, __LINE__,
               rc, bdberr);
    db->handle = old_bdb_handle;

    memset(newdb, 0xff, sizeof(struct dbtable));
    free(newdb);

    commit_schemas(table);
    update_dbstore(db);

    free(new_bdb_handle);
    return 0;
}

/* threads must be stopped for this to work
 * if there were changes on disk and we are NOT using low level meta table
 * this expects the table to be bdb_close_only already, if we are using the
 * llmeta this function will do it for us */
int reload_schema(char *table, const char *csc2, tran_type *tran)
{
    struct dbtable *db;
    int rc;
    int bdberr;
    int bthashsz;

    /* regardless of success, the fact that we are getting asked to do this is
     * enough to indicate that any backup taken during this period may be
     * suspect. */
    gbl_sc_commit_count++;

    db = get_dbtable_by_name(table);
    if (db == NULL) {
        logmsg(LOGMSG_ERROR, "reload_schema: invalid table %s\n", table);
        return -1;
    }

    if (csc2) {
        /* genuine schema change. */
        int rc = reload_csc2_schema(db, tran, csc2, table);
        if (rc)
            return rc;
    } else {
        void *old_bdb_handle, *new_bdb_handle;
        old_bdb_handle = db->handle;
        rc = bdb_close_only_sc(old_bdb_handle, tran, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "Error closing old db: %s\n", db->tablename);
            return 1;
        }

        /* TODO free the old bdb handle, right now we just leak memory */
        /* fastinit.  reopen table handle (should be fast), no faffing with
         * schemas */
        /* faffing with schema required. schema can change in fastinit */
        new_bdb_handle = bdb_open_more_tran(
            table, thedb->basedir, db->lrl, db->nix, (short *)db->ix_keylen,
            db->ix_dupes, db->ix_recnums, db->ix_datacopy, db->ix_datacopylen, db->ix_collattr,
            db->ix_nullsallowed, db->numblobs + 1, thedb->bdb_env, tran, 0,
            &bdberr);
        logmsg(LOGMSG_DEBUG,
               "reload_schema (fastinit case) handle %p bdberr %d\n",
               db->handle, bdberr);
        if (new_bdb_handle || bdberr != 0)
            return 1;

        rc = bdb_free_and_replace(old_bdb_handle, new_bdb_handle, &bdberr);
        if (rc || bdberr != 0) {
            logmsg(LOGMSG_ERROR, "%s:%d rc %d bdberr %d\n", __FILE__, __LINE__,
                   rc, bdberr);
            return 1;
        }
        db->handle = old_bdb_handle;
        set_odh_options_tran(db, tran);
        free(new_bdb_handle);
    }

    if (get_db_bthash_tran(db, &bthashsz, tran) != 0) bthashsz = 0;

    if (bthashsz) {
        logmsg(LOGMSG_INFO,
               "Rebuilding bthash for table %s, size %dkb per stripe\n",
               db->tablename, bthashsz);
        bdb_handle_dbp_add_hash(db->handle, bthashsz);
    }

    return 0;
}

void set_sc_flgs(struct schema_change_type *s)
{
    s->flg = 0;
    s->flg |= SC_CHK_PGSZ;
    s->flg |= SC_IDXRBLD;
}

int schema_change_headers(struct schema_change_type *s)
{
    return s->header_change;
}

struct schema_change_type *clone_schemachange_type(struct schema_change_type *sc)
{
    int is_protobuf = gbl_sc_protobuf, rc;
    struct schema_change_type *newsc;
    size_t sc_len = 0;
    uint8_t *p_buf = NULL, *p_buf_end = NULL, *buf = NULL;

    if (is_protobuf) {
        rc = pack_schema_change_protobuf(sc, (void **)&buf, &sc_len);
        if (rc != 0) {
            return NULL;
        }
    } else {
        sc_len = schemachange_packed_size(sc);
        p_buf = buf = calloc(1, sc_len);
        if (!p_buf)
            return NULL;

        p_buf_end = p_buf + sc_len;

        p_buf = buf_put_schemachange(sc, p_buf, p_buf_end);
        if (!p_buf) {
            free(buf);
            return NULL;
        }
    }

    newsc = new_schemachange_type();
    if (!newsc) {
        free(buf);
        return NULL;
    }

    if (is_protobuf) {
        size_t plen;
        rc = unpack_schema_change_protobuf(newsc, buf, &plen);
        if (rc != 0) {
            free_schema_change_type(newsc);
            free(buf);
            return NULL;
        }
        p_buf = p_buf_end = buf + plen;
    } else {
        p_buf = buf;
        p_buf = buf_get_schemachange(newsc, p_buf, p_buf_end);
        if (!p_buf) {
            free_schema_change_type(newsc);
            free(buf);
            return NULL;
        }
    }

    newsc->nothrevent = sc->nothrevent;
    newsc->pagesize = sc->pagesize;
    newsc->retry_bad_genids = sc->retry_bad_genids;
    newsc->dryrun = sc->dryrun;
    newsc->use_new_genids = newsc->use_new_genids;
    newsc->finalize = sc->finalize;
    newsc->is_osql = sc->is_osql;
    newsc->timepartition_name = sc->timepartition_name;
    newsc->timepartition_version = sc->timepartition_version;
    newsc->partition = sc->partition;
    newsc->usedbtablevers = sc->usedbtablevers;

    if (!p_buf) {
        free_schema_change_type(newsc);
        free(buf);
        return NULL;
    }

    free(buf);
    return newsc;
}
