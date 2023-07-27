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
    sc->add_state = SC_NOT_ADD;
    /* default values: no change */
    sc->headers = -1;
    sc->compress = -1;
    sc->compress_blobs = -1;
    sc->ip_updates = -1;
    sc->instant_sc = -1;
    sc->persistent_seq = -1;
    sc->dbnum = -1; /* -1 = not changing, anything else = set value */
    sc->original_master_node[0] = 0;
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
    if (s->newcsc2) {
        free(s->newcsc2);
        s->newcsc2 = NULL;
    }
    if (s->sc_convert_done) {
        free(s->sc_convert_done);
        s->sc_convert_done = NULL;
    }

    free_dests(s);
    Pthread_mutex_destroy(&s->mtx);
    Pthread_mutex_destroy(&s->livesc_mtx);

    Pthread_cond_destroy(&s->condStart);
    Pthread_mutex_destroy(&s->mtxStart);

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
    switch (p->type) {
    case PARTITION_NONE:
    case PARTITION_REMOVE:
        return sizeof(p->type);
    case PARTITION_ADD_TIMED:
    case PARTITION_ADD_MANUAL:
        return sizeof(p->type) + sizeof(p->u.tpt.period) +
               sizeof(p->u.tpt.retention) + sizeof(p->u.tpt.start);
    case PARTITION_MERGE:
        return sizeof(p->type) + sizeof(p->u.mergetable.tablename) +
               sizeof(p->u.mergetable.version);
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
        sizeof(s->index_to_rebuild) + sizeof(s->original_master_node) +
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

void *buf_put_schemachange(struct schema_change_type *s, void *p_buf, void *p_buf_end)
{

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

    p_buf = buf_put(&s->original_master_node, sizeof(s->original_master_node),
                    p_buf, p_buf_end);

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
            s->newcsc2_len = -1;
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
        (uint8_t *)buf_get(&s->original_master_node,
                           sizeof(s->original_master_node), p_buf, p_buf_end);

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

    return p_buf;
}

void *buf_get_schemachange_v2(struct schema_change_type *s,
                              void *p_buf, void *p_buf_end)
{

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
            s->newcsc2_len = -1;
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
        (uint8_t *)buf_get(&s->original_master_node,
                           sizeof(s->original_master_node), p_buf, p_buf_end);

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
        p_buf = (uint8_t *)buf_get(&s->partition.u.tpt.period,
                                   sizeof(s->partition.u.tpt.period), p_buf,
                                   p_buf_end);
        p_buf = (uint8_t *)buf_get(&s->partition.u.tpt.retention,
                                   sizeof(s->partition.u.tpt.retention), p_buf,
                                   p_buf_end);
        p_buf = (uint8_t *)buf_get(&s->partition.u.tpt.start,
                                   sizeof(s->partition.u.tpt.start), p_buf,
                                   p_buf_end);
        break;
    }
    case PARTITION_MERGE: {
        p_buf = (uint8_t *)buf_no_net_get(s->partition.u.mergetable.tablename,
                                          sizeof(s->partition.u.mergetable.tablename),
                                          p_buf, p_buf_end);
        p_buf = (uint8_t *)buf_get(&s->partition.u.mergetable.version,
                                   sizeof(s->partition.u.mergetable.version), p_buf,
                                   p_buf_end);
        break;
    }
    }

    return p_buf;
}

/*********************************************************************************/

/* Packs a schema_change_type struct into an opaque binary buffer so that it can
 * be stored in the low level meta table and the schema change can be resumed by
 * a different master if necessary.
 * Returns 0 if successful or <0 if failed.
 * packed is set to a pointer to the packed data and is owned by callee if this
 * function succeeds */
int pack_schema_change_type(struct schema_change_type *s, void **packed,
                            size_t *packed_len)
{

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

    /* pack all the data */
    p_buf = buf_put_schemachange(s, p_buf, p_buf_end);

    if (p_buf != (uint8_t *)((char *)(*packed)) + *packed_len) {
        logmsg(LOGMSG_ERROR,
               "pack_schema_change_type: size of data written did not"
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

    if (s->newcsc2 && s->newcsc2_len == -1) { // is set to -1 on error
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

void set_schemachange_options_tran(struct schema_change_type *s, struct dbtable *db,
                                   struct scinfo *scinfo, tran_type *tran)
{
    int rc;

    /* Get properties from meta */
    rc = get_db_odh_tran(db, &scinfo->olddb_odh, tran);
    if (rc) scinfo->olddb_odh = 0;

    rc = get_db_compress_tran(db, &scinfo->olddb_compress, tran);
    if (rc) scinfo->olddb_compress = 0;

    rc = get_db_compress_blobs_tran(db, &scinfo->olddb_compress_blobs, tran);
    if (rc) scinfo->olddb_compress_blobs = 0;

    rc = get_db_inplace_updates_tran(db, &scinfo->olddb_inplace_updates, tran);
    if (rc) scinfo->olddb_inplace_updates = 0;

    rc = get_db_instant_schema_change_tran(db, &scinfo->olddb_instant_sc, tran);
    if (rc) scinfo->olddb_instant_sc = 0;

    /* Set schema_change_type properties */
    if (s->headers == -1) s->headers = db->odh;

    if (s->compress == -1) s->compress = scinfo->olddb_compress;

    if (s->compress_blobs == -1)
        s->compress_blobs = scinfo->olddb_compress_blobs;

    if (s->ip_updates == -1) s->ip_updates = scinfo->olddb_inplace_updates;

    if (s->instant_sc == -1) s->instant_sc = scinfo->olddb_instant_sc;
}

void set_schemachange_options(struct schema_change_type *s, struct dbtable *db,
                              struct scinfo *scinfo)
{
    set_schemachange_options_tran(s, db, scinfo, NULL);
}

/* helper function to reload csc2 schema */
static int reload_csc2_schema(struct dbtable *db, tran_type *tran,
                              const char *csc2, char *table)
{
    int bdberr;
    void *old_bdb_handle, *new_bdb_handle;
    struct dbtable *newdb;
    int changed = 0;
    int rc;

    int foundix = getdbidxbyname_ll(table);
    if (foundix == -1) {
        logmsg(LOGMSG_FATAL, "Couldn't find table <%s>\n", table);
        exit(1);
    }

    struct errstat err = {0};
    newdb = create_new_dbtable(thedb, table, (char *)csc2, db->dbnum, foundix,
                               1, 1, 0, &err);

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
    fix_lrl_ixlen_tran(tran);
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

struct schema_change_type *
clone_schemachange_type(struct schema_change_type *sc)
{
    struct schema_change_type *newsc;
    size_t sc_len = schemachange_packed_size(sc);
    uint8_t *p_buf, *p_buf_end, *buf;

    p_buf = buf = calloc(1, sc_len);
    if (!p_buf)
        return NULL;

    p_buf_end = p_buf + sc_len;

    p_buf = buf_put_schemachange(sc, p_buf, p_buf_end);
    if (!p_buf) {
        free(buf);
        return NULL;
    }

    newsc = new_schemachange_type();
    if (!newsc) {
        free(buf);
        return NULL;
    }

    p_buf = buf;
    p_buf = buf_get_schemachange(newsc, p_buf, p_buf_end);

    newsc->nothrevent = sc->nothrevent;
    newsc->pagesize = sc->pagesize;
    newsc->retry_bad_genids = sc->retry_bad_genids;
    newsc->dryrun = sc->dryrun;
    newsc->use_new_genids = newsc->use_new_genids;
    newsc->finalize = sc->finalize;
    newsc->finalize_only = sc->finalize_only;
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
