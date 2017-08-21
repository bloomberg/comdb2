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

#include "schemachange.h"
#include "sc_struct.h"
#include "logmsg.h"
#include "sc_csc2.h"
#include "sc_schema.h"

/************ SCHEMACHANGE TO BUF UTILITY FUNCTIONS
 * *****************************/

struct schema_change_type *init_schemachange_type(struct schema_change_type *sc)
{
    memset(sc, 0, sizeof(struct schema_change_type));
    sc->tran = NULL;
    sc->type = DBTYPE_TAGGED_TABLE;
    sc->sb = NULL;
    sc->newcsc2 = NULL;
    sc->nothrevent = 0;
    sc->onstack = 0;
    sc->live = 0;
    sc->drop_table = 0;
    sc->use_plan = 0;
    /* default values: no change */
    sc->headers = -1;
    sc->compress = -1;
    sc->compress_blobs = -1;
    sc->ip_updates = -1;
    sc->instant_sc = -1;
    sc->dbnum = -1; /* -1 = not changing, anything else = set value */
    sc->original_master_node[0] = 0;
    listc_init(&sc->dests, offsetof(struct dest, lnk));
    if (pthread_mutex_init(&sc->mtx, NULL)) {
        free_schema_change_type(sc);
        return NULL;
    }
    return sc;
}

struct schema_change_type *new_schemachange_type()
{
    struct schema_change_type *sc =
        (struct schema_change_type *)malloc(sizeof(struct schema_change_type));
    if (sc != NULL) sc = init_schemachange_type(sc);

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
    if (s) {
        if (s->newcsc2) {
            free(s->newcsc2);
            s->newcsc2 = NULL;
        }

        free_dests(s);
        pthread_mutex_destroy(&s->mtx);

        if (s->sb && s->must_close_sb) close_appsock(s->sb);
        if (!s->onstack) {
            free(s);
            s = NULL;
        }
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

size_t schemachange_packed_size(struct schema_change_type *s)
{
    s->table_len = strlen(s->table) + 1;
    s->fname_len = strlen(s->fname) + 1;
    s->aname_len = strlen(s->aname) + 1;
    s->spname_len = strlen(s->spname) + 1;
    s->newcsc2_len = (s->newcsc2) ? strlen(s->newcsc2) + 1 : 0;

    s->packed_len =
        sizeof(s->rqid) + sizeof(s->uuid) + sizeof(s->type) +
        sizeof(s->table_len) + s->table_len + sizeof(s->fname_len) +
        s->fname_len + sizeof(s->aname_len) + s->aname_len +
        sizeof(s->avgitemsz) + sizeof(s->fastinit) + sizeof(s->newdtastripe) +
        sizeof(s->blobstripe) + sizeof(s->live) + sizeof(s->addonly) +
        sizeof(s->fulluprecs) + sizeof(s->partialuprecs) +
        sizeof(s->alteronly) + sizeof(s->is_trigger) + sizeof(s->newcsc2_len) +
        s->newcsc2_len + sizeof(s->scanmode) + sizeof(s->delay_commit) +
        sizeof(s->force_rebuild) + sizeof(s->force_dta_rebuild) +
        sizeof(s->force_blob_rebuild) + sizeof(s->force) + sizeof(s->headers) +
        sizeof(s->header_change) + sizeof(s->compress) +
        sizeof(s->compress_blobs) + sizeof(s->ip_updates) +
        sizeof(s->instant_sc) + sizeof(s->doom) + sizeof(s->use_plan) +
        sizeof(s->commit_sleep) + sizeof(s->convert_sleep) +
        sizeof(s->same_schema) + sizeof(s->dbnum) + sizeof(s->flg) +
        sizeof(s->rebuild_index) + sizeof(s->index_to_rebuild) +
        sizeof(s->drop_table) + sizeof(s->original_master_node) +
        dests_field_packed_size(s) + sizeof(s->spname_len) + s->spname_len +
        sizeof(s->addsp) + sizeof(s->delsp) + sizeof(s->defaultsp) +
        sizeof(s->is_sfunc) + sizeof(s->is_afunc) + sizeof(s->addseq) +
        sizeof(s->dropseq) + sizeof(s->alterseq) + sizeof(s->seq_min_val) +
        sizeof(s->seq_max_val) + sizeof(s->seq_increment) +
        sizeof(s->seq_cycle) + sizeof(s->seq_start_val) +
        sizeof(s->seq_chunk_size) + sizeof(s->seq_restart_val) +
        sizeof(s->seq_modified);

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

void *buf_put_schemachange(struct schema_change_type *s, void *p_buf,
                           void *p_buf_end)
{

    if (p_buf >= p_buf_end) return NULL;

    p_buf = buf_put(&s->rqid, sizeof(s->rqid), p_buf, p_buf_end);

    p_buf = buf_no_net_put(&s->uuid, sizeof(s->uuid), p_buf, p_buf_end);

    p_buf = buf_put(&s->type, sizeof(s->type), p_buf, p_buf_end);

    p_buf = buf_put(&s->table_len, sizeof(s->table_len), p_buf, p_buf_end);

    p_buf = buf_no_net_put(s->table, s->table_len, p_buf, p_buf_end);

    p_buf = buf_put(&s->fname_len, sizeof(s->fname_len), p_buf, p_buf_end);

    p_buf = buf_no_net_put(s->fname, s->fname_len, p_buf, p_buf_end);

    p_buf = buf_put(&s->aname_len, sizeof(s->aname_len), p_buf, p_buf_end);

    p_buf = buf_no_net_put(s->aname, s->aname_len, p_buf, p_buf_end);

    p_buf = buf_put(&s->avgitemsz, sizeof(s->avgitemsz), p_buf, p_buf_end);

    p_buf = buf_put(&s->fastinit, sizeof(s->fastinit), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->newdtastripe, sizeof(s->newdtastripe), p_buf, p_buf_end);

    p_buf = buf_put(&s->blobstripe, sizeof(s->blobstripe), p_buf, p_buf_end);

    p_buf = buf_put(&s->live, sizeof(s->live), p_buf, p_buf_end);

    p_buf = buf_put(&s->addonly, sizeof(s->addonly), p_buf, p_buf_end);

    p_buf = buf_put(&s->fulluprecs, sizeof(s->fulluprecs), p_buf, p_buf_end);

    p_buf =
        buf_put(&s->partialuprecs, sizeof(s->partialuprecs), p_buf, p_buf_end);

    p_buf = buf_put(&s->alteronly, sizeof(s->alteronly), p_buf, p_buf_end);

    p_buf = buf_put(&s->is_trigger, sizeof(s->is_trigger), p_buf, p_buf_end);

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

    p_buf = buf_put(&s->ip_updates, sizeof(s->ip_updates), p_buf, p_buf_end);

    p_buf = buf_put(&s->instant_sc, sizeof(s->instant_sc), p_buf, p_buf_end);

    p_buf = buf_put(&s->doom, sizeof(s->doom), p_buf, p_buf_end);

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

    p_buf = buf_put(&s->drop_table, sizeof(s->drop_table), p_buf, p_buf_end);

    p_buf = buf_put_dests(s, p_buf, p_buf_end);

    p_buf = buf_put(&s->spname_len, sizeof(s->spname_len), p_buf, p_buf_end);
    p_buf = buf_no_net_put(s->spname, s->spname_len, p_buf, p_buf_end);
    p_buf = buf_put(&s->addsp, sizeof(s->addsp), p_buf, p_buf_end);
    p_buf = buf_put(&s->delsp, sizeof(s->delsp), p_buf, p_buf_end);
    p_buf = buf_put(&s->defaultsp, sizeof(s->defaultsp), p_buf, p_buf_end);
    p_buf = buf_put(&s->is_sfunc, sizeof(s->is_sfunc), p_buf, p_buf_end);
    p_buf = buf_put(&s->is_afunc, sizeof(s->is_afunc), p_buf, p_buf_end);

    p_buf = buf_put(&s->addseq, sizeof(s->addseq), p_buf, p_buf_end);
    p_buf = buf_put(&s->dropseq, sizeof(s->dropseq), p_buf, p_buf_end);
    p_buf = buf_put(&s->alterseq, sizeof(s->alterseq), p_buf, p_buf_end);

    p_buf = buf_put(&s->seq_min_val, sizeof(s->seq_min_val), p_buf, p_buf_end);
    p_buf = buf_put(&s->seq_max_val, sizeof(s->seq_max_val), p_buf, p_buf_end);
    p_buf =
        buf_put(&s->seq_increment, sizeof(s->seq_increment), p_buf, p_buf_end);
    p_buf = buf_put(&s->seq_cycle, sizeof(s->seq_cycle), p_buf, p_buf_end);
    p_buf =
        buf_put(&s->seq_start_val, sizeof(s->seq_start_val), p_buf, p_buf_end);
    p_buf = buf_put(&s->seq_chunk_size, sizeof(s->seq_chunk_size), p_buf,
                    p_buf_end);
    p_buf = buf_put(&s->seq_restart_val, sizeof(s->seq_restart_val), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&s->seq_modified, sizeof(s->seq_modified), p_buf, p_buf_end);

    return p_buf;
}

static const void *buf_get_dests(struct schema_change_type *s,
                                 const void *p_buf, void *p_buf_end)
{
    listc_init(&s->dests, offsetof(struct dest, lnk));

    int count;
    p_buf = (uint8_t *)buf_get(&count, sizeof(count), p_buf, p_buf_end);

    for (int i = 0; i < count; i++) {
        int w_len;
        p_buf = (uint8_t *)buf_get(&w_len, sizeof(w_len), p_buf, p_buf_end);
        char pfx[] = "dest"; // dest:method:xyz -- drop 'dest:' pfx
        if (w_len > sizeof(pfx)) {
            p_buf = (void *)buf_no_net_get(pfx, sizeof(pfx), p_buf, p_buf_end);

            int len = w_len - sizeof(pfx);
            struct dest *d = malloc(sizeof(struct dest));
            d->dest = malloc(len + 1);
            p_buf = (void *)buf_no_net_get(d->dest, len, p_buf, p_buf_end);
            d->dest[len] = '\0';
            listc_abl(&s->dests, d);
        } else {
            free_dests(s);
            return NULL;
        }
    }
    return p_buf;
}

void *buf_get_schemachange(struct schema_change_type *s, void *p_buf,
                           void *p_buf_end)
{

    if (p_buf >= p_buf_end) return NULL;

    bzero(s, sizeof(struct schema_change_type));

    p_buf = (uint8_t *)buf_get(&s->rqid, sizeof(s->rqid), p_buf, p_buf_end);

    p_buf =
        (uint8_t *)buf_no_net_get(&s->uuid, sizeof(s->uuid), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->type, sizeof(s->type), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->table_len, sizeof(s->table_len), p_buf,
                               p_buf_end);
    if (s->table_len != strlen((const char *)p_buf) + 1 ||
        s->table_len > sizeof(s->table)) {
        s->table_len = -1;
        return NULL;
    }
    p_buf = (uint8_t *)buf_no_net_get(s->table, s->table_len, p_buf, p_buf_end);

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

    p_buf =
        (uint8_t *)buf_get(&s->fastinit, sizeof(s->fastinit), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->newdtastripe, sizeof(s->newdtastripe), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->blobstripe, sizeof(s->blobstripe), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->live, sizeof(s->live), p_buf, p_buf_end);

    p_buf =
        (uint8_t *)buf_get(&s->addonly, sizeof(s->addonly), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->fulluprecs, sizeof(s->fulluprecs), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->partialuprecs, sizeof(s->partialuprecs),
                               p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->alteronly, sizeof(s->alteronly), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->is_trigger, sizeof(s->is_trigger), p_buf,
                               p_buf_end);

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

    p_buf = (uint8_t *)buf_get(&s->doom, sizeof(s->doom), p_buf, p_buf_end);

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

    p_buf = (uint8_t *)buf_get(&s->drop_table, sizeof(s->drop_table), p_buf,
                               p_buf_end);

    p_buf = (uint8_t *)buf_get_dests(s, p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->spname_len, sizeof(s->spname_len), p_buf,
                               p_buf_end);
    p_buf =
        (uint8_t *)buf_no_net_get(s->spname, s->spname_len, p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->addsp, sizeof(s->addsp), p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->delsp, sizeof(s->delsp), p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->defaultsp, sizeof(s->defaultsp), p_buf,
                               p_buf_end);
    p_buf =
        (uint8_t *)buf_get(&s->is_sfunc, sizeof(s->is_sfunc), p_buf, p_buf_end);
    p_buf =
        (uint8_t *)buf_get(&s->is_afunc, sizeof(s->is_afunc), p_buf, p_buf_end);

    p_buf = (uint8_t *)buf_get(&s->addseq, sizeof(s->addseq), p_buf, p_buf_end);
    p_buf =
        (uint8_t *)buf_get(&s->dropseq, sizeof(s->dropseq), p_buf, p_buf_end);
    p_buf =
        (uint8_t *)buf_get(&s->alterseq, sizeof(s->alterseq), p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->seq_min_val, sizeof(s->seq_min_val), p_buf,
                               p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->seq_max_val, sizeof(s->seq_max_val), p_buf,
                               p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->seq_increment, sizeof(s->seq_increment),
                               p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->seq_cycle, sizeof(s->seq_cycle), p_buf,
                               p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->seq_start_val, sizeof(s->seq_start_val),
                               p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->seq_chunk_size, sizeof(s->seq_chunk_size),
                               p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->seq_restart_val, sizeof(s->seq_restart_val),
                               p_buf, p_buf_end);
    p_buf = (uint8_t *)buf_get(&s->seq_modified, sizeof(s->seq_modified), p_buf,
                               p_buf_end);

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

    bzero(s, sizeof(struct schema_change_type));

    /* unpack all the data */
    p_buf = buf_get_schemachange(s, p_buf, p_buf_end);

    if (p_buf == NULL) {

        if (s->table_len < 0) {
            logmsg(
                LOGMSG_ERROR,
                "unpack_schema_change_type: length of table in packed"
                " data doesn't match specified length or it is longer then the "
                "array in schema_change_type\n");
            return -1;
        }

        if (s->fname_len < 0) {
            logmsg(
                LOGMSG_ERROR,
                "unpack_schema_change_type: length of fname in packed"
                " data doesn't match specified length or it is longer then the "
                "array in schema_change_type\n");
            return -1;
        }
        if (s->aname_len < 0) {
            logmsg(
                LOGMSG_ERROR,
                "unpack_schema_change_type: length of aname in packed"
                " data doesn't match specified length or it is longer then the "
                "array in schema_change_type\n");
            return -1;
        }
    }

    if (s->newcsc2 && s->newcsc2_len < 0) {
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
        info = ">Table supports in-place updates.\n";
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

    if (s->fastinit) sc_printf(s, "fastinit starting on table %s\n", s->table);

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
    }
}

void set_schemachange_options_tran(struct schema_change_type *s, struct dbtable *db,
                                   struct scinfo *scinfo, tran_type *tran)
{
    int rc;

    /* Get properties from meta */
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
    return set_schemachange_options_tran(s, db, scinfo, NULL);
}

int print_status(struct schema_change_type *s)
{
    struct dbtable *db = NULL;
    struct scinfo scinfo = {0};
    int bthashsz;

    db = get_dbtable_by_name(s->table);
    if (db == NULL) {
        sbuf2printf(s->sb, ">Table %s does not exists\n", s->table);
        sbuf2printf(s->sb, "FAILED\n");
        return -1;
    }

    /* Get info */
    set_schemachange_options(s, db, &scinfo);

    if (get_db_bthash(db, &bthashsz) != 0) bthashsz = 0;

    /* Print ondisk header information. */
    if (db->odh)
        sbuf2printf(s->sb, "> Table %s uses ondisk-headers.\n", s->table);
    else
        sbuf2printf(s->sb, "> Table %s does not use ondisk-headers.\n",
                    s->table);

    /* Print compressed table information */
    if (scinfo.olddb_compress)
        sbuf2printf(s->sb, "> Table %s is compressed.\n", s->table);
    else
        sbuf2printf(s->sb, "> Table %s is not compressed.\n", s->table);

    /* Print compressed blob information */
    if (scinfo.olddb_compress_blobs)
        sbuf2printf(s->sb, "> Table %s has compressed blobs.\n", s->table);
    else
        sbuf2printf(s->sb, "> Table %s has uncompressed blobs\n", s->table);

    /* Print inplace update information */
    if (scinfo.olddb_inplace_updates)
        sbuf2printf(s->sb, "> Table %s supports in-place updates.\n", s->table);
    else
        sbuf2printf(s->sb, "> Table %s does not support in-place updates.\n",
                    s->table);

    if (scinfo.olddb_instant_sc)
        sbuf2printf(s->sb, "> Table %s supports instant schema-change.\n",
                    s->table);
    else
        sbuf2printf(s->sb,
                    "> Table %s does not support instant schema-change.\n",
                    s->table);

    /* Print bthash information */
    if (bthashsz)
        sbuf2printf(s->sb, "> Table %s has bthash with size %dkb per stripe.\n",
                    s->table, bthashsz);
    else
        sbuf2printf(s->sb,
                    "> Table %s does not support datafile file bthash.\n",
                    s->table);

    sbuf2printf(s->sb, "SUCCESS\n");
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
    int foundix = -1;
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
        struct dbtable *newdb;
        int changed = 0;

        rc = dyns_load_schema_string((char *)csc2, thedb->envname, table);
        if (rc != 0) {
            return rc;
        }

        foundix = getdbidxbyname(table);
        if (foundix == -1) {
            logmsg(LOGMSG_FATAL, "Couldn't find table <%s>\n", table);
            exit(1);
        }

        /* TODO remove NULL arg; pre-llmeta holdover */
        newdb = newdb_from_schema(thedb, table, NULL, db->dbnum, foundix, 0);
        if (newdb == NULL) {
            /* shouldn't happen */
            backout_schemas(table);
            return 1;
        }
        newdb->dbnum = db->dbnum;
        if (add_cmacc_stmt(newdb, 1) != 0) {
            /* can happen if new schema has no .DEFAULT tag but needs one */
            backout_schemas(table);
            return 1;
        }
        newdb->meta = db->meta;
        newdb->dtastripe = gbl_dtastripe;

        changed = ondisk_schema_changed(table, newdb, NULL, NULL);
        /* let this fly, which will be ok for fastinit;
           master will catch early non-fastinit cases */
        if (changed < 0 && changed != SC_BAD_NEW_FIELD) {
            if (changed == -2) {
                logmsg(LOGMSG_ERROR, "Error reloading schema!\n");
            }
            /* shouldn't happen */
            backout_schemas(table);
            return 1;
        }

        logmsg(LOGMSG_DEBUG, "%s isopen %d\n", db->dbname,
               bdb_isopen(db->handle));

        /* the master doesn't tell the replicants to close the db
         * ahead of time */
        rc = bdb_close_only(db->handle, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "Error closing old db: %s\n", db->dbname);
            return 1;
        }

        /* reopen db */
        newdb->handle = bdb_open_more_tran(
            table, thedb->basedir, newdb->lrl, newdb->nix, newdb->ix_keylen,
            newdb->ix_dupes, newdb->ix_recnums, newdb->ix_datacopy,
            newdb->ix_collattr, newdb->ix_nullsallowed, newdb->numblobs + 1,
            thedb->bdb_env, tran, &bdberr);
        logmsg(LOGMSG_DEBUG, "reload_schema handle %08x bdberr %d\n",
               newdb->handle, bdberr);
        if (bdberr != 0 || newdb->handle == NULL) return 1;

        rc = bdb_get_csc2_highest(tran, table, &newdb->version, &bdberr);
        if (rc) {
            logmsg(LOGMSG_FATAL, "bdb_get_csc2_highest() failed! PANIC!!\n");
            /* FIXME */
            exit(1);
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

        rc = bdb_free_and_replace(db->handle, newdb->handle, &bdberr);
        if (rc)
            logmsg(LOGMSG_ERROR, "%s:%d bdb_free rc %d %d\n", __FILE__,
                   __LINE__, rc, bdberr);

        bdb_state_type *oldhandle = db->handle;

        free_db_and_replace(db, newdb);
        fix_constraint_pointers(db, newdb);
        memset(newdb, 0xff, sizeof(struct dbtable));
        free(newdb);

        commit_schemas(table);
        fix_lrl_ixlen_tran(tran);
        update_dbstore(db);

        free(oldhandle);
    } else {
        rc = bdb_close_only(db->handle, &bdberr);
        if (rc || bdberr != BDBERR_NOERROR) {
            logmsg(LOGMSG_ERROR, "Error closing old db: %s\n", db->dbname);
            return 1;
        }

        /* TODO free the old bdb handle, right now we just leak memory */
        /* fastinit.  reopen table handle (should be fast), no faffing with
         * schemas */
        /* faffing with schema required. schema can change in fastinit */
        db->handle = bdb_open_more_tran(
            table, thedb->basedir, db->lrl, db->nix, db->ix_keylen,
            db->ix_dupes, db->ix_recnums, db->ix_datacopy, db->ix_collattr,
            db->ix_nullsallowed, db->numblobs + 1, thedb->bdb_env, tran,
            &bdberr);
        logmsg(LOGMSG_DEBUG,
               "reload_schema (fastinit case) handle %08x bdberr %d\n",
               db->handle, bdberr);
        if (!db->handle || bdberr != 0) return 1;

        set_odh_options_tran(db, tran);
    }

    if (get_db_bthash_tran(db, &bthashsz, tran) != 0) bthashsz = 0;

    if (bthashsz) {
        logmsg(LOGMSG_INFO,
               "Rebuilding bthash for table %s, size %dkb per stripe\n",
               db->dbname, bthashsz);
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
