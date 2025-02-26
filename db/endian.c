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

#include <sys/types.h>
#include <inttypes.h>

#include <compile_time_assert.h>
#include <comdb2_dbinfo.h>

#include "endian_core.h"
#include "block_internal.h"
#include "comdb2.h"

/* constants */

/* TODO start move to comdb2_dbinfo.h */
enum {
    DB_INFO2_REQ_NO_HDR_LEN = 4 + (4 * DBINFO2_MAXRQS) + (4 * 64),
    DB_INFO2_STRUCT_LEN = REQ_HDR_LEN + DB_INFO2_REQ_NO_HDR_LEN
};
BB_COMPILE_TIME_ASSERT(db_info2_req_size,
                       sizeof(struct db_info2_req) == DB_INFO2_STRUCT_LEN);

enum {
    DB_INFO2_RESP_NO_HDR_NO_DATA_LEN = 4 + 4 + 4 + 4 + 4 + 4 + 4 + (4 * 6),
    DB_INFO2_RESP_STRUCT_LEN =
        REQ_HDR_LEN + DB_INFO2_RESP_NO_HDR_NO_DATA_LEN + DBINFOPC_STRUCT_LEN
};
BB_COMPILE_TIME_ASSERT(db_info2_resp_size, sizeof(struct db_info2_resp) ==
                                               DB_INFO2_RESP_STRUCT_LEN);

enum { KEYINFO_LEN = 4 + 4 + 4 + 4 + (1 * 64) };
BB_COMPILE_TIME_ASSERT(keyinfo_size, sizeof(struct keyinfo) == KEYINFO_LEN);

enum {
    TBLINFO_NO_KEYINFO_LEN = (1 * 32) + 4 + 4 + 4 + 4 + (4 * 8),
    TBLINFO_STRUCT_LEN = TBLINFO_NO_KEYINFO_LEN + KEYINFO_LEN
};
BB_COMPILE_TIME_ASSERT(tblinfo_size,
                       sizeof(struct tblinfo) == TBLINFO_STRUCT_LEN);

enum {
    DB_INFO2_TBL_INFO_NO_TBLINFO_LEN = 4 + (4 * 8),
    DB_INFO2_TBL_INFO_STRUCT_LEN =
        DB_INFO2_TBL_INFO_NO_TBLINFO_LEN + TBLINFO_STRUCT_LEN
};
BB_COMPILE_TIME_ASSERT(db_info2_tbl_info_size,
                       sizeof(struct db_info2_tbl_info) ==
                           DB_INFO2_TBL_INFO_STRUCT_LEN);

enum { NODE_INFO_LEN = (1 * 64) + 4 };
BB_COMPILE_TIME_ASSERT(node_info_size,
                       sizeof(struct node_info) == NODE_INFO_LEN);

enum {
    DB_INFO2_CLUSTER_INFO_NO_NODE_INFO_LEN =
        4 + 4 + 4 + 4 + 4 + 4 + 4 + (4 * 7),
    DB_INFO2_CLUSTER_INFO_STRUCT_LEN =
        DB_INFO2_CLUSTER_INFO_NO_NODE_INFO_LEN + NODE_INFO_LEN
};
BB_COMPILE_TIME_ASSERT(db_info2_cluster_info_size,
                       sizeof(struct db_info2_cluster_info) ==
                           DB_INFO2_CLUSTER_INFO_STRUCT_LEN);

enum {
    DB_INFO2_STATS_LEN = 4 + 4 + 4 + 4 + 8 + 8 + 4 + 2 + 2 + 2 + 2 + 2 + 2,

    /* in this case we don't want to pack/unpack the tail padding because it
     * is at the end of the req, there is no further data packed after it */
    DB_INFO2_STATS_PAD_TAIL = (2 * 4)
};
BB_COMPILE_TIME_ASSERT(db_info2_stats_size,
                       sizeof(struct db_info2_stats) ==
                           (DB_INFO2_STATS_LEN + DB_INFO2_STATS_PAD_TAIL));

enum { DB_INFO2_UNKNOWN_LEN = 4 };
BB_COMPILE_TIME_ASSERT(db_info2_unknown_size,
                       sizeof(struct db_info2_unknown) == DB_INFO2_UNKNOWN_LEN);

uint8_t *req_hdr_put(const struct req_hdr *p_req_hdr, uint8_t *p_buf,
                     const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || REQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    /* TODO do we really want to write to the first 7 bytes? */
    p_buf =
        buf_put(&(p_req_hdr->ver1), sizeof(p_req_hdr->ver1), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_req_hdr->ver2), sizeof(p_req_hdr->ver2), p_buf, p_buf_end);
    p_buf = buf_put(&(p_req_hdr->luxref), sizeof(p_req_hdr->luxref), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_req_hdr->opcode), sizeof(p_req_hdr->opcode), p_buf,
                    p_buf_end);

    return p_buf;
}

const uint8_t *req_hdr_get(struct req_hdr *p_req_hdr, const uint8_t *p_buf,
                           const uint8_t *p_buf_end, int flags)
{
    if (p_buf_end < p_buf || REQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    /* comdbg routines store the opcode as an integer in the second word of the
     * buffer. If that's the origin of the request, read it "correctly" */
    if (flags & COMDBG_FLAG_FROM_LE) {
        p_req_hdr->opcode = flags & 0xffff;

        // ver1, ver2
        p_buf = buf_skip(6, (void*) p_buf, p_buf_end);
        // luxref
        p_buf = buf_get(&p_req_hdr->luxref, sizeof(p_req_hdr->luxref), p_buf, p_buf_end);
        // opcode (skip, see above)
        p_buf = buf_skip(1, (void*) p_buf, p_buf_end);
    }
    else {
        p_buf =
            buf_get(&(p_req_hdr->ver1), sizeof(p_req_hdr->ver1), p_buf, p_buf_end);
        p_buf =
            buf_get(&(p_req_hdr->ver2), sizeof(p_req_hdr->ver2), p_buf, p_buf_end);
        p_buf = buf_get(&(p_req_hdr->luxref), sizeof(p_req_hdr->luxref), p_buf,
                p_buf_end);
        p_buf = buf_get(&(p_req_hdr->opcode), sizeof(p_req_hdr->opcode), p_buf,
                p_buf_end);
    }

    return p_buf;
}

uint8_t *coherent_req_put(const struct coherent_req *p_coherent_req,
                          uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || COHERENT_REQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_coherent_req->coherent),
                    sizeof(p_coherent_req->coherent), p_buf, p_buf_end);
    p_buf = buf_put(&(p_coherent_req->fromdb), sizeof(p_coherent_req->fromdb),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *coherent_req_get(struct coherent_req *p_coherent_req,
                                const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || COHERENT_REQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_coherent_req->coherent),
                    sizeof(p_coherent_req->coherent), p_buf, p_buf_end);
    p_buf = buf_get(&(p_coherent_req->fromdb), sizeof(p_coherent_req->fromdb),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *make_node_incoherent_req_put(
    const struct make_node_incoherent_req *p_make_node_incoherent_req,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || MAKE_NODE_INCOHERENT_REQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_make_node_incoherent_req->node),
                    sizeof(p_make_node_incoherent_req->node), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *make_node_incoherent_req_get(
    struct make_node_incoherent_req *p_make_node_incoherent_req,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || MAKE_NODE_INCOHERENT_REQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_make_node_incoherent_req->node),
                    sizeof(p_make_node_incoherent_req->node), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *tran_req_put(const struct tran_req *p_tran_req, uint8_t *p_buf,
                      const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || TRAN_REQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_tran_req->id), sizeof(p_tran_req->id), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *tran_req_get(struct tran_req *p_tran_req, const uint8_t *p_buf,
                            const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || TRAN_REQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_tran_req->id), sizeof(p_tran_req->id), p_buf, p_buf_end);

    return p_buf;
}

/* TODO move all these dbinfo funcs to todbinfo.c */

/* doesn't put the 8 byte proccm hdr at the beginning */
uint8_t *db_info2_req_no_hdr_put(const struct db_info2_req *p_db_info2_req,
                                 uint8_t *p_buf, const uint8_t *p_buf_end)
{
    unsigned i;

    if (p_buf_end < p_buf || DB_INFO2_REQ_NO_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_db_info2_req->ninforq), sizeof(p_db_info2_req->ninforq),
                    p_buf, p_buf_end);

    for (i = 0; i < p_db_info2_req->ninforq && i < DBINFO2_MAXRQS; ++i)
        p_buf = buf_put(&(p_db_info2_req->inforqs[i]),
                        sizeof(p_db_info2_req->inforqs[i]), p_buf, p_buf_end);
    p_buf += sizeof(p_db_info2_req->inforqs[0]) * (DBINFO2_MAXRQS - i);

    p_buf = buf_put(&(p_db_info2_req->fluff), sizeof(p_db_info2_req->fluff),
                    p_buf, p_buf_end);

    return p_buf;
}

/* doesn't get the 8 byte proccm hdr at the beginning */
const uint8_t *db_info2_req_no_hdr_get(struct db_info2_req *p_db_info2_req,
                                       const uint8_t *p_buf,
                                       const uint8_t *p_buf_end)
{
    unsigned i;

    if (p_buf_end < p_buf || DB_INFO2_REQ_NO_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_db_info2_req->ninforq), sizeof(p_db_info2_req->ninforq),
                    p_buf, p_buf_end);

    for (i = 0; i < p_db_info2_req->ninforq && i < DBINFO2_MAXRQS; ++i)
        p_buf = buf_get(&(p_db_info2_req->inforqs[i]),
                        sizeof(p_db_info2_req->inforqs[i]), p_buf, p_buf_end);
    p_buf += sizeof(p_db_info2_req->inforqs[0]) * (DBINFO2_MAXRQS - i);

    p_buf = buf_get(&(p_db_info2_req->fluff), sizeof(p_db_info2_req->fluff),
                    p_buf, p_buf_end);

    return p_buf;
}

/* doesn't put data, only the hdr */
uint8_t *dbinfopc_no_data_put(const struct dbinfopc *p_dbinfopc, uint8_t *p_buf,
                              const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DBINFOPC_NO_DATA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_dbinfopc->type), sizeof(p_dbinfopc->type), p_buf,
                    p_buf_end);
    p_buf =
        buf_put(&(p_dbinfopc->len), sizeof(p_dbinfopc->len), p_buf, p_buf_end);

    return p_buf;
}

/* doesn't get data, only the hdr */
const uint8_t *dbinfopc_no_data_get(struct dbinfopc *p_dbinfopc,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DBINFOPC_NO_DATA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_dbinfopc->type), sizeof(p_dbinfopc->type), p_buf,
                    p_buf_end);
    p_buf =
        buf_get(&(p_dbinfopc->len), sizeof(p_dbinfopc->len), p_buf, p_buf_end);

    return p_buf;
}

/* doesn't put the 8 byte proccm hdr at the beginning or the dbinfopc data at
 * the end */
uint8_t *
db_info2_resp_no_hdr_no_data_put(const struct db_info2_resp *p_db_info2_resp,
                                 uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        DB_INFO2_RESP_NO_HDR_NO_DATA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_db_info2_resp->comdb2resp),
                    sizeof(p_db_info2_resp->comdb2resp), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_resp->lrl), sizeof(p_db_info2_resp->lrl),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_resp->nix), sizeof(p_db_info2_resp->nix),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_resp->shmflags),
                    sizeof(p_db_info2_resp->shmflags), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_resp->ninfo), sizeof(p_db_info2_resp->ninfo),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_resp->moreflags),
                    sizeof(p_db_info2_resp->moreflags), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_resp->physrep_src_dbnum),
                    sizeof(p_db_info2_resp->physrep_src_dbnum), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_resp->fluff), sizeof(p_db_info2_resp->fluff),
                    p_buf, p_buf_end);

    return p_buf;
}

/* doesn't get the 8 byte proccm hdr at the beginning or the dbinfopc data at
 * the end */
const uint8_t *
db_info2_resp_no_hdr_no_data_get(struct db_info2_resp *p_db_info2_resp,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        DB_INFO2_RESP_NO_HDR_NO_DATA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_db_info2_resp->comdb2resp),
                    sizeof(p_db_info2_resp->comdb2resp), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_resp->lrl), sizeof(p_db_info2_resp->lrl),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_resp->nix), sizeof(p_db_info2_resp->nix),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_resp->shmflags),
                    sizeof(p_db_info2_resp->shmflags), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_resp->ninfo), sizeof(p_db_info2_resp->ninfo),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_resp->fluff), sizeof(p_db_info2_resp->fluff),
                    p_buf, p_buf_end);

    return p_buf;
}

/* doesn't put the tblinfo data at the end, only the hdr */
uint8_t *db_info2_tbl_info_no_tblinfo_put(
    const struct db_info2_tbl_info *p_db_info2_tbl_info, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        DB_INFO2_TBL_INFO_NO_TBLINFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_db_info2_tbl_info->ntables),
                    sizeof(p_db_info2_tbl_info->ntables), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_tbl_info->fluff),
                    sizeof(p_db_info2_tbl_info->fluff), p_buf, p_buf_end);

    return p_buf;
}

/* doesn't get the tblinfo data at the end, only the hdr */
const uint8_t *
db_info2_tbl_info_no_tblinfo_get(struct db_info2_tbl_info *p_db_info2_tbl_info,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        DB_INFO2_TBL_INFO_NO_TBLINFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_db_info2_tbl_info->ntables),
                    sizeof(p_db_info2_tbl_info->ntables), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_tbl_info->fluff),
                    sizeof(p_db_info2_tbl_info->fluff), p_buf, p_buf_end);

    return p_buf;
}

/* doesn't put the keyinfo data at the end */
uint8_t *tblinfo_no_keyinfo_put(const struct tblinfo *p_tblinfo, uint8_t *p_buf,
                                const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || TBLINFO_NO_KEYINFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_tblinfo->table_name),
                           sizeof(p_tblinfo->table_name), p_buf, p_buf_end);
    p_buf = buf_put(&(p_tblinfo->table_lux), sizeof(p_tblinfo->table_lux),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_tblinfo->table_dbnum), sizeof(p_tblinfo->table_dbnum),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_tblinfo->table_lrl), sizeof(p_tblinfo->table_lrl),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_tblinfo->table_nix), sizeof(p_tblinfo->table_nix),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_tblinfo->fluff), sizeof(p_tblinfo->fluff), p_buf,
                    p_buf_end);

    return p_buf;
}

/* doesn't get the keyinfo data at the end */
const uint8_t *tblinfo_no_keyinfo_get(struct tblinfo *p_tblinfo,
                                      const uint8_t *p_buf,
                                      const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || TBLINFO_NO_KEYINFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_tblinfo->table_name),
                           sizeof(p_tblinfo->table_name), p_buf, p_buf_end);
    p_buf = buf_get(&(p_tblinfo->table_lux), sizeof(p_tblinfo->table_lux),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_tblinfo->table_dbnum), sizeof(p_tblinfo->table_dbnum),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_tblinfo->table_lrl), sizeof(p_tblinfo->table_lrl),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_tblinfo->table_nix), sizeof(p_tblinfo->table_nix),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_tblinfo->fluff), sizeof(p_tblinfo->fluff), p_buf,
                    p_buf_end);

    return p_buf;
}

uint8_t *keyinfo_put(const struct keyinfo *p_keyinfo, uint8_t *p_buf,
                     const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || KEYINFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_keyinfo->keylen), sizeof(p_keyinfo->keylen), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_keyinfo->dupes), sizeof(p_keyinfo->dupes), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_keyinfo->recnums), sizeof(p_keyinfo->recnums), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_keyinfo->primary), sizeof(p_keyinfo->primary), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_keyinfo->keytag), sizeof(p_keyinfo->keytag),
                           p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *keyinfo_get(struct keyinfo *p_keyinfo, const uint8_t *p_buf,
                           const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || KEYINFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_keyinfo->keylen), sizeof(p_keyinfo->keylen), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_keyinfo->dupes), sizeof(p_keyinfo->dupes), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_keyinfo->recnums), sizeof(p_keyinfo->recnums), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_keyinfo->primary), sizeof(p_keyinfo->primary), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_keyinfo->keytag), sizeof(p_keyinfo->keytag),
                           p_buf, p_buf_end);

    return p_buf;
}

/* doesn't put the tblinfo data at the end, only the hdr */
uint8_t *db_info2_cluster_info_no_node_info_put(
    const struct db_info2_cluster_info *p_db_info2_cluster_info, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        DB_INFO2_CLUSTER_INFO_NO_NODE_INFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_db_info2_cluster_info->master),
                    sizeof(p_db_info2_cluster_info->master), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_db_info2_cluster_info->syncmode),
                sizeof(p_db_info2_cluster_info->syncmode), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_cluster_info->formkey_enabled),
                    sizeof(p_db_info2_cluster_info->formkey_enabled), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_db_info2_cluster_info->retry),
                    sizeof(p_db_info2_cluster_info->retry), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_cluster_info->port),
                    sizeof(p_db_info2_cluster_info->port), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_db_info2_cluster_info->nsiblings),
                sizeof(p_db_info2_cluster_info->nsiblings), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_db_info2_cluster_info->incoherent),
                sizeof(p_db_info2_cluster_info->incoherent), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_cluster_info->fluff),
                    sizeof(p_db_info2_cluster_info->fluff), p_buf, p_buf_end);

    return p_buf;
}

/* doesn't get the tblinfo data at the end, only the hdr */
const uint8_t *db_info2_cluster_info_no_node_info_get(
    struct db_info2_cluster_info *p_db_info2_cluster_info, const uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        DB_INFO2_CLUSTER_INFO_NO_NODE_INFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_db_info2_cluster_info->master),
                    sizeof(p_db_info2_cluster_info->master), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_db_info2_cluster_info->syncmode),
                sizeof(p_db_info2_cluster_info->syncmode), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_cluster_info->formkey_enabled),
                    sizeof(p_db_info2_cluster_info->formkey_enabled), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_db_info2_cluster_info->retry),
                    sizeof(p_db_info2_cluster_info->retry), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_cluster_info->port),
                    sizeof(p_db_info2_cluster_info->port), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_db_info2_cluster_info->nsiblings),
                sizeof(p_db_info2_cluster_info->nsiblings), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_db_info2_cluster_info->incoherent),
                sizeof(p_db_info2_cluster_info->incoherent), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_cluster_info->fluff),
                    sizeof(p_db_info2_cluster_info->fluff), p_buf, p_buf_end);

    return p_buf;
}

/* doesn't put the tblinfo data at the end, only the hdr */
uint8_t *node_info_put(const struct node_info *p_node_info, uint8_t *p_buf,
                       const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || NODE_INFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_node_info->hostname),
                           sizeof(p_node_info->hostname), p_buf, p_buf_end);
    p_buf = buf_put(&(p_node_info->node), sizeof(p_node_info->node), p_buf,
                    p_buf_end);

    return p_buf;
}

/* doesn't get the tblinfo data at the end, only the hdr */
const uint8_t *node_info_get(struct node_info *p_node_info,
                             const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || NODE_INFO_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_node_info->hostname),
                           sizeof(p_node_info->hostname), p_buf, p_buf_end);
    p_buf = buf_get(&(p_node_info->node), sizeof(p_node_info->node), p_buf,
                    p_buf_end);

    return p_buf;
}

uint8_t *db_info2_stats_put(const struct db_info2_stats *p_db_info2_stats,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DB_INFO2_STATS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_db_info2_stats->starttime),
                    sizeof(p_db_info2_stats->starttime), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->n_qtraps),
                    sizeof(p_db_info2_stats->n_qtraps), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->n_appsock),
                    sizeof(p_db_info2_stats->n_appsock), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->n_sql), sizeof(p_db_info2_stats->n_sql),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->cache_hits),
                    sizeof(p_db_info2_stats->cache_hits), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->cache_misses),
                    sizeof(p_db_info2_stats->cache_misses), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->n_retries),
                    sizeof(p_db_info2_stats->n_retries), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->q_max_conf),
                    sizeof(p_db_info2_stats->q_max_conf), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->q_mean_reached),
                    sizeof(p_db_info2_stats->q_mean_reached), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->q_max_reached),
                    sizeof(p_db_info2_stats->q_max_reached), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->thr_max),
                    sizeof(p_db_info2_stats->thr_max), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->thr_maxwr),
                    sizeof(p_db_info2_stats->thr_maxwr), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->thr_cur),
                    sizeof(p_db_info2_stats->thr_cur), p_buf, p_buf_end);
    p_buf = buf_put(&(p_db_info2_stats->padding0),
                    sizeof(p_db_info2_stats->padding0), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *db_info2_iostats_put(const struct db_info2_iostats *p_iostats,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DB_INFO2_STATS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_iostats->sz), sizeof(p_iostats->sz), p_buf, p_buf_end);
    p_buf = buf_skip(sizeof(int), p_buf, p_buf_end);

    p_buf = buf_put(&(p_iostats->page_reads), sizeof(p_iostats->page_reads),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_iostats->page_writes), sizeof(p_iostats->page_writes),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_iostats->cachesz), sizeof(p_iostats->cachesz), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_iostats->cachereqs), sizeof(p_iostats->cachereqs),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_iostats->cachehits), sizeof(p_iostats->cachehits),
                    p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_iostats->reqs), sizeof(p_iostats->reqs), p_buf, p_buf_end);
    p_buf = buf_put(&(p_iostats->sql_statements),
                    sizeof(p_iostats->sql_statements), p_buf, p_buf_end);
    p_buf = buf_put(&(p_iostats->sql_steps), sizeof(p_iostats->sql_steps),
                    p_buf, p_buf_end);

    return p_buf;
}
const uint8_t *db_info2_stats_get(struct db_info2_stats *p_db_info2_stats,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DB_INFO2_STATS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_db_info2_stats->starttime),
                    sizeof(p_db_info2_stats->starttime), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->n_qtraps),
                    sizeof(p_db_info2_stats->n_qtraps), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->n_appsock),
                    sizeof(p_db_info2_stats->n_appsock), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->n_sql), sizeof(p_db_info2_stats->n_sql),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->cache_hits),
                    sizeof(p_db_info2_stats->cache_hits), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->cache_misses),
                    sizeof(p_db_info2_stats->cache_misses), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->n_retries),
                    sizeof(p_db_info2_stats->n_retries), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->q_max_conf),
                    sizeof(p_db_info2_stats->q_max_conf), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->q_mean_reached),
                    sizeof(p_db_info2_stats->q_mean_reached), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->q_max_reached),
                    sizeof(p_db_info2_stats->q_max_reached), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->thr_max),
                    sizeof(p_db_info2_stats->thr_max), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->thr_maxwr),
                    sizeof(p_db_info2_stats->thr_maxwr), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->thr_cur),
                    sizeof(p_db_info2_stats->thr_cur), p_buf, p_buf_end);
    p_buf = buf_get(&(p_db_info2_stats->padding0),
                    sizeof(p_db_info2_stats->padding0), p_buf, p_buf_end);

    return p_buf;
}

/* doesn't put the 8 byte proccm hdr at the beginning or the config line data at
 * the end */
uint8_t *db_proxy_config_rsp_no_hdr_no_lines_put(
    const struct db_proxy_config_rsp *p_db_proxy_config_rsp, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        DB_PROXY_CONFIG_RSP_NO_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_db_proxy_config_rsp->nlines),
                    sizeof(p_db_proxy_config_rsp->nlines), p_buf, p_buf_end);
    /* off doesn't actually store anything, it's just used as a handle to find
     * where the config lines should be put (I guess you could think of it as
     * storing the length of the first line) */
    /*p_buf = buf_put(&(p_db_proxy_config_rsp->off), */
    /*sizeof(p_db_proxy_config_rsp->off), p_buf, p_buf_end);*/

    return p_buf;
}

/* doesn't get the 8 byte proccm hdr at the beginning or the config line data at
 * the end */
const uint8_t *db_proxy_config_rsp_no_hdr_no_lines_get(
    struct db_proxy_config_rsp *p_db_proxy_config_rsp, const uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        DB_PROXY_CONFIG_RSP_NO_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_db_proxy_config_rsp->nlines),
                    sizeof(p_db_proxy_config_rsp->nlines), p_buf, p_buf_end);
    /* off doesn't actually store anything, it's just used as a handle to find
     * where the config lines should be put (I guess you could think of it as
     * storing the length of the first line) */
    /*p_buf = buf_get(&(p_db_proxy_config_rsp->off), */
    /*sizeof(p_db_proxy_config_rsp->off), p_buf, p_buf_end);*/

    return p_buf;
}

uint8_t *db_info2_unknown_put(const struct db_info2_unknown *p_db_info2_unknown,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DB_INFO2_UNKNOWN_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_db_info2_unknown->opcode),
                    sizeof(p_db_info2_unknown->opcode), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *db_info2_unknown_get(struct db_info2_unknown *p_db_info2_unknown,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || DB_INFO2_UNKNOWN_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_db_info2_unknown->opcode),
                    sizeof(p_db_info2_unknown->opcode), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *server_datetime_get(server_datetime_t *p_server_datetime,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_DATETIME_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_server_datetime->flag), sizeof(p_server_datetime->flag),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_server_datetime->sec), sizeof(p_server_datetime->sec),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_server_datetime->msec), sizeof(p_server_datetime->msec),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *server_datetimeus_get(server_datetimeus_t *p_server_datetimeus,
                                     const uint8_t *p_buf,
                                     const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_DATETIMEUS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_server_datetimeus->flag),
                    sizeof(p_server_datetimeus->flag), p_buf, p_buf_end);
    p_buf = buf_get(&(p_server_datetimeus->sec),
                    sizeof(p_server_datetimeus->sec), p_buf, p_buf_end);
    p_buf = buf_get(&(p_server_datetimeus->usec),
                    sizeof(p_server_datetimeus->usec), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *server_intv_ym_get(server_intv_ym_t *p_server_intv_ym,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_INTV_YM_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_server_intv_ym->flag), sizeof(p_server_intv_ym->flag),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_server_intv_ym->months),
                    sizeof(p_server_intv_ym->months), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *server_intv_ds_get(server_intv_ds_t *p_server_intv_ds,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_INTV_DS_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_server_intv_ds->flag), sizeof(p_server_intv_ds->flag),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_server_intv_ds->sec), sizeof(p_server_intv_ds->sec),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_server_intv_ds->msec), sizeof(p_server_intv_ds->msec),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *server_intv_dsus_get(server_intv_dsus_t *p_server_intv_dsus,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || SERVER_INTV_DSUS_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_server_intv_dsus->flag),
                    sizeof(p_server_intv_dsus->flag), p_buf, p_buf_end);
    p_buf = buf_get(&(p_server_intv_dsus->sec), sizeof(p_server_intv_dsus->sec),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_server_intv_dsus->usec),
                    sizeof(p_server_intv_dsus->usec), p_buf, p_buf_end);

    return p_buf;
}
