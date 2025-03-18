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
#include <endian_core.h>

#include "block_internal.h"
#include "osqlcomm.h"
#include <flibc.h>

#define BYTES_PER_WORD 4

uint8_t *block_req_put(const struct block_req *p_block_req, uint8_t *p_buf,
                       const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOCK_REQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_block_req->flags), sizeof(p_block_req->flags), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_block_req->offset), sizeof(p_block_req->offset), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_block_req->num_reqs), sizeof(p_block_req->num_reqs),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *block_req_get(struct block_req *p_block_req,
                             const uint8_t *p_buf, const uint8_t *p_buf_end,
                             int comdbg_flags)
{
    if (p_buf_end < p_buf || BLOCK_REQ_LEN > (p_buf_end - p_buf))
        return NULL;
    GETFUNC

    p_buf = getfunc(&(p_block_req->flags), sizeof(p_block_req->flags), p_buf,
                    p_buf_end);
    p_buf = getfunc(&(p_block_req->offset), sizeof(p_block_req->offset), p_buf,
                    p_buf_end);
    p_buf = getfunc(&(p_block_req->num_reqs), sizeof(p_block_req->num_reqs),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *block_fwd_put(const struct block_fwd *p_block_fwd, uint8_t *p_buf,
                       const uint8_t *p_buf_end, int comdbg_flags)
{
    if (p_buf_end < p_buf || BLOCK_FWD_LEN > (p_buf_end - p_buf))
        return NULL;
    PUTFUNC

    p_buf = putfunc(&(p_block_fwd->flags),
                    sizeof(p_block_fwd->flags), p_buf, p_buf_end);
    p_buf = putfunc(&(p_block_fwd->offset), sizeof(p_block_fwd->offset), p_buf,
                    p_buf_end);
    p_buf = putfunc(&(p_block_fwd->num_reqs), sizeof(p_block_fwd->num_reqs),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *block_fwd_get(struct block_fwd *p_block_fwd,
                             const uint8_t *p_buf, const uint8_t *p_buf_end, int comdbg_flags)
{
    if (p_buf_end < p_buf || BLOCK_FWD_LEN > (p_buf_end - p_buf))
        return NULL;
    GETFUNC

    p_buf = getfunc(&(p_block_fwd->flags),
                    sizeof(p_block_fwd->flags), p_buf, p_buf_end);
    p_buf = getfunc(&(p_block_fwd->offset), sizeof(p_block_fwd->offset), p_buf,
                    p_buf_end);
    p_buf = getfunc(&(p_block_fwd->num_reqs), sizeof(p_block_fwd->num_reqs),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *block_nested_put(const struct block_nested *p_block_nested,
                          uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOCK_NESTED_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_block_nested->len), sizeof(p_block_nested->len), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_block_nested->rmtdbopcode),
                    sizeof(p_block_nested->rmtdbopcode), p_buf, p_buf_end);
    p_buf = buf_put(&(p_block_nested->mycpu), sizeof(p_block_nested->mycpu),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_block_nested->dbnum), sizeof(p_block_nested->dbnum),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_block_nested->master), sizeof(p_block_nested->master),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *block_nested_get(struct block_nested *p_block_nested,
                                const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOCK_NESTED_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_block_nested->len), sizeof(p_block_nested->len), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_block_nested->rmtdbopcode),
                    sizeof(p_block_nested->rmtdbopcode), p_buf, p_buf_end);
    p_buf = buf_get(&(p_block_nested->mycpu), sizeof(p_block_nested->mycpu),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_block_nested->dbnum), sizeof(p_block_nested->dbnum),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_block_nested->master), sizeof(p_block_nested->master),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *block_rsp_put(const struct block_rsp *p_block_rsp, uint8_t *p_buf,
                       const uint8_t *p_buf_end, int comdbg_flags)
{
    if (p_buf_end < p_buf || BLOCK_RSP_LEN > (p_buf_end - p_buf))
        return NULL;
    PUTFUNC

    p_buf = putfunc(&(p_block_rsp->num_completed),
                    sizeof(p_block_rsp->num_completed), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *block_rsp_get(struct block_rsp *p_block_rsp,
                             const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOCK_RSP_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_block_rsp->num_completed),
                    sizeof(p_block_rsp->num_completed), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *block_rspkl_put(const struct block_rspkl *p_block_rspkl,
                         uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOCK_RSPKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_block_rspkl->num_completed),
                    sizeof(p_block_rspkl->num_completed), p_buf, p_buf_end);
    p_buf = buf_put(&(p_block_rspkl->numerrs), sizeof(p_block_rspkl->numerrs),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *block_rspkl_pos_put(const struct block_rspkl_pos *p_block_rspkl_pos,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOCK_RSPKL_POS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_block_rspkl_pos->num_completed),
                    sizeof(p_block_rspkl_pos->num_completed), p_buf, p_buf_end);
    p_buf =
        buf_no_net_put(&(p_block_rspkl_pos->position),
                       sizeof(p_block_rspkl_pos->position), p_buf, p_buf_end);
    p_buf = buf_put(&(p_block_rspkl_pos->numerrs),
                    sizeof(p_block_rspkl_pos->numerrs), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *block_rspkl_get(struct block_rspkl *p_block_rspkl,
                               const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOCK_RSPKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_block_rspkl->num_completed),
                    sizeof(p_block_rspkl->num_completed), p_buf, p_buf_end);
    p_buf = buf_get(&(p_block_rspkl->numerrs), sizeof(p_block_rspkl->numerrs),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *block_err_put(const struct block_err *p_block_err, uint8_t *p_buf,
                       const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOCK_ERR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_block_err->blockop_num),
                    sizeof(p_block_err->blockop_num), p_buf, p_buf_end);
    p_buf = buf_put(&(p_block_err->errcode), sizeof(p_block_err->errcode),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_block_err->ixnum), sizeof(p_block_err->ixnum), p_buf,
                    p_buf_end);

    return p_buf;
}

const uint8_t *block_err_get(struct block_err *p_block_err,
                             const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOCK_ERR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_block_err->blockop_num),
                    sizeof(p_block_err->blockop_num), p_buf, p_buf_end);
    p_buf = buf_get(&(p_block_err->errcode), sizeof(p_block_err->errcode),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_block_err->ixnum), sizeof(p_block_err->ixnum), p_buf,
                    p_buf_end);

    return p_buf;
}

/* longblock put/get functions */

uint8_t *longblock_req_pre_hdr_put(
    const struct longblock_req_pre_hdr *p_longblock_req_pre_hdr, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LONGBLOCK_REQ_PRE_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_longblock_req_pre_hdr->flags),
                    sizeof(p_longblock_req_pre_hdr->flags), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
longblock_req_pre_hdr_get(struct longblock_req_pre_hdr *p_longblock_req_pre_hdr,
                          const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LONGBLOCK_REQ_PRE_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_longblock_req_pre_hdr->flags),
                    sizeof(p_longblock_req_pre_hdr->flags), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *longblock_fwd_pre_hdr_put(
    const struct longblock_fwd_pre_hdr *p_longblock_fwd_pre_hdr, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LONGBLOCK_FWD_PRE_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_longblock_fwd_pre_hdr->flags),
                sizeof(p_longblock_fwd_pre_hdr->flags), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
longblock_fwd_pre_hdr_get(struct longblock_fwd_pre_hdr *p_longblock_fwd_pre_hdr,
                          const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || LONGBLOCK_FWD_PRE_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_longblock_fwd_pre_hdr->flags),
                sizeof(p_longblock_fwd_pre_hdr->flags), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
longblock_req_hdr_put(const struct longblock_req_hdr *p_longblock_req_hdr,
                      uint8_t *p_buf, const uint8_t *p_buf_end)
{
    unsigned i;

    if (p_buf_end < p_buf || LONGBLOCK_REQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_longblock_req_hdr->offset),
                    sizeof(p_longblock_req_hdr->offset), p_buf, p_buf_end);

    for (i = 0; i < (sizeof(p_longblock_req_hdr->trnid) /
                     sizeof(p_longblock_req_hdr->trnid[0]));
         ++i)
        p_buf =
            buf_put(&(p_longblock_req_hdr->trnid[i]),
                    sizeof(p_longblock_req_hdr->trnid[i]), p_buf, p_buf_end);

    p_buf = buf_put(&(p_longblock_req_hdr->num_reqs),
                    sizeof(p_longblock_req_hdr->num_reqs), p_buf, p_buf_end);
    p_buf = buf_put(&(p_longblock_req_hdr->curpiece),
                    sizeof(p_longblock_req_hdr->curpiece), p_buf, p_buf_end);
    p_buf = buf_put(&(p_longblock_req_hdr->docommit),
                    sizeof(p_longblock_req_hdr->docommit), p_buf, p_buf_end);
    p_buf = buf_put(&(p_longblock_req_hdr->tot_reqs),
                    sizeof(p_longblock_req_hdr->tot_reqs), p_buf, p_buf_end);
    p_buf = buf_put(&(p_longblock_req_hdr->reserved),
                    sizeof(p_longblock_req_hdr->reserved), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
longblock_req_hdr_get(struct longblock_req_hdr *p_longblock_req_hdr,
                      const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    unsigned i;

    if (p_buf_end < p_buf || LONGBLOCK_REQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_longblock_req_hdr->offset),
                    sizeof(p_longblock_req_hdr->offset), p_buf, p_buf_end);

    for (i = 0; i < (sizeof(p_longblock_req_hdr->trnid) /
                     sizeof(p_longblock_req_hdr->trnid[0]));
         ++i)
        p_buf =
            buf_get(&(p_longblock_req_hdr->trnid[i]),
                    sizeof(p_longblock_req_hdr->trnid[i]), p_buf, p_buf_end);

    p_buf = buf_get(&(p_longblock_req_hdr->num_reqs),
                    sizeof(p_longblock_req_hdr->num_reqs), p_buf, p_buf_end);
    p_buf = buf_get(&(p_longblock_req_hdr->curpiece),
                    sizeof(p_longblock_req_hdr->curpiece), p_buf, p_buf_end);
    p_buf = buf_get(&(p_longblock_req_hdr->docommit),
                    sizeof(p_longblock_req_hdr->docommit), p_buf, p_buf_end);
    p_buf = buf_get(&(p_longblock_req_hdr->tot_reqs),
                    sizeof(p_longblock_req_hdr->tot_reqs), p_buf, p_buf_end);
    p_buf = buf_get(&(p_longblock_req_hdr->reserved),
                    sizeof(p_longblock_req_hdr->reserved), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *longblock_rsp_put(const struct longblock_rsp *p_longblock_rsp,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    unsigned i;

    if (p_buf_end < p_buf || LONGBLOCK_RSP_LEN > (p_buf_end - p_buf))
        return NULL;

    for (i = 0; i < (sizeof(p_longblock_rsp->trnid) /
                     sizeof(p_longblock_rsp->trnid[0]));
         ++i)
        p_buf = buf_put(&(p_longblock_rsp->trnid[i]),
                        sizeof(p_longblock_rsp->trnid[i]), p_buf, p_buf_end);

    p_buf = buf_put(&(p_longblock_rsp->rc), sizeof(p_longblock_rsp->rc), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_longblock_rsp->reserved),
                    sizeof(p_longblock_rsp->reserved), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *longblock_rsp_get(struct longblock_rsp *p_longblock_rsp,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    unsigned i;

    if (p_buf_end < p_buf || LONGBLOCK_RSP_LEN > (p_buf_end - p_buf))
        return NULL;

    for (i = 0; i < (sizeof(p_longblock_rsp->trnid) /
                     sizeof(p_longblock_rsp->trnid[0]));
         ++i)
        p_buf = buf_get(&(p_longblock_rsp->trnid[i]),
                        sizeof(p_longblock_rsp->trnid[i]), p_buf, p_buf_end);

    p_buf = buf_get(&(p_longblock_rsp->rc), sizeof(p_longblock_rsp->rc), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_longblock_rsp->reserved),
                    sizeof(p_longblock_rsp->reserved), p_buf, p_buf_end);

    return p_buf;
}

/* packedreq put/get functions */

uint8_t *packedreq_hdr_put(const struct packedreq_hdr *p_packedreq_hdr,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_hdr->opcode), sizeof(p_packedreq_hdr->opcode),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_hdr->nxt), sizeof(p_packedreq_hdr->nxt),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_hdr_get(struct packedreq_hdr *p_packedreq_hdr,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end, 
                                 int comdbg_flags)
{
    if (p_buf_end < p_buf || PACKEDREQ_HDR_LEN > (p_buf_end - p_buf))
        return NULL;
    GETFUNC

    p_buf = getfunc(&(p_packedreq_hdr->opcode), sizeof(p_packedreq_hdr->opcode),
                    p_buf, p_buf_end);
    p_buf = getfunc(&(p_packedreq_hdr->nxt), sizeof(p_packedreq_hdr->nxt),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_add_put(const struct packedreq_add *p_packedreq_add,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADD_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_add->lrl), sizeof(p_packedreq_add->lrl),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_add_get(struct packedreq_add *p_packedreq_add,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end,
                                 int comdbg_flags)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADD_LEN > (p_buf_end - p_buf))
        return NULL;
    GETFUNC

    p_buf = getfunc(&(p_packedreq_add->lrl), sizeof(p_packedreq_add->lrl),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_addsec_put(const struct packedreq_addsec *p_packedreq_addsec,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADDSEC_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_addsec->ixnum),
                    sizeof(p_packedreq_addsec->ixnum), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_addsec_get(struct packedreq_addsec *p_packedreq_addsec,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end,
                                    int comdbg_flags)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADDSEC_LEN > (p_buf_end - p_buf))
        return NULL;
    GETFUNC

    p_buf = getfunc(&(p_packedreq_addsec->ixnum),
                    sizeof(p_packedreq_addsec->ixnum), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_seq_put(const struct packedreq_seq *p_packedreq_seq,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SEQ_OLD_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_seq->seq1), sizeof(p_packedreq_seq->seq1),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_seq->seq2), sizeof(p_packedreq_seq->seq2),
                    p_buf, p_buf_end);

    /* old packedreq_seq won't have this part */
    if (sizeof(p_packedreq_seq->seq3) <= (p_buf_end - p_buf))
        p_buf = buf_put(&(p_packedreq_seq->seq3), sizeof(p_packedreq_seq->seq3),
                        p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_seq_get(struct packedreq_seq *p_packedreq_seq,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SEQ_OLD_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_seq->seq1), sizeof(p_packedreq_seq->seq1),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_seq->seq2), sizeof(p_packedreq_seq->seq2),
                    p_buf, p_buf_end);

    /* old packedreq_seq won't have this part */
    if (sizeof(p_packedreq_seq->seq3) > (p_buf_end - p_buf))
        p_packedreq_seq->seq3 = 0;

    else
        p_buf = buf_get(&(p_packedreq_seq->seq3), sizeof(p_packedreq_seq->seq3),
                        p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_del_put(const struct packedreq_del *p_packedreq_del,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DEL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_del->lrl), sizeof(p_packedreq_del->lrl),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_del->rrn), sizeof(p_packedreq_del->rrn),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_seq2_put(const struct packedreq_seq2 *p_packedreq_seq,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SEQ2_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_put(&(p_packedreq_seq->seq),
                           sizeof(p_packedreq_seq->seq), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_seq2_get(struct packedreq_seq2 *p_packedreq_seq,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SEQ2_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_no_net_get(&(p_packedreq_seq->seq),
                           sizeof(p_packedreq_seq->seq), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_del_get(struct packedreq_del *p_packedreq_del,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end,
                                 int comdbg_flags)
{
    if (p_buf_end < p_buf || PACKEDREQ_DEL_LEN > (p_buf_end - p_buf))
        return NULL;
    GETFUNC

    p_buf = getfunc(&(p_packedreq_del->lrl), sizeof(p_packedreq_del->lrl),
                    p_buf, p_buf_end);
    p_buf = getfunc(&(p_packedreq_del->rrn), sizeof(p_packedreq_del->rrn),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_delsec_put(const struct packedreq_delsec *p_packedreq_delsec,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELSEC_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_delsec->ixnum),
                    sizeof(p_packedreq_delsec->ixnum), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_delsec_get(struct packedreq_delsec *p_packedreq_delsec,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end,
                                    int comdbg_flags)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELSEC_LEN > (p_buf_end - p_buf))
        return NULL;
    GETFUNC

    p_buf = getfunc(&(p_packedreq_delsec->ixnum),
                    sizeof(p_packedreq_delsec->ixnum), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_upvrrn_put(const struct packedreq_upvrrn *p_packedreq_upvrrn,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPVRRN_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_upvrrn->rrn), sizeof(p_packedreq_upvrrn->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_upvrrn->vptr4),
                    sizeof(p_packedreq_upvrrn->vptr4), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_upvrrn->vlen4),
                    sizeof(p_packedreq_upvrrn->vlen4), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_upvrrn_get(struct packedreq_upvrrn *p_packedreq_upvrrn,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end,
                                    int comdbg_flags)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPVRRN_LEN > (p_buf_end - p_buf))
        return NULL;
    GETFUNC

    p_buf = getfunc(&(p_packedreq_upvrrn->rrn), sizeof(p_packedreq_upvrrn->rrn),
                    p_buf, p_buf_end);
    p_buf = getfunc(&(p_packedreq_upvrrn->vptr4),
                    sizeof(p_packedreq_upvrrn->vptr4), p_buf, p_buf_end);
    p_buf = getfunc(&(p_packedreq_upvrrn->vlen4),
                    sizeof(p_packedreq_upvrrn->vlen4), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
packedreq_updrrnkl_put(const struct packedreq_updrrnkl *p_packedreq_updrrnkl,
                       uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPDRRNKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_updrrnkl->reserved),
                    sizeof(p_packedreq_updrrnkl->reserved), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrnkl->rrn),
                    sizeof(p_packedreq_updrrnkl->rrn), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrnkl->taglen),
                    sizeof(p_packedreq_updrrnkl->taglen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrnkl->rlen),
                    sizeof(p_packedreq_updrrnkl->rlen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrnkl->rofft),
                    sizeof(p_packedreq_updrrnkl->rofft), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrnkl->vlen),
                    sizeof(p_packedreq_updrrnkl->vlen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrnkl->vofft),
                    sizeof(p_packedreq_updrrnkl->vofft), p_buf, p_buf_end);
    p_buf =
        buf_no_net_put(&(p_packedreq_updrrnkl->genid),
                       sizeof(p_packedreq_updrrnkl->genid), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_packedreq_updrrnkl->fldnullmap),
                           sizeof(p_packedreq_updrrnkl->fldnullmap), p_buf,
                           p_buf_end);

    return p_buf;
}

const uint8_t *
packedreq_updrrnkl_get(struct packedreq_updrrnkl *p_packedreq_updrrnkl,
                       const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPDRRNKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_updrrnkl->reserved),
                    sizeof(p_packedreq_updrrnkl->reserved), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrnkl->rrn),
                    sizeof(p_packedreq_updrrnkl->rrn), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrnkl->taglen),
                    sizeof(p_packedreq_updrrnkl->taglen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrnkl->rlen),
                    sizeof(p_packedreq_updrrnkl->rlen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrnkl->rofft),
                    sizeof(p_packedreq_updrrnkl->rofft), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrnkl->vlen),
                    sizeof(p_packedreq_updrrnkl->vlen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrnkl->vofft),
                    sizeof(p_packedreq_updrrnkl->vofft), p_buf, p_buf_end);
    p_buf =
        buf_no_net_get(&(p_packedreq_updrrnkl->genid),
                       sizeof(p_packedreq_updrrnkl->genid), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_packedreq_updrrnkl->fldnullmap),
                           sizeof(p_packedreq_updrrnkl->fldnullmap), p_buf,
                           p_buf_end);

    return p_buf;
}

uint8_t *packedreq_use_put(const struct packedreq_use *p_packedreq_use,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_USE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_use->dbnum), sizeof(p_packedreq_use->dbnum),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_use_get(struct packedreq_use *p_packedreq_use,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_USE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_use->dbnum), sizeof(p_packedreq_use->dbnum),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_usekl_put(const struct packedreq_usekl *p_packedreq_usekl,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_USEKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_usekl->dbnum),
                    sizeof(p_packedreq_usekl->dbnum), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_usekl->taglen),
                    sizeof(p_packedreq_usekl->taglen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_usekl->reserve),
                    sizeof(p_packedreq_usekl->reserve), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_usekl_get(struct packedreq_usekl *p_packedreq_usekl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_USEKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_usekl->dbnum),
                    sizeof(p_packedreq_usekl->dbnum), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_usekl->taglen),
                    sizeof(p_packedreq_usekl->taglen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_usekl->reserve),
                    sizeof(p_packedreq_usekl->reserve), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_tzset_put(const struct packedreq_tzset *p_packedreq_tzset,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_TZSET_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_tzset->tznamelen),
                    sizeof(p_packedreq_tzset->tznamelen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_tzset->reserve),
                    sizeof(p_packedreq_tzset->reserve), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_tzset_get(struct packedreq_tzset *p_packedreq_tzset,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_TZSET_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_tzset->tznamelen),
                    sizeof(p_packedreq_tzset->tznamelen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_tzset->reserve),
                    sizeof(p_packedreq_tzset->reserve), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_scsmsk_put(const struct packedreq_scsmsk *p_packedreq_scsmsk,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SCSMSK_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_scsmsk->stripe_mask),
                    sizeof(p_packedreq_scsmsk->stripe_mask), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_scsmsk->has_addkl),
                    sizeof(p_packedreq_scsmsk->has_addkl), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_scsmsk->reserved),
                    sizeof(p_packedreq_scsmsk->reserved), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_scsmsk_get(struct packedreq_scsmsk *p_packedreq_scsmsk,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SCSMSK_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_scsmsk->stripe_mask),
                    sizeof(p_packedreq_scsmsk->stripe_mask), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_scsmsk->has_addkl),
                    sizeof(p_packedreq_scsmsk->has_addkl), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_scsmsk->reserved),
                    sizeof(p_packedreq_scsmsk->reserved), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_adddta_put(const struct packedreq_adddta *p_packedreq_adddta,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADDDTA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_adddta->reclen),
                    sizeof(p_packedreq_adddta->reclen), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_adddta_get(struct packedreq_adddta *p_packedreq_adddta,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADDDTA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_adddta->reclen),
                    sizeof(p_packedreq_adddta->reclen), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
packedreq_updbykey_put(const struct packedreq_updbykey *p_packedreq_updbykey,
                       uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPDBYKEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_updbykey->reserve),
                    sizeof(p_packedreq_updbykey->reserve), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updbykey->taglen),
                    sizeof(p_packedreq_updbykey->taglen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updbykey->reclen),
                    sizeof(p_packedreq_updbykey->reclen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updbykey->keyname),
                    sizeof(p_packedreq_updbykey->keyname), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_packedreq_updbykey->fldnullmap),
                           sizeof(p_packedreq_updbykey->fldnullmap), p_buf,
                           p_buf_end);

    return p_buf;
}

const uint8_t *
packedreq_updbykey_get(struct packedreq_updbykey *p_packedreq_updbykey,
                       const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPDBYKEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_updbykey->reserve),
                    sizeof(p_packedreq_updbykey->reserve), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updbykey->taglen),
                    sizeof(p_packedreq_updbykey->taglen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updbykey->reclen),
                    sizeof(p_packedreq_updbykey->reclen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updbykey->keyname),
                    sizeof(p_packedreq_updbykey->keyname), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_packedreq_updbykey->fldnullmap),
                           sizeof(p_packedreq_updbykey->fldnullmap), p_buf,
                           p_buf_end);

    return p_buf;
}

uint8_t *packedreq_addkl_put(const struct packedreq_addkl *p_packedreq_addkl,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADDKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_addkl->reserve),
                    sizeof(p_packedreq_addkl->reserve), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_addkl->taglen),
                    sizeof(p_packedreq_addkl->taglen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_addkl->reclen),
                    sizeof(p_packedreq_addkl->reclen), p_buf, p_buf_end);
    p_buf =
        buf_no_net_put(&(p_packedreq_addkl->fldnullmap),
                       sizeof(p_packedreq_addkl->fldnullmap), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_addkl_get(struct packedreq_addkl *p_packedreq_addkl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADDKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_addkl->reserve),
                    sizeof(p_packedreq_addkl->reserve), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_addkl->taglen),
                    sizeof(p_packedreq_addkl->taglen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_addkl->reclen),
                    sizeof(p_packedreq_addkl->reclen), p_buf, p_buf_end);
    p_buf =
        buf_no_net_get(&(p_packedreq_addkl->fldnullmap),
                       sizeof(p_packedreq_addkl->fldnullmap), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_addkey_put(const struct packedreq_addkey *p_packedreq_addkey,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADDKEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_addkey->ixnum),
                    sizeof(p_packedreq_addkey->ixnum), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_addkey->rrn), sizeof(p_packedreq_addkey->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_addkey->keylen),
                    sizeof(p_packedreq_addkey->keylen), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_addkey_get(struct packedreq_addkey *p_packedreq_addkey,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_ADDKEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_addkey->ixnum),
                    sizeof(p_packedreq_addkey->ixnum), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_addkey->rrn), sizeof(p_packedreq_addkey->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_addkey->keylen),
                    sizeof(p_packedreq_addkey->keylen), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_keyop_put(const struct packedreq_keyop *p_packedreq_keyop,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_KEYOP_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_keyop->ixnum),
                    sizeof(p_packedreq_keyop->ixnum), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_keyop->rrn), sizeof(p_packedreq_keyop->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_keyop->keylen),
                    sizeof(p_packedreq_keyop->keylen), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_keyop_get(struct packedreq_keyop *p_packedreq_keyop,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_KEYOP_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_keyop->ixnum),
                    sizeof(p_packedreq_keyop->ixnum), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_keyop->rrn), sizeof(p_packedreq_keyop->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_keyop->keylen),
                    sizeof(p_packedreq_keyop->keylen), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_delete_put(const struct packedreq_delete *p_packedreq_delete,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELETE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_delete->rrn), sizeof(p_packedreq_delete->rrn),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_delete_get(struct packedreq_delete *p_packedreq_delete,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELETE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_delete->rrn), sizeof(p_packedreq_delete->rrn),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_delkl_put(const struct packedreq_delkl *p_packedreq_delkl,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_delkl->rrn), sizeof(p_packedreq_delkl->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_delkl->reserve),
                    sizeof(p_packedreq_delkl->reserve), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_delkl->taglen),
                    sizeof(p_packedreq_delkl->taglen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_delkl->reclen),
                    sizeof(p_packedreq_delkl->reclen), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_packedreq_delkl->pad0),
                           sizeof(p_packedreq_delkl->pad0), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_packedreq_delkl->genid),
                           sizeof(p_packedreq_delkl->genid), p_buf, p_buf_end);
    p_buf =
        buf_no_net_put(&(p_packedreq_delkl->fldnullmap),
                       sizeof(p_packedreq_delkl->fldnullmap), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_delkl_get(struct packedreq_delkl *p_packedreq_delkl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_delkl->rrn), sizeof(p_packedreq_delkl->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_delkl->reserve),
                    sizeof(p_packedreq_delkl->reserve), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_delkl->taglen),
                    sizeof(p_packedreq_delkl->taglen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_delkl->reclen),
                    sizeof(p_packedreq_delkl->reclen), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_packedreq_delkl->pad0),
                           sizeof(p_packedreq_delkl->pad0), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_packedreq_delkl->genid),
                           sizeof(p_packedreq_delkl->genid), p_buf, p_buf_end);
    p_buf =
        buf_no_net_get(&(p_packedreq_delkl->fldnullmap),
                       sizeof(p_packedreq_delkl->fldnullmap), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_delkey_put(const struct packedreq_delkey *p_packedreq_delkey,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELKEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_delkey->ixnum),
                    sizeof(p_packedreq_delkey->ixnum), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_delkey->rrn), sizeof(p_packedreq_delkey->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_delkey->keylen),
                    sizeof(p_packedreq_delkey->keylen), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_delkey_get(struct packedreq_delkey *p_packedreq_delkey,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELKEY_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_delkey->ixnum),
                    sizeof(p_packedreq_delkey->ixnum), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_delkey->rrn), sizeof(p_packedreq_delkey->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_delkey->keylen),
                    sizeof(p_packedreq_delkey->keylen), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_updrrn_put(const struct packedreq_updrrn *p_packedreq_updrrn,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPDRRN_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_updrrn->rrn), sizeof(p_packedreq_updrrn->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrn->rlen),
                    sizeof(p_packedreq_updrrn->rlen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrn->rofft),
                    sizeof(p_packedreq_updrrn->rofft), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrn->vlen),
                    sizeof(p_packedreq_updrrn->vlen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_updrrn->vofft),
                    sizeof(p_packedreq_updrrn->vofft), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_updrrn_get(struct packedreq_updrrn *p_packedreq_updrrn,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPDRRN_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_updrrn->rrn), sizeof(p_packedreq_updrrn->rrn),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrn->rlen),
                    sizeof(p_packedreq_updrrn->rlen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrn->rofft),
                    sizeof(p_packedreq_updrrn->rofft), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrn->vlen),
                    sizeof(p_packedreq_updrrn->vlen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_updrrn->vofft),
                    sizeof(p_packedreq_updrrn->vofft), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_qblob_put(const struct packedreq_qblob *p_packedreq_qblob,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_QBLOB_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_qblob->blobno),
                    sizeof(p_packedreq_qblob->blobno), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_qblob->length),
                    sizeof(p_packedreq_qblob->length), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_qblob->frag_len),
                    sizeof(p_packedreq_qblob->frag_len), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_qblob->reserve),
                    sizeof(p_packedreq_qblob->reserve), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_qblob_get(struct packedreq_qblob *p_packedreq_qblob,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_QBLOB_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_qblob->blobno),
                    sizeof(p_packedreq_qblob->blobno), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_qblob->length),
                    sizeof(p_packedreq_qblob->length), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_qblob->frag_len),
                    sizeof(p_packedreq_qblob->frag_len), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_qblob->reserve),
                    sizeof(p_packedreq_qblob->reserve), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
packedreq_rngdelkl_put(const struct packedreq_rngdelkl *p_packedreq_rngdelkl,
                       uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_RNGDELKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_packedreq_rngdelkl->max_records),
                sizeof(p_packedreq_rngdelkl->max_records), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_packedreq_rngdelkl->max_time_ms),
                sizeof(p_packedreq_rngdelkl->max_time_ms), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_rngdelkl->reserved),
                    sizeof(p_packedreq_rngdelkl->reserved), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_rngdelkl->taglen),
                    sizeof(p_packedreq_rngdelkl->taglen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_rngdelkl->keylen),
                    sizeof(p_packedreq_rngdelkl->keylen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_rngdelkl->pstrlen),
                    sizeof(p_packedreq_rngdelkl->pstrlen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_rngdelkl->reclen),
                    sizeof(p_packedreq_rngdelkl->reclen), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_packedreq_rngdelkl->fldnullmap1),
                           sizeof(p_packedreq_rngdelkl->fldnullmap1), p_buf,
                           p_buf_end);
    p_buf = buf_no_net_put(&(p_packedreq_rngdelkl->fldnullmap2),
                           sizeof(p_packedreq_rngdelkl->fldnullmap2), p_buf,
                           p_buf_end);

    return p_buf;
}

const uint8_t *
packedreq_rngdelkl_get(struct packedreq_rngdelkl *p_packedreq_rngdelkl,
                       const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_RNGDELKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_packedreq_rngdelkl->max_records),
                sizeof(p_packedreq_rngdelkl->max_records), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_packedreq_rngdelkl->max_time_ms),
                sizeof(p_packedreq_rngdelkl->max_time_ms), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_rngdelkl->reserved),
                    sizeof(p_packedreq_rngdelkl->reserved), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_rngdelkl->taglen),
                    sizeof(p_packedreq_rngdelkl->taglen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_rngdelkl->keylen),
                    sizeof(p_packedreq_rngdelkl->keylen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_rngdelkl->pstrlen),
                    sizeof(p_packedreq_rngdelkl->pstrlen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_rngdelkl->reclen),
                    sizeof(p_packedreq_rngdelkl->reclen), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_packedreq_rngdelkl->fldnullmap1),
                           sizeof(p_packedreq_rngdelkl->fldnullmap1), p_buf,
                           p_buf_end);
    p_buf = buf_no_net_get(&(p_packedreq_rngdelkl->fldnullmap2),
                           sizeof(p_packedreq_rngdelkl->fldnullmap2), p_buf,
                           p_buf_end);

    return p_buf;
}

uint8_t *
packedreq_setflags_put(const struct packedreq_setflags *p_packedreq_setflags,
                       uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SETFLAGS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_setflags->flags),
                    sizeof(p_packedreq_setflags->flags), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
packedreq_setflags_get(struct packedreq_setflags *p_packedreq_setflags,
                       const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SETFLAGS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_setflags->flags),
                    sizeof(p_packedreq_setflags->flags), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_custom_put(const struct packedreq_custom *p_packedreq_custom,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_CUSTOM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_custom->reserved),
                    sizeof(p_packedreq_custom->reserved), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_custom->opnamelen),
                    sizeof(p_packedreq_custom->opnamelen), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_custom_get(struct packedreq_custom *p_packedreq_custom,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_CUSTOM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_custom->reserved),
                    sizeof(p_packedreq_custom->reserved), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_custom->opnamelen),
                    sizeof(p_packedreq_custom->opnamelen), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_qadd_put(const struct packedreq_qadd *p_packedreq_qadd,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_QADD_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_qadd->reserved),
                    sizeof(p_packedreq_qadd->reserved), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_qadd->qnamelen),
                    sizeof(p_packedreq_qadd->qnamelen), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_qadd_get(struct packedreq_qadd *p_packedreq_qadd,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_QADD_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_qadd->reserved),
                    sizeof(p_packedreq_qadd->reserved), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_qadd->qnamelen),
                    sizeof(p_packedreq_qadd->qnamelen), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_tran_put(const struct packedreq_tran *p_packedreq_tran,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_TRAN_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_tran->dbnum), sizeof(p_packedreq_tran->dbnum),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_packedreq_tran->pad0),
                           sizeof(p_packedreq_tran->pad0), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_tran->tid), sizeof(p_packedreq_tran->tid),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_tran->reserved),
                    sizeof(p_packedreq_tran->reserved), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_tran_get(struct packedreq_tran *p_packedreq_tran,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_TRAN_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_tran->dbnum), sizeof(p_packedreq_tran->dbnum),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_packedreq_tran->pad0),
                           sizeof(p_packedreq_tran->pad0), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_tran->tid), sizeof(p_packedreq_tran->tid),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_tran->reserved),
                    sizeof(p_packedreq_tran->reserved), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_sql_put(const struct packedreq_sql *p_packedreq_sql,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SQL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_sql->sqlqlen),
                    sizeof(p_packedreq_sql->sqlqlen), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_sql_get(struct packedreq_sql *p_packedreq_sql,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SQL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_sql->sqlqlen),
                    sizeof(p_packedreq_sql->sqlqlen), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_set_modnum_put(
    const struct packedreq_set_modnum *p_packedreq_set_modnum, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SET_MODNUM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_set_modnum->modnum),
                    sizeof(p_packedreq_set_modnum->modnum), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
packedreq_set_modnum_get(struct packedreq_set_modnum *p_packedreq_set_modnum,
                         const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SET_MODNUM_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_set_modnum->modnum),
                    sizeof(p_packedreq_set_modnum->modnum), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
packedreq_delolder_put(const struct packedreq_delolder *p_packedreq_delolder,
                       uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELOLDER_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_delolder->timestamp),
                    sizeof(p_packedreq_delolder->timestamp), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_delolder->count),
                    sizeof(p_packedreq_delolder->count), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_delolder->reserved),
                    sizeof(p_packedreq_delolder->reserved), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
packedreq_delolder_get(struct packedreq_delolder *p_packedreq_delolder,
                       const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DELOLDER_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_delolder->timestamp),
                    sizeof(p_packedreq_delolder->timestamp), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_delolder->count),
                    sizeof(p_packedreq_delolder->count), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_delolder->reserved),
                    sizeof(p_packedreq_delolder->reserved), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *packedreq_sql_params_put(
    const struct packedreq_sql_params *p_packedreq_sql_params, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SQL_PARMS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_packedreq_sql_params->sqlqlen),
                    sizeof(p_packedreq_sql_params->sqlqlen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_sql_params->taglen),
                    sizeof(p_packedreq_sql_params->taglen), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_packedreq_sql_params->nullbytes),
                sizeof(p_packedreq_sql_params->nullbytes), p_buf, p_buf_end);
    p_buf = buf_put(&(p_packedreq_sql_params->numblobs),
                    sizeof(p_packedreq_sql_params->numblobs), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
packedreq_sql_params_get(struct packedreq_sql_params *p_packedreq_sql_params,
                         const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_SQL_PARMS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_packedreq_sql_params->sqlqlen),
                    sizeof(p_packedreq_sql_params->sqlqlen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_sql_params->taglen),
                    sizeof(p_packedreq_sql_params->taglen), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_packedreq_sql_params->nullbytes),
                sizeof(p_packedreq_sql_params->nullbytes), p_buf, p_buf_end);
    p_buf = buf_get(&(p_packedreq_sql_params->numblobs),
                    sizeof(p_packedreq_sql_params->numblobs), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *fstblk_header_put(const struct fstblk_header *p_fstblk_header,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_HEADER_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_fstblk_header->type), sizeof(p_fstblk_header->type),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *fstblk_header_get(struct fstblk_header *p_fstblk_header,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_HEADER_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_fstblk_header->type), sizeof(p_fstblk_header->type),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *fstblk_rspok_put(const struct fstblk_rspok *p_fstblk_rspok,
                          uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_RSPOK_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_fstblk_rspok->fluff), sizeof(p_fstblk_rspok->fluff),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *fstblk_rspok_get(struct fstblk_rspok *p_fstblk_rspok,
                                const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_RSPOK_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_fstblk_rspok->fluff), sizeof(p_fstblk_rspok->fluff),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *fstblk_rsperr_put(const struct fstblk_rsperr *p_fstblk_rsperr,
                           uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_RSPERR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_fstblk_rsperr->num_completed),
                    sizeof(p_fstblk_rsperr->num_completed), p_buf, p_buf_end);
    p_buf = buf_put(&(p_fstblk_rsperr->rcode), sizeof(p_fstblk_rsperr->rcode),
                    p_buf, p_buf_end);
    return p_buf;
}

/*
uint8_t *fstblk_rsperr_rcode_put(const struct fstblk_rsperr_rcode
        *p_fstblk_rsperr_rcode, uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if(p_buf_end < p_buf || FSTBLK_RSPERR_RCODE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_fstblk_rsperr_rcode->rcode),
            sizeof(p_fstblk_rsperr_rcode->rcode), p_buf, p_buf_end);
    return p_buf;
}

uint8_t *fstblk_rsperr_rcode_get(struct fstblk_rsperr *p_fstblk_rsperr_rcode,
        const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if(p_buf_end < p_buf || FSTBLK_RSPERR_RCODE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_fstblk_rsperr_rcode->rcode),
            sizeof(p_fstblk_rsperr_rcode->rcode), p_buf, p_buf_end);
    return p_buf;
}
*/

const uint8_t *fstblk_rsperr_get(struct fstblk_rsperr *p_fstblk_rsperr,
                                 const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_RSPERR_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_fstblk_rsperr->num_completed),
                    sizeof(p_fstblk_rsperr->num_completed), p_buf, p_buf_end);
    p_buf = buf_get(&(p_fstblk_rsperr->rcode), sizeof(p_fstblk_rsperr->rcode),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *fstblk_pre_rspkl_put(const struct fstblk_pre_rspkl *p_fstblk_pre_rspkl,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_PRE_RSPKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_fstblk_pre_rspkl->fluff),
                    sizeof(p_fstblk_pre_rspkl->fluff), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *fstblk_pre_rspkl_get(struct fstblk_pre_rspkl *p_fstblk_pre_rspkl,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_PRE_RSPKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_fstblk_pre_rspkl->fluff),
                    sizeof(p_fstblk_pre_rspkl->fluff), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *fstblk_rspkl_put(const struct fstblk_rspkl *p_fstblk_rspkl,
                          uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_RSPKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_fstblk_rspkl->num_completed),
                    sizeof(p_fstblk_rspkl->num_completed), p_buf, p_buf_end);
    p_buf = buf_put(&(p_fstblk_rspkl->numerrs), sizeof(p_fstblk_rspkl->numerrs),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *fstblk_rspkl_get(struct fstblk_rspkl *p_fstblk_rspkl,
                                const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || FSTBLK_RSPKL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_fstblk_rspkl->num_completed),
                    sizeof(p_fstblk_rspkl->num_completed), p_buf, p_buf_end);
    p_buf = buf_get(&(p_fstblk_rspkl->numerrs), sizeof(p_fstblk_rspkl->numerrs),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *client_blob_type_get(struct client_blob_type *p_client_blob,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_BLOB_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_client_blob->notnull), sizeof(p_client_blob->notnull),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_blob->length), sizeof(p_client_blob->length),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_blob->padding0), sizeof(p_client_blob->padding0),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_client_blob->padding1), sizeof(p_client_blob->padding1),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *client_blob_type_put(const struct client_blob_type *p_client_blob,
                              uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || CLIENT_BLOB_TYPE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_client_blob->notnull), sizeof(p_client_blob->notnull),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_blob->length), sizeof(p_client_blob->length),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_blob->padding0), sizeof(p_client_blob->padding0),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_client_blob->padding1), sizeof(p_client_blob->padding1),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *oprec_type_get(struct oprec *p_oprec, const uint8_t *p_buf,
                              const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OPREC_SIZE > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_oprec->seqno), sizeof(p_oprec->seqno), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_oprec->optype), sizeof(p_oprec->optype), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_oprec->blkpos), sizeof(p_oprec->blkpos), p_buf, p_buf_end);
    p_buf = client_blob_type_get(&(p_oprec->ops), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *oprec_type_put(const struct oprec *p_oprec, uint8_t *p_buf,
                        const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || OPREC_SIZE > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_oprec->seqno), sizeof(p_oprec->seqno), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_oprec->optype), sizeof(p_oprec->optype), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_oprec->blkpos), sizeof(p_oprec->blkpos), p_buf, p_buf_end);
    p_buf = client_blob_type_put(&(p_oprec->ops), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *commitrec_type_get(struct commitrec *p_crec,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || COMMITREC_SIZE > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&p_crec->seqno, sizeof(p_crec->seqno), p_buf, p_buf_end);
    p_buf = buf_get(&p_crec->seed, sizeof(p_crec->seed), p_buf, p_buf_end);
    p_buf = buf_get(&p_crec->nops, sizeof(p_crec->nops), p_buf, p_buf_end);
    p_buf =
        buf_get(&p_crec->padding0, sizeof(p_crec->padding0), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *commitrec_type_put(const struct commitrec *p_crec, uint8_t *p_buf,
                            const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || COMMITREC_SIZE > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&p_crec->seqno, sizeof(p_crec->seqno), p_buf, p_buf_end);
    p_buf = buf_put(&p_crec->seed, sizeof(p_crec->seed), p_buf, p_buf_end);
    p_buf = buf_put(&p_crec->nops, sizeof(p_crec->nops), p_buf, p_buf_end);
    p_buf =
        buf_put(&p_crec->padding0, sizeof(p_crec->padding0), p_buf, p_buf_end);

    return p_buf;
}

uint8_t *
packedreq_dbglog_cookie_put(const struct packedreq_dbglog_cookie *p_cookie,
                            uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DBGLOG_COOKIE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_cookie->op), sizeof(p_cookie->op), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_cookie->padding), sizeof(p_cookie->padding),
                           p_buf, p_buf_end);
    p_buf = buf_put(&(p_cookie->cookie), sizeof(p_cookie->cookie), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_cookie->queryid), sizeof(p_cookie->queryid), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_cookie->nbytes), sizeof(p_cookie->nbytes), p_buf,
                    p_buf_end);

    return p_buf;
}

const uint8_t *
packedreq_dbglog_cookie_get(struct packedreq_dbglog_cookie *p_cookie,
                            const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_DBGLOG_COOKIE_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_cookie->op), sizeof(p_cookie->op), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_cookie->padding), sizeof(p_cookie->padding),
                           p_buf, p_buf_end);
    p_buf = buf_get(&(p_cookie->cookie), sizeof(p_cookie->cookie), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_cookie->queryid), sizeof(p_cookie->queryid), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_cookie->nbytes), sizeof(p_cookie->nbytes), p_buf,
                    p_buf_end);

    return p_buf;
}

uint8_t *packedreq_uptbl_put(const struct packedreq_uptbl *p_uptbl,
                             uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPTBL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_uptbl->nrecs), sizeof(p_uptbl->nrecs), p_buf, p_buf_end);
    p_buf = buf_put(&(p_uptbl->pad), sizeof(p_uptbl->pad), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_uptbl->genid), sizeof(p_uptbl->genid), p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *packedreq_uptbl_get(struct packedreq_uptbl *p_uptbl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_UPTBL_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_uptbl->nrecs), sizeof(p_uptbl->nrecs), p_buf, p_buf_end);
    p_buf = buf_get(&(p_uptbl->pad), sizeof(p_uptbl->pad), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_uptbl->genid), sizeof(p_uptbl->genid), p_buf, p_buf_end);

    return p_buf;
}

/**
 * Turn an offset into a ptr.  This routine sucks, but what can we do, all the
 * offsets in block_reqs are absolute 1 based word offsets into the fstsnd
 * buffer.
 * @param p_buf     pointer to the buffer
 * @param offset    1 based from from the begining of fstsnd buffer in words
 * @return pointer that offset referenced on success; NULL otherwise
 */
const uint8_t *ptr_from_one_based_word_offset(const uint8_t *p_buf, int offset)
{
    if (offset < 1)
        return NULL;

    /* remove 1 to create a 0 based offset, convert to a byte offset */
    return &p_buf[(offset - 1) * BYTES_PER_WORD];
}

/**
 * Turn a ptr into an offset.  This routine sucks, but what can we do, all the
 * offsets in block_reqs are absolute 1 based word offsets into the fstsnd
 * buffer.
 * Note: no sanity checks are performed.
 * Note: offset may not be exact if ptr is not on a word boundry.
 * @param p_buf_start   start of the buffer
 * @param p_buf     pointer into buffer to calculate offset for
 * @return 1 based from from the begining of the buffer in words
 */
int one_based_word_offset_from_ptr(const uint8_t *p_buf_start,
                                   const uint8_t *p_buf)
{
    /* change to a word offset, round up to the next word, add 1 to create 1
     * based offset */
    return (((p_buf - p_buf_start) + (BYTES_PER_WORD - 1)) / BYTES_PER_WORD) +
           1;
}

const uint8_t *packedreq_pragma_get(struct packed_pragma *p_pragma,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PACKEDREQ_PRAGMA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&p_pragma->type, sizeof(p_pragma->type), p_buf, p_buf_end);
    p_buf = buf_get(&p_pragma->len, sizeof(p_pragma->len), p_buf, p_buf_end);
    return p_buf;
}

const uint8_t *query_limits_req_get(struct query_limits_req *req,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end)
{
    p_buf = buf_get(&req->have_max_cost, sizeof(req->have_max_cost), p_buf,
                    p_buf_end);
    p_buf = buf_get(&req->max_cost, sizeof(req->max_cost), p_buf, p_buf_end);
    p_buf = buf_get(&req->have_allow_tablescans,
                    sizeof(req->have_allow_tablescans), p_buf, p_buf_end);
    p_buf = buf_get(&req->allow_tablescans, sizeof(req->allow_tablescans),
                    p_buf, p_buf_end);
    p_buf = buf_get(&req->have_allow_temptables,
                    sizeof(req->have_allow_temptables), p_buf, p_buf_end);
    p_buf = buf_get(&req->allow_temptables, sizeof(req->allow_temptables),
                    p_buf, p_buf_end);

    p_buf = buf_get(&req->have_max_cost_warning,
                    sizeof(req->have_max_cost_warning), p_buf, p_buf_end);
    p_buf = buf_get(&req->max_cost_warning, sizeof(req->max_cost_warning),
                    p_buf, p_buf_end);
    p_buf = buf_get(&req->have_tablescans_warning,
                    sizeof(req->have_tablescans_warning), p_buf, p_buf_end);
    p_buf = buf_get(&req->tablescans_warning, sizeof(req->tablescans_warning),
                    p_buf, p_buf_end);
    p_buf = buf_get(&req->have_temptables_warning,
                    sizeof(req->have_temptables_warning), p_buf, p_buf_end);
    p_buf = buf_get(&req->temptables_warning, sizeof(req->temptables_warning),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *
client_endian_pragma_req_get(struct client_endian_pragma_req *req,
                             const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    p_buf = buf_get(&req->endian, sizeof(req->endian), p_buf, p_buf_end);
    p_buf = buf_get(&req->flags, sizeof(req->flags), p_buf, p_buf_end);
    return p_buf;
}

int blkseq_get_rcode(void *data, int datalen)
{
    int outrc = 0, snapinfo_outrc = 0, snapinfo = 0;
    uint8_t buf_fstblk[FSTBLK_MAX_BUF_LEN];
    uint8_t *p_fstblk_buf = buf_fstblk,
            *p_fstblk_buf_end = buf_fstblk + sizeof(buf_fstblk);
    int blkseq_line = 0;

    struct fstblk_header fstblk_header;
    p_fstblk_buf_end = buf_fstblk + datalen;

    if (datalen - 4 < sizeof(struct fstblk_header)) {
        blkseq_line = __LINE__;
        goto error;
    }

    if (datalen > sizeof(buf_fstblk)) {
        return IX_ACCESS;
    } else {
        memcpy(buf_fstblk, data, datalen - 4);
        datalen = datalen - 4;
    }
    if (!(p_fstblk_buf = (uint8_t *)fstblk_header_get(
              &fstblk_header, p_fstblk_buf, p_fstblk_buf_end))) {
        blkseq_line = __LINE__;
        goto error;
    }
    switch (fstblk_header.type) {
    case FSTBLK_RSPOK: {
        outrc = RC_OK;
        break;
    }

    case FSTBLK_RSPERR: {
        outrc = ERR_BLOCK_FAILED;
        break;
    }

    case FSTBLK_SNAP_INFO:
        snapinfo = 1; /* fallthrough */

    case FSTBLK_RSPKL: {
        errstat_t errstat;
        struct fstblk_pre_rspkl fstblk_pre_rspkl;
        struct fstblk_rspkl fstblk_rspkl;

        /* fluff */
        if (!(p_fstblk_buf = (uint8_t *)fstblk_pre_rspkl_get(
                  &fstblk_pre_rspkl, p_fstblk_buf, p_fstblk_buf_end))) {
            blkseq_line = __LINE__;
            goto error;
        }

        if (snapinfo) {
            if (!(p_fstblk_buf = (uint8_t *)buf_get(
                      &(snapinfo_outrc), sizeof(snapinfo_outrc), p_fstblk_buf,
                      p_fstblk_buf_end))) {
                blkseq_line = __LINE__;
                goto error;
            }
            if (snapinfo_outrc != 0) {
                if (!(p_fstblk_buf = (uint8_t *)osqlcomm_errstat_type_get(&errstat, p_fstblk_buf, p_fstblk_buf_end))) {
                    blkseq_line = __LINE__;
                    goto error;
                }
            }
            struct query_effects unused;
            if (!(p_fstblk_buf = (uint8_t *)osqlcomm_query_effects_get(
                      &unused, p_fstblk_buf, p_fstblk_buf_end))) {
                blkseq_line = __LINE__;
                goto error;
            }
        }

        if (!(p_fstblk_buf = (uint8_t *)fstblk_rspkl_get(
                  &fstblk_rspkl, p_fstblk_buf, p_fstblk_buf_end))) {
            blkseq_line = __LINE__;
            goto error;
        }

        if (fstblk_rspkl.numerrs > 0) {
            struct block_err err;

            if (!(p_fstblk_buf = (uint8_t *)block_err_get(&err, p_fstblk_buf,
                                                          p_fstblk_buf_end))) {
                blkseq_line = __LINE__;
                goto error;
            }

            if (snapinfo) {
                outrc = snapinfo_outrc;
            } else {
                switch (err.errcode) {
                case ERR_NO_RECORDS_FOUND:
                case ERR_CONVERT_DTA:
                case ERR_NULL_CONSTRAINT:
                case ERR_SQL_PREP:
                case ERR_CONSTR:
                case ERR_UNCOMMITTABLE_TXN:
                case ERR_DIST_ABORT:
                case ERR_NOMASTER:
                case ERR_NOTSERIAL:
                case ERR_SC:
                case ERR_TRAN_TOO_BIG:
                    outrc = err.errcode;
                    break;
                default:
                    outrc = ERR_BLOCK_FAILED;
                    break;
                }
            }
            if (outrc) {
                switch (outrc) {
                case ERR_NO_RECORDS_FOUND:
                case ERR_CONVERT_DTA:
                case ERR_NULL_CONSTRAINT:
                case ERR_SQL_PREP:
                case ERR_CONSTR:
                case ERR_UNCOMMITTABLE_TXN:
                case ERR_DIST_ABORT:
                case ERR_NOMASTER:
                case ERR_NOTSERIAL:
                case ERR_SC:
                case ERR_TRAN_TOO_BIG:
                    break;
                default:
                    outrc = outrc + err.errcode;
                    break;
                }
            }
        } else {
            outrc = RC_OK;
        }
        break;
    }

    default:
        logmsg(LOGMSG_ERROR, "%s: bad fstblk replay type %d\n", __func__,
               fstblk_header.type);
        blkseq_line = __LINE__;
        goto error;
    }
    return outrc;
error:
    logmsg(LOGMSG_ERROR, "%s:%d failed to read rcode from blkseq\n", __func__,
           blkseq_line);
    return -999;
}
