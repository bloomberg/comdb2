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

#ifndef __BLOCK_INTERNAL_H__
#define __BLOCK_INTERNAL_H__

/* db block request */
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include <plbitlib.h>
#include <alloca.h>

#include "comdb2.h"
#include "tag.h"
#include "types.h"
#include "translistener.h"

/* yet more detailed blockop error codes (OP_FAILED_INTERNAL + errcode) */
enum {
    ERR_FIND_RRN_GENID = 1,
    ERR_FORM_KEY = 2,
    ERR_ADD_RRN = 3,
    ERR_DEL_KEY = 4,
    ERR_DEL_DTA = 5,
    ERR_UPDATE = 6,
    ERR_ADD_BLOB = 7,
    ERR_DEL_BLOB = 8,
    ERR_FIND_CONSTRAINT = 9,
    ERR_MALLOC = 10,
    ERR_JAVASP_ABORT_OP = 11,
    ERR_SAVE_BLOBS = 12,
    ERR_UPD_GENIDS = 13,
    ERR_ADD_OPLOG = 14,
    ERR_UPD_BLOB = 15
};

/* coordinated transaction errors */
enum { ERR_COORD_UNKNOWN_TRANSACTION = 1, ERR_COORD_COMMIT_FAILED = 2 };

/* detailed blockop error codes */
enum {
    OP_FAILED_VERIFY = 4,
    OP_FAILED_CONVERSION = 301,
    OP_FAILED_BAD_REQUEST = 199,
    OP_FAILED_UNIQ = 2,
    OP_FAILED_INTERNAL = 1000,
    OP_FAILED_BDB = 2000 /* bdberr is added to this number */
};

/* Debug cookie opcodes */
enum { DEBUG_COOKIE_DUMP_PLAN = 1 };

/* start longblockreq structs */
struct longblock_req_pre_hdr {
    int flags;
};
enum { LONGBLOCK_REQ_PRE_HDR_LEN = 4 };
BB_COMPILE_TIME_ASSERT(longblock_req_pre_hdr_size,
                       sizeof(struct longblock_req_pre_hdr) ==
                           LONGBLOCK_REQ_PRE_HDR_LEN);

struct longblock_fwd_pre_hdr {
    int source_node;
};
enum { LONGBLOCK_FWD_PRE_HDR_LEN = 4 };
BB_COMPILE_TIME_ASSERT(longblock_fwd_pre_hdr_size,
                       sizeof(struct longblock_fwd_pre_hdr) ==
                           LONGBLOCK_FWD_PRE_HDR_LEN);

struct longblock_req_hdr {
    int offset;      /* offset of first packed request in data */
    u_int trnid[2];  /* transaction id; 0 on initial request */
    int num_reqs;    /* number of requests in this buffer */
    int curpiece;    /* current sequential piece of transaction.
                      * must match servers */
    int docommit;    /* normally 0..set to 1 when commit is
                      * requested */
    int tot_reqs;    /* total number of requests in transaction.
                      * normally 0, set on commit */
    int reserved[4]; /* nothing..reserved area */
};
enum { LONGBLOCK_REQ_HDR_LEN = 4 + (4 * 2) + 4 + 4 + 4 + 4 + (4 * 4) };
BB_COMPILE_TIME_ASSERT(longblock_req_hdr_pre_hdr_size,
                       sizeof(struct longblock_req_hdr) ==
                           LONGBLOCK_REQ_HDR_LEN);

struct longblock_rsp {
    u_int trnid[2];
    int rc;
    int reserved[10];
};
enum { LONGBLOCK_RSP_LEN = (4 * 2) + 4 + (4 * 10) };
BB_COMPILE_TIME_ASSERT(longblock_rsp_size,
                       sizeof(struct longblock_rsp) == LONGBLOCK_RSP_LEN);

/* end longblockreq structs */

/* start blockreq structs */

struct block_req {
    int flags;
    int offset;
    int num_reqs;
    /*more packed reqs... */
};
enum { BLOCK_REQ_LEN = 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(block_req_size,
                       sizeof(struct block_req) == BLOCK_REQ_LEN);

struct block_fwd {
    int source_node;
    int offset;
    int num_reqs;
    /*more packed reqs... */
};
enum { BLOCK_FWD_LEN = 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(block_fwd_size,
                       sizeof(struct block_fwd) == BLOCK_FWD_LEN);

struct block_nested {
    int len;
    int rmtdbopcode;
    int mycpu;
    int dbnum;
    int master;
};
enum { BLOCK_NESTED_LEN = 4 + 4 + 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(block_nested_size,
                       sizeof(struct block_nested) == BLOCK_NESTED_LEN);

struct block_rsp {
    int num_completed;
    /* rc's followed by rrn's followed by brc's packed here... */
};
/* TODO packed_rc_rrn_brc is probably a var len buf? */
enum { BLOCK_RSP_LEN = 4 };
BB_COMPILE_TIME_ASSERT(block_rsp_size,
                       sizeof(struct block_rsp) == BLOCK_RSP_LEN);

struct block_rspkl_pos {
    int num_completed;
    unsigned long long position;
    int numerrs;
    /* err packed here... */
};
enum { BLOCK_RSPKL_POS_LEN = 4 + 8 + 4 };

struct block_rspkl {
    int num_completed;
    int numerrs;
    /* err packed here... */
};
enum { BLOCK_RSPKL_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(block_rspkl_size,
                       sizeof(struct block_rspkl) == BLOCK_RSPKL_LEN);

struct block_err {
    int blockop_num;
    int errcode;
    int ixnum;
};
enum { BLOCK_ERR_LEN = 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(block_err_size,
                       sizeof(struct block_err) == BLOCK_ERR_LEN);

enum { BLKF_ERRSTAT = 1, BLKF_DONT_COMMIT = 2, BLKF_RETRY = 4 };

/* end blockreq structs */
/* start packedreq structs */

struct packedreq_hdr {
    int opcode;
    int nxt;
};
enum { PACKEDREQ_HDR_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_hdr_size,
                       sizeof(struct packedreq_hdr) == PACKEDREQ_HDR_LEN);

struct packedreq_add {
    int lrl; /*len of data record in words */
    /* packed keydat */
};
enum { PACKEDREQ_ADD_LEN = 4 };
BB_COMPILE_TIME_ASSERT(packedreq_add_size,
                       sizeof(struct packedreq_add) == PACKEDREQ_ADD_LEN);

struct packedreq_addsec {
    int ixnum;
    /* packed keyrrn */
};
enum { PACKEDREQ_ADDSEC_LEN = 4 };
BB_COMPILE_TIME_ASSERT(packedreq_addsec_size,
                       sizeof(struct packedreq_addsec) == PACKEDREQ_ADDSEC_LEN);

/* sent when we know the rrn, for updates, deletes */
struct packedreq_seq {
    int seq1;
    int seq2;
    int seq3; /* may not be present - older proxies do not send this */
};
enum {
    PACKEDREQ_SEQ_OLD_LEN = 4 + 4,
    PACKEDREQ_SEQ_LEN = PACKEDREQ_SEQ_OLD_LEN + 4
};
BB_COMPILE_TIME_ASSERT(packedreq_seq_size,
                       sizeof(struct packedreq_seq) == PACKEDREQ_SEQ_LEN);

/* blkseq version 2 */
struct packedreq_seq2 {
    uuid_t seq;
};
enum { PACKEDREQ_SEQ2_LEN = 16 };
BB_COMPILE_TIME_ASSERT(packedreq_seq2_size,
                       sizeof(struct packedreq_seq2) == PACKEDREQ_SEQ2_LEN);

struct packedreq_del {
    int lrl; /*data returned in old comdbg, nobody used it ... */
    int rrn; /*which rrn to del. */
    /* packed keydat */
};
enum { PACKEDREQ_DEL_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_del_size,
                       sizeof(struct packedreq_del) == PACKEDREQ_DEL_LEN);

struct packedreq_delsec {
    int ixnum;
    /* packed keyrrn */
};
enum { PACKEDREQ_DELSEC_LEN = 4 };
BB_COMPILE_TIME_ASSERT(packedreq_delsec_size,
                       sizeof(struct packedreq_delsec) == PACKEDREQ_DELSEC_LEN);

struct packedreq_upvrrn {
    int rrn;
    int vptr4;
    int vlen4;
    /*packed verify vdat + dlen + dat */
};
enum { PACKEDREQ_UPVRRN_LEN = 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_updvrrn_size,
                       sizeof(struct packedreq_upvrrn) == PACKEDREQ_UPVRRN_LEN);

struct packedreq_updrrnkl {
    int reserved[8];
    int rrn;
    int taglen;
    int rlen;
    int rofft;
    int vlen;
    int vofft;
    unsigned long long genid; /* generation id of the record */
    char fldnullmap[32];      /* field null bitmap */
    /*packed verify tag+ rec+vrec*/
};
enum {
    PACKEDREQ_UPDRRNKL_LEN = (4 * 8) + 4 + 4 + 4 + 4 + 4 + 4 + 8 + (1 * 32)
};
BB_COMPILE_TIME_ASSERT(packedreq_updrrnkl_size,
                       sizeof(struct packedreq_updrrnkl) ==
                           PACKEDREQ_UPDRRNKL_LEN);

struct packedreq_use {
    int dbnum;
};
enum { PACKEDREQ_USE_LEN = 4 };
BB_COMPILE_TIME_ASSERT(packedreq_use_size,
                       sizeof(struct packedreq_use) == PACKEDREQ_USE_LEN);

struct packedreq_usekl {
    int dbnum;
    int taglen; /* length of tag */
    int reserve[8];
    /* packed tag */
};
enum { PACKEDREQ_USEKL_LEN = 4 + 4 + (4 * 8) };
BB_COMPILE_TIME_ASSERT(packedreq_usekl_size,
                       sizeof(struct packedreq_usekl) == PACKEDREQ_USEKL_LEN);

struct packedreq_tzset {
    int tznamelen; /* length of tzname*/
    int reserve[8];
    /* packed tzname */
};
enum { PACKEDREQ_TZSET_LEN = 4 + (4 * 8) };
BB_COMPILE_TIME_ASSERT(packedreq_tzset_size,
                       sizeof(struct packedreq_tzset) == PACKEDREQ_TZSET_LEN);

/* schema change stripe mask: allows the client to tell the server
 * which stripes will be used so the server can know which locks to
 * grab without iterating through the request's blocks
 *
 * This can really only be done with BLOCK2_DELKL and BLOCK2_UPDKL
 * since they know what genid (and thus what stripe) they will be using.
 * In the case of BLOCK2_ADDKL, we just need to tell the server a keyless
 * add exists and the server can quickly add the proper stripe bit
 */
struct packedreq_scsmsk {
    unsigned stripe_mask; /* stripes that will be used same format as
                           * used by live_sc functions */
    int has_addkl;        /* 1 if there is a BLOCK2_ADDKL else 0 */
    int reserved[4];
};
enum { PACKEDREQ_SCSMSK_LEN = 4 + 4 + (4 * 4) };
BB_COMPILE_TIME_ASSERT(packedreq_scsmsk_size,
                       sizeof(struct packedreq_scsmsk) == PACKEDREQ_SCSMSK_LEN);

/*    THESE ARE REQUESTS FOR GENERIC COMDB2 API */
struct packedreq_adddta {
    int reclen; /*len of data record in bytes */
    /* packed record_data */
};
enum { PACKEDREQ_ADDDTA_LEN = 4 };
BB_COMPILE_TIME_ASSERT(packedreq_adddta_size,
                       sizeof(struct packedreq_adddta) == PACKEDREQ_ADDDTA_LEN);

struct packedreq_updbykey {
    int reserve[8];      /* reserved space */
    int taglen;          /* length of table tag in bytes */
    int reclen;          /* len of data record in bytes*/
    char keyname[32];    /* name of the key */
    char fldnullmap[32]; /* field null bitmap */
    /* packed tag + data record */
};
enum { PACKEDREQ_UPDBYKEY_LEN = (4 * 8) + 4 + 4 + (1 * 32) + (1 * 32) };
BB_COMPILE_TIME_ASSERT(packedreq_updbykey_size,
                       sizeof(struct packedreq_updbykey) ==
                           PACKEDREQ_UPDBYKEY_LEN);

struct packedreq_addkl {
    int reserve[8];      /* reserved space */
    int taglen;          /* length of table tag in bytes */
    int reclen;          /* len of data record in bytes*/
    char fldnullmap[32]; /* field null bitmap */
    /* packed tag + data record */
};
enum { PACKEDREQ_ADDKL_LEN = (4 * 8) + 4 + 4 + (1 * 32) };
BB_COMPILE_TIME_ASSERT(packedreq_addkl_size,
                       sizeof(struct packedreq_addkl) == PACKEDREQ_ADDKL_LEN);

struct packedreq_addkey {
    int ixnum;
    int rrn;
    int keylen;
    /* packed key_data */
};
enum { PACKEDREQ_ADDKEY_LEN = 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_addkey_size,
                       sizeof(struct packedreq_addkey) == PACKEDREQ_ADDKEY_LEN);

struct packedreq_keyop {
    int ixnum;
    int rrn;
    int keylen;
    /* packed key_data */
};
enum { PACKEDREQ_KEYOP_LEN = 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_keyop_size,
                       sizeof(struct packedreq_keyop) == PACKEDREQ_KEYOP_LEN);

struct packedreq_delete {
    int rrn; /*which rrn to del. */
};
enum { PACKEDREQ_DELETE_LEN = 4 };
BB_COMPILE_TIME_ASSERT(packedreq_delete_size,
                       sizeof(struct packedreq_delete) == PACKEDREQ_DELETE_LEN);

struct packedreq_delkl {
    int rrn;
    int reserve[8]; /* reserved space */
    int taglen;     /* length of table tag in bytes */
    int reclen;
    uint8_t pad0[4];
    unsigned long long genid; /* generation ID of pre-found record */
    char fldnullmap[32];      /* field null bitmap */
    /* packed tag + record to del.*/
};
enum { PACKEDREQ_DELKL_LEN = 4 + (4 * 8) + 4 + 4 + (1 * 4) + 8 + (1 * 32) };
BB_COMPILE_TIME_ASSERT(packedreq_delkl_size,
                       sizeof(struct packedreq_delkl) == PACKEDREQ_DELKL_LEN);

struct packedreq_delkey {
    int ixnum;
    int rrn;
    int keylen;
    /* packed key + rrn */
};
enum { PACKEDREQ_DELKEY_LEN = 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_delkey_size,
                       sizeof(struct packedreq_delkey) == PACKEDREQ_DELKEY_LEN);

struct packedreq_updrrn {
    int rrn;
    int rlen;
    int rofft;
    int vlen;
    int vofft;
    /* packed verify rec+vrec */
};
enum { PACKEDREQ_UPDRRN_LEN = 4 + 4 + 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_updrrn_size,
                       sizeof(struct packedreq_updrrn) == PACKEDREQ_UPDRRN_LEN);

struct packedreq_qblob {
    unsigned blobno;     /* blob number within the tag */
    unsigned length;     /* total length of blob */
    unsigned frag_len;   /* length of this fragment within the blob */
    unsigned reserve[5]; /* should be zero */
    /* packed frag_len bytes of blob data */
};
enum { PACKEDREQ_QBLOB_LEN = 4 + 4 + 4 + (4 * 5) };
BB_COMPILE_TIME_ASSERT(packedreq_qblob_size,
                       sizeof(struct packedreq_qblob) == PACKEDREQ_QBLOB_LEN);

struct packedreq_rngdelkl {
    int max_records; /* don't delete more than this number of recs */
    int max_time_ms; /* don't take longer than this many ms */
    int reserved[6];
    unsigned taglen;
    unsigned keylen;
    int pstrlen; /* -1 for full key search, >= 0 otherwise */
    unsigned reclen;
    char fldnullmap1[32]; /* field null bitmap for start key */
    char fldnullmap2[32]; /* field null bitmap for end key */
    /* packed tagname + keyname + pstr (if usepstr&1) + start_rec + end_rec */
};
enum {
    PACKEDREQ_RNGDELKL_LEN =
        4 + 4 + (4 * 6) + 4 + 4 + 4 + 4 + (1 * 32) + (1 * 32)
};
BB_COMPILE_TIME_ASSERT(packedreq_rngdelkl_size,
                       sizeof(struct packedreq_rngdelkl) ==
                           PACKEDREQ_RNGDELKL_LEN);

struct packedreq_setflags {
    int flags;
};
enum { PACKEDREQ_SETFLAGS_LEN = 4 };
BB_COMPILE_TIME_ASSERT(packedreq_setflags_size,
                       sizeof(struct packedreq_setflags) ==
                           PACKEDREQ_SETFLAGS_LEN);

struct packedreq_custom {
    int reserved[8];
    unsigned opnamelen; /* length of custom operation name */
    /* packed opname */
};
enum { PACKEDREQ_CUSTOM_LEN = (4 * 8) + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_custom_size,
                       sizeof(struct packedreq_custom) == PACKEDREQ_CUSTOM_LEN);

struct packedreq_qadd {
    int reserved[3];
    unsigned qnamelen; /* length of queue name */
    /* packed qname[1] */
};
enum { PACKEDREQ_QADD_LEN = (4 * 3) + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_qadd_size,
                       sizeof(struct packedreq_qadd) == PACKEDREQ_QADD_LEN);

struct packedreq_tran {
    int dbnum; /* db number of coordinator that originated
                * the request */
    uint8_t pad0[4];
    tranid_t tid;
    int reserved[9];
    uint8_t pad1[4];
};
enum {
    PACKEDREQ_TRAN_LEN = 4 + (1 * 4) + 8 + (4 * 9),

    /* in this case we don't want to pack/unpack the tail padding because it
     * is at the end of the req, there is no further data packed after it (ie
     * key data etc) */
    PACKEDREQ_TRAN_PAD_TAIL = (1 * 4)
};
BB_COMPILE_TIME_ASSERT(packedreq_tran_size,
                       sizeof(struct packedreq_tran) ==
                           (PACKEDREQ_TRAN_LEN + PACKEDREQ_TRAN_PAD_TAIL));

struct packedreq_sql {
    unsigned sqlqlen;
    /* packed sqlq */
};
enum { PACKEDREQ_SQL_LEN = 4 };
BB_COMPILE_TIME_ASSERT(packedreq_sql_size,
                       sizeof(struct packedreq_sql) == PACKEDREQ_SQL_LEN);

struct packedreq_set_modnum {
    int modnum;
};
enum { PACKEDREQ_SET_MODNUM_LEN = 4 };
BB_COMPILE_TIME_ASSERT(packedreq_set_modnum_size,
                       sizeof(struct packedreq_set_modnum) ==
                           PACKEDREQ_SET_MODNUM_LEN);

struct packedreq_delolder {
    int timestamp;
    int count;
    int reserved[8];
};
enum { PACKEDREQ_DELOLDER_LEN = 4 + 4 + (4 * 8) };
BB_COMPILE_TIME_ASSERT(packedreq_delolder_size,
                       sizeof(struct packedreq_delolder) ==
                           PACKEDREQ_DELOLDER_LEN);

struct packedreq_sql_params {
    unsigned sqlqlen;
    unsigned taglen;
    unsigned nullbytes;
    unsigned numblobs;
    /*char buf[1];*/
};
enum { PACKEDREQ_SQL_PARMS_LEN = 4 + 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_sql_parms_size,
                       sizeof(struct packedreq_sql_params) ==
                           PACKEDREQ_SQL_PARMS_LEN);

struct packedreq_dbglog_cookie {
    int op;
    int padding;
    unsigned long long cookie;
    int queryid;
    int nbytes;
    /* char data[1]; */
};
enum { PACKEDREQ_DBGLOG_COOKIE_LEN = 4 + 4 + 8 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(packedreq_dbglog_cookie_size,
                       sizeof(struct packedreq_dbglog_cookie) ==
                           PACKEDREQ_DBGLOG_COOKIE_LEN);

/* Blocksql query pragma. */
struct packed_pragma {
    int type;
    int len;
} pragma;
enum { PACKEDREQ_PRAGMA_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(packed_pragma_size,
                       sizeof(struct packed_pragma) == PACKEDREQ_PRAGMA_LEN);

struct packedreq_uptbl {
    unsigned int nrecs;
    int pad;
    unsigned long long genid;
};
enum { PACKEDREQ_UPTBL_LEN = 4 + 4 + 8 };
BB_COMPILE_TIME_ASSERT(packedreq_uptbl_size,
                       sizeof(struct packedreq_uptbl) == PACKEDREQ_UPTBL_LEN);
/* end packedreq structs */

/* start fstblk structs */

enum LIMITS {
    MAXBLOCKOPS = 5460 /*max # of block ops (limited by max # of rrns
                        in client-key response) */
    ,
    MAXLONGBLOCKOPS = 100000 /*max # of long block ops */
};

enum { BIGSNDDB = 13399 };

enum FSTBLK_CODES {
    FSTBLK_RSPOK = 0,
    FSTBLK_RSPERR = 1,
    FSTBLK_RSPKL = 2,
    FSTBLK_SNAP_INFO = 3
};

enum GTID_CODES { GTID_PREPARED = 1, GTID_COMMITED = 2 };

struct gtid_record {
    int type;
    int dbnum;
} coord;

struct fstblk_header {
    short type;
};
enum { FSTBLK_HEADER_LEN = 2 };
BB_COMPILE_TIME_ASSERT(fstblk_header_size,
                       sizeof(struct fstblk_header) == FSTBLK_HEADER_LEN);

/* replay info for a successful block op - store just the rrns array */
struct fstblk_rspok {
    short fluff;
    /*uint8_t   pad0[2];*/
    /*int       rrns[MAXBLOCKOPS];*/
};
enum { FSTBLK_RSPOK_LEN = 2 };
BB_COMPILE_TIME_ASSERT(fstblk_rspok_size,
                       sizeof(struct fstblk_rspok) == FSTBLK_RSPOK_LEN);

/* replay info for a failed block op - store some rrns (up to the failed
* request), and store the index of the op that failed and its rcode) */
struct fstblk_rsperr {
    short num_completed;
    uint8_t pad0[2];
    int rcode;
    /*int       rrns[MAXBLOCKOPS];*/
};
enum { FSTBLK_RSPERR_LEN = 2 + (1 * 2) + 4 };

BB_COMPILE_TIME_ASSERT(fstblk_rsperr_size,
                       sizeof(struct fstblk_rsperr) == FSTBLK_RSPERR_LEN);

struct fstblk_pre_rspkl {
    short fluff;
};
enum { FSTBLK_PRE_RSPKL_LEN = 2 };
BB_COMPILE_TIME_ASSERT(fstblk_pre_rspkl_size,
                       sizeof(struct fstblk_pre_rspkl) == FSTBLK_PRE_RSPKL_LEN);

struct fstblk_rspkl {
    int num_completed;
    int numerrs;
};
enum { FSTBLK_RSPKL_LEN = 8 };
BB_COMPILE_TIME_ASSERT(fstblk_rspkl_size,
                       sizeof(struct fstblk_rspkl) == FSTBLK_RSPKL_LEN);

typedef struct block_state {
    /* ptrs into the main fstsnd buf */
    const uint8_t *p_buf_req_start;
    const uint8_t *p_buf_req_end;
    const uint8_t *p_buf_next_start;

    /* if set, they point to malloc'd memory containing a backup of the
     * first (p_buf_saved - p_buf_saved_start) bytes of the origional req */
    uint8_t *p_buf_saved;
    uint8_t *p_buf_saved_start;
    const uint8_t *p_buf_saved_end;

    int numreq;
    char *source_host; /* source node if need to forward */
    int flags;
    long long ct_id_key;
    long long seqno;
    int pos;

    /* points to storage allocated by our prefault_helper_thread, IF
     * we are assigned one for this transaction.  otherwise NULL */
    unsigned char *pfk_bitmap;

    /* op in the block we are currently working on.  we look back at this
     * in record.c for prefaulting */
    int opnum;

    unsigned int pfkseq; /* the sequence number on the pfk buffer */

    int coordinated; /* commit status of transaction is externally driven
                      * - don't commit */

    int modnum;
    tranid_t tid;
    int longblock_single; /* longblock in a single buffer */
} block_state_t;

/* Used to store info needed by the range delete key forming routine. */
typedef struct {
    struct ireq *iq;
    void *trans;
    void *parent_trans;

    /* If a callback has an error then it can set this to bubble the error
     * code back to the block processor */
    int rc;
    int err;

    /* if 1 then we need to notify the java triggers of each deletion */
    int notifyjava;

    /* if saveblobs is 1 then the pre-delete callback will save the blobs of the
     * condemned record, allowing the post delete callback to pass them to the
     * delete listener stored procedure. */
    int saveblobs;
    blob_status_t oldblobs[MAXBLOBS];

    /* if we want to notify a stored procedure of each deletion then this should
     * be set. */
    struct javasp_trans_state *javasp_trans_handle;
} rngdel_info_t;

enum ct_etype { CTE_ADD = 1, CTE_DEL, CTE_UPD };

enum ct_constants {
    CTC_TEMP_TABLE_CACHE_SIZE = 256 /*kilobytes*/
};

struct forward_ct {
    unsigned long long genid;
    unsigned long long ins_keys;
    const uint8_t *p_buf_req_start;
    const uint8_t *p_buf_req_end;
    struct dbtable *usedb;
    int blkpos;
    int ixnum;
    int rrn;
    int optype;
};

struct backward_ct {
    struct dbtable *srcdb;
    struct dbtable *dstdb;
    int blkpos;
    int optype;
    char key[MAXKEYLEN];
    char newkey[MAXKEYLEN];
    int sixlen;
    int sixnum;
    int dixnum;
    int nonewrefs;
    int flags;
};

typedef struct cttbl_entry {
    int ct_type;
    union {
        struct forward_ct fwdct;
        struct backward_ct bwdct;
    } ctop;
} cte;

struct tran_req {
    tranid_t id;
};
enum { TRAN_REQ_LEN = sizeof(tranid_t) };

struct query_limits_req {
    int have_max_cost;
    double max_cost;
    int have_allow_tablescans;
    int allow_tablescans;
    int have_allow_temptables;
    int allow_temptables;

    int have_max_cost_warning;
    double max_cost_warning;
    int have_tablescans_warning;
    int tablescans_warning;
    int have_temptables_warning;
    int temptables_warning;
};

struct client_endian_pragma_req {
    int endian;
    int flags;
};

struct oprec {
    long long seqno;
    int optype;
    int blkpos;
    client_blob_tp ops;
};
enum { OPREC_SIZE = 8 + 4 + 4 + CLIENT_BLOB_TYPE_LEN };
BB_COMPILE_TIME_ASSERT(oprec_size, sizeof(struct oprec) == OPREC_SIZE);

struct commitrec {
    long long seqno;
    long long seed;
    int nops;
    int padding0;
};
enum { COMMITREC_SIZE = 8 + 8 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(commitrec_size,
                       sizeof(struct commitrec) == COMMITREC_SIZE);

/* lockset_req flags */
enum { SETLOCKS_FLAGS_REPLAY = 1, SETLOCKS_FLAGS_DEAD = 2 };

struct lockset_req {
    tranid_t id;
    int nlocks;
    int flags;
    unsigned long long rows[1];
};

int has_cascading_reverse_constraints(struct dbtable *tbl);

int insert_add_op(struct ireq *iq, block_state_t *blkstate, struct dbtable *usedb,
                  const uint8_t *p_buf_req_start, const uint8_t *p_buf_req_end,
                  int optype, int rrn, int ixnum, unsigned long long genid,
                  unsigned long long ins_keys, int blkpos);

int insert_del_op(block_state_t *blkstate, struct dbtable *srcdb, struct dbtable *dstdb,
                  int optype, int blkpos, void *inkey, void *innewkey,
                  int keylen, int sixnum, int dixnum, int nonewrefs, int flags);

int delayed_key_adds(struct ireq *iq, block_state_t *blkstate, void *trans,
                     int *blkpos, int *ixout, int *errout);
void *create_constraint_table(long long *ctid);
void *create_constraint_index_table(long long *ctid);
int delete_constraint_table(void *table);
int clear_constraints_tables(void);
int truncate_constraint_table(void *table);

int verify_add_constraints(struct javasp_trans_state *javasp_trans_handle,
                           struct ireq *iq, block_state_t *blkstate,
                           void *trans, int *errout);
int verify_del_constraints(struct javasp_trans_state *javasp_trans_handle,
                           struct ireq *iq, block_state_t *blkstate,
                           void *trans, blob_buffer_t *blobs, int *errout);
int check_delete_constraints(struct ireq *iq, void *trans,
                             block_state_t *blkstate, int op, void *rec_dta,
                             unsigned long long del_keys, int *errout);
int check_update_constraints(struct ireq *iq, void *trans,
                             block_state_t *blkstate, int op, void *rec_dta,
                             void *newrec_dta, unsigned long long del_keys,
                             int *errout);
void dump_all_constraints(struct dbenv *env);
void dump_constraints(struct dbtable *table);
void dump_rev_constraints(struct dbtable *table);

int restore_constraint_pointers(struct dbtable *tbl, struct dbtable *newdb);
int backout_constraint_pointers(struct dbtable *tbl, struct dbtable *newdb);

int do_twophase_commit(struct ireq *iq, tranid_t id, block_state_t *blkstate,
                       int initial_state, fstblkseq_t *seqnum);

void remember_modified_genid(block_state_t *blkstate, unsigned long long genid);

/* put/get functions */

uint8_t *tran_req_put(const struct tran_req *p_tran_req, uint8_t *p_buf,
                      const uint8_t *p_buf_end);
const uint8_t *tran_req_get(struct tran_req *p_tran_req, const uint8_t *p_buf,
                            const uint8_t *p_buf_end);

/* block put/get functions */

uint8_t *block_req_put(const struct block_req *p_block_req, uint8_t *p_buf,
                       const uint8_t *p_buf_end);
const uint8_t *block_req_get(struct block_req *p_block_req,
                             const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *block_fwd_put(const struct block_fwd *p_block_fwd, uint8_t *p_buf,
                       const uint8_t *p_buf_end);
const uint8_t *block_fwd_get(struct block_fwd *p_block_fwd,
                             const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *block_nested_put(const struct block_nested *p_block_nested,
                          uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *block_nested_get(struct block_nested *p_block_nested,
                                const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *block_rsp_put(const struct block_rsp *p_block_rsp, uint8_t *p_buf,
                       const uint8_t *p_buf_end);
const uint8_t *block_rsp_get(struct block_rsp *p_block_rsp,
                             const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *block_rspkl_put(const struct block_rspkl *p_block_rspkl,
                         uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *block_rspkl_get(struct block_rspkl *p_block_rspkl,
                               const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *block_rspkl_pos_put(const struct block_rspkl_pos *p_block_rspkl_pos,
                             uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *block_err_put(const struct block_err *p_block_err, uint8_t *p_buf,
                       const uint8_t *p_buf_end);
const uint8_t *block_err_get(struct block_err *p_block_err,
                             const uint8_t *p_buf, const uint8_t *p_buf_end);

/* longblock put/get functions */

uint8_t *longblock_req_pre_hdr_put(
    const struct longblock_req_pre_hdr *p_longblock_req_pre_hdr, uint8_t *p_buf,
    const uint8_t *p_buf_end);
const uint8_t *
longblock_req_pre_hdr_get(struct longblock_req_pre_hdr *p_longblock_req_pre_hdr,
                          const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *longblock_fwd_pre_hdr_put(
    const struct longblock_fwd_pre_hdr *p_longblock_fwd_pre_hdr, uint8_t *p_buf,
    const uint8_t *p_buf_end);
const uint8_t *
longblock_fwd_pre_hdr_get(struct longblock_fwd_pre_hdr *p_longblock_fwd_pre_hdr,
                          const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *
longblock_req_hdr_put(const struct longblock_req_hdr *p_longblock_req_hdr,
                      uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *
longblock_req_hdr_get(struct longblock_req_hdr *p_longblock_req_hdr,
                      const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *longblock_rsp_put(const struct longblock_rsp *p_longblock_rsp,
                           uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *longblock_rsp_get(struct longblock_rsp *p_longblock_rsp,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

/* packedreq put/get functions */

uint8_t *packedreq_hdr_put(const struct packedreq_hdr *p_packedreq_hdr,
                           uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_hdr_get(struct packedreq_hdr *p_packedreq_hdr,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

uint8_t *packedreq_add_put(const struct packedreq_add *p_packedreq_add,
                           uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_add_get(struct packedreq_add *p_packedreq_add,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

uint8_t *packedreq_addsec_put(const struct packedreq_addsec *p_packedreq_addsec,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_addsec_get(struct packedreq_addsec *p_packedreq_addsec,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *packedreq_seq_put(const struct packedreq_seq *p_packedreq_seq,
                           uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_seq_get(struct packedreq_seq *p_packedreq_seq,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

const uint8_t *packedreq_seq2_get(struct packedreq_seq2 *p_packedreq_seq,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end);
uint8_t *packedreq_seq2_put(const struct packedreq_seq2 *p_packedreq_seq,
                            uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *packedreq_del_put(const struct packedreq_del *p_packedreq_del,
                           uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_del_get(struct packedreq_del *p_packedreq_del,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

uint8_t *packedreq_delsec_put(const struct packedreq_delsec *p_packedreq_delsec,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_delsec_get(struct packedreq_delsec *p_packedreq_delsec,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *packedreq_upvrrn_put(const struct packedreq_upvrrn *p_packedreq_upvrrn,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_upvrrn_get(struct packedreq_upvrrn *p_packedreq_upvrrn,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *
packedreq_updrrnkl_put(const struct packedreq_updrrnkl *p_packedreq_updrrnkl,
                       uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *
packedreq_updrrnkl_get(struct packedreq_updrrnkl *p_packedreq_updrrnkl,
                       const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *packedreq_use_put(const struct packedreq_use *p_packedreq_use,
                           uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_use_get(struct packedreq_use *p_packedreq_use,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

uint8_t *packedreq_usekl_put(const struct packedreq_usekl *p_packedreq_usekl,
                             uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_usekl_get(struct packedreq_usekl *p_packedreq_usekl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end);

uint8_t *packedreq_tzset_put(const struct packedreq_tzset *p_packedreq_tzset,
                             uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_tzset_get(struct packedreq_tzset *p_packedreq_tzset,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end);

uint8_t *packedreq_scsmsk_put(const struct packedreq_scsmsk *p_packedreq_scsmsk,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_scsmsk_get(struct packedreq_scsmsk *p_packedreq_scsmsk,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *packedreq_adddta_put(const struct packedreq_adddta *p_packedreq_adddta,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_adddta_get(struct packedreq_adddta *p_packedreq_adddta,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *
packedreq_updbykey_put(const struct packedreq_updbykey *p_packedreq_updbykey,
                       uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *
packedreq_updbykey_get(struct packedreq_updbykey *p_packedreq_updbykey,
                       const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *packedreq_addkl_put(const struct packedreq_addkl *p_packedreq_addkl,
                             uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_addkl_get(struct packedreq_addkl *p_packedreq_addkl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end);

uint8_t *packedreq_addkey_put(const struct packedreq_addkey *p_packedreq_addkey,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_addkey_get(struct packedreq_addkey *p_packedreq_addkey,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *packedreq_keyop_put(const struct packedreq_keyop *p_packedreq_keyop,
                             uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_keyop_get(struct packedreq_keyop *p_packedreq_keyop,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end);

uint8_t *packedreq_delete_put(const struct packedreq_delete *p_packedreq_delete,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_delete_get(struct packedreq_delete *p_packedreq_delete,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *packedreq_delkl_put(const struct packedreq_delkl *p_packedreq_delkl,
                             uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_delkl_get(struct packedreq_delkl *p_packedreq_delkl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end);

uint8_t *packedreq_delkey_put(const struct packedreq_delkey *p_packedreq_delkey,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_delkey_get(struct packedreq_delkey *p_packedreq_delkey,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *packedreq_updrrn_put(const struct packedreq_updrrn *p_packedreq_updrrn,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_updrrn_get(struct packedreq_updrrn *p_packedreq_updrrn,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *packedreq_qblob_put(const struct packedreq_qblob *p_packedreq_qblob,
                             uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_qblob_get(struct packedreq_qblob *p_packedreq_qblob,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end);

uint8_t *
packedreq_rngdelkl_put(const struct packedreq_rngdelkl *p_packedreq_rngdelkl,
                       uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *
packedreq_rngdelkl_get(struct packedreq_rngdelkl *p_packedreq_rngdelkl,
                       const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *
packedreq_setflags_put(const struct packedreq_setflags *p_packedreq_setflags,
                       uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *
packedreq_setflags_get(struct packedreq_setflags *p_packedreq_setflags,
                       const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *packedreq_custom_put(const struct packedreq_custom *p_packedreq_custom,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_custom_get(struct packedreq_custom *p_packedreq_custom,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *packedreq_qadd_put(const struct packedreq_qadd *p_packedreq_qadd,
                            uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_qadd_get(struct packedreq_qadd *p_packedreq_qadd,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end);

uint8_t *packedreq_tran_put(const struct packedreq_tran *p_packedreq_tran,
                            uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_tran_get(struct packedreq_tran *p_packedreq_tran,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end);

uint8_t *packedreq_sql_put(const struct packedreq_sql *p_packedreq_sql,
                           uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *packedreq_sql_get(struct packedreq_sql *p_packedreq_sql,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

uint8_t *packedreq_set_modnum_put(
    const struct packedreq_set_modnum *p_packedreq_set_modnum, uint8_t *p_buf,
    const uint8_t *p_buf_end);
const uint8_t *
packedreq_set_modnum_get(struct packedreq_set_modnum *p_packedreq_set_modnum,
                         const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *
packedreq_delolder_put(const struct packedreq_delolder *p_packedreq_delolder,
                       uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *
packedreq_delolder_get(struct packedreq_delolder *p_packedreq_delolder,
                       const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *packedreq_sql_params_put(
    const struct packedreq_sql_params *p_packedreq_sql_params, uint8_t *p_buf,
    const uint8_t *p_buf_end);

const uint8_t *
packedreq_sql_params_get(struct packedreq_sql_params *p_packedreq_sql_params,
                         const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *
packedreq_dbglog_cookie_put(const struct packedreq_dbglog_cookie *p_cookie,
                            uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *
packedreq_dbglog_cookie_get(struct packedreq_dbglog_cookie *p_cookie,
                            const uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *packedreq_pragma_get(struct packed_pragma *p_pragma,
                                    const uint8_t *p_but,
                                    const uint8_t *p_buf_end);

uint8_t *packedreq_uptbl_put(const struct packedreq_uptbl *p_uptbl,
                             uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *packedreq_uptbl_get(struct packedreq_uptbl *p_uptbl,
                                   const uint8_t *p_buf,
                                   const uint8_t *p_buf_end);

uint8_t *fstblk_header_put(const struct fstblk_header *p_fstblk_header,
                           uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *fstblk_header_get(struct fstblk_header *p_fstblk_header,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

uint8_t *fstblk_rspok_put(const struct fstblk_rspok *p_fstblk_rspok,
                          uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *fstblk_rspok_get(struct fstblk_rspok *p_fstblk_rspok,
                                const uint8_t *p_buf, const uint8_t *p_buf_end);

uint8_t *fstblk_rsperr_put(const struct fstblk_rsperr *p_fstblk_rsperr,
                           uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *fstblk_rsperr_get(struct fstblk_rsperr *p_fstblk_rsperr,
                                 const uint8_t *p_buf,
                                 const uint8_t *p_buf_end);

uint8_t *fstblk_pre_rspkl_put(const struct fstblk_pre_rspkl *p_fstblk_pre_rspkl,
                              uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *fstblk_pre_rspkl_get(struct fstblk_pre_rspkl *p_fstblk_pre_rspkl,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

uint8_t *fstblk_rspkl_put(const struct fstblk_rspkl *p_fstblk_rspkl,
                          uint8_t *p_buf, const uint8_t *p_buf_end);
const uint8_t *fstblk_rspkl_get(struct fstblk_rspkl *p_fstblk_rspkl,
                                const uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *client_blob_type_get(struct client_blob_type *p_client_blob,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);
uint8_t *client_blob_type_put(const struct client_blob_type *p_client_blob,
                              uint8_t *p_buf, const uint8_t *p_buf_end);

const uint8_t *oprec_type_get(struct oprec *p_oprec, const uint8_t *p_buf,
                              const uint8_t *p_buf_end);
uint8_t *oprec_type_put(const struct oprec *p_oprec, uint8_t *p_buf,
                        const uint8_t *p_buf_end);

const uint8_t *commitrec_type_get(struct commitrec *p_oprec,
                                  const uint8_t *p_buf,
                                  const uint8_t *p_buf_end);
uint8_t *commitrec_type_put(const struct commitrec *p_oprec, uint8_t *p_buf,
                            const uint8_t *p_buf_end);

const uint8_t *ptr_from_one_based_word_offset(const uint8_t *p_buf, int offset);
int one_based_word_offset_from_ptr(const uint8_t *p_buf_start,
                                   const uint8_t *p_buf);

const uint8_t *query_limits_req_get(struct query_limits_req *req,
                                    const uint8_t *p_buf,
                                    const uint8_t *p_buf_end);

const uint8_t *
client_endian_pragma_req_get(struct client_endian_pragma_req *req,
                             const uint8_t *p_buf, const uint8_t *p_buf_end);

int block_state_next(struct ireq *iq, block_state_t *p_blkstate);

int block_state_set_next(struct ireq *iq, block_state_t *p_blkstate,
                         int offset);

#endif
