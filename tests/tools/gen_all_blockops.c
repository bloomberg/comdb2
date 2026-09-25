#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <cdb2api.h>

/* OP codes */
#define OP_BLOCK 100

/* Block operation opcodes (from db/comdb2.h) */
#define BLOCK_ADDSL      110
#define BLOCK_ADDSEC     111
#define BLOCK_SECAFPRI   112
#define BLOCK_ADNOD      113
#define BLOCK_DELSC      114
#define BLOCK_DELSEC     115
#define BLOCK_DELNOD     116
#define BLOCK_UPVRRN     117
#define BLOCK2_ADDDTA    130
#define BLOCK2_ADDKEY    131
#define BLOCK2_DELDTA    132
#define BLOCK2_DELKEY    133
#define BLOCK2_UPDATE    134
#define BLOCK2_ADDKL     135
#define BLOCK2_DELKL     136
#define BLOCK2_UPDKL     137
#define BLOCK2_ADDKL_POS 138
#define BLOCK2_UPDKL_POS 139
#define BLOCK_DEBUG      777
#define BLOCK_SEQ        800
#define BLOCK_USE        801
#define BLOCK2_USE       802
#define BLOCK2_QBLOB     804
#define BLOCK2_RNGDELKL  805
#define BLOCK_SETFLAGS   806
#define BLOCK2_CUSTOM    807
#define BLOCK2_QADD      808
#define BLOCK2_TZ        810
#define BLOCK2_DELOLDER  812
#define BLOCK2_TRAN      813
#define BLOCK2_MODNUM    814
#define BLOCK2_SOCK_SQL  815
#define BLOCK2_SCSMSK    816
#define BLOCK2_RECOM     817
#define BLOCK2_UPDBYKEY  818
#define BLOCK2_SERIAL    819
#define BLOCK2_DBGLOG_COOKIE 821
#define BLOCK2_PRAGMA    822
#define BLOCK2_SNAPISOL  823
#define BLOCK2_SEQV2     824
#define BLOCK2_UPTBL     825

/* Block request flags */
#define BLKF_ERRSTAT 1

/* Struct sizes (from block_internal.h) */
#define REQ_HDR_LEN          8
#define BLOCK_REQ_LEN        12
#define PACKEDREQ_HDR_LEN    8
#define PACKEDREQ_ADD_LEN    4
#define PACKEDREQ_ADDSEC_LEN 4
#define PACKEDREQ_SEQ_LEN    12
#define PACKEDREQ_SEQ2_LEN   16
#define PACKEDREQ_DEL_LEN    8
#define PACKEDREQ_DELSEC_LEN 4
#define PACKEDREQ_UPVRRN_LEN 12
#define PACKEDREQ_UPDRRNKL_LEN ((4*8)+4+4+4+4+4+4+8+32)
#define PACKEDREQ_USE_LEN    4
#define PACKEDREQ_USEKL_LEN  (4+4+(4*8))
#define PACKEDREQ_TZSET_LEN  (4+(4*8))
#define PACKEDREQ_SCSMSK_LEN (4+4+(4*4))
#define PACKEDREQ_ADDDTA_LEN 4
#define PACKEDREQ_UPDBYKEY_LEN ((4*8)+4+4+32+32)
#define PACKEDREQ_ADDKL_LEN  ((4*8)+4+4+32)
#define PACKEDREQ_ADDKEY_LEN (4+4+4)
#define PACKEDREQ_DELETE_LEN 4
#define PACKEDREQ_DELKL_LEN  (4+(4*8)+4+4+4+8+32)
#define PACKEDREQ_DELKEY_LEN (4+4+4)
#define PACKEDREQ_UPDRRN_LEN (4+4+4+4+4)
#define PACKEDREQ_QBLOB_LEN  (4+4+4+(4*5))
#define PACKEDREQ_RNGDELKL_LEN (4+4+(4*6)+4+4+4+4+32+32)
#define PACKEDREQ_SETFLAGS_LEN 4
#define PACKEDREQ_CUSTOM_LEN ((4*8)+4)
#define PACKEDREQ_QADD_LEN   ((4*3)+4)
#define PACKEDREQ_TRAN_LEN   (4+4+8+(4*9))
#define PACKEDREQ_SQL_LEN    4
#define PACKEDREQ_SET_MODNUM_LEN 4
#define PACKEDREQ_DELOLDER_LEN (4+4+(4*8))
#define PACKEDREQ_DBGLOG_COOKIE_LEN (4+4+8+4+4)
#define PACKEDREQ_PRAGMA_LEN (4+4)
#define PACKEDREQ_UPTBL_LEN  (4+4+8)

/* Helpers */
static void put_u32(uint8_t *dst, uint32_t val)
{
    uint32_t nval = htonl(val);
    memcpy(dst, &nval, 4);
}

static void put_u16(uint8_t *dst, uint16_t val)
{
    uint16_t nval = htons(val);
    memcpy(dst, &nval, 2);
}

static void put_u64_nonet(uint8_t *dst, uint64_t val)
{
    memcpy(dst, &val, 8);
}

static void put_bytes(uint8_t *dst, const void *src, size_t len)
{
    memcpy(dst, src, len);
}

static int one_based_word_offset(const uint8_t *start, const uint8_t *ptr)
{
    return ((int)(ptr - start) + 3) / 4 + 1;
}

#define ERR_BLOCK_FAILED 220

/* Response struct sizes */
#define BLOCK_RSPKL_LEN      8
#define BLOCK_RSPKL_POS_LEN  16
#define BLOCK_ERR_LEN         12
#define ERRSTAT_LEN           256

static uint32_t get_u32(const uint8_t *src)
{
    uint32_t val;
    memcpy(&val, src, 4);
    return ntohl(val);
}

static cdb2_hndl_tp *db_hndl;

/* ---------------------------------------------------------------------------
 * Transports.
 *
 * By default requests go through cdb2api's tagged interface, which only the
 * master will run: a replicant answers a tagged request it cannot handle with
 * "retry against the master".  With a port argument we instead speak to the
 * sockreqtest appsock, which submits the request the way Bloomberg's
 * socket_request plugin does, and that a replicant *does* forward to the
 * master.  Pointing this tool at a replicant's port is therefore what
 * exercises the forward-and-wait-for-reply path.
 * ------------------------------------------------------------------------ */

#define SOCKRSP_LEN 20
#define MAX_BUFFER_SIZE 65536

static int appsock_fd = -1;
static uint8_t appsock_rsp[MAX_BUFFER_SIZE];

static int appsock_connect(const char *host, int port)
{
    struct addrinfo hints = {0}, *res, *ai;
    char service[16];
    int fd = -1;

    snprintf(service, sizeof(service), "%d", port);
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    if (getaddrinfo(host, service, &hints, &res) != 0)
        return -1;
    for (ai = res; ai; ai = ai->ai_next) {
        fd = socket(ai->ai_family, ai->ai_socktype, ai->ai_protocol);
        if (fd < 0)
            continue;
        if (connect(fd, ai->ai_addr, ai->ai_addrlen) == 0)
            break;
        close(fd);
        fd = -1;
    }
    freeaddrinfo(res);
    return fd;
}

static int writeall(int fd, const void *buf, size_t len)
{
    const uint8_t *p = buf;
    while (len) {
        ssize_t n = write(fd, p, len);
        if (n <= 0)
            return -1;
        p += n;
        len -= n;
    }
    return 0;
}

static int readall(int fd, void *buf, size_t len)
{
    uint8_t *p = buf;
    while (len) {
        ssize_t n = read(fd, p, len);
        if (n <= 0)
            return -1;
        p += n;
        len -= n;
    }
    return 0;
}

/* One request per appsock invocation, so the command line is resent each
 * time. Returns the db rcode and points *rsp at the block response. */
static int appsock_submit(const uint8_t *buf, int len, const uint8_t **rsp,
                          int *rsp_len)
{
    uint8_t hdr[SOCKRSP_LEN];
    uint32_t wire_len = htonl((uint32_t)len);
    int followlen;

    if (writeall(appsock_fd, "sockreqtest\n", 12) != 0 ||
        writeall(appsock_fd, &wire_len, sizeof(wire_len)) != 0 ||
        writeall(appsock_fd, buf, len) != 0)
        return -1;

    if (readall(appsock_fd, hdr, sizeof(hdr)) != 0)
        return -1;

    followlen = (int)get_u32(hdr + 16);
    if (followlen < 0 || followlen > (int)sizeof(appsock_rsp))
        return -1;
    if (followlen && readall(appsock_fd, appsock_rsp, followlen) != 0)
        return -1;

    *rsp = appsock_rsp;
    *rsp_len = followlen;
    return (int)get_u32(hdr + 8); /* sockrsp_t::rcode */
}

static int cdb2_submit(const uint8_t *buf, int len, const uint8_t **rsp,
                       int *rsp_len)
{
    int32_t lux = 0, flags = 0;
    int rc;

    cdb2_clearbindings(db_hndl);
    if (cdb2_bind_param(db_hndl, "buffer", CDB2_BLOB, (void *)buf, len) ||
        cdb2_bind_param(db_hndl, "lux", CDB2_INTEGER, &lux, sizeof(lux)) ||
        cdb2_bind_param(db_hndl, "flags", CDB2_INTEGER, &flags, sizeof(flags))) {
        fprintf(stderr, "cdb2_bind_param: %s\n", cdb2_errstr(db_hndl));
        return -1;
    }

    rc = cdb2_run_statement(db_hndl,
             "exec procedure comdb2_legacy(@buffer, @lux, @flags)");
    if (rc) {
        fprintf(stderr, "cdb2_run_statement rc=%d: %s\n", rc,
                cdb2_errstr(db_hndl));
        return -1;
    }

    *rsp = cdb2_column_value(db_hndl, 1);
    *rsp_len = cdb2_column_size(db_hndl, 1);
    return (int)*(int32_t *)cdb2_column_value(db_hndl, 0);
}

static int submit(const uint8_t *buf, int len, const uint8_t **rsp, int *rsp_len)
{
    return appsock_fd >= 0 ? appsock_submit(buf, len, rsp, rsp_len)
                           : cdb2_submit(buf, len, rsp, rsp_len);
}

static int is_position_mode(int opcode)
{
    return opcode == BLOCK2_ADDKL_POS || opcode == BLOCK2_UPDKL_POS;
}

static int is_keyless(int opcode)
{
    switch (opcode) {
    case BLOCK2_RNGDELKL:
    case BLOCK2_UPDBYKEY:
    case BLOCK2_ADDKL:
    case BLOCK2_ADDKL_POS:
    case BLOCK2_DELKL:
    case BLOCK2_UPDKL:
    case BLOCK2_UPDKL_POS:
    case BLOCK2_QBLOB:
    case BLOCK2_QADD:
    case BLOCK2_SOCK_SQL:
    case BLOCK2_RECOM:
    case BLOCK2_SNAPISOL:
    case BLOCK2_SERIAL:
        return 1;
    default:
        return 0;
    }
}

#define NUM_SUBOPS 3
#define MAIN_OP_IDX 2

static void send_request(const char *name, int opcode,
                         const uint8_t *payload, size_t payload_len)
{
    /* Sub-op 1: BLOCK2_USE - packedreq_usekl (40 bytes) + "t" (1 byte) */
    static const char USE_TABLE[] = "t";
    int use_taglen = (int)sizeof(USE_TABLE) - 1;
    size_t use_payload_len = PACKEDREQ_USEKL_LEN + use_taglen;
    size_t use_op_raw = PACKEDREQ_HDR_LEN + use_payload_len;
    size_t use_op_padded = (use_op_raw + 3) & ~(size_t)3;

    /* Sub-op 2: BLOCK2_SEQV2 - packedreq_seq2 (16 bytes) */
    size_t seq_op_raw = PACKEDREQ_HDR_LEN + PACKEDREQ_SEQ2_LEN;
    size_t seq_op_padded = (seq_op_raw + 3) & ~(size_t)3;

    /* Sub-op 3: the actual opcode */
    size_t main_op_raw = PACKEDREQ_HDR_LEN + payload_len;
    size_t main_op_padded = (main_op_raw + 3) & ~(size_t)3;

    size_t total_len = REQ_HDR_LEN + BLOCK_REQ_LEN +
                       use_op_padded + seq_op_padded + main_op_padded;

    uint8_t *buf = calloc(1, total_len);
    if (!buf) {
        fprintf(stderr, "malloc failed for %s\n", name);
        return;
    }
    uint8_t *p = buf;

    /* req_hdr (8 bytes) */
    put_u32(p, 0);       p += 4;  /* ver1 */
    put_u16(p, 0);       p += 2;  /* ver2 */
    *p++ = 0;                      /* luxref */
    *p++ = (uint8_t)OP_BLOCK;     /* opcode */

    /* block_req (12 bytes) - fill offsets after we know end */
    uint8_t *block_req_pos = p;
    p += BLOCK_REQ_LEN;

    /* --- Sub-op 1: BLOCK2_USE --- */
    uint8_t *use_hdr_pos = p;
    p += PACKEDREQ_HDR_LEN;

    put_u32(p, 0); p += 4;                        /* dbnum */
    put_u32(p, (uint32_t)use_taglen); p += 4;     /* taglen */
    p += 4 * 8;                                    /* reserve[8] */
    put_bytes(p, USE_TABLE, use_taglen); p += use_taglen;
    p = buf + REQ_HDR_LEN + BLOCK_REQ_LEN + use_op_padded;

    /* --- Sub-op 2: BLOCK2_SEQV2 (replay protection) --- */
    uint8_t *seq_hdr_pos = p;
    p += PACKEDREQ_HDR_LEN;

    /* 16 random bytes for the uuid */
    for (int i = 0; i < 16; i++)
        *p++ = (uint8_t)(rand() & 0xff);
    p = buf + REQ_HDR_LEN + BLOCK_REQ_LEN + use_op_padded + seq_op_padded;

    /* --- Sub-op 3: the actual opcode --- */
    uint8_t *op_hdr_pos = p;
    p += PACKEDREQ_HDR_LEN;

    if (payload_len > 0)
        memcpy(p, payload, payload_len);

    /* compute offsets from buffer start */
    size_t off_after_use = REQ_HDR_LEN + BLOCK_REQ_LEN + use_op_padded;
    size_t off_after_seq = off_after_use + seq_op_padded;
    int use_nxt_offset = one_based_word_offset(buf, buf + off_after_use);
    int seq_nxt_offset = one_based_word_offset(buf, buf + off_after_seq);
    int end_offset = one_based_word_offset(buf, buf + total_len);

    /* fill block_req: flags, offset (end), num_reqs=3 */
    put_u32(block_req_pos + 0, BLKF_ERRSTAT);
    put_u32(block_req_pos + 4, (uint32_t)end_offset);
    put_u32(block_req_pos + 8, 3);

    /* fill BLOCK2_USE hdr */
    put_u32(use_hdr_pos + 0, (uint32_t)BLOCK2_USE);
    put_u32(use_hdr_pos + 4, (uint32_t)use_nxt_offset);

    /* fill BLOCK2_SEQV2 hdr */
    put_u32(seq_hdr_pos + 0, (uint32_t)BLOCK2_SEQV2);
    put_u32(seq_hdr_pos + 4, (uint32_t)seq_nxt_offset);

    /* fill main op hdr */
    put_u32(op_hdr_pos + 0, (uint32_t)opcode);
    put_u32(op_hdr_pos + 4, (uint32_t)end_offset);

    const uint8_t *rsp = NULL;
    int rsp_len = 0;
    int db_rc = submit(buf, (int)total_len, &rsp, &rsp_len);

    if (db_rc < 0) {
        fprintf(stderr, "%s: submit failed\n", name);
        free(buf);
        return;
    }

    if (db_rc == ERR_BLOCK_FAILED) {
        const uint8_t *body = rsp + REQ_HDR_LEN;
        int body_len = rsp_len - REQ_HDR_LEN;

        if (!rsp || body_len < 4) {
            printf("%-30s opcode=%3d  rc=%d (short response)\n",
                   name, opcode, db_rc);
        } else if (is_keyless(opcode)) {
            /* keyless format:
             * position mode: block_rspkl_pos(16) [+ block_err(12)]
             * normal:        block_rspkl(8)      [+ block_err(12)] */
            int rspkl_len = is_position_mode(opcode) ? BLOCK_RSPKL_POS_LEN
                                                     : BLOCK_RSPKL_LEN;
            int numerrs_off = is_position_mode(opcode) ? 12 : 4;
            int numerrs = (int)get_u32(body + numerrs_off);
            int blkop = 0, errcode = 0, ixnum = 0;
            int err_off = rspkl_len;

            if (numerrs > 0 && body_len >= rspkl_len + BLOCK_ERR_LEN) {
                blkop   = (int)get_u32(body + err_off + 0);
                errcode = (int)get_u32(body + err_off + 4);
                ixnum   = (int)get_u32(body + err_off + 8);
                err_off += BLOCK_ERR_LEN;
            }

            const char *errstr = "";
            if (body_len >= err_off + ERRSTAT_LEN)
                errstr = (const char *)(body + err_off + 16);

            printf("%-30s opcode=%3d  rc=%d numerrs=%d "
                   "blockop=%d errcode=%d ixnum=%d errstr=\"%s\"\n",
                   name, opcode, db_rc, numerrs, blkop, errcode, ixnum,
                   errstr);
        } else {
            /* legacy format:
             * block_rsp(4: num_completed) + rcodes(4*num_reqs) +
             * rrns(4*num_reqs) + borcodes(4*num_reqs)
             * The failing op's rcode is at index MAIN_OP_IDX */
            int num_completed = (int)get_u32(body);
            int errcode = 0;
            int rcode_off = 4 + MAIN_OP_IDX * 4;
            if (body_len >= rcode_off + 4)
                errcode = (int)get_u32(body + rcode_off);

            printf("%-30s opcode=%3d  rc=%d "
                   "num_completed=%d errcode=%d\n",
                   name, opcode, db_rc, num_completed, errcode);
        }
    } else {
        printf("%-30s opcode=%3d  rc=%d\n", name, opcode, db_rc);
    }

    free(buf);
}

/* Dummy data */
static const char TAG_DEFAULT[] = ".DEFAULT";
static const char KEY_NAME[] = "KEY0";
static const uint8_t DUMMY_REC[8] = {0xAA,0xAA,0xAA,0xAA,0xAA,0xAA,0xAA,0xAA};
static const uint8_t DUMMY_KEY[8] = {0xBB,0xBB,0xBB,0xBB,0xBB,0xBB,0xBB,0xBB};
static const char DUMMY_SQL[] = "SELECT 1";
static const char DUMMY_TZ[] = "UTC";
static const char DUMMY_QNAME[] = "testq";
static const char DUMMY_OPNAME[] = "testop";
static const uint8_t DUMMY_BLOB[4] = {0xCC,0xCC,0xCC,0xCC};
static const uint8_t DUMMY_UUID[16] = {0x11,0x11,0x11,0x11,0x11,0x11,0x11,0x11,
                                        0x11,0x11,0x11,0x11,0x11,0x11,0x11,0x11};
#define DUMMY_GENID  0x0000000100000001ULL
#define DUMMY_RRN    2
#define DUMMY_IXNUM  0
#define DUMMY_DBNUM  1

/* --- Per-opcode generators --- */

static void gen_BLOCK_ADDSL(void)
{
    int reclen = (int)sizeof(DUMMY_REC);
    int ixkeylen = 8;
    int datoff = (ixkeylen + 3) & ~3;
    int lrl = reclen / 4;

    size_t payload_len = PACKEDREQ_ADD_LEN + datoff + reclen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)lrl); p += 4;
    put_bytes(p, DUMMY_KEY, ixkeylen); p += datoff;
    put_bytes(p, DUMMY_REC, reclen);

    send_request("BLOCK_ADDSL", BLOCK_ADDSL, payload, payload_len);
    free(payload);
}

static void gen_addsec_op(const char *name, int opcode)
{
    uint8_t payload[PACKEDREQ_ADDSEC_LEN];
    memset(payload, 0, sizeof(payload));
    put_u32(payload, (uint32_t)DUMMY_IXNUM);

    send_request(name, opcode, payload, sizeof(payload));
}

static void gen_BLOCK_DELSC(void)
{
    int ixkeylen = 8;
    size_t payload_len = PACKEDREQ_DEL_LEN + ixkeylen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, 0); p += 4;
    put_u32(p, (uint32_t)DUMMY_RRN); p += 4;
    put_bytes(p, DUMMY_KEY, ixkeylen);

    send_request("BLOCK_DELSC", BLOCK_DELSC, payload, payload_len);
    free(payload);
}

static void gen_delsec_op(const char *name, int opcode)
{
    uint8_t payload[PACKEDREQ_DELSEC_LEN];
    memset(payload, 0, sizeof(payload));
    put_u32(payload, (uint32_t)DUMMY_IXNUM);

    send_request(name, opcode, payload, sizeof(payload));
}

static void gen_BLOCK_UPVRRN(void)
{
    int vlen = (int)sizeof(DUMMY_REC);
    int newlen = (int)sizeof(DUMMY_REC);
    int vlen4 = vlen / 4;
    int newlen4 = newlen / 4;

    size_t payload_len = PACKEDREQ_UPVRRN_LEN + vlen + 4 + newlen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)DUMMY_RRN); p += 4;
    put_u32(p, 0); p += 4;
    put_u32(p, (uint32_t)vlen4); p += 4;
    put_bytes(p, DUMMY_REC, vlen); p += vlen;
    put_u32(p, (uint32_t)newlen4); p += 4;
    put_bytes(p, DUMMY_REC, newlen);

    send_request("BLOCK_UPVRRN", BLOCK_UPVRRN, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_ADDDTA(void)
{
    int reclen = (int)sizeof(DUMMY_REC);
    size_t payload_len = PACKEDREQ_ADDDTA_LEN + reclen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)reclen); p += 4;
    put_bytes(p, DUMMY_REC, reclen);

    send_request("BLOCK2_ADDDTA", BLOCK2_ADDDTA, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_ADDKEY(void)
{
    int keylen = (int)sizeof(DUMMY_KEY);
    size_t payload_len = PACKEDREQ_ADDKEY_LEN + keylen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)DUMMY_IXNUM); p += 4;
    put_u32(p, (uint32_t)DUMMY_RRN); p += 4;
    put_u32(p, (uint32_t)keylen); p += 4;
    put_bytes(p, DUMMY_KEY, keylen);

    send_request("BLOCK2_ADDKEY", BLOCK2_ADDKEY, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_DELDTA(void)
{
    uint8_t payload[PACKEDREQ_DELETE_LEN];
    memset(payload, 0, sizeof(payload));
    put_u32(payload, (uint32_t)DUMMY_RRN);

    send_request("BLOCK2_DELDTA", BLOCK2_DELDTA, payload, sizeof(payload));
}

static void gen_BLOCK2_DELKEY(void)
{
    int keylen = (int)sizeof(DUMMY_KEY);
    size_t payload_len = PACKEDREQ_DELKEY_LEN + keylen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)DUMMY_IXNUM); p += 4;
    put_u32(p, (uint32_t)DUMMY_RRN); p += 4;
    put_u32(p, (uint32_t)keylen); p += 4;
    put_bytes(p, DUMMY_KEY, keylen);

    send_request("BLOCK2_DELKEY", BLOCK2_DELKEY, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_UPDATE(void)
{
    int rlen = (int)sizeof(DUMMY_REC);
    int vlen = (int)sizeof(DUMMY_REC);
    size_t payload_len = PACKEDREQ_UPDRRN_LEN + rlen + vlen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)DUMMY_RRN); p += 4;
    put_u32(p, (uint32_t)rlen); p += 4;
    put_u32(p, 0); p += 4;
    put_u32(p, (uint32_t)vlen); p += 4;
    put_u32(p, 0); p += 4;
    put_bytes(p, DUMMY_REC, rlen); p += rlen;
    put_bytes(p, DUMMY_REC, vlen);

    send_request("BLOCK2_UPDATE", BLOCK2_UPDATE, payload, payload_len);
    free(payload);
}

static void gen_addkl_op(const char *name, int opcode)
{
    int taglen = (int)sizeof(TAG_DEFAULT) - 1;
    int reclen = (int)sizeof(DUMMY_REC);
    size_t payload_len = PACKEDREQ_ADDKL_LEN + taglen + reclen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    p += 4 * 8;
    put_u32(p, (uint32_t)taglen); p += 4;
    put_u32(p, (uint32_t)reclen); p += 4;
    p += 32;
    put_bytes(p, TAG_DEFAULT, taglen); p += taglen;
    put_bytes(p, DUMMY_REC, reclen);

    send_request(name, opcode, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_DELKL(void)
{
    int taglen = (int)sizeof(TAG_DEFAULT) - 1;
    int reclen = (int)sizeof(DUMMY_REC);
    size_t payload_len = PACKEDREQ_DELKL_LEN + taglen + reclen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)DUMMY_RRN); p += 4;
    p += 4 * 8;
    put_u32(p, (uint32_t)taglen); p += 4;
    put_u32(p, (uint32_t)reclen); p += 4;
    p += 4;
    put_u64_nonet(p, DUMMY_GENID); p += 8;
    p += 32;
    put_bytes(p, TAG_DEFAULT, taglen); p += taglen;
    put_bytes(p, DUMMY_REC, reclen);

    send_request("BLOCK2_DELKL", BLOCK2_DELKL, payload, payload_len);
    free(payload);
}

static void gen_updkl_op(const char *name, int opcode)
{
    int taglen = (int)sizeof(TAG_DEFAULT) - 1;
    int rlen = (int)sizeof(DUMMY_REC);
    size_t payload_len = PACKEDREQ_UPDRRNKL_LEN + taglen + rlen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    p += 4 * 8;
    put_u32(p, (uint32_t)DUMMY_RRN); p += 4;
    put_u32(p, (uint32_t)taglen); p += 4;
    put_u32(p, (uint32_t)rlen); p += 4;
    put_u32(p, 0); p += 4;
    put_u32(p, 0); p += 4;
    put_u32(p, 0); p += 4;
    put_u64_nonet(p, DUMMY_GENID); p += 8;
    p += 32;
    put_bytes(p, TAG_DEFAULT, taglen); p += taglen;
    put_bytes(p, DUMMY_REC, rlen);

    send_request(name, opcode, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_UPDBYKEY(void)
{
    int taglen = (int)sizeof(TAG_DEFAULT) - 1;
    int reclen = (int)sizeof(DUMMY_REC);
    size_t payload_len = PACKEDREQ_UPDBYKEY_LEN + taglen + reclen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    p += 4 * 8;
    put_u32(p, (uint32_t)taglen); p += 4;
    put_u32(p, (uint32_t)reclen); p += 4;
    memset(p, 0, 32);
    put_bytes(p, KEY_NAME, sizeof(KEY_NAME) - 1);
    p += 32;
    p += 32;
    put_bytes(p, TAG_DEFAULT, taglen); p += taglen;
    put_bytes(p, DUMMY_REC, reclen);

    send_request("BLOCK2_UPDBYKEY", BLOCK2_UPDBYKEY, payload, payload_len);
    free(payload);
}

static void gen_BLOCK_SEQ(void)
{
    uint8_t payload[PACKEDREQ_SEQ_LEN];
    memset(payload, 0, sizeof(payload));
    put_u32(payload + 0, 1);
    put_u32(payload + 4, 2);
    put_u32(payload + 8, 3);

    send_request("BLOCK_SEQ", BLOCK_SEQ, payload, sizeof(payload));
}

static void gen_BLOCK2_SEQV2(void)
{
    uint8_t payload[PACKEDREQ_SEQ2_LEN];
    put_bytes(payload, DUMMY_UUID, 16);

    send_request("BLOCK2_SEQV2", BLOCK2_SEQV2, payload, sizeof(payload));
}

static void gen_BLOCK_DEBUG(void)
{
    send_request("BLOCK_DEBUG", BLOCK_DEBUG, NULL, 0);
}

static void gen_BLOCK_USE(void)
{
    uint8_t payload[PACKEDREQ_USE_LEN];
    memset(payload, 0, sizeof(payload));
    put_u32(payload, (uint32_t)DUMMY_DBNUM);

    send_request("BLOCK_USE", BLOCK_USE, payload, sizeof(payload));
}

static void gen_BLOCK2_USE(void)
{
    int taglen = (int)sizeof(TAG_DEFAULT) - 1;
    size_t payload_len = PACKEDREQ_USEKL_LEN + taglen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)DUMMY_DBNUM); p += 4;
    put_u32(p, (uint32_t)taglen); p += 4;
    p += 4 * 8;
    put_bytes(p, TAG_DEFAULT, taglen);

    send_request("BLOCK2_USE", BLOCK2_USE, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_TZ(void)
{
    int tzlen = (int)sizeof(DUMMY_TZ) - 1;
    size_t payload_len = PACKEDREQ_TZSET_LEN + tzlen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)tzlen); p += 4;
    p += 4 * 8;
    put_bytes(p, DUMMY_TZ, tzlen);

    send_request("BLOCK2_TZ", BLOCK2_TZ, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_QBLOB(void)
{
    int frag_len = (int)sizeof(DUMMY_BLOB);
    int blob_len = frag_len;
    size_t payload_len = PACKEDREQ_QBLOB_LEN + frag_len;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, 0); p += 4;
    put_u32(p, (uint32_t)blob_len); p += 4;
    put_u32(p, (uint32_t)frag_len); p += 4;
    p += 4 * 5;
    put_bytes(p, DUMMY_BLOB, frag_len);

    send_request("BLOCK2_QBLOB", BLOCK2_QBLOB, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_RNGDELKL(void)
{
    int taglen = (int)sizeof(TAG_DEFAULT) - 1;
    int keylen = (int)sizeof(KEY_NAME) - 1;
    int reclen = (int)sizeof(DUMMY_REC);
    size_t payload_len = PACKEDREQ_RNGDELKL_LEN + taglen + keylen + reclen + reclen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, 100); p += 4;
    put_u32(p, 1000); p += 4;
    p += 4 * 6;
    put_u32(p, (uint32_t)taglen); p += 4;
    put_u32(p, (uint32_t)keylen); p += 4;
    put_u32(p, (uint32_t)-1); p += 4;
    put_u32(p, (uint32_t)reclen); p += 4;
    p += 32;
    p += 32;
    put_bytes(p, TAG_DEFAULT, taglen); p += taglen;
    put_bytes(p, KEY_NAME, keylen); p += keylen;
    put_bytes(p, DUMMY_REC, reclen); p += reclen;
    put_bytes(p, DUMMY_REC, reclen);

    send_request("BLOCK2_RNGDELKL", BLOCK2_RNGDELKL, payload, payload_len);
    free(payload);
}

static void gen_BLOCK_SETFLAGS(void)
{
    uint8_t payload[PACKEDREQ_SETFLAGS_LEN];
    memset(payload, 0, sizeof(payload));
    put_u32(payload, 0);

    send_request("BLOCK_SETFLAGS", BLOCK_SETFLAGS, payload, sizeof(payload));
}

static void gen_BLOCK2_CUSTOM(void)
{
    int opnamelen = (int)sizeof(DUMMY_OPNAME) - 1;
    size_t payload_len = PACKEDREQ_CUSTOM_LEN + opnamelen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    p += 4 * 8;
    put_u32(p, (uint32_t)opnamelen); p += 4;
    put_bytes(p, DUMMY_OPNAME, opnamelen);

    send_request("BLOCK2_CUSTOM", BLOCK2_CUSTOM, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_QADD(void)
{
    int qnamelen = (int)sizeof(DUMMY_QNAME) - 1;
    size_t payload_len = PACKEDREQ_QADD_LEN + qnamelen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    p += 4 * 3;
    put_u32(p, (uint32_t)qnamelen); p += 4;
    put_bytes(p, DUMMY_QNAME, qnamelen);

    send_request("BLOCK2_QADD", BLOCK2_QADD, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_TRAN(void)
{
    uint8_t payload[PACKEDREQ_TRAN_LEN];
    memset(payload, 0, sizeof(payload));
    uint8_t *p = payload;

    put_u32(p, (uint32_t)DUMMY_DBNUM); p += 4;
    p += 4;
    uint64_t tid = 12345;
    put_u64_nonet(p, tid); p += 8;

    send_request("BLOCK2_TRAN", BLOCK2_TRAN, payload, sizeof(payload));
}

static void gen_BLOCK2_DELOLDER(void)
{
    uint8_t payload[PACKEDREQ_DELOLDER_LEN];
    memset(payload, 0, sizeof(payload));
    put_u32(payload + 0, 1000000);
    put_u32(payload + 4, 10);

    send_request("BLOCK2_DELOLDER", BLOCK2_DELOLDER, payload, sizeof(payload));
}

static void gen_sql_op(const char *name, int opcode)
{
    int sqlqlen = (int)sizeof(DUMMY_SQL) - 1;
    size_t payload_len = PACKEDREQ_SQL_LEN + sqlqlen;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, (uint32_t)sqlqlen); p += 4;
    put_bytes(p, DUMMY_SQL, sqlqlen);

    send_request(name, opcode, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_MODNUM(void)
{
    uint8_t payload[PACKEDREQ_SET_MODNUM_LEN];
    memset(payload, 0, sizeof(payload));
    put_u32(payload, 42);

    send_request("BLOCK2_MODNUM", BLOCK2_MODNUM, payload, sizeof(payload));
}

static void gen_BLOCK2_SCSMSK(void)
{
    uint8_t payload[PACKEDREQ_SCSMSK_LEN];
    memset(payload, 0, sizeof(payload));
    put_u32(payload + 0, 0x0F);
    put_u32(payload + 4, 1);

    send_request("BLOCK2_SCSMSK", BLOCK2_SCSMSK, payload, sizeof(payload));
}

static void gen_BLOCK2_DBGLOG_COOKIE(void)
{
    int nbytes = 4;
    size_t payload_len = PACKEDREQ_DBGLOG_COOKIE_LEN + nbytes;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, 1); p += 4;
    put_u32(p, 0); p += 4;
    put_u64_nonet(p, 0xDEADBEEFCAFEBABEULL); p += 8;
    put_u32(p, 99); p += 4;
    put_u32(p, (uint32_t)nbytes); p += 4;
    uint8_t cookie_data[4] = {0xDD,0xDD,0xDD,0xDD};
    put_bytes(p, cookie_data, nbytes);

    send_request("BLOCK2_DBGLOG_COOKIE", BLOCK2_DBGLOG_COOKIE, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_PRAGMA(void)
{
    int pragma_data_len = 4;
    size_t payload_len = PACKEDREQ_PRAGMA_LEN + pragma_data_len;
    uint8_t *payload = calloc(1, payload_len);
    uint8_t *p = payload;

    put_u32(p, 1); p += 4;
    put_u32(p, (uint32_t)pragma_data_len); p += 4;
    memset(p, 0, pragma_data_len);

    send_request("BLOCK2_PRAGMA", BLOCK2_PRAGMA, payload, payload_len);
    free(payload);
}

static void gen_BLOCK2_UPTBL(void)
{
    uint8_t payload[PACKEDREQ_UPTBL_LEN];
    memset(payload, 0, sizeof(payload));
    uint8_t *p = payload;

    put_u32(p, 1); p += 4;
    put_u32(p, 0); p += 4;
    put_u64_nonet(p, DUMMY_GENID);

    send_request("BLOCK2_UPTBL", BLOCK2_UPTBL, payload, sizeof(payload));
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <dbname> <host> [port]\n", argv[0]);
        return 1;
    }

    const char *dbname = argv[1];
    const char *host = argv[2];
    int rc;

    srand((unsigned)(time(NULL) ^ (getpid() << 16)));

    if (argc > 3) {
        appsock_fd = appsock_connect(host, atoi(argv[3]));
        if (appsock_fd < 0) {
            fprintf(stderr, "cannot connect to %s:%s\n", host, argv[3]);
            return 1;
        }
    } else {
        rc = cdb2_open(&db_hndl, dbname, host, CDB2_SET_TAGGED | CDB2_DIRECT_CPU);
        if (rc) {
            fprintf(stderr, "cdb2_open(%s, %s) rc=%d: %s\n",
                    dbname, host, rc, cdb2_errstr(db_hndl));
            return 1;
        }
    }

    gen_BLOCK_ADDSL();
    gen_addsec_op("BLOCK_ADDSEC", BLOCK_ADDSEC);
    gen_addsec_op("BLOCK_SECAFPRI", BLOCK_SECAFPRI);
    gen_addsec_op("BLOCK_ADNOD", BLOCK_ADNOD);
    gen_BLOCK_DELSC();
    gen_delsec_op("BLOCK_DELSEC", BLOCK_DELSEC);
    gen_delsec_op("BLOCK_DELNOD", BLOCK_DELNOD);
    gen_BLOCK_UPVRRN();
    gen_BLOCK2_ADDDTA();
    gen_BLOCK2_ADDKEY();
    gen_BLOCK2_DELDTA();
    gen_BLOCK2_DELKEY();
    gen_BLOCK2_UPDATE();
    gen_addkl_op("BLOCK2_ADDKL", BLOCK2_ADDKL);
    gen_BLOCK2_DELKL();
    gen_updkl_op("BLOCK2_UPDKL", BLOCK2_UPDKL);
    gen_addkl_op("BLOCK2_ADDKL_POS", BLOCK2_ADDKL_POS);
    gen_updkl_op("BLOCK2_UPDKL_POS", BLOCK2_UPDKL_POS);
    gen_BLOCK2_UPDBYKEY();
    gen_BLOCK_DEBUG();
    gen_BLOCK_SEQ();
    gen_BLOCK2_SEQV2();
    gen_BLOCK_USE();
    gen_BLOCK2_USE();
    gen_BLOCK2_TZ();
    gen_BLOCK2_QBLOB();
    gen_BLOCK2_RNGDELKL();
    gen_BLOCK_SETFLAGS();
    gen_BLOCK2_CUSTOM();
    gen_BLOCK2_QADD();
    gen_BLOCK2_TRAN();
    gen_BLOCK2_DELOLDER();
    gen_sql_op("BLOCK2_SOCK_SQL", BLOCK2_SOCK_SQL);
    gen_sql_op("BLOCK2_RECOM", BLOCK2_RECOM);
    gen_sql_op("BLOCK2_SNAPISOL", BLOCK2_SNAPISOL);
    gen_sql_op("BLOCK2_SERIAL", BLOCK2_SERIAL);
    gen_BLOCK2_MODNUM();
    gen_BLOCK2_SCSMSK();
    gen_BLOCK2_DBGLOG_COOKIE();
    gen_BLOCK2_PRAGMA();
    gen_BLOCK2_UPTBL();

    if (appsock_fd >= 0)
        close(appsock_fd);
    else
        cdb2_close(db_hndl);
    return 0;
}
