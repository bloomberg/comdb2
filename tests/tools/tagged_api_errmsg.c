#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <arpa/inet.h>
#include <cdb2api.h>

#define OP_FIND 4
#define OP_BLOCK 100
#define BLOCK2_ADDKL 135
#define BLKF_ERRSTAT 1

#define REQ_HDR_LEN 8
#define BLOCK_REQ_LEN 12
#define PACKEDREQ_HDR_LEN 8
#define PACKEDREQ_ADDKL_LEN ((4 * 8) + 4 + 4 + 32)

/* errstat tail right after the reply header: errval, errhdrlen, reserved[2], errstr */
#define ERRSTAT_STR_OFF (REQ_HDR_LEN + 16)
#define ERRSTAT_LEN 256

static cdb2_hndl_tp *hndl;

static void put_u32(uint8_t *dst, uint32_t val)
{
    uint32_t nval = htonl(val);
    memcpy(dst, &nval, 4);
}

static int one_based_word_offset(const uint8_t *start, const uint8_t *ptr)
{
    return ((int)(ptr - start) + 3) / 4 + 1;
}

static void submit(const char *name, uint8_t *buf, int len)
{
    int32_t lux = 0, flags = 0;

    cdb2_clearbindings(hndl);
    cdb2_bind_param(hndl, "buffer", CDB2_BLOB, buf, len);
    cdb2_bind_param(hndl, "lux", CDB2_INTEGER, &lux, sizeof(lux));
    cdb2_bind_param(hndl, "flags", CDB2_INTEGER, &flags, sizeof(flags));

    int rc = cdb2_run_statement(hndl, "exec procedure comdb2_legacy(@buffer, @lux, @flags)");
    if (rc) {
        printf("%s run rc %d: %s\n", name, rc, cdb2_errstr(hndl));
        return;
    }

    int db_rc = *(int32_t *)cdb2_column_value(hndl, 0);
    const char *rsp = cdb2_column_value(hndl, 1);
    int rsp_len = cdb2_column_size(hndl, 1);

    const char *errstr = "";
    if (rsp && rsp_len >= REQ_HDR_LEN + ERRSTAT_LEN)
        errstr = rsp + ERRSTAT_STR_OFF;
    printf("%s rc=%d errstr=\"%s\"\n", name, db_rc, errstr);
}

static void send_write(void)
{
    uint8_t buf[REQ_HDR_LEN + BLOCK_REQ_LEN + PACKEDREQ_HDR_LEN + PACKEDREQ_ADDKL_LEN] = {0};
    uint8_t *end = buf + sizeof(buf);
    int end_offset = one_based_word_offset(buf, end);

    buf[REQ_HDR_LEN - 1] = OP_BLOCK;

    uint8_t *breq = buf + REQ_HDR_LEN;
    put_u32(breq + 0, BLKF_ERRSTAT);
    put_u32(breq + 4, end_offset);
    put_u32(breq + 8, 1);

    uint8_t *op = breq + BLOCK_REQ_LEN;
    put_u32(op + 0, BLOCK2_ADDKL);
    put_u32(op + 4, end_offset);

    submit("write", buf, sizeof(buf));
}

static void send_read(void)
{
    uint8_t buf[REQ_HDR_LEN] = {0};
    buf[REQ_HDR_LEN - 1] = OP_FIND;
    submit("read", buf, sizeof(buf));
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <dbname> <host>\n", argv[0]);
        return 1;
    }

    int rc = cdb2_open(&hndl, argv[1], argv[2], CDB2_SET_TAGGED | CDB2_DIRECT_CPU);
    if (rc) {
        fprintf(stderr, "cdb2_open(%s, %s) rc=%d: %s\n", argv[1], argv[2], rc, cdb2_errstr(hndl));
        return 1;
    }

    send_write();
    send_read();

    cdb2_close(hndl);
    return 0;
}
