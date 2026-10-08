/* Send tagged requests on behalf of another program, the way a proxy does:
 * pass the origin program as the 4th argument of comdb2_legacy.  Each request
 * uses a new connection, so the connection's clientstats reference is
 * released at close. */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <arpa/inet.h>
#include <cdb2api.h>

struct req_hdr {
    int ver1;
    short ver2;
    unsigned char luxref;
    unsigned char opcode;
};

struct block_req {
    int flags;
    int offset;
    int num_reqs;
};

struct packedreq_hdr {
    int opcode;
    int nxt;
};

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <dbname> <host> <origin> <count>\n", argv[0]);
        return 1;
    }

    const char *dbname = argv[1];
    const char *host = argv[2];
    char *origin = argv[3];
    int count = atoi(argv[4]);
    int lux = 0;
    int flags = 0;

    /* A block request with one bad sub-request: it's rejected with an error,
     * which is all we need to go through handle_ireq. */
    char buf[64] = {0};
    struct req_hdr hdr = {0};
    struct block_req breq = {0};
    struct packedreq_hdr preq = {0};
    hdr.opcode = 100;
    breq.num_reqs = htonl(1);
    breq.offset = htonl(sizeof(hdr) + sizeof(breq));
    preq.opcode = 800;
    memcpy(buf, &hdr, sizeof(hdr));
    memcpy(buf + sizeof(hdr), &breq, sizeof(breq));
    memcpy(buf + sizeof(hdr) + sizeof(breq), &preq, sizeof(preq));

    for (int i = 0; i < count; i++) {
        cdb2_hndl_tp *hndl;
        int rc = cdb2_open(&hndl, dbname, host, CDB2_SET_TAGGED | CDB2_DIRECT_CPU);
        if (rc) {
            fprintf(stderr, "cdb2_open(%s, %s) rc=%d: %s\n", dbname, host, rc, cdb2_errstr(hndl));
            return 1;
        }
        cdb2_bind_param(hndl, "buffer", CDB2_BLOB, buf, sizeof(buf));
        cdb2_bind_param(hndl, "lux", CDB2_INTEGER, &lux, sizeof(lux));
        cdb2_bind_param(hndl, "flags", CDB2_INTEGER, &flags, sizeof(flags));
        cdb2_bind_param(hndl, "origin", CDB2_CSTRING, origin, strlen(origin));
        rc = cdb2_run_statement(hndl, "exec procedure comdb2_legacy(@buffer, @lux, @flags, @origin)");
        if (rc) {
            fprintf(stderr, "run rc %d: %s\n", rc, cdb2_errstr(hndl));
            cdb2_close(hndl);
            return 1;
        }
        while ((rc = cdb2_next_record(hndl)) == CDB2_OK)
            ;
        cdb2_close(hndl);
    }
    return 0;
}
