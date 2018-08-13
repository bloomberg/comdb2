#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>
#include <stdarg.h>
#include <stdint.h>

#include <cdb2api.h>

#include "strbuf.h"

int sync(cdb2_hndl_tp *from, int64_t maxfrom, cdb2_hndl_tp *to, int64_t maxto) {
    int64_t nops;
    int rc;

    rc = truncate();

    for (int64_t seqno = maxto+1; seqno <= maxfrom; seqno++) {
        int blkpos = (int) query_int(from, "select max(blkpos) from comdb2_oplog where seqno = %lld", seqno);
        // printf("seqno %lld blkpos %d\n", seqno, blkpos);
        int rc;
        rc = apply_seqno(from, to, seqno, blkpos);
        if (rc) {
            fprintf(stderr, "Failed for seqno %lld\n", seqno);
            return 1;
        }
    }
    return 0;
} 

int apply(char *fromdb, char *todb) {
    int64_t maxfrom, maxto;
    int rc;

    cdb2_hndl_tp *from, *to;
    rc = cdb2_open(&from, fromdb, "default", 0);
    if (rc) {
        fprintf(stderr, "can't open %s\n", fromdb);
        return 1;
    }
    rc = cdb2_open(&to, todb, "local", 0);
    if (rc) {
        fprintf(stderr, "can't open %s\n", todb);
        return 1;
    }
    maxfrom = query_int(from, "select max(seqno) from comdb2_oplog");
    maxto = query_int(to, "select max(seqno) from comdb2_oplog");
    // printf("from %lld to %lld\n", (long long) maxfrom, (long long) maxto);
    return sync(from, maxfrom, to, maxto);
}

int main(int argc, char *argv[]) {
    char *fromdb, *todb;

    if (getenv("CDB2_CONFIG"))
        cdb2_set_comdb2db_config(getenv("CDB2_CONFIG"));

    if (argc != 3) {
        fprintf(stderr, "usage: fromdb todb\n");
        return 1;
    }
    fromdb = argv[1];
    todb = argv[2];

    return apply(fromdb, todb);
}
