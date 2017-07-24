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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <stddef.h>
#include <string.h>
#include <strings.h>

#include "comdb2.h"
#include "csc2c.h"
#include "remote.h"

static char *tormtreq2a(int opcode)
{
    switch (opcode) {
    case CDB2_OPINIT:
        return "init";
    case CDB2_OPFIND:
        return "find";
    case CDB2_OPNEXT:
        return "next";
    case CDB2_OPPREV:
        return "prev";
    case CDB2_OPFRRN:
        return "findrrn";
    case CDB2_OPLDUP:
        return "fldup";
    default:
        return "???";
    }
}

/* routines to service forwarded index find calls from other comdb2s */
int tormtfind_int(int opcode, struct ireq *iq)
{
#if 0
    struct find_req *req;
    struct find_rsp *rsp;
    int rc;
    char *dta;
    char key[MAXKEYLEN];
    int rrn;
    unsigned long long genid;
    struct dbtable *db;
    int fndlen;
    int ix;
    char *bp;
    int rnum;
    struct ireq iqv = {0};
    int i;


    req = (struct find_req*) iq->rq;
    rsp = (struct find_rsp*) iq->rq;

#if 0
    {
        printf("%s ixnum %d keylen %d key ", tormtreq2a(req->opcode), req->ixnum, req->keylen);
        for (i = 0; i < req->keylen; i++)
            printf("%02x", ((unsigned char*) req->dta)[i]);
        printf("\n");
    }
#endif

    db = get_dbtable_by_name(req->table);
    if (db == NULL)
        return ERR_BADREQ;
    dta = malloc(getdatsize(db));
    ix = req->ixnum;
    iqv = *iq;
    iqv.usedb = db;

    if (req->ixnum < 0 || req->ixnum >= db->nix)
        return ERR_BADREQ;
    if (req->keylen < 0 || req->keylen > MAXKEYLEN)
        return ERR_BADREQ;

again:
    if (opcode == OP_RMTFIND)
        rc = ix_find(&iqv, req->ixnum, req->dta, req->keylen, key, &rrn, &genid, dta, &fndlen, getdatsize(db));
    else if (opcode == OP_RMTFINDLASTDUP)
        rc = ix_find_last_dup_rnum(&iqv, req->ixnum, req->dta, req->keylen, key, &rrn, &genid, dta, &fndlen, &rnum, getdatsize(db));
    else {
        free(dta);
        return ERR_BADREQ;
    }


    iq->ixused = req->ixnum;
    iq->ixstepcnt=1;
    if (is_good_ix_find_rc(rc)) {
        rsp->keylen = getkeysize(db, ix);
        rsp->dtalen = fndlen;
        bp = rsp->dta;
        memcpy(bp, key, rsp->keylen);
        bp += rsp->keylen;
        memcpy(bp, dta, rsp->dtalen);
    }
    else {
        rsp->keylen = 0;
        rsp->dtalen = 0;
    }
    rsp->rrn = rrn;
    rsp->genid = genid;
    free(dta);
    iq->reply_len = offsetof(struct find_rsp, dta) + rsp->keylen + rsp->dtalen;
    rsp->cksum = 0;
    rsp->cksum = crc32c(((int*) rsp) + 2, iq->reply_len-8);

    return rc;
#else
    fprintf(stderr, "%s: TODO littlendian\n", __func__);
    abort();
#endif
}

int tormtfind(struct ireq *iq) { return tormtfind_int(OP_RMTFIND, iq); }

int tormtfindlastdup(struct ireq *iq)
{
    return tormtfind_int(OP_RMTFINDLASTDUP, iq);
}

int tormtfindnext_int(int opcode, struct ireq *iq)
{
#if 0
    struct findnext_req *req;
    struct findnext_rsp *rsp;
    char *bp;
    struct dbtable *db;
    char *dta;
    char key[MAXKEYLEN];
    int rc;
    int fndrrn;
    unsigned long long genid;
    int fndlen;
    int ix;
    struct ireq iqv = {0};


    req = (struct findnext_req*) iq->rq;
    rsp = (struct findnext_rsp*) iq->rq;

    db = get_dbtable_by_name(req->table);
    if (db == NULL)
        return ERR_BADREQ;

    if (req->ixnum < 0 || req->ixnum >= db->nix)
        return ERR_BADREQ;
    if (req->keylen < 0 || req->keylen > MAXKEYLEN)
        return ERR_BADREQ;
#if 0
    {
        int len;
        int i;
        printf("%s ixnum %d keylen %d key ", tormtreq2a(req->opcode), req->ixnum, req->keylen);
        for (i = 0; i < req->keylen; i++)
            printf("%02x", ((unsigned char*) req->dta)[i]);
        len = getkeysize(db, req->ixnum);
        printf(" rrn %d last ", req->lastrrn);
        for (i = 0; i < len; i++)
            printf("%02x", ((unsigned char*) req->dta)[req->keylen + i]);
        printf("\n");
    }
#endif


    iqv = *iq;
    iqv.usedb = db;
    dta = malloc(getdatsize(db));
    ix = req->ixnum;
    iq->ixused = ix;
    iq->ixstepcnt=1;
    genid = req->lastgenid;
#if 0
    printf("prev genid %016llx key ", genid);
    hexdumpdta(req->dta + req->keylen, getkeysize(db, ix));
    printf("\n");
#endif
again:
    if (opcode == OP_RMTFINDNEXT)
        rc = ix_next(&iqv, req->ixnum, req->dta, req->keylen, req->dta + req->keylen, 
                     req->lastrrn, genid, key, &fndrrn, &genid, dta, &fndlen, getdatsize(db), 0);
    else if (opcode == OP_RMTFINDPREV)
        rc = ix_prev(&iqv, req->ixnum, req->dta, req->keylen, req->dta + req->keylen, 
                     req->lastrrn, genid, key, &fndrrn, &genid, dta, &fndlen, getdatsize(db), 0);
    else {
        free(dta);
        return ERR_BADREQ;
    }


    if (rc == IX_FND || rc == IX_NOTFND || rc == IX_FNDMORE || rc == IX_PASTEOF) {
        rsp->keylen = getkeysize(db, ix);
        rsp->dtalen = fndlen;
        bp = rsp->dta;
        memcpy(bp, key, rsp->keylen);
        bp += rsp->keylen;
        memcpy(bp, dta, rsp->dtalen);

#if 0
        printf("found genid %016llx key ", genid);
        hexdumpdta(key, getkeysize(db, ix));
        printf("\n");
#endif
    }
    else {
        rsp->keylen = 0;
        rsp->dtalen = 0;
    }
    rsp->foundrrn = fndrrn;
    rsp->genid = genid;
    free(dta);
    iq->reply_len = offsetof(struct findnext_rsp, dta) + rsp->keylen + rsp->dtalen;
    rsp->cksum = 0;
    rsp->cksum = crc32c(((int*) rsp) + 2, iq->reply_len-8);
    return rc;
#else
    fprintf(stderr, "%s: TODO littlendian\n", __func__);
    abort();
#endif
}

int tormtfindnext(struct ireq *iq)
{
    return tormtfindnext_int(OP_RMTFINDNEXT, iq);
}

int tormtfindprev(struct ireq *iq)
{
    return tormtfindnext_int(OP_RMTFINDPREV, iq);
}

int tormtfindrrn(struct ireq *iq)
{
#if 0
    struct findrrn_req *req;
    struct findrrn_rsp *rsp;
    int rc;
    char key[MAXKEYLEN];
    char *dta;
    struct dbtable *db;
    int rrn;
    unsigned long long genid;
    int fndlen;
    struct ireq iqv = {0};


    req = (struct findrrn_req*) iq->rq;
    rsp = (struct findrrn_rsp*) iq->rq;
    db = get_dbtable_by_name(req->table);
    if (db == NULL)
        return ERR_BADREQ;

#if 0
    {
        int len;
        printf("%s rrn %d\n", tormtreq2a(req->opcode), req->rrn);
    }
#endif


    dta = malloc(getdatsize(db));
    rrn = req->rrn;
    genid = req->genid;
    iqv = *iq;
    iqv.usedb = db;

    rc = ix_find_by_rrn_and_genid(&iqv, req->rrn, genid, dta, &fndlen, getdatsize(db));
    if (rc) {
        rsp->dtalen = 0;
    }
    else 
    {
       rsp->dtalen = fndlen;
       memcpy(rsp->dta, dta, rsp->dtalen);
    }
    iq->reply_len = offsetof(struct findrrn_rsp, dta) + rsp->dtalen;
    rsp->cksum = 0;
    rsp->cksum = crc32c(((int*) rsp) + 2, iq->reply_len-8);
    free(dta);
    return rc;
#else
    fprintf(stderr, "%s: TODO littlendian\n", __func__);
    abort();
#endif
}
