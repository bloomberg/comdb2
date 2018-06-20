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

#include <signal.h>
#include <pthread.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <stdarg.h>
#include <stdio.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>

#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include <net_types.h>
#include "comdb2.h"
#include "util.h"
#include <queue.h>
#include "prefault.h"
#include <assert.h>
#include <compile_time_assert.h>
#include <netinet/in.h>

extern int gbl_ready;

static void send_to_all(struct dbenv *dbenv, void *dta, int dtalen, int flush)
{
    int rc;
    int count;
    const char *hostlist[REPMAX];
    int i;

    count = net_get_all_nodes_connected(dbenv->handle_sibling, hostlist);

    for (i = 0; i < count; i++) {
        rc = net_send(dbenv->handle_sibling, hostlist[i], NET_PREFAULT2_OPS,
                      dta, dtalen, flush);
    }
}

/* big enough to store the base part of any message */
#define BASESZ 64

struct pfrq_old_new_key {
    short type;
    short dbix;
    short ixnum;
    short keylen;
    uint8_t keydta[1];
};
enum { PFRQ_OLD_NEW_KEY_OFFSET = 2 + 2 + 2 + 2 };
BB_COMPILE_TIME_ASSERT(pfrq_old_new_key_len,
                       offsetof(struct pfrq_old_new_key, keydta) ==
                           PFRQ_OLD_NEW_KEY_OFFSET);

static uint8_t *
pfrq_old_new_key_put(const struct pfrq_old_new_key *p_pfrq_old_new_key,
                     uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PFRQ_OLD_NEW_KEY_OFFSET > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_pfrq_old_new_key->type),
                    sizeof(p_pfrq_old_new_key->type), p_buf, p_buf_end);
    p_buf = buf_put(&(p_pfrq_old_new_key->dbix),
                    sizeof(p_pfrq_old_new_key->dbix), p_buf, p_buf_end);
    p_buf = buf_put(&(p_pfrq_old_new_key->ixnum),
                    sizeof(p_pfrq_old_new_key->ixnum), p_buf, p_buf_end);
    p_buf = buf_put(&(p_pfrq_old_new_key->keylen),
                    sizeof(p_pfrq_old_new_key->keylen), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
pfrq_old_new_key_get(struct pfrq_old_new_key *p_pfrq_old_new_key,
                     const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PFRQ_OLD_NEW_KEY_OFFSET > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_pfrq_old_new_key->type),
                    sizeof(p_pfrq_old_new_key->type), p_buf, p_buf_end);
    p_buf = buf_get(&(p_pfrq_old_new_key->dbix),
                    sizeof(p_pfrq_old_new_key->dbix), p_buf, p_buf_end);
    p_buf = buf_get(&(p_pfrq_old_new_key->ixnum),
                    sizeof(p_pfrq_old_new_key->ixnum), p_buf, p_buf_end);
    p_buf = buf_get(&(p_pfrq_old_new_key->keylen),
                    sizeof(p_pfrq_old_new_key->keylen), p_buf, p_buf_end);

    return p_buf;
}

struct pfrq_olddata {
    short type;
    short dbix;
    int genid[2];
};
enum { PFRQ_OLDDATA_LEN = 2 + 2 + (2 * 4) };
BB_COMPILE_TIME_ASSERT(pfrq_olddata_len,
                       sizeof(struct pfrq_olddata) == PFRQ_OLDDATA_LEN);

static uint8_t *pfrq_olddata_put(const struct pfrq_olddata *p_pfrq_olddata,
                                 uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PFRQ_OLDDATA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_pfrq_olddata->type), sizeof(p_pfrq_olddata->type),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_pfrq_olddata->dbix), sizeof(p_pfrq_olddata->dbix),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_pfrq_olddata->genid),
                           sizeof(p_pfrq_olddata->genid), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *pfrq_olddata_get(struct pfrq_olddata *p_pfrq_olddata,
                                       const uint8_t *p_buf,
                                       const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PFRQ_OLDDATA_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_pfrq_olddata->type), sizeof(p_pfrq_olddata->type),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_pfrq_olddata->dbix), sizeof(p_pfrq_olddata->dbix),
                    p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_pfrq_olddata->genid),
                           sizeof(p_pfrq_olddata->genid), p_buf, p_buf_end);

    return p_buf;
}

struct pfrq_olddata_oldkeys {
    short type;
    short dbix;
    int genid[2];
};
enum { PFRQ_OLDDATA_OLDKEYS_LEN = 2 + 2 + (2 * 4) };

static uint8_t *pfrq_olddata_oldkeys_put(
    const struct pfrq_olddata_oldkeys *p_pfrq_olddata_oldkeys, uint8_t *p_buf,
    const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PFRQ_OLDDATA_OLDKEYS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_pfrq_olddata_oldkeys->type),
                    sizeof(p_pfrq_olddata_oldkeys->type), p_buf, p_buf_end);
    p_buf = buf_put(&(p_pfrq_olddata_oldkeys->dbix),
                    sizeof(p_pfrq_olddata_oldkeys->dbix), p_buf, p_buf_end);
    p_buf =
        buf_no_net_put(&(p_pfrq_olddata_oldkeys->genid),
                       sizeof(p_pfrq_olddata_oldkeys->genid), p_buf, p_buf_end);

    return p_buf;
}

static const uint8_t *
pfrq_olddata_oldkeys_get(struct pfrq_olddata_oldkeys *p_pfrq_olddata_oldkeys,
                         const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || PFRQ_OLDDATA_OLDKEYS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_pfrq_olddata_oldkeys->type),
                    sizeof(p_pfrq_olddata_oldkeys->type), p_buf, p_buf_end);
    p_buf = buf_get(&(p_pfrq_olddata_oldkeys->dbix),
                    sizeof(p_pfrq_olddata_oldkeys->dbix), p_buf, p_buf_end);
    p_buf =
        buf_no_net_get(&(p_pfrq_olddata_oldkeys->genid),
                       sizeof(p_pfrq_olddata_oldkeys->genid), p_buf, p_buf_end);

    return p_buf;
}

struct pfrq_olddata_oldkeys_newkeys {
    short type;
    short dbix;
    int genid[2];
    short taglen;
    short reclen;
    /* tag and record follow */
};
enum { PFRQ_OLDDATA_OLDKEYS_NEWKEYS_LEN = 2 + 2 + (2 * 4) + 2 + 2 };
BB_COMPILE_TIME_ASSERT(pfrq_olddata_oldkeys_newkeys_len,
                       sizeof(struct pfrq_olddata_oldkeys_newkeys) ==
                           PFRQ_OLDDATA_OLDKEYS_NEWKEYS_LEN);

static uint8_t *pfrq_olddata_oldkeys_newkeys_put(
    const struct pfrq_olddata_oldkeys_newkeys *p_pfrq_olddata_oldkeys_newkeys,
    uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        PFRQ_OLDDATA_OLDKEYS_NEWKEYS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_put(&(p_pfrq_olddata_oldkeys_newkeys->type),
                sizeof(p_pfrq_olddata_oldkeys_newkeys->type), p_buf, p_buf_end);
    p_buf =
        buf_put(&(p_pfrq_olddata_oldkeys_newkeys->dbix),
                sizeof(p_pfrq_olddata_oldkeys_newkeys->dbix), p_buf, p_buf_end);
    p_buf = buf_no_net_put(&(p_pfrq_olddata_oldkeys_newkeys->genid),
                           sizeof(p_pfrq_olddata_oldkeys_newkeys->genid), p_buf,
                           p_buf_end);
    p_buf = buf_put(&(p_pfrq_olddata_oldkeys_newkeys->taglen),
                    sizeof(p_pfrq_olddata_oldkeys_newkeys->taglen), p_buf,
                    p_buf_end);
    p_buf = buf_put(&(p_pfrq_olddata_oldkeys_newkeys->reclen),
                    sizeof(p_pfrq_olddata_oldkeys_newkeys->reclen), p_buf,
                    p_buf_end);

    return p_buf;
}

static const uint8_t *pfrq_olddata_oldkeys_newkeys_get(
    struct pfrq_olddata_oldkeys_newkeys *p_pfrq_olddata_oldkeys_newkeys,
    const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf ||
        PFRQ_OLDDATA_OLDKEYS_NEWKEYS_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf =
        buf_get(&(p_pfrq_olddata_oldkeys_newkeys->type),
                sizeof(p_pfrq_olddata_oldkeys_newkeys->type), p_buf, p_buf_end);
    p_buf =
        buf_get(&(p_pfrq_olddata_oldkeys_newkeys->dbix),
                sizeof(p_pfrq_olddata_oldkeys_newkeys->dbix), p_buf, p_buf_end);
    p_buf = buf_no_net_get(&(p_pfrq_olddata_oldkeys_newkeys->genid),
                           sizeof(p_pfrq_olddata_oldkeys_newkeys->genid), p_buf,
                           p_buf_end);
    p_buf = buf_get(&(p_pfrq_olddata_oldkeys_newkeys->taglen),
                    sizeof(p_pfrq_olddata_oldkeys_newkeys->taglen), p_buf,
                    p_buf_end);
    p_buf = buf_get(&(p_pfrq_olddata_oldkeys_newkeys->reclen),
                    sizeof(p_pfrq_olddata_oldkeys_newkeys->reclen), p_buf,
                    p_buf_end);

    return p_buf;
}

int broadcast_prefault(struct dbenv *dbenv, pfrq_t *qdata)
{
    int err = 0;
    int ln;
    uint8_t *p_buf;
    const uint8_t *p_buf_end;
    unsigned char *dta;
    unsigned char stackbuf[BASESZ];

    /* setup the output buffer */
    switch (qdata->type) {
    case PFRQ_OLDKEY:
    case PFRQ_NEWKEY:
        ln = qdata->len + BASESZ;
        p_buf = dta = malloc(ln);
        p_buf_end = p_buf + ln;
        break;

    case PFRQ_OLDDATA_OLDKEYS_NEWKEYS:
        ln = qdata->len + strlen(qdata->tag) + BASESZ;
        p_buf = dta = malloc(ln);
        p_buf_end = p_buf + ln;
        break;

    case PFRQ_OLDDATA:
    case PFRQ_OLDDATA_OLDKEYS:
        ln = BASESZ;
        p_buf = dta = stackbuf;
        p_buf_end = p_buf + ln;
        break;

    default:
        return 0;
    }

    switch (qdata->type) {
    case PFRQ_OLDKEY:
    case PFRQ_NEWKEY: {
        struct pfrq_old_new_key pfrq_old_new_key_req;
        dbenv->prefault_stats.num_prfq_key_broadcast++;

        pfrq_old_new_key_req.type = qdata->type;
        pfrq_old_new_key_req.dbix = qdata->db->dbs_idx;
        pfrq_old_new_key_req.ixnum = qdata->index;
        pfrq_old_new_key_req.keylen = qdata->len;

        /* write the buffer */
        if (!(p_buf = pfrq_old_new_key_put(&pfrq_old_new_key_req, p_buf,
                                           p_buf_end))) {
            fprintf(stderr, "%s: line %d error writing pfrq structure\n",
                    __func__, __LINE__);
            err = 1;
        }

        /* write the key data */
        if (!err &&
            !(p_buf =
                  buf_no_net_put(qdata->key, qdata->len, p_buf, p_buf_end))) {
            fprintf(stderr, "%s: line %d error writing pfrq key payload\n",
                    __func__, __LINE__);
            err = 1;
        }
        break;
    }
    case PFRQ_OLDDATA: {
        struct pfrq_olddata pfrq_olddata_req;
        dbenv->prefault_stats.num_prfq_data_broadcast++;

        pfrq_olddata_req.type = qdata->type;
        pfrq_olddata_req.dbix = qdata->db->dbs_idx;
        memcpy(&(pfrq_olddata_req.genid), &qdata->genid,
               sizeof(unsigned long long));

        if (!(p_buf = pfrq_olddata_put(&pfrq_olddata_req, p_buf, p_buf_end))) {
            fprintf(stderr, "%s: line %d error writing pfrq olddata\n",
                    __func__, __LINE__);
            err = 1;
        }
        break;
    }
    case PFRQ_OLDDATA_OLDKEYS: {
        struct pfrq_olddata_oldkeys pfrq_olddata_oldkeys_req;
        dbenv->prefault_stats.num_prfq_data_keys_broadcast++;

        pfrq_olddata_oldkeys_req.type = qdata->type;
        pfrq_olddata_oldkeys_req.dbix = qdata->db->dbs_idx;
        memcpy(&(pfrq_olddata_oldkeys_req.genid), &qdata->genid,
               sizeof(unsigned long long));

        if (!(p_buf = pfrq_olddata_oldkeys_put(&pfrq_olddata_oldkeys_req, p_buf,
                                               p_buf_end))) {
            fprintf(stderr, "%s: line %d error writing pfrq olddata oldkeys\n",
                    __func__, __LINE__);
            err = 1;
        }
        break;
    }
    case PFRQ_OLDDATA_OLDKEYS_NEWKEYS: {
        struct pfrq_olddata_oldkeys_newkeys pfrq_olddata_oldkeys_newkeys_req;
        dbenv->prefault_stats.num_prfq_data_keys_newkeys_broadcast++;

        pfrq_olddata_oldkeys_newkeys_req.type = qdata->type;
        pfrq_olddata_oldkeys_newkeys_req.dbix = qdata->db->dbs_idx;
        pfrq_olddata_oldkeys_newkeys_req.taglen = strlen(qdata->tag) + 1;
        pfrq_olddata_oldkeys_newkeys_req.reclen = qdata->len;
        memcpy(&pfrq_olddata_oldkeys_newkeys_req.genid, &qdata->genid,
               sizeof(unsigned long long));

        if (!(p_buf = pfrq_olddata_oldkeys_newkeys_put(
                  &pfrq_olddata_oldkeys_newkeys_req, p_buf, p_buf_end))) {

            fprintf(stderr, "%s: line %d error writing pfrq olddata oldkeys "
                            "newkeys header\n",
                    __func__, __LINE__);
            err = 1;
        }

        if (!err &&
            !(p_buf = buf_no_net_put(qdata->tag,
                                     pfrq_olddata_oldkeys_newkeys_req.taglen,
                                     p_buf, p_buf_end))) {
            fprintf(stderr, "%s: line %d error writing pfrq olddata oldkeys "
                            "tag\n",
                    __func__, __LINE__);
            err = 1;
        }

        if (!err &&
            !(p_buf = buf_no_net_put(qdata->record, qdata->len, p_buf,
                                     p_buf_end))) {
            fprintf(stderr, "%s: line %d error writing pfrq olddata oldkeys "
                            "record\n",
                    __func__, __LINE__);
            err = 1;
        }

        break;
    }
    }

    /* now send it to all nodes */

    if (!err) {
        int dtalen = (p_buf - dta);
        send_to_all(dbenv, dta, dtalen, qdata->flush);
    }

    switch (qdata->type) {
    case PFRQ_OLDKEY:
    case PFRQ_NEWKEY:
    case PFRQ_OLDDATA_OLDKEYS_NEWKEYS:
        free(dta);
    }

    return 0;
}

int process_broadcast_prefault(struct dbenv *dbenv, unsigned char *dta,
                               int dtalen, int is_tcp)
{
    int rc;
    const uint8_t *p_buf;
    const uint8_t *p_buf_end;
    unsigned short *type_peek;
    unsigned short *dbix_peek;
    unsigned short dbix;
    pfrq_t *qdata;

    p_buf = dta;
    p_buf_end = p_buf + dtalen;

    if (!gbl_ready)
        return 0;

    if (dbenv == NULL)
        return 0;

    if (thedb->stopped)
        return 0;

    qdata = malloc(sizeof(pfrq_t));

    /* dont re-broadcast */
    qdata->broadcast = 0;

    /* do it locally */
    qdata->dolocal = 1;

    type_peek = (unsigned short *)(dta);

    dbix_peek = (unsigned short *)(dta + sizeof(unsigned short));

    qdata->type = ntohs(*type_peek);

    dbix = ntohs(*dbix_peek);

    if (dbix > (thedb->num_dbs - 1)) {
        free(qdata);
        return 0;
    }

    qdata->db = dbenv->dbs[dbix];

    if (qdata->db->dbtype != DBTYPE_TAGGED_TABLE) {
        /*
        fprintf(stderr, "process_broadcast_prefault: rcvd pflt for bad tbl %d
        '%s'\n", (int)tmpshort, qdata->db->dbname);
        */

        free(qdata);
        return 0;
    }

    switch (qdata->type) {
    default:
        /*
        fprintf(stderr, "process_broadcast_prefault: bad pflt type %d\n",
              (int)qdata->type);
        */
        free(qdata);
        return 0;

    case PFRQ_OLDKEY:
    case PFRQ_NEWKEY: {
        struct pfrq_old_new_key pfrq_old_new_key_req;
        dbenv->prefault_stats.num_prfq_key_received++;

        if (!(p_buf = pfrq_old_new_key_get(&pfrq_old_new_key_req, p_buf,
                                           p_buf_end))) {
            fprintf(stderr, "%s: line %d error reading pfrq structure\n",
                    __func__, __LINE__);
            free(qdata);
            return 0;
        }

        qdata->type = pfrq_old_new_key_req.type;
        qdata->db->dbs_idx = pfrq_old_new_key_req.dbix;
        qdata->index = pfrq_old_new_key_req.ixnum;
        qdata->len = pfrq_old_new_key_req.keylen;

        if (qdata->len > sizeof(qdata->key)) {
            fprintf(stderr, "%s: line %d invalid keylenth in pfrq structure\n",
                    __func__, __LINE__);
            free(qdata);
            return 0;
        }

        if (!(p_buf =
                  buf_no_net_get(qdata->key, qdata->len, p_buf, p_buf_end))) {
            fprintf(stderr, "%s: line %d error copying key data\n", __func__,
                    __LINE__);
            free(qdata);
            return 0;
        }
        break;
    }
    case PFRQ_OLDDATA: {
        struct pfrq_olddata pfrq_olddata_req;
        dbenv->prefault_stats.num_prfq_data_received++;

        if (!(p_buf = pfrq_olddata_get(&pfrq_olddata_req, p_buf, p_buf_end))) {
            fprintf(stderr, "%s: line %d error reading pfrq olddata\n",
                    __func__, __LINE__);
            free(qdata);
            return 0;
        }

        qdata->type = pfrq_olddata_req.type;
        qdata->db->dbs_idx = pfrq_olddata_req.dbix;
        memcpy(&qdata->genid, &pfrq_olddata_req.genid,
               sizeof(unsigned long long));
        break;
    }
    case PFRQ_OLDDATA_OLDKEYS: {
        struct pfrq_olddata_oldkeys pfrq_olddata_oldkeys_req;
        dbenv->prefault_stats.num_prfq_data_keys_received++;

        if (!(p_buf = pfrq_olddata_oldkeys_get(&pfrq_olddata_oldkeys_req, p_buf,
                                               p_buf_end))) {
            fprintf(stderr, "%s: line %d error reading pfrq olddata\n",
                    __func__, __LINE__);
            free(qdata);
            return 0;
        }

        qdata->type = pfrq_olddata_oldkeys_req.type;
        qdata->db->dbs_idx = pfrq_olddata_oldkeys_req.dbix;
        memcpy(&qdata->genid, &pfrq_olddata_oldkeys_req.genid,
               sizeof(unsigned long long));
        break;
    }
    case PFRQ_OLDDATA_OLDKEYS_NEWKEYS: {
        struct pfrq_olddata_oldkeys_newkeys pfrq_olddata_oldkeys_newkeys_req;
        dbenv->prefault_stats.num_prfq_data_keys_newkeys_received++;

        if (!(p_buf = pfrq_olddata_oldkeys_newkeys_get(
                  &pfrq_olddata_oldkeys_newkeys_req, p_buf, p_buf_end))) {
            fprintf(stderr, "%s: line %d error writing pfrq olddata oldkeys "
                            "newkeys header\n",
                    __func__, __LINE__);
            free(qdata);
            return 0;
        }

        qdata->type = pfrq_olddata_oldkeys_newkeys_req.type;
        qdata->db->dbs_idx = pfrq_olddata_oldkeys_newkeys_req.dbix;
        qdata->taglen = pfrq_olddata_oldkeys_newkeys_req.taglen - 1;
        qdata->len = pfrq_olddata_oldkeys_newkeys_req.reclen;
        memcpy(&qdata->genid, &pfrq_olddata_oldkeys_newkeys_req.genid,
               sizeof(unsigned long long));

        if (qdata->taglen < 0) {
            free(qdata);
            return 0;
        }

        /* len is unsigned
        if(qdata->len < 0)
        {
            free(qdata);
            return 0;
        }
        */

        qdata->tag = malloc(qdata->taglen + 1);

        if (!(p_buf = buf_no_net_get(qdata->tag, qdata->taglen + 1, p_buf,
                                     p_buf_end))) {
            fprintf(stderr, "%s: line %d error reading pfrq olddata oldkeys "
                            "newkeys tag data\n",
                    __func__, __LINE__);
            free(qdata->tag);
            free(qdata);
            return 0;
        }

        qdata->tag[qdata->taglen] = '\0';

        qdata->record = malloc(qdata->len);

        if (!(p_buf = buf_no_net_get(qdata->record, qdata->len, p_buf,
                                     p_buf_end))) {
            fprintf(stderr, "%s: line %d error reading pfrq olddata oldkeys "
                            "newkeys record data\n",
                    __func__, __LINE__);
            free(qdata->tag);
            free(qdata->record);
            free(qdata);
            return 0;
        }
        break;
    }
    }

    /* we have no iq! */
    qdata->iq = NULL;
    qdata->opnum = 0;
    qdata->helper_thread = -1;
    qdata->seqnum = 0;

    rc = enque_pfault_ll(dbenv, qdata);

    if (rc != 0) {
        if (qdata->type == PFRQ_OLDDATA_OLDKEYS_NEWKEYS) {
            free(qdata->tag);
            free(qdata->record);
        }
        free(qdata);
    }

    return 0;
}
