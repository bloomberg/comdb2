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

#include <stdio.h>
#include <stdlib.h>
#include <alloca.h>
#include <errno.h>
#include <sys/types.h>
#include <netinet/in.h>
#include "comdb2.h"
#include "tag.h"
#include "bdb_sqlstat1.h"
#include "logmsg.h"

#define is_stat1(x) ((x)[11] == '1')
#define is_stat2(x) ((x)[11] == '2')
#define is_stat4(x) ((x)[11] == '4')

/*
 * Retrieve the field named 'fld' from the sqlite_stat1 ondisk record passed in
 * as an argument.  This converts everything to null-terminated cstrings.
 */
void *get_field_from_sqlite_stat_rec(struct ireq *iq, const void *rec,
                                     const char *fld)
{
    struct schema *s;
    const struct field *f;
    void *rtn;
    uint8_t *in;
    int rc;
    int fix;
    int flen;
    int outfsz;
    int outtype;
    int null = 0;

    s = iq->usedb->schema;
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s: cannot find schema for sqlite_stat1\n", __func__);
        return NULL;
    }

    fix = find_field_idx_in_tag(s, fld);
    if (fix < 0) {
        logmsg(LOGMSG_ERROR, "%s: couldn't find '%s' field in %s's ONDISK tag\n",
                __func__, fld, iq->usedb->dbname);
        return NULL;
    }

    f = schema_get_field_n((const struct schema *)s, fix);
    flen = field_get_length(f);
    rtn = malloc(flen - 1);

    switch (field_get_type(f)) {
    case SERVER_BCSTR:
        outtype = CLIENT_CSTR;
        break;
    case SERVER_BYTEARRAY:
        outtype = CLIENT_BYTEARRAY;
        break;
    case SERVER_BINT:
        outtype = CLIENT_INT;
        break;
    default:
        logmsg(LOGMSG_ERROR, "%s: unmapped type\n", __func__);
        break;
    }

    if (0 == strcmp(fld, "samplelen")) {
        in = ((uint8_t *)rec) + f->offset + 1;
        memcpy(rtn, in, sizeof(int));
        outfsz = flen;
        rc = 0;
    } else {
        rc = SERVER_to_CLIENT(field_get_buf(f, rec), flen, field_get_type(f),
                              field_get_conv_opts(f), NULL, 0, rtn, flen - 1,
                              outtype, &null, &outfsz, NULL, NULL);
    }

    if (-1 == rc) {
        logmsg(LOGMSG_ERROR, "%s: SERVER_to_CLIENT returns %d\n", __func__, rc);
        free(rtn);
        return NULL;
    }

    if (outfsz > flen + 1) {
        logmsg(LOGMSG_ERROR, "%s: SERVER_to_CLIENT: unexpected outlen: %d > %d\n",
                __func__, outfsz, flen + 1);
        free(rtn);
        return NULL;
    }

    return rtn;
}

/*
 * Determines whether there is a pre-existing sqlite_stat1 record for this
 * column and index.  'rec' is an sqlite_stat1 record in .ONDISK format.
 * Determine if this record already exists in sqlite_stat1.  If it does,
 * return the record's genid.
 */
int sqlstat_find_record(struct ireq *iq, void *trans, const void *rec,
                        unsigned long long *genid)
{
    struct dbtable *sdb;
    int rc;
    int fndrc;
    int fndlen;
    int rrn;
    char key[MAXKEYLEN];
    char fndkey[MAXKEYLEN];

    /* set db */
    sdb = iq->usedb;
    rc = stag_to_stag_buf(sdb->dbname, ".ONDISK", rec, ".ONDISK_IX_0", key,
                          NULL);

    if (rc)
        return ERR_CONVERT_IX;

    fndrc = ix_find_trans(iq, trans, 0, key, getkeysize(sdb, 0), fndkey, &rrn,
                          genid, NULL, &fndlen, MAXKEYLEN);

    /* return immediately for internal retry */
    if (RC_INTERNAL_RETRY == fndrc) {
        return fndrc;
    }

    /* found it */
    if (fndrc == IX_FND) {
        return 0;
    }

    /* didn't find it */
    if (fndrc == IX_NOTFND || fndrc == IX_EMPTY || fndrc == IX_PASTEOF) {
        return 1;
    }

    /* error */
    logmsg(LOGMSG_ERROR, "%s: ix_find_trans returns %d\n", __func__, fndrc);
    return -1;
}

/* given this tbl, ix and stat, create an ondisk record */
int stat1_ondisk_record(struct ireq *iq, char *tbl, char *ix, char *stat,
                        void **out)
{
    struct schema *s;
    unsigned char *rec;
    int rc;
    int fix;

    /* find the tag schema */
    s = iq->usedb->schema;
    if (!s) {
        logmsg(LOGMSG_ERROR, "%s: cannot find sqlite_stat1??\n", __func__);
        return -1;
    }

    /* this is a large record (32 + 32 + 4096 + overhead) so malloc */
    rec = malloc(s->recsize);

    /* cycle through fields & punt if we see something we don't recognize */
    for (fix = 0; fix < s->nmembers; fix++) {
        struct field *f = &s->member[fix];
        char *cur = NULL;
        int outdtsz = 0, null = 0;

        if (0 == strcmp(f->name, "tbl")) {
            cur = tbl;
        } else if (0 == strcmp(f->name, "idx")) {
            cur = ix;
        } else if (0 == strcmp(f->name, "stat")) {
            if (!stat) {
                cur = "";
                null = 1;
            } else {
                cur = stat;
            }
        } else {
            logmsg(LOGMSG_ERROR, "%s: unknown field ('%s') in sqlite_stat1\n",
                    __func__, f->name);
            free(rec);
            return -1;
        }

        rc = CLIENT_to_SERVER(cur, strlen(cur) + 1, CLIENT_CSTR, null, NULL,
                              NULL, rec + f->offset, f->len, f->type, 0,
                              &outdtsz, &f->convopts, NULL);

        /* this shouldn't fail.  punt if it does */
        if (-1 == rc) {
            logmsg(LOGMSG_ERROR, "%s: CLIENT_to_SERVER returns %d\n", __func__, rc);
            free(rec);
            return -1;
        }
    }

    *out = rec;
    return 0;
}

/* just like sqlstat_find_record but it actuallys gets the rest of the
 * record in rec */
int sqlstat_find_get_record(struct ireq *iq, void *trans, void *rec,
                            unsigned long long *genid)
{
    struct dbtable *sdb;
    int rc;
    int fndrc;
    int fndlen;
    int rrn;
    char key[MAXKEYLEN];
    char fndkey[MAXKEYLEN];

    /* set db */
    sdb = iq->usedb;
    rc = stag_to_stag_buf(sdb->dbname, ".ONDISK", rec, ".ONDISK_IX_0", key,
                          NULL);

    if (rc)
        return ERR_CONVERT_IX;

    fndrc = ix_find_trans(iq, trans, 0, key, getkeysize(sdb, 0), fndkey, &rrn,
                          genid, rec, &fndlen, MAXKEYLEN);

    /* return immediately for internal retry */
    if (RC_INTERNAL_RETRY == fndrc) {
        return fndrc;
    }

    /* found it */
    if (fndrc == IX_FND) {
        return 0;
    }

    /* didn't find it */
    if (fndrc == IX_NOTFND || fndrc == IX_EMPTY || fndrc == IX_PASTEOF) {
        return 1;
    }

    /* error */
    logmsg(LOGMSG_ERROR, "%s: ix_find_trans returns %d\n", __func__, fndrc);
    return -1;
}
