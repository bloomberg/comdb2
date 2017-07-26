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

#include "comdb2.h"
#include "tag.h"
#include "types.h"
#include "csctypes.h"
#include "block_internal.h"
#include "localrep.h"
#include "endian_core.h"
#include <flibc.h>

/* Local replicant logging routines */
int local_replicant_log_add(struct ireq *iq, void *trans, void *od_dta,
                            blob_buffer_t *blobs, int *opfailcode)
{
    struct schema *s;
    struct schema *server_schema = NULL;
    void *client_buf = NULL;
    uint8_t *serialize_buf = NULL;
    int sz;
    int nulls[32] = {0};
    int i;
    comdb2_field_type fields[512];
    uint8_t *p;
    struct field *fld;
    int offset = 0;
    struct dbtable *savedb;
    int rc;
    const uint8_t *lim;

    if (!iq->usedb->do_local_replication)
        return 0;

    s = find_tag_schema(iq->usedb->dbname, ".ONDISK_CLIENT");
    if (s == NULL) {
        return OP_FAILED_INTERNAL;
    }

    sz = get_size_of_schema(s);
    client_buf = malloc(sz);

    /* Now get the client version of the record and flush it out
       in a format the client can understand.  It goes out in this format to
       some intermediary machine (gsrv) from where it goes out via rmque. */
    rc = stag_to_ctag_buf_tz(
        iq->usedb->dbname, ".ONDISK", od_dta, -1, ".ONDISK_CLIENT", client_buf,
        (unsigned char *)nulls, 0, NULL, NULL, "US/Eastern");

    /* We send down an array of comdb2_field_types.  The offset field is the
       field in the
       offset of the data value in the buffer.  This is a bit wasteful but
       simple.
       It's especially simple for the receving side - it just calls
       dbd_dtag_bind()
       with the names, offsets and types all set up.  */

    /* pass 1: run through the schema and see how much space we'll need */
    sz = sizeof(int) + MAXTABLELEN;
    sz += s->nmembers * sizeof(comdb2_field_type);
    for (i = 0; i < s->nmembers; i++) {
        /* no point in allocating space for null values */
        if (btst(nulls, i))
            continue;
        fld = &s->member[i];
        /* not a blob - get size from schema */
        if (fld->blob_index == -1)
            sz += fld->len;
        else {
            /* vutf8 and fits in inline portion */
            if (fld->type == CLIENT_VUTF8 &&
                blobs[fld->blob_index].exists == 0) {
                if (server_schema == NULL) {
                    /* Vutf8 isn't present in client schemas. If it doesn't fit
                     * in the field,
                     * it's handled as a blob, below.   If it does, handle it
                     * here.
                     * That necessitates looking up the value in the ondisk
                     * schema. */
                    server_schema =
                        find_tag_schema(iq->usedb->dbname, ".ONDISK");
                    if (server_schema == NULL) {
                        printf("can't find schema for %s\n", iq->usedb->dbname);
                        free(client_buf);
                        return OP_FAILED_INTERNAL;
                    }
                }
                sz += (server_schema->member[i].len -
                       5); /* 5 bytes for the size field */
            } else {
                sz += blobs[fld->blob_index].length;
            }
        }
    }

    serialize_buf = malloc(sz);
    lim = serialize_buf + sz;
    p = serialize_buf;
    rc = ERR_BADREQ;
    if ((p = buf_put(&s->nmembers, sizeof(int), p, lim)) == NULL)
        goto err;
    if ((p = buf_no_net_put(iq->usedb->dbname, strlen(iq->usedb->dbname), p,
                            lim)) == NULL)
        goto err;
    if ((p = buf_zero_put(MAXTABLELEN - strlen(iq->usedb->dbname), p, lim)) ==
        NULL)
        goto err;

    offset = sizeof(int) + MAXTABLELEN;
    offset += s->nmembers * sizeof(comdb2_field_type);

    /* pass 2 - build the type entries */
    for (i = 0; i < s->nmembers; i++) {

        /* Always iterate through every field so the receiver will set null
         * values explicitly. */
        fld = &s->member[i];

        strcpy(fields[i].name, fld->name);
        client_type_to_csc2_type(fld->type, fld->len, &fields[i].type);

        if (fld->blob_index == -1)
            fields[i].len = fld->len;
        else {
            if (fld->type == CLIENT_VUTF8 &&
                blobs[fld->blob_index].exists == 0) {

                if (server_schema == NULL) {
                    server_schema =
                        find_tag_schema(iq->usedb->dbname, ".ONDISK");
                    if (server_schema == NULL) {
                        printf("can't find schema for %s\n", iq->usedb->dbname);
                        free(client_buf);
                        return OP_FAILED_INTERNAL;
                    }
                }

                fields[i].len = server_schema->member[i].len - 5;
                fields[i].type = COMDB2_CSTR;
            } else {
                fields[i].len = blobs[fld->blob_index].length;
            }
        }
        fields[i].off = offset;

        /* null bit */
        fields[i].reserved[0] = btst(nulls, i);

        if ((p = comdb2_field_type_put(&fields[i], p, lim)) == NULL)
            goto err;

        if (!fields[i].reserved[0])
            offset += fields[i].len;
    }

    /* pass 3 - build the oplog entry */
    for (i = 0; i < s->nmembers; i++) {
        fld = &s->member[i];

        /* Copy the actual value if it's not null.
         * NOTE: we converted to client type here which is big-endian regardless
         * of the
         * architecture we are running on.  So we get away with a memcpy.  The
         * field
         * metadata needs to be big-endian so we flip it above. */
        fields[i].reserved[0] = btst(nulls, i);
        if (!fields[i].reserved[0]) {
            if (fld->blob_index == -1) {
                /* copy from record */
                memcpy(p, ((char *)client_buf) + fld->offset, fld->len);
                p += fld->len;
            } else {
                if (fld->type == CLIENT_VUTF8 &&
                    blobs[fld->blob_index].exists == 0) {
                    uint8_t sz[5];
                    unsigned inline_len;
                    int isnull;
                    int outdtsz;
                    struct field_conv_opts conv = {0, 0};

                    memcpy(p, ((int8_t *)od_dta) +
                                  server_schema->member[i].offset + 5,
                           server_schema->member[i].len - 5);

/* kludge size */
#ifdef _LINUX_SOURCE
                    conv.flags |= FLD_CONV_LENDIAN;
#endif
                    memcpy(&sz,
                           ((int8_t *)od_dta) + server_schema->member[i].offset,
                           5);
                    rc = SERVER_UINT_to_CLIENT_UINT(sz, 5, &conv, NULL,
                                                    &inline_len, 4, &isnull,
                                                    &outdtsz, NULL, NULL);
                    if (rc) {
                        fprintf(
                            stderr,
                            "table %s field %: can't determine length rc %d\n",
                            iq->usedb->dbname, fld->name, rc);
                        rc = OP_FAILED_INTERNAL;
                        goto err;
                    }
                    /* null terminate if 0-length */
                    if (inline_len == 0)
                        p[0] = 0;

                    p += server_schema->member[i].len - 5;
                } else {
                    memcpy(p, blobs[fld->blob_index].data, fields[i].len);
                    p += fields[i].len;
                }
            }
        }
    }

    /* don't need the client record buffer anymore */
    rc = 0;
err:
    if (client_buf)
        free(client_buf);

    /* This calls add_record recursively.  Not because I'm into that sort of
       thing,
       but because for once it makes sense and I don't want to duplicate all
       that logic.
       We never log comdb2_oplog table inserts (see above) so it won't loop.
       (my name is NOT Steve) */
    savedb = iq->usedb;
    iq->usedb = get_dbtable_by_name("comdb2_oplog");
    if (rc == 0)
        rc = add_oplog_entry(iq, trans, LCL_OP_ADD, serialize_buf, sz);
    iq->usedb = savedb;
    /* don't need this anymore whether or not we failed */
    if (serialize_buf)
        free(serialize_buf);
    if (rc != 0) {
        *opfailcode = OP_FAILED_INTERNAL + ERR_ADD_OPLOG;
    }
    return rc;
}

int local_replicant_log_delete_for_update(struct ireq *iq, void *trans, int rrn,
                                          unsigned long long genid,
                                          int *opfailcode)
{
    /* log the delete.  once the update succeeds we log the add
       - otherwise the whole thing gets aborted. */
    long long id;
    struct dbtable *savedb;
    struct delop {
        char table[MAXTABLELEN];
        long long id;
    } delop;
    void *tmpbuf;
    int recsz;
    int fndlen;
    int rc;

    if (!iq->usedb->do_local_replication)
        return 0;

    recsz = get_size_of_schema(iq->usedb->schema);
    tmpbuf = malloc(recsz);
    if (!tmpbuf) {
        fprintf(stderr, "%s: out of memory for tmpbuf %d\n", __func__, recsz);
        *opfailcode = OP_FAILED_INTERNAL + ERR_MALLOC;
        rc = ERR_INTERNAL;
        return rc;
    }
    rc = ix_find_by_rrn_and_genid_tran(iq, rrn, genid, tmpbuf, &fndlen, recsz,
                                       trans);
    if (iq->debug)
        reqprintf(iq,
                  "ix_find_by_rrn_and_genid_tran RRN %d VGENID 0x%llx RC %d",
                  rrn, genid, rc);
    /* printf("genid %016llx rrn %d rc %d\n", vgenid, rrn, rc); */
    if (rc == 0) {
        long long id;
        strcpy(delop.table, iq->usedb->dbname);
        id = get_record_unique_id(iq->usedb, tmpbuf);
        delop.id = id;

        savedb = iq->usedb;
        iq->usedb = get_dbtable_by_name("comdb2_oplog");
        rc = add_oplog_entry(iq, trans, LCL_OP_DEL, &delop, sizeof(delop));
        iq->usedb = savedb;
        free(tmpbuf);
        if (rc != 0) {
            *opfailcode = OP_FAILED_INTERNAL + ERR_ADD_OPLOG;
            return rc;
        }
    } else {
        free(tmpbuf);
        *opfailcode = OP_FAILED_VERIFY;
        return rc;
    }
    return 0;
}

int local_replicant_log_delete(struct ireq *iq, void *trans, void *od_dta,
                               int *opfailcode)
{
    long long id;
    struct dbtable *savedb;
    struct delop {
        char table[MAXTABLELEN];
        long long id;
    } delop;
    int rc;

    if (!iq->usedb->do_local_replication)
        return 0;

    strcpy(delop.table, iq->usedb->dbname);

    id = get_record_unique_id(iq->usedb, od_dta);
    delop.id = id;

    savedb = iq->usedb;
    iq->usedb = get_dbtable_by_name("comdb2_oplog");
    rc = add_oplog_entry(iq, trans, LCL_OP_DEL, &delop, sizeof(delop));
    iq->usedb = savedb;
    if (rc != 0)
        *opfailcode = OP_FAILED_INTERNAL + ERR_ADD_OPLOG;
    return rc;
}

int local_replicant_log_add_for_update(struct ireq *iq, void *trans, int rrn,
                                       unsigned long long new_genid,
                                       int *opfailcode)
{
    struct schema *s, *server_schema = NULL;
    void *server_buf = NULL;
    void *client_buf = NULL;
    void *serialize_buf;
    int sz;
    int nulls[32] = {0};
    int i;
    comdb2_field_type fields[512];
    uint8_t *p;
    struct field *fld;
    int offset = 0;
    struct dbtable *savedb;
    int fndlen;
    int odsz, clsz;
    unsigned long long oldgenid;
    int rc;
    int using_newblobs = 0;
    blob_status_t newblobs[MAXBLOBS] = {0};
    uint8_t *lim;

    /* NOTE: 80% of this routine is a copy-and-paste job from
     * local_replicant_log_add.
     * One day, when there's nothing better to do, fix that. */

    if (!iq->usedb->do_local_replication)
        return 0;

    if (!iq->usedb->do_local_replication)
        return 0;

    s = find_tag_schema(iq->usedb->dbname, ".ONDISK");
    if (s == NULL) {
        rc = OP_FAILED_INTERNAL;
        goto done;
    }
    odsz = get_size_of_schema(s);
    server_buf = malloc(odsz);

    s = find_tag_schema(iq->usedb->dbname, ".ONDISK_CLIENT");
    if (s == NULL) {
        free(server_buf);
        server_buf = NULL;
        rc = OP_FAILED_INTERNAL;
        goto done;
    }
    clsz = get_size_of_schema(s);
    client_buf = malloc(clsz);

    /* find the NEW record, transactionally */
    rc = ix_find_by_rrn_and_genid_tran(iq, rrn, new_genid, server_buf, &fndlen,
                                       odsz, trans);
    if (rc) {
        free(client_buf);
        client_buf = NULL;
        free(server_buf);
        server_buf = NULL;
        goto done;
    }

    /* also need the new blobs, so get those. */
    rc = save_old_blobs(iq, trans, ".ONDISK", server_buf, rrn, new_genid,
                        newblobs);
    if (rc)
        goto done;
    using_newblobs = 1;

    /* Now get the client version of the record and flush it out
       in a format the client can understand.  It goes out in this format to
       some intermediary machine (gsrv) from where it goes out via rmque. */
    rc = stag_to_ctag_buf_tz(
        iq->usedb->dbname, ".ONDISK", server_buf, -1, ".ONDISK_CLIENT",
        client_buf, (unsigned char *)nulls, 0, NULL, NULL, "US/Eastern");

    /* We send down an array of comdb2_field_types.  The offset field is the
       field in the
       offset of the data value in the buffer.  This is a bit wasteful but
       simple.
       It's especially simple for the receving side - it just calls
       dbd_dtag_bind()
       with the names, offsets and types all set up.  */

    /* pass 1: run through the schema and see how much space we'll need */
    sz = sizeof(int) + MAXTABLELEN;
    sz += s->nmembers * sizeof(comdb2_field_type);
    for (i = 0; i < s->nmembers; i++) {
        /* no point in allocating space for null values */
        if (btst(nulls, i))
            continue;
        fld = &s->member[i];
        /* not a blob - get size from schema */
        if (fld->blob_index == -1)
            sz += fld->len;
        else {
            if (fld->type == CLIENT_VUTF8 &&
                newblobs[0].blobptrs[fld->blob_index] == NULL) {
                if (server_schema == NULL) {
                    /* Vutf8 isn't present in client schemas. If it doesn't fit
                     * in the field,
                     * it's handled as a blob, below.   If it does, handle it
                     * here.
                     * That necessitates looking up the value in the ondisk
                     * schema. */
                    server_schema =
                        find_tag_schema(iq->usedb->dbname, ".ONDISK");
                    if (server_schema == NULL) {
                        printf("can't find schema for %s\n", iq->usedb->dbname);
                        free(client_buf);
                        return OP_FAILED_INTERNAL;
                    }
                }
                sz += (server_schema->member[i].len -
                       5); /* 5 bytes for the size field */
            } else {
                sz += newblobs[0].bloblens[fld->blob_index];
            }
        }
    }

    serialize_buf = malloc(sz);
    p = (uint8_t *)serialize_buf;
    lim = p + sz;

    rc = ERR_BADREQ;
    if ((p = buf_put(&s->nmembers, sizeof(int), p, lim)) == NULL)
        goto err;
    if ((p = buf_no_net_put(iq->usedb->dbname, strlen(iq->usedb->dbname), p,
                            lim)) == NULL)
        goto err;
    if ((p = buf_zero_put(MAXTABLELEN - strlen(iq->usedb->dbname), p, lim)) ==
        NULL)
        goto err;

    offset = sizeof(int) + MAXTABLELEN;
    offset += s->nmembers * sizeof(comdb2_field_type);

    /* copy fields */
    for (i = 0; i < s->nmembers; i++) {
        fld = &s->member[i];

        strcpy(fields[i].name, fld->name);
        client_type_to_csc2_type(fld->type, fld->len, &fields[i].type);

        /* null bit */
        fields[i].reserved[0] = btst(nulls, i);
        if (!fields[i].reserved[0]) {
            if (fld->blob_index == -1)
                fields[i].len = fld->len;
            else {

                if (server_schema == NULL) {
                    server_schema =
                        find_tag_schema(iq->usedb->dbname, ".ONDISK");
                    if (server_schema == NULL) {
                        printf("can't find schema for %s\n", iq->usedb->dbname);
                        free(client_buf);
                        return OP_FAILED_INTERNAL;
                    }
                }

                if (fld->type == CLIENT_VUTF8 &&
                    newblobs[0].blobptrs[fld->blob_index] == NULL) {
                    fields[i].len = server_schema->member[i].len - 5;
                    fields[i].type = COMDB2_CSTR;
                } else {
                    fields[i].len = newblobs[0].bloblens[fld->blob_index];
                }
            }
        } else {
            fields[i].len = 0;
        }

        fields[i].off = offset;

        if ((p = comdb2_field_type_put(&fields[i], p, lim)) == NULL)
            goto err;

        if (!fields[i].reserved[0])
            offset += fields[i].len;
    }

    /* pass 2 - build the oplog entry */
    for (i = 0; i < s->nmembers; i++) {
        fld = &s->member[i];

        if (!fields[i].reserved[0]) {
            /* again - already converted to big endian above, memcpy works */
            if (fld->blob_index == -1) {
                /* copy from record */
                memcpy(p, ((char *)client_buf) + fld->offset, fld->len);
                p += fld->len;
            } else {
                if (fld->type == CLIENT_VUTF8 &&
                    newblobs[0].blobptrs[fld->blob_index] == NULL) {
                    uint8_t sz[5];
                    unsigned inline_len;
                    int isnull;
                    int outdtsz;
                    struct field_conv_opts conv = {0, 0};

                    memcpy(p, ((int8_t *)server_buf) +
                                  server_schema->member[i].offset + 5,
                           server_schema->member[i].len - 5);

#ifdef _LINUX_SOURCE
                    conv.flags |= FLD_CONV_LENDIAN;
#endif
                    memcpy(&sz, ((int8_t *)server_buf) +
                                    server_schema->member[i].offset,
                           5);
                    rc = SERVER_UINT_to_CLIENT_UINT(sz, 5, &conv, NULL,
                                                    &inline_len, 4, &isnull,
                                                    &outdtsz, NULL, NULL);
                    if (rc) {
                        fprintf(
                            stderr,
                            "table %s field %: can't determine length rc %d\n",
                            iq->usedb->dbname, fld->name, rc);
                        rc = OP_FAILED_INTERNAL;
                        goto err;
                    }
                    /* null terminate if 0-length */
                    if (inline_len == 0)
                        p[0] = 0;

                    p += server_schema->member[i].len - 5;
                } else {
                    memcpy(p, newblobs[0].blobptrs[fld->blob_index],
                           fields[i].len);
                    p += fields[i].len;
                }
            }
        }
    }

    rc = 0;
err:

    /* This calls add_record recursively.  Not because I'm into that sort of
       thing,
       but because for once it makes sense and I don't want to duplicate all
       that logic.
       We never log comdb2_oplog table inserts (see above) so it won't loop. */
    savedb = iq->usedb;
    iq->usedb = get_dbtable_by_name("comdb2_oplog");
    rc = add_oplog_entry(iq, trans, LCL_OP_ADD, serialize_buf, sz);
    iq->usedb = savedb;
    /* don't need this anymore whether or not we failed */
    free(serialize_buf);
    if (rc != 0) {
        *opfailcode = OP_FAILED_INTERNAL + ERR_ADD_OPLOG;
        goto done;
    }

done:
    if (using_newblobs)
        free_blob_status_data(newblobs);
    if (client_buf)
        free(client_buf);
    if (server_buf)
        free(server_buf);
    return rc;
}

/* Write an entry to the comdb2_oplog table. */
int add_oplog_entry(struct ireq *iq, void *trans, int type, void *logrec,
                    int logsz)
{
    int rc;
    int nulls[32] = {0};
    int err, fix;
    int rrn;
    const uint8_t *p_buf_tag_name;
    const uint8_t *p_buf_tag_name_end;
    uint8_t *p_buf_data;
    const uint8_t *p_buf_data_end;
    uint8_t buf[OPREC_SIZE] = {0};
    unsigned long long genid;
    struct oprec rec;
    blob_buffer_t blobs[MAXBLOBS] = {0};
    char *p;
    struct dbtable *db;
    struct ireq aiq;

    p_buf_tag_name = (const uint8_t *)"log";
    p_buf_tag_name_end = p_buf_tag_name + 3;

    /* from comdb2_oplog.csc2 -> cmdb2h */
    db = get_dbtable_by_name("comdb2_oplog");
    if (db == NULL)
        return 0;

    blobs[0].data = logrec;
    blobs[0].length = logsz;
    blobs[0].collected = logsz;

    rec.seqno = iq->blkstate->seqno;
    rec.optype = (int)type;
    rec.blkpos = iq->blkstate->pos;
    if (logrec != NULL) {
        rec.ops.notnull = 1;
        rec.ops.length = logsz;
        blobs[0].exists = 1;
    } else {
        rec.ops.notnull = 0;
        rec.ops.length = 0;
        blobs[0].exists = 0;
    }
    rec.ops.padding0 = rec.ops.padding1 = 0;

    p_buf_data = buf;
    p_buf_data_end = p_buf_data + sizeof(buf);

    if (!oprec_type_put(&rec, p_buf_data, p_buf_data_end)) {
        fprintf(stderr, "%s line %d: endian conversion error\n", __func__,
                __LINE__);
        return -1;
    }

    init_fake_ireq(thedb, &aiq);
    aiq.usedb = db;
    aiq.jsph = iq->jsph;

    if (iq->debug)
        reqpushprefixf(iq, __func__);

    rc = add_record(&aiq, trans, p_buf_tag_name, p_buf_tag_name_end, p_buf_data,
                    p_buf_data_end, (unsigned char *)nulls, blobs, MAXBLOBS,
                    &err, &fix, &rrn, &genid, -1ULL, BLOCK2_ADDKL, 0,
                    RECFLAGS_DYNSCHEMA_NULLS_ONLY | RECFLAGS_NO_CONSTRAINTS);

    iq->blkstate->pos++;

    iq->oplog_numops++;

    if (iq->debug)
        reqpopprefixes(iq, 1);
    return rc;
}

int add_local_commit_entry(struct ireq *iq, void *trans, long long seqno,
                           long long seed, int nops)
{
    int rc;
    unsigned char nulls[32] = {0};
    const uint8_t *p_buf_tag_name;
    const uint8_t *p_buf_tag_name_end;
    uint8_t *p_buf_data;
    const uint8_t *p_buf_data_end;
    uint8_t buf[COMMITREC_SIZE] = {0};
    int err, fix;
    int rrn;
    unsigned long long genid;
    blob_buffer_t blobs[MAXBLOBS] = {0};
    struct commitrec rec;

    struct dbtable *db;

    p_buf_tag_name = (const uint8_t *)"log";
    p_buf_tag_name_end = p_buf_tag_name + 3;

    db = get_dbtable_by_name("comdb2_commit_log");
    if (db == NULL)
        return 0;

    rec.seqno = seqno;
    rec.seed = seed;
    rec.nops = nops;

    p_buf_data = (uint8_t *)buf;
    p_buf_data_end = p_buf_data + sizeof(buf);

    if (!commitrec_type_put(&rec, p_buf_data, p_buf_data_end)) {
        fprintf(stderr, "%s:%d endian conversion error\n", __FILE__, __LINE__);
        return -1;
    }
    iq->usedb = db;
    if (iq->debug)
        reqpushprefixf(iq, __func__);

    rc = add_record(iq, trans, p_buf_tag_name, p_buf_tag_name_end, p_buf_data,
                    p_buf_data_end, nulls, blobs, MAXBLOBS, &err, &fix, &rrn,
                    &genid, -1ULL, BLOCK2_ADDKL, 0,
                    RECFLAGS_DYNSCHEMA_NULLS_ONLY | RECFLAGS_NO_CONSTRAINTS);

    if (iq->debug)
        reqpopprefixes(iq, 1);
    return rc;
}

int local_replicant_write_clear(struct dbtable *db)
{
    struct schema *s;
    int rc;
    tran_type *trans = NULL;
    long long seqno;
    int nretries = 0;
    struct ireq iq;
    int arc;
    char table[32];
    struct block_state blkstate = {0};

    /* skip if not needed */
    if (gbl_replicate_local == 0 || get_dbtable_by_name("comdb2_oplog") == NULL)
        return 0;

    s = find_tag_schema(db->dbname, ".ONDISK_CLIENT");
    if (s == NULL) {
        return OP_FAILED_INTERNAL;
    }

    /* Return immediately if this table is not replicated */
    if (-1 == find_field_idx_in_tag(s, "comdb2_seqno")) {
        return 0;
    }

    strncpy(table, db->dbname, sizeof(table));

    table[31] = 0;

    init_fake_ireq(thedb, &iq);
    iq.use_handle = thedb->bdb_env;

    iq.blkstate = &blkstate;
again:
    nretries++;
    if (trans) {
        arc = trans_abort(&iq, trans);
        if (arc) {
            printf("toclear: trans_abort rc %d\n", arc);
            trans = NULL;
            goto done;
        }
        trans = NULL;
    }
    if (nretries > gbl_maxretries)
        return RC_INTERNAL_RETRY;
    rc = trans_start(&iq, NULL, &trans);
    if (rc) {
        printf("toclear: trans_start rc %d\n", rc);
        goto done;
    }

    rc = get_next_seqno(trans, &seqno);
    if (rc) {
        if (rc != RC_INTERNAL_RETRY) {
            printf("get_next_seqno unexpected rc %d\n", rc);
            goto done;
        } else
            goto again;
    }
    iq.blkstate->seqno = seqno;
    iq.blkstate->pos = 0;
    rc = add_oplog_entry(&iq, trans, LCL_OP_CLEAR, table, sizeof(table));
    if (rc == RC_INTERNAL_RETRY)
        goto again;
    if (rc) {
        printf("toclear: add_oplog_entry(clear) rc %d\n", rc);
        goto done;
    }
    iq.blkstate->seqno = seqno;
    iq.blkstate->pos = 1;
    rc = add_oplog_entry(&iq, trans, LCL_OP_COMMIT, NULL, 0);
    if (rc == RC_INTERNAL_RETRY)
        goto again;
    if (rc) {
        printf("toclear: add_oplog_entry(commit) rc %d\n", rc);
        goto done;
    }
    rc = trans_commit(&iq, trans, gbl_mynode);
    if (rc) {
        printf("toclear: commit rc %d\n", rc);
        goto done;
    }
    trans = NULL;

done:
    if (trans) {
        arc = trans_abort(&iq, trans);
        if (arc)
            printf("toclear: trans_abort rc %d\n", arc);
    }
    return rc;
}
