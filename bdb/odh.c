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

/*
 * Routines for manipulating the OnDisk Header (ODH)
 *
 * In the future we may add fields to the ODH.  When that happens this module
 * gets more complicated as we will have to maintain binary compatibiiuty with
 * previous revisions.  For now though the ODH is 7 bytes which keeps it simple.
 *
 * The initial design of this made it easy to plug in to the existing bdb
 * code.  As this gets more established we can make a few optimisations to avoid
 * excessive copying.  Probably having the db layer reserve space in ondisk
 * buffers for the header would be a big win here.  Also we'd like the client
 * side api to do the compression of large blobs and pass the zlib compressed
 * data down the wire unmolested.
 */

#include <alloca.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <fsnap.h>

#include "bdb_int.h"
#include <locks.h>
#include "locks_wrap.h"

#include "compress.h"
#include <zlib.h>
#include <comdb2rle.h>

#include <lz4.h>
#include <logmsg.h>

#if LZ4_VERSION_NUMBER < 10701
#define LZ4_compress_default LZ4_compress_limitedOutput
#endif


static void read_odh(const void *buf, struct odh *odh);
static void write_odh(void *buf, const struct odh *odh, uint8_t flags);

/*
 * Map of the 7-byte on disk header:
 *
 *    fieldname       bit  byte-boundry
 * _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _ _
 *    flags             . idx 0, byte 1
 *                      .
 *                      .
 *                      .
 *                      . 2 -+
 *                      . 1  |- Compress
 *                      . 0 _+
 * _ _ _ _ _ _ _ _ _ _ _._ _ _ _ _ _ _ _
 *    csc2vers          . idx 1, byte 2
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 * _ _ _ _ _ _ _ _ _ _ _._ _ _ _ _ _ _ _
 *    updateid          . idx 2, byte 3
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 *                      ._ _ _ _ _ _ _ _
 *                      . idx 3, byte 4
 *                      .
 *                      .
 * _ _ _ _ _ _ _ _ _ _ _.
 *    length            .
 *                      .
 *                      .
 *                      ._ _ _ _ _ _ _ _
 *                      . idx 4, byte 5
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 *                      ._ _ _ _ _ _ _ _
 *                      . idx 5, byte 6
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 *                      ._ _ _ _ _ _ _ _
 *                      . idx 6, byte 7
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 *                      .
 * _ _ _ _ _ _ _ _ _ _ _._ _ _ _ _ _ _ _
 */

/* Return 1 if ip-updates are enabled.  Does not care about schema-change */
inline int ip_updates_enabled(bdb_state_type *bdb_state)
{
    if ((bdb_state->attr
             ->dtastripe) && /* non-dtastripe already update in-place */
        (bdb_state
             ->ondisk_header) && /* require the header to store the updateid */
        (bdb_state->inplace_updates)) {
        return 1;
    } else {
        return 0;
    }
}

inline int bdb_have_ipu(bdb_state_type *bdb_state)
{
    return ip_updates_enabled(bdb_state);
}

const char *bdb_algo2compr(int alg)
{
    switch (alg) {
    case BDB_COMPRESS_NONE:
        return "none";
    case BDB_COMPRESS_ZLIB:
        return "zlib";
    case BDB_COMPRESS_RLE8:
        return "rle8";
    case BDB_COMPRESS_CRLE:
        return "crle";
    case BDB_COMPRESS_LZ4:
        return "lz4 ";
    default:
        return "????";
    }
}

int bdb_compr2algo(const char *a)
{
    if (strcasecmp(a, "zlib") == 0)
        return BDB_COMPRESS_ZLIB;
    if (strcasecmp(a, "rle8") == 0)
        return BDB_COMPRESS_RLE8;
    if (strcasecmp(a, "crle") == 0)
        return BDB_COMPRESS_CRLE;
    if (strncasecmp(a, "lz4", 3) == 0)
        return BDB_COMPRESS_LZ4;
    return BDB_COMPRESS_NONE;
}

static inline char *snodhf(char *buf, size_t buflen, const struct odh *odh)
{
    snprintf(buf, buflen, "length:%u updid:%u csc2vers:%u flags:%x (compr %s)",
             (unsigned)odh->length, (unsigned)odh->updateid,
             (unsigned)odh->csc2vers, (unsigned)odh->flags,
             bdb_algo2compr(odh->flags & ODH_FLAG_COMPR_MASK));
    return buf;
}

/* Subset of write_odh which pokes only the updateid. */
void poke_updateid(void *buf, int updid)
{
    uint8_t *out = buf;
    /* byte 1: flags */
    out++;
    /* byte 2: csc2 version */
    out++;
    /* byte 3: high 8 bits of updateid */
    *out = (updid >> 4);
    out++;
    /* byte 4: low 4 bits of updateid, preserve the highest 4 bits of length. */
    (*out) = ((updid << 4) & 0xf0) | (*out & 0x0f);
    /**out = ((updid << 4) & 0xf0) | ((len >> 24) & 0x0f);*/
}

/* You MUST range check updateid and length before calling this or it can get
 * ugly if bits are set that shouldn't be set */
static void write_odh(void *buf, const struct odh *odh, uint8_t flags)
{
    uint32_t len = odh->length;
    uint16_t updid = odh->updateid;
    uint8_t *out = buf;

    /* byte 1: flags */
    *out = flags;
    out++;
    /* byte 2: csc2 version */
    *out = odh->csc2vers;
    out++;
    /* byte 3: high 8 bits of updateid */
    *out = (updid >> 4);
    out++;
    /* byte 4: low 4 bits of updateid and then highest 4 bits of length */
    *out = ((updid << 4) & 0xf0) | ((len >> 24) & 0x0f);
    out++;
    /* bytes 5-7: remaining bits of length */
    *out = ((len >> 16) & 0xff);
    out++;
    *out = ((len >> 8) & 0xff);
    out++;
    *out = (len & 0xff);
}

static void read_odh(const void *buf, struct odh *odh)
{
    const uint8_t *in = buf;
    uint32_t len;
    uint16_t updid;

    /* byte 1: flags */
    odh->flags = *in;
    in++;
    /* byte 2: csc2 version */
    odh->csc2vers = *in;
    in++;
    /* byte 3: high 8 bits of updateid */
    updid = (*in << 4);
    in++;
    /* byte 4: low 4 bits of updateid and then highest 4 bits of length */
    updid |= (*in >> 4);
    odh->updateid = updid;

    /* bytes 5-7: remaining bits of length */
    len = ((*in & 0x0f) << 24);
    in++;
    len |= ((*in & 0xff) << 16);
    in++;
    len |= ((*in & 0xff) << 8);
    in++;
    len |= (*in & 0xff);

    odh->length = len;
}

void init_odh(bdb_state_type *bdb_state, struct odh *odh, void *rec,
              size_t reclen, int is_blob)
{
    odh->length = reclen;
    odh->updateid = 0;
    if (bdb_state->instant_schema_change)
        odh->csc2vers = bdb_state->version;
    else
        odh->csc2vers = 0;
    odh->flags = 0;
    odh->recptr = rec;
    if (is_blob) {
        odh->flags |= (bdb_state->compress_blobs & ODH_FLAG_COMPR_MASK);
    } else {
        odh->flags |= (bdb_state->compress & ODH_FLAG_COMPR_MASK);
    }
}

/* Pack a record ready for storage on disk with the ODH (if enabled).
 *
 * Input:
 *    bdb_state
 *    odh            - ODH details to store for this record.
 *    odh->recptr    - The raw record.
 *    to             - If not NULL then this is a provided buffer that we can
 *                     compose the record in.  It should be
 *                     odh->length + ODH_SIZE_RESERVE bytes long.
 *    tolen          - Length of to buffer in bytes.
 *
 * Output:
 *    *recptr        - Points to the packed record.
 *    *recsize       - The size of the packed record in bytes.
 *    *freeptr       - If we allocated memory then this will point to the
 *                     memory that the caller must free once the packed record
 *                     is no longer needed.
 *
 * Note: recsize is a uint32_t* so that we can point to a DBT->size field.
 */
int bdb_pack(bdb_state_type *bdb_state, const struct odh *odh, void *to,
             size_t tolen, void **recptr, uint32_t *recsize, void **freeptr)
{
    int rc;

    *freeptr = NULL;

    if (bdb_state->ondisk_header) {
        void *mallocmem = NULL;
        uint8_t flags = odh->flags;
        int alg;

        /* We will need a buffer to do this in.  Eventually we'll refactor all
         * the
         * way down to db/ so that when we first allocate a data buffer for the
         * record we'll have ODH_SIZE_RESERVE spare before the record data so we
         * can avoid an allocate and copy here.. but for now we have to copy. */
        if (to) {
            if (tolen < odh->length + ODH_SIZE) {
                logmsg(LOGMSG_ERROR, "%s:ERROR: to buffer too small at %u bytes for "
                                "%u byte record\n",
                        __func__, (unsigned)tolen, (unsigned)odh->length);
                return EINVAL;
            }
            mallocmem = NULL;
        } else {
            if ((odh->length + ODH_SIZE) > bdb_state->bmaszthresh)
                mallocmem =
                    comdb2_bmalloc(bdb_state->bma, odh->length + ODH_SIZE);
            else
                mallocmem = malloc(odh->length + ODH_SIZE);

            if (!mallocmem) {
                rc = errno;
                logmsg(LOGMSG_ERROR, "%s: out of memory %u\n", __func__,
                        (unsigned)odh->length + ODH_SIZE);
                return rc;
            }
            to = mallocmem;
        }

        alg = (odh->length) ? flags & ODH_FLAG_COMPR_MASK : BDB_COMPRESS_NONE;

        switch (alg) {
        case BDB_COMPRESS_ZLIB: {
            /* The zlib documentation says that the destination buffer must be
             * the size of the input buffer + 0.01% or something like that.
             * I'm going to ignore that.
             * If my compressed data ends up larger than the uncompressed
             * version
             * then I'm not interested.
             */
            uLongf destLen = odh->length;
            rc = compress2(((Bytef *)to) + ODH_SIZE, &destLen,
                           (const Bytef *)odh->recptr, odh->length,
                           bdb_state->attr->zlib_level);
            switch (rc) {
            default:
                /* oh dear */
                logmsg(LOGMSG_ERROR, "%s: compress2: rcode %d %s\n", __func__, rc,
                        zError(rc));
            /* fall through */
            case Z_BUF_ERROR:
                /* output buffer wasn't large enough.  since it was the same
                 * size
                 * as the input buffer then if we get this i don't want
                 * compression anyway! */
                alg = BDB_COMPRESS_NONE;
                if (bdb_state->attr->ztrace) {
                    logmsg(LOGMSG_USER, "no compression gain for %u bytes\n",
                           (unsigned)odh->length);
                }
                break;
            case Z_OK:
                /* all good */
                if (bdb_state->attr->ztrace) {
                   logmsg(LOGMSG_USER, "%s compressed %u bytes -> %u\n", bdb_state->name,
                           (unsigned)odh->length, (unsigned)destLen);
                }
                *recsize = destLen + ODH_SIZE;
                break;
            }
            break;
        }

        case BDB_COMPRESS_RLE8:
            rc = rle8_compress(odh->recptr, odh->length,
                               ((Bytef *)to) + ODH_SIZE, odh->length);
            if (rc < 0) {
                alg = BDB_COMPRESS_NONE;
                if (bdb_state->attr->ztrace) {
                    logmsg(LOGMSG_USER, "no rle8 compression gain for %u bytes\n",
                           (unsigned)odh->length);
                }
            } else {
                if (bdb_state->attr->ztrace) {
                    logmsg(LOGMSG_USER, "%s rle8 compressed %u bytes -> %u\n",
                           bdb_state->name, (unsigned)odh->length,
                           (unsigned)rc);
                }
                *recsize = rc + ODH_SIZE;
            }
            break;

        case BDB_COMPRESS_CRLE: {
            Comdb2RLE rle = {.in = odh->recptr,
                             .insz = odh->length,
                             .out = (uint8_t *)to + ODH_SIZE,
                             .outsz = odh->length - 1};
            if (compressComdb2RLE_hints(&rle, bdb_state->fld_hints) == 0)
                *recsize = rle.outsz + ODH_SIZE;
            else
                alg = BDB_COMPRESS_NONE;
            break;
        }

        case BDB_COMPRESS_LZ4:
            if ((rc = LZ4_compress_default(
                     odh->recptr, (char *)to + ODH_SIZE, odh->length,
                     odh->length - 1)) == 0) {
                alg = BDB_COMPRESS_NONE;
            } else {
                *recsize = rc + ODH_SIZE;
            }
            break;
        }

        if (alg == BDB_COMPRESS_NONE) {
            /* No compression, or compression was no good. */
            memcpy(((char *)to) + ODH_SIZE, odh->recptr, odh->length);
            *recsize = odh->length + ODH_SIZE;
            flags &= ~ODH_FLAG_COMPR_MASK;
        }
        write_odh(to, odh, flags);
        *recptr = to;
        *freeptr = mallocmem;
    } else {
        if (odh->updateid != 0 || odh->flags != 0 || odh->csc2vers != 0) {
            logmsg(LOGMSG_ERROR, 
                    "%s:ERROR attempting to set ODH fields but no ODH for %s\n", __func__, bdb_state->name);
            return EINVAL;
        }
        *recptr = odh->recptr;
        *recsize = odh->length;
    }

    return 0;
}

/* Unpack a data or blob record and read back the ODH.
 *
 * Input:
 *    bdb_state
 *    from        - data record to unpack
 *    fromlen     - size of data record in bytes
 *    to          - A user provided buffer that can be used to decompress the
 *                  record.  If this buffer is too small for the decompressed
 *                  record then the unpacking will fail.  If this is NULL and
 *                  we need to decompress then we malloc() enough memory and
 *                  use that.
 *    tolen       - Size of buffer pointer to by to.
 *    updateid    - the updateid we're expecting to see from the on-disk
 *                  header.  It will be ignored if it is set to less than 0
 *
 * Output:
 *    odh         - the ODH read from the record (we set this even if
 *                  ODH isn't supported for this table).
 *    odh->recptr - The decompressed record.
 *    *freeptr    - If we allocated memory for decompression then this will be
 *                  set to that memory which must be free()'d by the caller,
 *                  otherwise this will come back NULL.
 *                  If you provide a to buffer then freeptr can be NULL.
 *
 * You can rely on the following behaviors:
 *
 *    to!=NULL => this function will not malloc, *freeptr will come back NULL
 *
 *    *freeptr==NULL => odh->recptr points to within the original from buffer
 *       - This way caller knows that it cannot free data->data
 *
 *    *freeptr!=NULL => *freeptr==odh->recptr
 */
static int bdb_unpack_updateid(bdb_state_type *bdb_state, const void *from,
                               size_t fromlen, void *to, size_t tolen,
                               struct odh *odh, int updateid, void **freeptr,
                               int verify_updateid, int force_odh)
{
    void *mallocmem = NULL;
    const int ver_bytes = 2;
    int do_uncompress = 1;

    if (freeptr) {
        *freeptr = NULL;
    }

    if (bdb_state->ondisk_header) {

        int alg;

        if (fromlen < ODH_SIZE) {
            logmsg(LOGMSG_ERROR, "%s:ERROR: data size %u too small for ODH\n",
                    __func__, (unsigned)fromlen);
            return DB_ODH_CORRUPT;
        }

        read_odh(from, odh);

        if ((verify_updateid) && (updateid >= 0) &&
            (odh->updateid != updateid)) {
            return DB_NOTFOUND;
        }

        if (verify_updateid == 0) {
            if (updateid >= 0 && (odh->updateid > updateid)) {
                logmsg(LOGMSG_ERROR, 
                    "Updated record race odh->updateid = %u updateid = %u\n",
                    odh->updateid, updateid);
                return DB_NOTFOUND;
            }
        }

        alg = odh->flags & ODH_FLAG_COMPR_MASK;

        if (alg != BDB_COMPRESS_NONE) {
            uLongf destLen;
            int rc;

            /* Need a decompression buffer */
            if (to) {
                if (tolen < odh->length) {
                    logmsg(LOGMSG_ERROR, 
                        "%s:ERROR: need a %u decompression buffer, given %u\n",
                        __func__, (unsigned)odh->length, (unsigned)tolen);
                    return EINVAL;
                }
                mallocmem = NULL;
            } else {
                /* allocate +2 to support instant sc */
                if (freeptr == NULL) {
                    do_uncompress = 0;
                } else {
                    if ((odh->length + ver_bytes) > bdb_state->bmaszthresh)
                        to = comdb2_bmalloc(bdb_state->bma,
                                            odh->length + ver_bytes);
                    else
                        to = malloc(odh->length + ver_bytes);

                    if (!to) {
                        rc = errno;
                        logmsg(LOGMSG_ERROR, "%s: out of memory %u\n", __func__,
                                (unsigned)odh->length);
                        return rc;
                    }
                    mallocmem = to;
                }
            }

            destLen = odh->length;
            if (do_uncompress == 0) {
                /* Do nothing */
            } else if (alg == BDB_COMPRESS_ZLIB) {
                rc = uncompress(to, &destLen, ((Bytef *)from) + ODH_SIZE,
                                fromlen - ODH_SIZE);

                if (rc != Z_OK) {
                    logmsg(LOGMSG_ERROR, "%s:uncompress gave %d %s %u->%u\n",
                            __func__, rc, zError(rc),
                            (unsigned)fromlen - ODH_SIZE,
                            (unsigned)odh->length);
                    goto err;
                }

                if (destLen != odh->length) {
                    logmsg(LOGMSG_ERROR, 
                            "%s:ERROR decompressed to %u bytes should be %u\n",
                            __func__, (unsigned)destLen, (unsigned)odh->length);
                    goto err;
                }
            } else if (alg == BDB_COMPRESS_RLE8) {
                rc = rle8_decompress(((const char *)from) + ODH_SIZE,
                                     fromlen - ODH_SIZE, to, odh->length);
                if (rc != odh->length) {
                    logmsg(LOGMSG_ERROR, 
                            "%s:ERROR rle_decompress rc %d expected %u\n",
                            __func__, rc, (unsigned)odh->length);
                    goto err;
                }
            } else if (alg == BDB_COMPRESS_CRLE) {
                Comdb2RLE rle = {.in = (uint8_t *)from + ODH_SIZE,
                                 .insz = fromlen - ODH_SIZE,
                                 .out = to,
                                 .outsz = odh->length};
                rc = decompressComdb2RLE(&rle);
                if (rc || rle.outsz != odh->length) {
                    logmsg(LOGMSG_ERROR, "%s:ERROR decompressComdb2RLE rc: %d "
                                    "outsz: %lu expected: %u\n",
                            __func__, rc, rle.outsz, odh->length);
                    goto err;
                }
            } else if (alg == BDB_COMPRESS_LZ4) {
                rc = LZ4_decompress_safe((char *)from + ODH_SIZE, to,
                                         (fromlen - ODH_SIZE), odh->length);
                if (rc != odh->length) {
                    goto err;
                }
            }

            /* Successfully decompressed */
            if (freeptr) {
                *freeptr = mallocmem;
            }
            odh->recptr = to;
        } else {
            /* No compression, just point to where the record data lives in
             * this record. */
            if (odh->length != fromlen - ODH_SIZE) {
                logmsg(LOGMSG_ERROR, "%s:ERROR: odh->length=%u, fromlen=%u\n",
                        __func__, (unsigned)odh->length, (unsigned)fromlen);
                return DB_ODH_CORRUPT;
            }
            odh->recptr = ((char *)from) + ODH_SIZE;
        }

        /* Older dbs with odh have version 0.
         * However, they are version 1 of the schema */
        if (odh->csc2vers == 0) {
            odh->csc2vers = 1;
        }
    } else {
        odh->length = fromlen;
        odh->updateid = 0;
        odh->csc2vers = 0;
        odh->flags = 0;
        odh->recptr = (void *)from;
    }

    return 0;

err:
    if (mallocmem)
        free(mallocmem);
    return DB_UNCOMPRESS_ERR;
}

int bdb_unpack(bdb_state_type *bdb_state, const void *from, size_t fromlen,
               void *to, size_t tolen, struct odh *odh, void **freeptr)
{
    return bdb_unpack_updateid(bdb_state, from, fromlen, to, tolen, odh, -1,
                               freeptr, 1, 0);
}

int bdb_unpack_force_odh(bdb_state_type *bdb_state, const void *from,
                         size_t fromlen, void *to, size_t tolen,
                         struct odh *odh, void **freeptr)
{
    return bdb_unpack_updateid(bdb_state, from, fromlen, to, tolen, odh, -1,
                               freeptr, 1, 1);
}

static int bdb_write_updateid(bdb_state_type *bdb_state, void *buf,
                              size_t buflen, int updateid)
{
    struct odh odh;

    if (!bdb_state->ondisk_header) {
        return -1;
    }

    if (buflen < ODH_SIZE) {
        logmsg(LOGMSG_FATAL, "%s:ERROR: data size %u too small for ODH\n", __func__,
                (unsigned)buflen);
        abort();
        // return DB_ODH_CORRUPT;
    }

    read_odh(buf, &odh);

    if (updateid != odh.updateid) {
        odh.updateid = updateid;
        write_odh(buf, &odh, odh.flags);
    }

    return 0;
}

int bdb_retrieve_updateid(bdb_state_type *bdb_state, const void *from,
                          size_t fromlen)
{
    struct odh odh_in;

    if (!bdb_state->ondisk_header) {
        return -1;
    }

    if (fromlen < ODH_SIZE) {
        logmsg(LOGMSG_FATAL, "%s:ERROR: data size %u too small for ODH\n", __func__,
                (unsigned)fromlen);
        abort();
        // return DB_ODH_CORRUPT;
    }

    read_odh(from, &odh_in);

    return odh_in.updateid;
}

/* Remove the ondisk header from the DBT data following a successful find
 * operations.  flags should be the flags used in the get/cget function.
 * If the DBT uses DBT_MALLOC and we have an error then we will free the
 * memory before returning.  We don't do this for DBT_REALLOC as it is
 * assumed that the caller is handling its affairs.
 *
 * This takes a pointer to updateid as both an input and output argument.
 *
 * INPUT:
 *
 * (*updateid < 0)   => do not verify against odh.updateid
 * (*updateid >= 0)  => fail with a DB_NOTFOUND if (odh.updateid != *updateid)
 *
 * OUTPUT:
 *
 * (*updateid)       => record's updateid or 0 if ondisk_headers are disabled
 *
 */

static int bdb_unpack_dbt_verify_updateid(bdb_state_type *bdb_state, DBT *data,
                                          int *updateid, uint8_t *ver,
                                          int flags, int verify_updateid)
{
    int rc;
    struct odh odh = {0};
    void *buf;

    if (!bdb_state->ondisk_header && *updateid > 0) {
        logmsg(LOGMSG_FATAL, "%s: called with ondisk_header disabled but a valid "
                        "updateid %d\n",
                __func__, *updateid);
        abort();
        return EINVAL;
    }

    if (!bdb_state->ondisk_header) {
        *updateid = -1;
        return 0;
    }

    if (data->flags & DB_DBT_PARTIAL) {
        logmsg(LOGMSG_FATAL, "%s: called with bad data->flags %u\n", __func__,
                (unsigned)flags);
        abort();
        return EINVAL;
    }

    if (flags & (DB_MULTIPLE | DB_MULTIPLE_KEY)) {
        /* can't handle this right now terribly sorry */
        logmsg(LOGMSG_FATAL, "%s: called with bad flags %u\n", __func__,
                (unsigned)flags);
        abort();
        return EINVAL;
    }

    /*
    printf("Unpack %u bytes:\n", data->size);
    fsnapf(stdout, data->data, data->size);
    */
    rc = bdb_unpack_updateid(bdb_state, data->data, data->size, NULL, 0, &odh,
                             *updateid, &buf, verify_updateid, 0);

    if (rc == 0) {
        /*
        char s[128];
        printf("Got ODH[%s] with data:\n", snodhf(s, sizeof(s), &odh));
        fsnapf(stdout, odh.recptr, odh.length);
        */
        if (buf) {
            if (data->flags & DB_DBT_MALLOC) {
                free(data->data);
                data->data = odh.recptr;
            } else if (data->flags & DB_DBT_USERMEM) {
                if (odh.length > data->ulen) {
                    logmsg(LOGMSG_ERROR, 
                            "%s: record has length %u, supplied buffer is %u\n", __func__, 
                            (unsigned)odh.length,
                            (unsigned)data->ulen);
                    free(buf);
                    return ENOMEM;
                }
                memcpy(data->data, odh.recptr, odh.length);
                free(buf);
            } else {
                free(buf);
                logmsg(LOGMSG_FATAL, "%s: called with bad data->flags %u\n",
                        __func__, (unsigned)flags);
                abort();
                return EINVAL;
            }
        } else if (data->data != odh.recptr) {
            /*memmove the record down in the data->data buffer over the odh. */
            memmove(data->data, odh.recptr, odh.length);
        }
        data->size = odh.length;
        *updateid = odh.updateid;
        *ver = odh.csc2vers;
    }
    return rc;
}

int bdb_update_updateid(bdb_state_type *bdb_state, DBC *dbcp,
                        unsigned long long oldgenid,
                        unsigned long long newgenid)
{
    DBT key, data;
    struct odh myodh;
    int rc, oldupdateid, newupdateid;
    char ondiskh[ODH_SIZE];

    /* fail if there are no ondisk headers */
    if (!ip_updates_enabled(bdb_state)) {
        return -1;
    }

    /* fail if these genids are not otherwise equivalent */
    if (bdb_inplace_cmp_genids(bdb_state, oldgenid, newgenid) != 0) {
        return -2;
    }

    memset(&key, 0, sizeof(key));
    memset(&data, 0, sizeof(data));

    oldupdateid = get_updateid_from_genid(bdb_state, oldgenid);
    newupdateid = get_updateid_from_genid(bdb_state, newgenid);
    oldgenid = get_search_genid(bdb_state, oldgenid);

    key.data = &oldgenid;
    key.size = sizeof(unsigned long long);

    data.data = &ondiskh;
    data.size = sizeof(ondiskh);
    data.ulen = sizeof(ondiskh);
    data.dlen = sizeof(ondiskh);
    data.doff = 0;
    data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    rc = dbcp->c_get(dbcp, &key, &data, DB_SET);

    if (rc != 0) {
        return rc;
    }

    read_odh(ondiskh, &myodh);
    if (myodh.updateid != oldupdateid) {
        return DB_NOTFOUND;
    }

    myodh.updateid = newupdateid;

    write_odh(ondiskh, &myodh, myodh.flags);

    return dbcp->c_put(dbcp, &key, &data, DB_CURRENT);
}

/* find-with-cursor simply to verify the genid */
int bdb_cposition(bdb_state_type *bdb_state, DBC *dbcp, DBT *key,
                  u_int32_t flags)
{
    int rc;
    struct odh odh_in;
    DBT data = {0};
    int updateid = -1, compare_upid = 0;
    char ondiskh[ODH_SIZE];
    unsigned long long *genptr = NULL;

    if (!ip_updates_enabled(bdb_state)) {
        data.data = NULL;
        data.size = 0;
        data.ulen = 0;
        data.dlen = 0;
        data.doff = 0;
        data.flags = (DB_DBT_USERMEM | DB_DBT_PARTIAL);
        return dbcp->c_get(dbcp, key, &data, DB_SET);
    }

    if (key->size >= 8) {
        genptr = (unsigned long long *)key->data;
        updateid = get_updateid_from_genid(bdb_state, *genptr);
        *genptr = get_search_genid(bdb_state, *genptr);
        compare_upid = !(flags == DB_SET_RANGE);
    }

    data.data = &ondiskh;
    data.size = sizeof(ondiskh);
    data.ulen = sizeof(ondiskh);
    data.dlen = sizeof(ondiskh);
    data.doff = 0;
    data.flags = DB_DBT_USERMEM | DB_DBT_PARTIAL;

    rc = dbcp->c_get(dbcp, key, &data, flags);

    if (rc == 0) {
        read_odh(ondiskh, &odh_in);
        if ((compare_upid) && (odh_in.updateid != updateid)) {
            rc = DB_NOTFOUND;
        } else {
            updateid = odh_in.updateid;
        }
    }

    if (key->size >= 8 && updateid >= 0) {
        /* reset the updateid if modified before the c_get */
        if (genptr != NULL) {
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }

        /* set updateid if c_get malloc'd new memory */
        if (genptr != key->data) {
            genptr = (unsigned long long *)key->data;
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }
    }

    return rc;
}

/* list berkdb-requests find-requests that we verify */
static inline int check_updid(u_int32_t flags)
{
    /* Isolate the bits we care about. */
    flags &= (DB_OPFLAGS_MASK);
    switch (flags) {
    case DB_SET:
    case DB_SET_RANGE:
    case DB_SET_RECNO:
        return 1;
        break;

    default:
        return 0;
        break;
    }
}

int bdb_cget(bdb_state_type *bdb_state, DBC *dbcp, DBT *key, DBT *data,
             u_int32_t flags)
{
    int rc, updateid = -1, odh_uid = -1, compare_upid = 0,
            ipu = ip_updates_enabled(bdb_state);
    unsigned long long *genptr = NULL;

    /* if this is an exact find, verify the update id */
    if (ipu && key->size >= 8 && check_updid(flags)) {
        genptr = (unsigned long long *)key->data;
        updateid = get_updateid_from_genid(bdb_state, *genptr);
        *genptr = get_search_genid(bdb_state, *genptr);
        compare_upid = !(flags == DB_SET_RANGE);
    }

    rc = dbcp->c_get(dbcp, key, data, flags);

    if (ipu && rc == 0) {
        odh_uid = bdb_retrieve_updateid(bdb_state, data->data, data->size);
        if ((compare_upid) && (odh_uid != updateid)) {
            rc = DB_NOTFOUND;
            if (data->flags & DB_DBT_MALLOC) {
                free(data->data);
            }
        } else {
            updateid = odh_uid;
        }
    }

    if (ipu && key->size >= 8 && updateid >= 0)
    /*if(rc == 0 && updateid >= 0)*/
    {
        /* reset the updateid if modified before the c_get */
        if (genptr != NULL) {
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }

        /* set updateid if c_get malloc'd new memory */
        if (genptr != key->data) {
            genptr = (unsigned long long *)key->data;
            *genptr = set_updateid(bdb_state, odh_uid, *genptr);
        }
    }

    return rc;
}

static int bdb_cget_unpack_int(bdb_state_type *bdb_state, DBC *dbcp, DBT *key,
                               DBT *data, uint8_t *ver, u_int32_t flags,
                               int verify_updateid)
{
    int rc, updateid = -1, ipu = ip_updates_enabled(bdb_state);
    unsigned long long *genptr = NULL;

    /* if this is an exact find, modify & verify the update id */
    if (ipu && key->size >= 8 && check_updid(flags)) {
        genptr = (unsigned long long *)key->data;
        if ((flags & DB_OPFLAGS_MASK) != DB_SET_RANGE)
            updateid = get_updateid_from_genid(bdb_state, *genptr);
        *genptr = get_search_genid(bdb_state, *genptr);
    }

    rc = dbcp->c_get(dbcp, key, data, flags);

    if (rc == 0) {
        /* This uses the verify_update argument to determine whether it should
         * fail for mismatched updateids if updateid >= 0.  It will always
         * return
         * the ondisk-header updateid on success */
        rc = bdb_unpack_dbt_verify_updateid(bdb_state, data, &updateid, ver,
                                            flags, verify_updateid);

        /* bad rcode: free any memory the c_get allocated */
        if (rc != 0 && data->flags & DB_DBT_MALLOC) {
            free(data->data);
        }
    }

    if (ipu && key->size >= 8 && updateid >= 0)
    /*if(rc == 0 && updateid >= 0)*/
    {
        /* reset the updateid if modified before the c_get */
        if (genptr != NULL) {
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }

        /* set updateid if c_get malloc'd new memory */
        if (genptr != key->data) {
            genptr = (unsigned long long *)key->data;
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }
    }

    return rc;
}

int bdb_cget_unpack(bdb_state_type *bdb_state, DBC *dbcp, DBT *key, DBT *data,
                    uint8_t *ver, u_int32_t flags)
{
    return bdb_cget_unpack_int(bdb_state, dbcp, key, data, ver, flags, 1);
}

/* The updateid-agnostic version of this code. */
int bdb_cget_unpack_blob(bdb_state_type *bdb_state, DBC *dbcp, DBT *key,
                         DBT *data, uint8_t *ver, u_int32_t flags)
{
    return bdb_cget_unpack_int(bdb_state, dbcp, key, data, ver, flags, 0);
}

/* as above, but for DB->get instead of DBC->c_get. */
static int bdb_get_unpack_int(bdb_state_type *bdb_state, DB *db, DB_TXN *tid,
                              DBT *key, DBT *data, uint8_t *ver,
                              u_int32_t flags, int verify_updateid)
{
    int rc, updateid = -1, ipu = ip_updates_enabled(bdb_state);
    unsigned long long *genptr = NULL;

    if (ipu && key->size >= 8) {
        genptr = (unsigned long long *)key->data;
        updateid = get_updateid_from_genid(bdb_state, *genptr);
        *genptr = get_search_genid(bdb_state, *genptr);
    }

    rc = db->get(db, tid, key, data, flags);

    if (rc == 0) {
        /* This will fail for mismatched updateids if updateid >= 0.
         * It will always return the correct updateid on success */
        rc = bdb_unpack_dbt_verify_updateid(bdb_state, data, &updateid, ver,
                                            flags, verify_updateid);

        /* bad rcode: free any memory the c_get allocated */
        if (rc != 0 && data->flags & DB_DBT_MALLOC) {
            free(data->data);
        }
    }

    if (ipu && key->size >= 8 && updateid >= 0)
    /*if(rc == 0 && updateid >= 0)*/
    {
        /* reset the updateid if modified before the c_get */
        if (genptr != NULL) {
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }

        /* set updateid if c_get malloc'd new memory */
        if (genptr != key->data) {
            genptr = (unsigned long long *)key->data;
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }
    }

    return rc;
}

int bdb_get_unpack(bdb_state_type *bdb_state, DB *db, DB_TXN *tid,
                   DBT *key, DBT *data, uint8_t *ver, u_int32_t flags)
{
    return bdb_get_unpack_int(bdb_state, db, tid, key, data, ver, flags, 1);
}

int bdb_get_unpack_blob(bdb_state_type *bdb_state, DB *db, DB_TXN *tid,
                        DBT *key, DBT *data, uint8_t *ver, u_int32_t flags)
{
    return bdb_get_unpack_int(bdb_state, db, tid, key, data, ver, flags, 0);
}

int bdb_prepare_put_pack_updateid(bdb_state_type *bdb_state, int is_blob,
                                  DBT *data, DBT *data2, int updateid,
                                  void **freeptr, void *stackbuf, int odhready)
{
    struct odh odh;

    int rc;

    memcpy(data2, data, sizeof(DBT));

    if (odhready) {
        /* If the record is preprocessed on a replicant, use it as is. */
        assert(is_blob);
        *freeptr = NULL;
        rc = 0;
    } else {
        init_odh(bdb_state, &odh, data->data, data->size, is_blob);

        if (updateid > 0) {
            odh.updateid = updateid;
        }

        rc = bdb_pack(bdb_state, &odh, stackbuf, odh.length + ODH_SIZE_RESERVE,
                      &data2->data, &data2->size, freeptr);
    }

    if (rc == 0)
        data2->ulen = data2->size;
    return rc;
}

/* This has to be a macro because the alloca() has to be called by the
 * calling function.. */
#define ALLOC_STACKBUF(sz) ((sz) < 16 * 1024 ? alloca((sz)) : NULL)

/* rewrite the data with an updated updateid (no compress logic) */
int bdb_put(bdb_state_type *bdb_state, DB *db, DB_TXN *tid, DBT *key, DBT *data,
            u_int32_t flags)
{
    int rc, updateid = -1, ipu = ip_updates_enabled(bdb_state);
    unsigned long long *genptr = NULL;

    if (ipu && key->size >= 8) {
        genptr = (unsigned long long *)key->data;
        updateid = get_updateid_from_genid(bdb_state, *genptr);
        *genptr = get_search_genid(bdb_state, *genptr);
    }

    /* updateid is coded in this genid, but odh is disabled? */
    if (updateid > 0 && !bdb_state->ondisk_header) {
        logmsg(LOGMSG_FATAL, "%s: called with ondisk_header disabled but a valid "
                        "updateid %d\n",
                __func__, updateid);
        abort();
    }

    if (updateid >= 0) {
        bdb_write_updateid(bdb_state, data->data, data->size, updateid);
    }

    rc = db->put(db, tid, key, data, flags);

    if (ipu && updateid >= 0 && key->size >= 8) {
        /* reset the updateid if modified before the c_get */
        if (genptr != NULL) {
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }

        /* set updateid if c_get malloc'd new memory */
        if (genptr != key->data) {
            genptr = (unsigned long long *)key->data;
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }
    }

    return rc;
}

int bdb_put_pack(bdb_state_type *bdb_state, int is_blob, DB *db, DB_TXN *tid,
                 DBT *key, DBT *data, u_int32_t flags, int odhready)
{
    DBT data2;
    void *mallocmem;
    int rc, updateid = -1, ipu = ip_updates_enabled(bdb_state);
    unsigned long long *genptr = NULL;

    if (ipu && key->size >= 8) {
        genptr = (unsigned long long *)key->data;
        updateid = get_updateid_from_genid(bdb_state, *genptr);
        *genptr = get_search_genid(bdb_state, *genptr);
    }

    /* updateid is coded in this genid, but odh is disabled? */
    if (updateid > 0 && !bdb_state->ondisk_header) {
        logmsg(LOGMSG_FATAL, "%s: called with ondisk_header disabled but a valid "
                        "updateid %d\n",
                __func__, updateid);
        abort();
    }

    if (!bdb_state->ondisk_header) {
        return db->put(db, tid, key, data, flags);
    }

    rc = bdb_prepare_put_pack_updateid(
        bdb_state, is_blob, data, &data2, updateid, &mallocmem,
        ALLOC_STACKBUF(data->size + ODH_SIZE_RESERVE), odhready);

    if (rc == 0) {
        rc = db->put(db, tid, key, &data2, flags);
        free(mallocmem);
    }

    if (ipu && updateid >= 0 && key->size >= 8) {
        /* reset the updateid if modified before the c_get */
        if (genptr != NULL) {
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }

        /* set updateid if c_get malloc'd new memory */
        if (genptr != key->data) {
            genptr = (unsigned long long *)key->data;
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }
    }
    return rc;
}

int bdb_cput_pack(bdb_state_type *bdb_state, int is_blob, DBC *dbcp, DBT *key,
                  DBT *data, u_int32_t flags)
{
    DBT data2;
    void *mallocmem;
    int rc;
    int updateid = -1, ipu = ip_updates_enabled(bdb_state);
    unsigned long long *genptr = NULL;

    if (ipu && key->size >= 8) {
        genptr = (unsigned long long *)key->data;
        updateid = get_updateid_from_genid(bdb_state, *genptr);
        *genptr = get_search_genid(bdb_state, *genptr);
    }

    /* updateid is coded in this genid, but odh is disabled? */
    if (updateid > 0 && !bdb_state->ondisk_header) {
        logmsg(LOGMSG_FATAL, "%s: called with ondisk_header disabled but a valid "
                        "updateid %d\n",
                __func__, updateid);
        abort();
    }

    if (!bdb_state->ondisk_header) {
        return dbcp->c_put(dbcp, key, data, flags);
    }

    rc = bdb_prepare_put_pack_updateid(
        bdb_state, is_blob, data, &data2, updateid, &mallocmem,
        ALLOC_STACKBUF(data->size + ODH_SIZE_RESERVE), 0);

    if (rc == 0) {
        rc = dbcp->c_put(dbcp, key, &data2, flags);
        if (mallocmem) {
            free(mallocmem);
        }
    }

    if (ipu && updateid >= 0 && key->size >= 8) {
        /* reset the updateid if modified before the c_get */
        if (genptr != NULL) {
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }

        /* set updateid if c_get malloc'd new memory */
        if (genptr != key->data) {
            genptr = (unsigned long long *)key->data;
            *genptr = set_updateid(bdb_state, updateid, *genptr);
        }
    }

    return rc;
}

void bdb_set_odh_options(bdb_state_type *bdb_state, int odh, int compression,
                         int blob_compression)
{
    print(bdb_state,
          "BDB options set: ODH %d compression %d blob_compression %d\n", odh,
          compression, blob_compression);

    bdb_state->ondisk_header = odh;
    bdb_state->compress = compression;
    bdb_state->compress_blobs = blob_compression;
}

inline int bdb_get_csc2_version(bdb_state_type *bdb_state)
{
    return bdb_state->version;
}

inline void bdb_set_csc2_version(bdb_state_type *bdb_state, uint8_t version)
{
    if (bdb_state == NULL) {
        logmsg(LOGMSG_ERROR, "%s(NULL)!!\n", __func__);
        return;
    }
    bdb_state->version = version;
}

inline void bdb_set_instant_schema_change(bdb_state_type *bdb_state, int isc)
{
    if (bdb_state == NULL) {
        logmsg(LOGMSG_ERROR, "%s(NULL)!!\n", __func__);
        return;
    }
    bdb_state->instant_schema_change = isc;
}

inline void bdb_set_inplace_updates(bdb_state_type *bdb_state, int ipu)
{
    if (bdb_state == NULL) {
        logmsg(LOGMSG_ERROR, "%s(NULL)!!\n", __func__);
        return;
    }
    if (bdb_state->ondisk_header) {
        bdb_state->inplace_updates = ipu;
    }
}

inline void bdb_set_datacopy_odh(bdb_state_type *bdb_state, int cdc)
{
    if (bdb_state == NULL) {
        logmsg(LOGMSG_ERROR, "%s(NULL)!!\n", __func__);
        return;
    }
    /*printf("%s(table: %s datacopy odh: %d)\n", __func__, bdb_state->name,
     * cdc);*/
    bdb_state->datacopy_odh = cdc;
}

int bdb_validate_compression_alg(int alg)
{
    switch (alg) {
    case BDB_COMPRESS_NONE:
    case BDB_COMPRESS_ZLIB:
    case BDB_COMPRESS_RLE8:
    case BDB_COMPRESS_CRLE:
        return alg;
    }
    return -1;
}

inline void bdb_get_compr_flags(bdb_state_type *bdb_state, int *odh, int *compr,
                                int *blob_compr)
{
    *odh = bdb_state->ondisk_header;
    *compr = bdb_state->compress;
    *blob_compr = bdb_state->compress_blobs;
}

inline void bdb_set_fld_hints(bdb_state_type *bdb_state, uint16_t *hints)
{
    if (bdb_state->fld_hints)
        free(bdb_state->fld_hints);
    bdb_state->fld_hints = hints;
}

inline void bdb_cleanup_fld_hints(bdb_state_type *bdb_state)
{
    if (bdb_state && bdb_state->fld_hints) {
        free(bdb_state->fld_hints);
        bdb_state->fld_hints = NULL;
    }
}

int bdb_unpack_heap(bdb_state_type *bdb_state, void *in, size_t inlen,
                    void **out, size_t *outlen, void **freeptr)
{
    struct odh odh = {0};
    int rc;

    /* Force ODH in case that a schema change
       is removing the ODH from the table. */
    rc = bdb_unpack_force_odh(bdb_state, in, inlen, NULL, 0, &odh, freeptr);

    if (rc != 0) {
        *out = NULL;
        free(*freeptr);
    } else {
        *outlen = odh.length;
        *out = odh.recptr;
    }

    return rc;
}

int bdb_pack_heap(bdb_state_type *bdb_state, void *in, size_t inlen, void **out,
                  size_t *outlen, void **freeptr)
{
    uint32_t recsz = 0;
    int rc;

    if (!bdb_state->ondisk_header)
        return -1;
    struct odh odh;
    init_odh(bdb_state, &odh, in, inlen, 1);
    rc = bdb_pack(bdb_state, &odh, NULL, 0, out, &recsz, freeptr);
    if (rc == 0)
        *outlen = recsz;
    else
        free(*freeptr);
    return rc;
}
