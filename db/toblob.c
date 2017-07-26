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
 * The blob cache system.  When someone does a find using a tag with blobs
 * and *if* the blob data is too large to send back in the buffer then we
 * cache the blob data in this module for a while.  The client is then
 * supposed to send requests for the cached data.
 *
 * Why cache?  Why not just read the requested blob fragment for each blob
 * fragment request?  Because the record may have changed under our feet,
 * in which case we'd have to send back "ERR_BLOB_CHANGED" codes and the client
 * would have to repeat the find.  If the record is being updated a lot it is
 * conceivable that the client will never get a look in.
 *
 * If we have a very blob intensive database then this should probably be
 * revisted for efficiency - we use a single lock for all tags and all tables
 * and we do a lot under it.  A more finely grained scheme might be possible.
 *
 * The blob cache key at first might seem a bit odd.  Each cached blob is
 * keyed by rrn+length+genid+tablename+tagname, unless it was extracted via
 * dynamic tags where it is keyed by rrn+length+genid+tablename+dyntag_extra.
 * The key for static tags is the way it is because it means that the key
 * can be constructed deterministically based on the record retrieved; if it
 * is already cached from a previous request there is no need to re-cache it.
 * This is not possible with dynamic tags because the tag name is not persistent
 * between requests, so I tacked on an extra int which is returned by the
 * find request.
 */

#include <errno.h>
#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <stddef.h>
#include <pthread.h>

#include <epochlib.h>
#include <plhash.h>
#include <comdb2.h>

#include <list.h>
#include <compile_time_assert.h>

#include <str0.h>

#include "types.h"
#include <netinet/in.h>

#include "debug_switches.h"
#include <logmsg.h>

/* ****************************************************************************
 *
 * BLOB CACHING SUBYSTEM
 *
 * Hash table - used for finding blobs as required.
 * List - used to keep cached blobs in order of age so they can be purged.
 *  New blobs get added to the bottom.
 *  Blobs get purged from the top.
 */

typedef struct {
    int rrn;
    unsigned total_length;
    unsigned long long genid;
    unsigned dyntag_extra1; /* 0, unless it is a dynamic tag */
    unsigned dyntag_extra2; /* 0, unless it is a dynamic tag */
    char tablename[MAXTABLELEN + 2];
    char tagname[MAXTAGLEN + 2]; /* .DYNT. for dynamic tags */
    /* be wary when forming keys - memset the struct to zero in case the
     * compiler decided to add padding... */
} cached_blob_key_t;

typedef struct cached_blob {
    /* First bytes are the key. */
    cached_blob_key_t key;

    /* When this was cached - when it gets old, we purge it. */
    int cache_time;

    int numblobs;
    size_t bloblens[MAXBLOBS];
    size_t bloboffs[MAXBLOBS];
    char *blobptrs[MAXBLOBS];

    LINKC_T(struct cached_blob) linkv;
} cached_blob_t;

typedef struct {
    /* Historical counts */
    unsigned retrieved;
    unsigned purged;
    unsigned cached;
    unsigned collisions;
    unsigned recovered;

    /* Right now */
    unsigned cached_now;
    unsigned cached_now_bytes;

    /* The most blobs we've ever had cached at once. */
    unsigned cached_max;
    unsigned cached_max_bytes;
} blob_stats_t;

/* How long to keep blobs hanging around. (in seconds) */
int gbl_blob_maxage = 10;

/* For debugging - causes blobs to get "lost" after each find and exercises
 * the code path that tries to recover lost blob data. */
int gbl_blob_lose_debug = 0;

/* The maximum amount of blob data in bytes to allow to be cached at once.
 * No matter what this is set to we will always allow at least one blob to be
 * cached, even if it alone is above this limit. */
unsigned gbl_max_blob_cache_bytes = 128 * 1024 * 1024;

/* Verbosity level */
int gbl_blob_vb = 0;

static void free_cached_blob(cached_blob_t *blob);
static void refresh_cached_blob(cached_blob_t *blob);
static void *cache_blob_data_int(struct ireq *iq, int rrn,
                                 unsigned long long genid, const char *table,
                                 const char *tag, unsigned *extra1,
                                 unsigned *extra2, int numblobs,
                                 size_t *bloblens, size_t *bloboffs,
                                 void **blobptrs, size_t total_length);

static pthread_mutex_t blobmutex;
static hash_t *blobhash = NULL;
static LISTC_T(cached_blob_t) bloblist;
static blob_stats_t stats;
static unsigned dyntag_next_extra = 1;

static void blobmem_init(void);

#define LOCK_BLOB_MUTEX()                                                      \
    pthread_mutex_lock(&blobmutex);                                            \
    comdb2bma_mark_locked(blobmem);
#define UNLOCK_BLOB_MUTEX()                                                    \
    comdb2bma_mark_unlocked(blobmem);                                          \
    pthread_mutex_unlock(&blobmutex);

void blob_print_stats(void)
{
    logmsg(LOGMSG_USER, "blob subsystem status:-\n");
    logmsg(LOGMSG_USER, "  blob max age         : %d secs\n", gbl_blob_maxage);
    logmsg(LOGMSG_USER, "  losing blobs?        : %d\n", gbl_blob_lose_debug);
    logmsg(LOGMSG_USER, "  cache max sz bytes   : %u\n", gbl_max_blob_cache_bytes);
    logmsg(LOGMSG_ERROR, "  no-touch update cnt  : %llu\n", gbl_untouched_blob_cnt);
    logmsg(LOGMSG_ERROR, "  inplace-update cnt   : %llu\n", gbl_inplace_blob_cnt);
    logmsg(LOGMSG_ERROR, "  upd-genid only cnt   : %llu\n", gbl_update_genid_blob_cnt);
    logmsg(LOGMSG_ERROR, "  del-on-update cnt    : %llu\n", gbl_delupd_blob_cnt);
    logmsg(LOGMSG_ERROR, "  add-on-update cnt    : %llu\n", gbl_addupd_blob_cnt);
    logmsg(LOGMSG_ERROR, " historical counts:-\n");
    logmsg(LOGMSG_ERROR, "  num retrieved        : %u\n", stats.retrieved);
    logmsg(LOGMSG_ERROR, "  num purged           : %u\n", stats.purged);
    logmsg(LOGMSG_ERROR, "  num cached           : %u\n", stats.cached);
    logmsg(LOGMSG_ERROR, "  num collisions       : %u\n", stats.collisions);
    logmsg(LOGMSG_ERROR, "  num recovered        : %u\n", stats.recovered);
    logmsg(LOGMSG_ERROR, " cache size:-\n");
    logmsg(LOGMSG_ERROR, "  current keys         : %u\n", stats.cached_now);
    logmsg(LOGMSG_ERROR, "  current bytes        : %u\n", stats.cached_now_bytes);
    logmsg(LOGMSG_ERROR, "  historical max keys  : %u\n", stats.cached_max);
    logmsg(LOGMSG_ERROR, "  historical max bytes : %u\n", stats.cached_max_bytes);
}

int init_blob_cache(void)
{
    if (pthread_mutex_init(&blobmutex, NULL) != 0) {
        logmsg(LOGMSG_ERROR, "init_blob_cache: cannot init mutex: %s\n",
                strerror(errno));
        return -1;
    }

    blobhash = hash_init(sizeof(cached_blob_key_t));
    if (!blobhash) {
        logmsg(LOGMSG_ERROR, "init_blob_cache: error creating hash table\n");
        return -1;
    }

    listc_init(&bloblist, offsetof(cached_blob_t, linkv));
    memset(&stats, 0, sizeof(stats));

    blobmem_init();

    return 0;
}

/* Must call this under lock */
static void free_cached_blob(cached_blob_t *blob)
{
    int ii;
    hash_delk(blobhash, blob);
    listc_rfl(&bloblist, blob);
    for (ii = 0; ii < blob->numblobs; ii++)
        if (blob->blobptrs[ii])
            free(blob->blobptrs[ii]);
    stats.cached_now--;
    stats.cached_now_bytes -= blob->key.total_length;
    free(blob);
}

/* Move a blob to the bottom of the list and reset its cache time.
 * Must call this under lock. */
static void refresh_cached_blob(cached_blob_t *blob)
{
    blob->cache_time = time_epoch();
    if (bloblist.bot != blob) {
        listc_rfl(&bloblist, blob);
        listc_abl(&bloblist, blob);
    }
}

int cache_blob_data(struct ireq *iq, int rrn, unsigned long long genid,
                    const char *table, const char *tag, unsigned *extra1,
                    unsigned *extra2, int numblobs, size_t *bloblens,
                    size_t *bloboffs, void **blobptrs, size_t total_length)
{
    int rc = 0;

    /* another thread may be blocking at malloc while holding blobmutex.
       therefore before locking blobmutex, yield my priority first
       to make sure i don't deadlock here */

    comdb2bma_yield_all();

    LOCK_BLOB_MUTEX();

    if (cache_blob_data_int(iq, rrn, genid, table, tag, extra1, extra2,
                            numblobs, bloblens, bloboffs, blobptrs,
                            total_length) == NULL)
        rc = -1;

    UNLOCK_BLOB_MUTEX();

    return rc;
}

/* must be called under lock.  if *extra is zero on input and tag==".DYNT."
 * then *extra will be written.  this function takes ownership of the memory
 * and might free it, so the caller should not attempt to access or free the
 * pointers passed in.  instead the caller should use the pointer returned. */
void *cache_blob_data_int(struct ireq *iq, int rrn, unsigned long long genid,
                          const char *table, const char *tag, unsigned *extra1,
                          unsigned *extra2, int numblobs, size_t *bloblens,
                          size_t *bloboffs, void **blobptrs,
                          size_t total_length)
{
    cached_blob_t *blob;
    cached_blob_t *blob_existing;
    int rc, ii;

    if (numblobs > MAXBLOBS) {
        logmsg(LOGMSG_ERROR, "cache_blob_data: numblobs too large at %d\n",
                numblobs);
        goto err;
    }

    /* make space for this blob - purge old blobs until we have enough space */
    if (gbl_max_blob_cache_bytes > 0) {
        while (bloblist.top &&
               stats.cached_now_bytes + total_length >
                   gbl_max_blob_cache_bytes) {
            free_cached_blob(bloblist.top);
            stats.purged++;
        }
    }

    blob = malloc(sizeof(cached_blob_t));
    if (!blob) {
        logmsg(LOGMSG_ERROR, "cache_blob_data: out of memory allocating "
                        "%u:%s:%d:%llu with %d blobs\n",
                total_length, table, rrn, genid, numblobs);
        goto err;
    }

    memset(&blob->key, 0, sizeof(blob->key));
    blob->key.rrn = rrn;
    blob->key.genid = genid;
    blob->key.total_length = (unsigned)total_length;
    strncpy(blob->key.tablename, table, sizeof(blob->key.tablename));
    strncpy(blob->key.tagname, tag, sizeof(blob->key.tagname));
    if (*extra1 == 0 && *extra2 == 0 && strcasecmp(tag, ".DYNT.") == 0) {
        /* assign the "extra" value for dynamic tags if it is not already
         * assigned. */
        *extra1 = 0;
        *extra2 = dyntag_next_extra;
        if (++dyntag_next_extra == 0)
            dyntag_next_extra = 1;
    }
    blob->key.dyntag_extra1 = *extra1;
    blob->key.dyntag_extra2 = *extra2;

    if ((blob_existing = hash_find(blobhash, &blob->key))) {
        /* We already have this blob cached - free the memory and return the
         * existing cache entry.  This can happen if we do a find on a record
         * that another thread/process is halfway through retrieving. */
        free(blob);
        for (ii = 0; ii < numblobs; ii++)
            if (blobptrs[ii])
                free(blobptrs[ii]);
        stats.collisions++;
        return blob_existing;
    }

    blob->cache_time = time_epoch();
    blob->numblobs = numblobs;
    memcpy(blob->bloblens, bloblens, sizeof(size_t) * numblobs);
    memcpy(blob->bloboffs, bloboffs, sizeof(size_t) * numblobs);
    memcpy(blob->blobptrs, blobptrs, sizeof(void *) * numblobs);

    rc = hash_add(blobhash, blob);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "cache_blob_data: error adding blob "
                        "%u:%s:%s:%d:%llu+%u+%u with %d blobs: hash_add "
                        "rc=%d\n",
                total_length, table, tag, rrn, genid, *extra1, *extra2,
                numblobs, rc);
        free(blob);
        goto err;
    } else {
        if (iq->debug)
            reqprintf(iq, "CACHED BLOB LEN %u TBL %s TAG %s RRN %d GENID %llu "
                          "EXTRA %u+%u NUMBLOBS %d\n",
                      total_length, table, tag, rrn, genid, *extra1, *extra2,
                      numblobs);
        listc_abl(&bloblist, blob);
        stats.cached++;
        stats.cached_now++;
        stats.cached_now_bytes += blob->key.total_length;
        if (stats.cached_now > stats.cached_max)
            stats.cached_max = stats.cached_now;
        if (stats.cached_now_bytes > stats.cached_max_bytes)
            stats.cached_max_bytes = stats.cached_now_bytes;
    }

    return blob;

err:
    for (ii = 0; ii < numblobs; ii++)
        if (blobptrs[ii])
            free(blobptrs[ii]);
    return NULL;
}

void purge_old_cached_blobs(void)
{
    if (gbl_blob_maxage > 0) {
        int now = time_epoch();

        LOCK_BLOB_MUTEX();
        // can't safely purge old blobs if there're threads blocking on malloc()
        if (comdb2bma_nblocks(blobmem) > 0) {

            while (bloblist.top &&
                   now - bloblist.top->cache_time > gbl_blob_maxage) {
                if (gbl_blob_vb > 0) {
                    logmsg(LOGMSG_USER, 
                        "purged %u:%s:%s:%d:%llu+%u:%u with %d blobs\n",
                        bloblist.top->key.total_length,
                        bloblist.top->key.tablename, bloblist.top->key.tagname,
                        bloblist.top->key.rrn, bloblist.top->key.genid,
                        bloblist.top->key.dyntag_extra1,
                        bloblist.top->key.dyntag_extra2,
                        bloblist.top->numblobs);
                }
                free_cached_blob(bloblist.top);
                stats.purged++;
            }
        }

        UNLOCK_BLOB_MUTEX();
    }
}

/* ****************************************************************************
 *
 * BLOB ASK REQUEST HANDLER
 *
 * The request gives an offset but not a length - the db returns as much data
 * as it can squeeze into the buffer.  If it hits the end of the data then
 * it frees the cached blob, so it's important that clients send their requests
 * in order.
 */

struct blobask_req {
    unsigned int total_length; /* for sanity checking */
    int rrn;
    unsigned long long genid;
    unsigned int fetch_off;  /* offset to start fetching from */
    unsigned int tblnamelen; /* length of table name string */
    unsigned int taglen;     /* length of tag name */
    unsigned int extra1;     /* dyntag extra */
    unsigned int extra2;     /* dyntag extra */
    unsigned int reserved; /* must be zero */ /*char data[1];*/
    char data[1];
};

enum { BLOBASK_REQ_LEN = 4 + 4 + 8 + 4 + 4 + 4 + 4 + 4 + 4 };
BB_COMPILE_TIME_ASSERT(blobask_req_size,
                       offsetof(struct blobask_req, data) == BLOBASK_REQ_LEN);

struct blobask_rsp {
    unsigned int fraglen;  /* how much data returned */
    unsigned int reserved; /* must be zero */
    char data[1];
};

enum { BLOBASK_RSP_LEN = 4 + 4 };
BB_COMPILE_TIME_ASSERT(blobask_rsp_len,
                       offsetof(struct blobask_rsp, data) == BLOBASK_RSP_LEN);

uint8_t *blobask_req_put(const struct blobask_req *p_blobask_req,
                         uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOBASK_REQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_blobask_req->total_length),
                    sizeof(p_blobask_req->total_length), p_buf, p_buf_end);
    p_buf = buf_put(&(p_blobask_req->rrn), sizeof(p_blobask_req->rrn), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_put(&(p_blobask_req->genid),
                           sizeof(p_blobask_req->genid), p_buf, p_buf_end);
    p_buf = buf_put(&(p_blobask_req->fetch_off),
                    sizeof(p_blobask_req->fetch_off), p_buf, p_buf_end);
    p_buf = buf_put(&(p_blobask_req->tblnamelen),
                    sizeof(p_blobask_req->tblnamelen), p_buf, p_buf_end);
    p_buf = buf_put(&(p_blobask_req->taglen), sizeof(p_blobask_req->taglen),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_blobask_req->extra1), sizeof(p_blobask_req->extra1),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_blobask_req->extra2), sizeof(p_blobask_req->extra2),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_blobask_req->reserved), sizeof(p_blobask_req->reserved),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *blobask_req_get(struct blobask_req *p_blobask_req,
                               const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOBASK_REQ_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_blobask_req->total_length),
                    sizeof(p_blobask_req->total_length), p_buf, p_buf_end);
    p_buf = buf_get(&(p_blobask_req->rrn), sizeof(p_blobask_req->rrn), p_buf,
                    p_buf_end);
    p_buf = buf_no_net_get(&(p_blobask_req->genid),
                           sizeof(p_blobask_req->genid), p_buf, p_buf_end);
    p_buf = buf_get(&(p_blobask_req->fetch_off),
                    sizeof(p_blobask_req->fetch_off), p_buf, p_buf_end);
    p_buf = buf_get(&(p_blobask_req->tblnamelen),
                    sizeof(p_blobask_req->tblnamelen), p_buf, p_buf_end);
    p_buf = buf_get(&(p_blobask_req->taglen), sizeof(p_blobask_req->taglen),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_blobask_req->extra1), sizeof(p_blobask_req->extra1),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_blobask_req->extra2), sizeof(p_blobask_req->extra2),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_blobask_req->reserved), sizeof(p_blobask_req->reserved),
                    p_buf, p_buf_end);

    return p_buf;
}

uint8_t *blobask_rsp_put(const struct blobask_rsp *p_blobask_rsp,
                         uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOBASK_RSP_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_put(&(p_blobask_rsp->fraglen), sizeof(p_blobask_rsp->fraglen),
                    p_buf, p_buf_end);
    p_buf = buf_put(&(p_blobask_rsp->reserved), sizeof(p_blobask_rsp->reserved),
                    p_buf, p_buf_end);

    return p_buf;
}

const uint8_t *blobask_rsp_get(struct blobask_rsp *p_blobask_rsp,
                               const uint8_t *p_buf, const uint8_t *p_buf_end)
{
    if (p_buf_end < p_buf || BLOBASK_RSP_LEN > (p_buf_end - p_buf))
        return NULL;

    p_buf = buf_get(&(p_blobask_rsp->fraglen), sizeof(p_blobask_rsp->fraglen),
                    p_buf, p_buf_end);
    p_buf = buf_get(&(p_blobask_rsp->reserved), sizeof(p_blobask_rsp->reserved),
                    p_buf, p_buf_end);

    return p_buf;
}

int toblobask(struct ireq *iq)
{
    cached_blob_key_t key;
    cached_blob_t *blob;
    char *schemaname;
    int rc;
    int is_dynt = 0;
    char table[MAXTABLELEN + 1];
    char cachetag[MAXTAGLEN + 1]; /* as used in the cache */

    struct blobask_req req;
    struct blobask_rsp rsp;

    uint8_t *p_buf_out;
    uint8_t *p_buf_out_rsp_start;
    uint8_t *p_buf_out_rsp_blob_start;

    /* get our own p_buf_out so that if we fail we don't touch iq's copy */
    p_buf_out = iq->p_buf_out;

    if (!(iq->p_buf_in =
              blobask_req_get(&req, iq->p_buf_in, iq->p_buf_in_end))) {
        if (iq->debug)
            reqprintf(iq, "REQ BUF TOO SHORT");
        return ERR_BADREQ;
    }

    if (req.fetch_off >= req.total_length || req.tblnamelen > MAXTABLELEN) {
        if (iq->debug)
            reqprintf(iq, "BAD BLOBASK fetch_off=%u total_length=%u "
                          "tblnamelen=%d taglen=%d\n",
                      req.fetch_off, req.total_length, req.tblnamelen,
                      req.taglen);
        return ERR_BADREQ;
    }

    if (req.tblnamelen > sizeof(table)) {
        if (iq->debug)
            reqprintf(iq, "TABLE NAME TOO LONG");
        return ERR_BADREQ;
    }

    if (!(iq->p_buf_in = buf_no_net_get(&table, req.tblnamelen, iq->p_buf_in,
                                        iq->p_buf_in_end))) {
        if (iq->debug)
            reqprintf(iq, "NO ROOM FOR TABLE NAME LENGTH");
        return ERR_BADREQ;
    }
    table[req.tblnamelen] = '\0';
    iq->usedb = get_dbtable_by_name(table);

    if (iq->usedb == NULL) {
        if (iq->debug)
            reqprintf(iq, "BAD TBL NAME %s", table);
        return ERR_BADREQ;
    }

    if (req.taglen == 0) {
        /* no sense falling back on .DEFAULT, as default tag is blobless
         * anyway */
        if (iq->debug)
            reqprintf(iq, "ZERO TAG LENGTH");
        return ERR_BADREQ;
    }

    /* have read up to data */
    if (!strncasecmp((const char *)iq->p_buf_in, ".DYNT.", 6)) {
        char tmp[6];
        is_dynt = 1;
        strncpy0(cachetag, ".DYNT.", sizeof(cachetag));
    } else {
        if (req.taglen > MAXTAGLEN) {
            if (iq->debug)
                reqprintf(iq, "BAD BLOBASK fetch_off=%u total_length=%u "
                              "tblnamelen=%d taglen=%d\n",
                          req.fetch_off, req.total_length, req.tblnamelen,
                          req.taglen);
            return ERR_BADREQ;
        }
        /* copy, don't consume */
        memcpy(cachetag, iq->p_buf_in, req.taglen);
        cachetag[req.taglen] = '\0';
    }

    schemaname = (char *)iq->p_buf_in;

    memset(&key, 0, sizeof(key));
    key.rrn = req.rrn;
    key.genid = req.genid;
    key.total_length = req.total_length;
    key.dyntag_extra1 = req.extra1;
    key.dyntag_extra2 = req.extra2;
    strncpy(key.tablename, table, sizeof(key.tablename));
    strncpy(key.tagname, cachetag, sizeof(key.tagname));
    reqlog_logf(iq->reqlogger, REQL_INFO, "key=%d:0x%llx:%u:%u:%u:%s:%s",
                key.rrn, key.genid, key.total_length, key.dyntag_extra1,
                key.dyntag_extra2, key.tablename, key.tagname);

    /* find the cached blob */
    LOCK_BLOB_MUTEX();

    blob = hash_find(blobhash, &key);

    if (!blob) {
        blob_status_t blb;
        struct schema *dynschema = NULL;
        char dbtag[MAXTAGLEN + 1];

        /* blob not found.  we should lookup the record again and get the
         * blobs back if the genid hasn't changed. */
        if (iq->debug)
            reqprintf(iq, "BLOB ASK %u:%s:%s:%d:%llu+%u:%u NOT FOUND\n",
                      key.total_length, table, cachetag, key.rrn, key.genid,
                      key.dyntag_extra1, key.dyntag_extra2);

        if (is_dynt) {
            dynschema =
                (struct schema *)new_dynamic_schema(schemaname, req.taglen, 0);
            if (dynschema == NULL) {
                if (iq->debug)
                    reqprintf(iq, "CANT ALLOCATE DYNAMIC SCHEMA '%s' %s\n",
                              schemaname, table);
                rc = ERR_BADREQ;
                goto error;
            }
            add_tag_schema(table, dynschema);
            bzero(dbtag, sizeof(dbtag));
            strncpy0(dbtag, dynschema->tag, sizeof(dbtag));
        } else {
            if (req.taglen >= sizeof(dbtag)) {
                if (iq->debug)
                    reqprintf(iq, "TAG NAME IS TOO LONG '%s' (%d>%d)\n",
                              cachetag, req.taglen, sizeof(dbtag) - 1);
                rc = ERR_BADREQ;
                goto error;
            }
            bzero(dbtag, sizeof(dbtag));
            memcpy(dbtag, cachetag, req.taglen);
        }

        if (gather_blob_data(iq, dbtag, &blb, ".ONDISK") != 0)
            rc = ERR_INTERNAL;
        else {
            rc = ix_find_blobs_by_rrn_and_genid(
                iq, req.rrn, req.genid, blb.numcblobs, blb.cblob_disk_ixs,
                blb.bloblens, blb.bloboffs, (void **)blb.blobptrs);
            if (rc == 0) {
                /* Make sure the blob data we just got matches what is
                 * expected.  Note that a genid check has already been
                 * made when we did the find request.  This should be
                 * sufficient, but for sanity purposes we'll make sure the
                 * lengths we got back match what is expected by the client.
                 * If they don't this either means a garbled request from
                 * the client, or a genid recycling issue (eeek!)
                 */
                size_t new_length = 0;
                int cblob;

                for (cblob = 0; cblob < blb.numcblobs; cblob++)
                    new_length += blb.bloblens[cblob];

                if (new_length != req.total_length) {
                    static int throttle = 100;
                    if (throttle > 0 || iq->debug)
                        reqprintf(iq, "BLOB ASK %u:%s:%s:%d:%llu+%u:%u "
                                      "RECOVERED BUT WITH WRONG LENGTH %u\n",
                                  key.total_length, table, cachetag, key.rrn,
                                  key.genid, key.dyntag_extra1,
                                  key.dyntag_extra2, new_length);
                    if (throttle > 0)
                        throttle--;
                    for (cblob = 0; cblob < blb.numcblobs; cblob++)
                        if (blb.blobptrs[cblob])
                            free(blb.blobptrs[cblob]);
                    rc = ERR_VERIFY;
                } else {
                    /* Huzzah!  Cache the blob data and fall through into
                     * sending blob data back to the client.  This function
                     * takes ownership of the blob pointers and may free
                     * them so don't use them again. */
                    blob = cache_blob_data_int(
                        iq, req.rrn, req.genid, table, cachetag, &req.extra1,
                        &req.extra2, blb.numcblobs, blb.bloblens, blb.bloboffs,
                        (void **)blb.blobptrs, new_length);
                    if (!blob)
                        rc = ERR_INTERNAL;
                    stats.recovered++;
                }
            } else {
                /* Not found, or some other error.  Nothing we can do - the
                 * client will have to retry if it wants to.
                 * Note that rcode was set by ix_find_blobs_by_rrn_and_genid
                 * above. */
            }
        }

        free_dynamic_schema(table, dynschema);
    }

    /* starting write- no more reads */
    iq->p_buf_in_end = iq->p_buf_in = NULL;

    if (BLOBASK_RSP_LEN > (iq->p_buf_out_end - p_buf_out))
        return ERR_BADREQ;

    p_buf_out_rsp_start = p_buf_out;
    p_buf_out += BLOBASK_RSP_LEN;
    p_buf_out_rsp_blob_start = p_buf_out;

    /* We found the cached blob, or re-read it from disk. */
    if (blob) {
        size_t avail_space;
        size_t fetch_len;
        size_t fetch_off;
        size_t fetch_left;
        size_t track_off;
        int ii;
        int hit_end;

        if (iq->debug)
            reqprintf(iq, "BLOB ASK %u:%s:%s:%d:%llu+%u:%u FOUND %p\n",
                      key.total_length, table, cachetag, key.rrn, key.genid,
                      key.dyntag_extra1, key.dyntag_extra2, blob);

        fetch_off = (size_t)req.fetch_off;
        avail_space = (size_t)(iq->p_buf_out_end - p_buf_out);
        fetch_left = req.total_length - fetch_off;
        if (fetch_left <= avail_space) {
            fetch_len = fetch_left;
            hit_end = 1;
        } else {
            fetch_len = avail_space;
            hit_end = 0;
        }

        /* Debugging aid; makes us free the blob so the next request will
         * have to find it again. */
        if (gbl_blob_lose_debug)
            hit_end = 1;

        rsp.reserved = 0;
        rsp.fraglen = fetch_len;

        /* find the start of the data to use */
        track_off = 0;
        for (ii = 0; ii < blob->numblobs; ii++) {
            if (blob->bloblens[ii] > 0) {
                if (track_off + blob->bloblens[ii] >= fetch_off) {
                    size_t start, len;

                    /* we need to take data from this blob */
                    if (fetch_off > track_off)
                        start = fetch_off - track_off;
                    else
                        start = 0;
                    len = blob->bloblens[ii] - start;
                    if (len > fetch_len)
                        len = fetch_len;

                    p_buf_out = buf_no_net_put(
                        blob->blobptrs[ii] + blob->bloboffs[ii] + start, len,
                        p_buf_out, iq->p_buf_out_end);
                    fetch_len -= len;
                }
                track_off += blob->bloblens[ii];
            }
        }

        if (hit_end) {
            free_cached_blob(blob);
            stats.retrieved++;
        } else {
            refresh_cached_blob(blob);
        }

        rc = 0;
    }

error:
    UNLOCK_BLOB_MUTEX();

    if (rc == ERR_VERIFY) {
        /* verify error, we need to retry the request */
        rc = IX_NOTFND;
    }

    if (blobask_rsp_put(&rsp, p_buf_out_rsp_start, p_buf_out_rsp_blob_start) !=
        p_buf_out_rsp_blob_start)
        return ERR_BADREQ;
    iq->p_buf_out = p_buf_out;

    return rc;
}

/* These functions support various blob requests (well, blobask and find). */

/* collect blob schema information in preparation for a find request.
 * returns 0 on success, -1 on error */
int gather_blob_data(struct ireq *iq, const char *tag, blob_status_t *b,
                     const char *to_tag)
{
    int cblob;
    memset(b, 0, sizeof(*b));
    b->numcblobs = get_schema_blob_count(iq->usedb->dbname, tag);
    for (cblob = 0; cblob < b->numcblobs; cblob++) {
        int diskblob, blob_idx;
        diskblob = blob_no_to_blob_no(iq->usedb->dbname, tag, cblob, to_tag);
        blob_idx = get_schema_blob_field_idx(iq->usedb->dbname, tag, cblob);
        if (diskblob < 0 || blob_idx < 0) {
            if (iq->debug)
                reqprintf(
                    iq,
                    "%s ERR SORTING CLIENT BLOB %d DISKBLOB %d SCHEMA IDX %d",
                    req2a(iq->opcode), cblob, diskblob, blob_idx);
            return -1;
        }
        b->cblob_disk_ixs[cblob] = diskblob;
        b->cblob_tag_ixs[cblob] = blob_idx;
    }
    return 0;
}

int gather_blob_data_byname(const char *dbname, const char *tag,
                            blob_status_t *b)
{
    int cblob;
    memset(b, 0, sizeof(*b));
    b->numcblobs = get_schema_blob_count(dbname, tag);
    for (cblob = 0; cblob < b->numcblobs; cblob++) {
        int diskblob, blob_idx;
        diskblob = blob_no_to_blob_no(dbname, tag, cblob, ".ONDISK");
        blob_idx = get_schema_blob_field_idx(dbname, tag, cblob);
        if (diskblob < 0 || blob_idx < 0) {
            logmsg(LOGMSG_ERROR, "BLOB LOOKUP ERR SORTING CLIENT BLOB %d DISKBLOB "
                            "%d SCHEMA IDX %d",
                    cblob, diskblob, blob_idx);
            return -1;
        }
        b->cblob_disk_ixs[cblob] = diskblob;
        b->cblob_tag_ixs[cblob] = blob_idx;
    }
    return 0;
}

void free_blob_status_data_int(blob_status_t *b, int reset)
{
    int ii;
    for (ii = 0; ii < MAXBLOBS; ii++)
        if (b->blobptrs[ii])
            free(b->blobptrs[ii]);
    if (reset)
        memset(b, 0, sizeof(*b));
}

void free_blob_status_data(blob_status_t *b)
{

    free_blob_status_data_int(b, 1);
}
void free_blob_status_data_noreset(blob_status_t *b)
{

    free_blob_status_data_int(b, 0);
}

/* will perform the check for only one blob, blob_index is needed
 * to define the offset in the void *record.
 * cblob is the location in b->blobptr and other arrays where this
 * object has been loaded (from sqlglue for instance).
 */
static int check_one_blob(struct ireq *iq, int isondisk, const char *tag,
                          struct schema *schema, blob_status_t *b, const void *record,
                          int repair_mode, int blob_index, int cblob)

{
    int outrc = 0;
    int inconsistent;
    client_blob_tp *blob;
    client_blob_tp tempblob;
    int bfldno = b->cblob_tag_ixs[blob_index];

    if (isondisk) {
        int outnull, outdtsz;
        void *serverblobptr;
        serverblobptr = get_field_ptr_in_buf(schema, bfldno, record);
        blob = &tempblob;
        SERVER_BLOB_to_CLIENT_BLOB(
            serverblobptr, 5 /* length */, NULL /* conversion options */,
            NULL /*blob*/, blob, sizeof(client_blob_tp), &outnull, &outdtsz,
            NULL /* conversion options */, NULL /*blob*/);
    } else {
        blob = get_field_ptr_in_buf(schema, bfldno, record);
        if (blob == NULL) {
            if (iq->debug)
                reqprintf(iq, "%s ERR GETTING BLOB FIELD PTR BLOB %d IX %d",
                          req2a(iq->opcode), blob_index, bfldno);
            return 0;
        }
        b->blobflds[cblob] = blob;

#if 0
        printf("blob %d %s notnull %d len %d\n", cblob, schema->member[bfldno].name,
                blob->notnull, blob->length);
        if (b->blobptrs[cblob])
            fsnapf(stdout, b->blobptrs[cblob], blob->length);
#endif
    }

    /* if we found a schema earlier, and the blob's tag ix is in range, and
     * this blob is a vutf8 string, and the string was small enough to fit
     * in the record itself, then the blob should be NULL */
    if (schema && bfldno >= 0 && bfldno < schema->nmembers &&
        (schema->member[bfldno].type == SERVER_VUTF8 ||
         schema->member[bfldno].type == SERVER_BLOB2) &&
        ntohl(blob->length) <= schema->member[bfldno].len - 5 /*blob hdr*/) {
        inconsistent = b->blobptrs[cblob] != NULL;
    }
    /* otherwise, fall back to regular blob checks */
    else {
        inconsistent =
            (!blob->notnull && b->blobptrs[cblob] != NULL) ||
            (blob->notnull && b->blobptrs[cblob] == NULL &&
             (schema->member[bfldno].type != CLIENT_VUTF8 ||
              ntohl(blob->length) > schema->member[bfldno].len)) ||
            (blob->notnull && b->bloblens[cblob] != ntohl(blob->length));
        if (inconsistent) {
            if ((!blob->notnull && b->blobptrs[cblob] != NULL))
                logmsg(LOGMSG_ERROR, "cblob=%d blob->notnull = %p b->blobptrs[cblob] = %p\n",
                        cblob, blob->notnull, b->blobptrs[cblob]);

            if (blob->notnull && b->blobptrs[cblob] == NULL &&
                (schema->member[bfldno].type != CLIENT_VUTF8 ||
                 ntohl(blob->length) > schema->member[bfldno].len))
                logmsg(LOGMSG_ERROR, "bfldno=%d schema->member[bfldno].type=%d "
                                "ntohl(blob->length)=%d "
                                "schema->member[[bfldno].len=%d\n",
                        bfldno, schema->member[bfldno].type,
                        ntohl(blob->length), schema->member[bfldno].len);

            if (blob->notnull && b->bloblens[cblob] != ntohl(blob->length))
                logmsg(LOGMSG_ERROR, 
                    "b->bloblens[cblob]=%d ntohl(blob->length)=%d diff=%d\n",
                    b->bloblens[cblob], ntohl(blob->length),
                    b->bloblens[cblob] - ntohl(blob->length));
        }
    }

    if (inconsistent) {
        /* The blobs are inconsistent with the record.
         * Caller should handle this situation by re-reading the record.
         * This is a very high level "deadlock" situation, as (unless
         * there's a bug) it means that the record got changed under
         * our feet. */
        /*
          fprintf(stderr, "INCONSISTENT\n");
          fprintf(stderr,"blob %d notnull %d length %u\n",
          cblob, blob->notnull, blob->length);
          fprintf(stderr, "got bloblen %u bloboffs %u blobptr %p\n",
          b->bloblens[cblob], b->bloboffs[cblob], b->blobptrs[cblob]);
          if(b->blobptrs[cblob])
          {
          fsnapf(stderr, b->blobptrs[cblob],
          b->bloblens[cblob] > 128 ? 128 : b->bloblens[cblob]);
          }
        */

        /* Great - there was a bug!  It was found
         * that when a blob got updated to be NULL the blob data wasn't
         * being removed from the blob file.  Use return code -2 to
         * signal this back to the caller.  Schema change can then ignore
         * this particular situation so that we can rebuild our way out
         * of database corruption. */
        if (!blob->notnull && b->blobptrs[cblob] != NULL) {
            if (debug_switch_ignore_extra_blobs())
                return -1;
            logmsg(LOGMSG_WARN, "%s: repairing blob %d\n", __func__, cblob);
            free(b->blobptrs[cblob]);
            b->blobptrs[cblob] = NULL;
            b->bloblens[cblob] = 0;
            b->bloboffs[cblob] = 0;
            if (repair_mode)
                outrc = -2;
        } else
            return -1;
    }

    if (blob->notnull)
        b->total_length += ntohl(blob->length);
    return outrc;
}

/* Returns 0 if the blob data is consistent with the record, -1 if it
 * is not.  If the record is in ondisk format then we do not fill in
 * the b->blobflds pointers. */
static int check_blob_consistency_int(struct ireq *iq, const char *table,
                                      const char *tag, blob_status_t *b,
                                      const void *record, int repair_mode)
{
    int outrc = 0;
    /* try to get the schema for this table/tag */
    struct schema *schema = find_tag_schema(table, tag);
    int isondisk = is_tag_ondisk_sc(schema);

    b->total_length = 0;

    if (isondisk < 0) {
        logmsg(LOGMSG_ERROR, "check_blob_consistency: is_tag_ondisk table %s tag %s "
                        "error\n",
                table, tag);
        return -1;
    }

    for (int cblob = 0; cblob < b->numcblobs; cblob++) {
        int outrc = check_one_blob(iq, isondisk, tag, schema, b, record,
                                   repair_mode, cblob, cblob);
        if (outrc == -1)
            return outrc;
    }

    return outrc;
}

/* do what we do in check_blob_consistency_int(iq, table, tag, b, record, 0);
 * but for one blob only.
 */
int check_one_blob_consistency(struct ireq *iq, const char *table,
                               const char *tag, blob_status_t *b, void *record,
                               int blob_index, int cblob)
{
    int outrc = 0;
    struct schema *schema = find_tag_schema(table, tag);
    int isondisk = is_tag_ondisk_sc(schema);

    b->total_length = 0;

    if (isondisk < 0) {
        logmsg(LOGMSG_ERROR, "check_blob_consistency: is_tag_ondisk table %s tag %s "
                        "error\n",
                table, tag);
        return -1;
    }

    int repair_mode = 0;

    return check_one_blob(iq, isondisk, tag, schema, b, record, repair_mode,
                          blob_index, cblob);
}

int check_blob_consistency(struct ireq *iq, const char *table, const char *tag,
                           blob_status_t *b, const void *record)
{
    return check_blob_consistency_int(iq, table, tag, b, record, 0);
}

int check_and_repair_blob_consistency(struct ireq *iq, const char *table,
                                      const char *tag, blob_status_t *b,
                                      const void *record)
{
    return check_blob_consistency_int(iq, table, tag, b, record, 1);
}

static void blobmem_init(void)
{
    if (gbl_blob_sz_thresh_bytes == ~(0U))
        return;

    blobmem = comdb2bma_create(0, gbl_blobmem_cap, "lblk+blob", &blobmutex);
    if (blobmem == NULL) {
        logmsg(LOGMSG_ERROR, "%s %d: failed to create blobmem\n", __FILE__,
                __LINE__);
        abort();
    }
}
