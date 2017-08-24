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
   all the custom lock primitives.

   hashed row lock:  24  bytes  :  fileid(20) + hashed row lock(4)
   row lock:         30  bytes  :  fileid(20) + fluff(2)  + genid(8)
   minmax lock:      31  bytes  :  fileid(20) + fluff(10) + min0/max1(1)
   stripe lock:      20  bytes  :  fileid(20)
   table lock:       32  bytes  :  shorttablename(28) + crc32(4)
   sequence lock:    34  bytes  :  shortseqname(30) + crc32(4)

   additionally berkeley already uses the following locks:

   page lock:        28  bytes  :  pgno(4) + fileid(20) + type(4)
   recovery lock:     4  bytes  :  inrecovery(4)

   locks are kept unique in the namespace by virtue of the length of their name

   routines in this file DO NOT obtain bdb locks.  it's presumed that whatever
   caller managed to get low enough level to need a custom lock is already
   running with a bdb library lock held.  it would be weird if this werent
   the case.
*/

#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/socketvar.h>
#include <sys/uio.h>
#include <unistd.h>
#include <assert.h>
#include <stddef.h>

#include <db.h>
#include <epochlib.h>

#include <ctrace.h>

#include <net.h>
#include "bdb_int.h"
#include "locks.h"
#include "dbinc/hmac.h"
#include "genid.h"
#include "crc32c.h"

#include <plbitlib.h> /* for bset/btst */
#include <logmsg.h>

extern int __dbreg_get_name(DB_ENV *, u_int8_t *, char **);

static inline u_int32_t resolve_locker_id(tran_type *tran)
{
    if (tran->tranclass == TRANCLASS_LOGICAL) {
        assert(NULL == tran->tid);
        return tran->logical_lid;
    } else {
        return tran->tid->txnid;
    }
}

/* See comment at the top of this file for lock types. */
int bdb_describe_lock_dbt(DB_ENV *dbenv, DBT *dbtlk, char *out, int outlen)
{
    char *file;
    int lklen = dbtlk->size;
    void *lkname = dbtlk->data;
    int rc;

    struct berk_page_lock {
        unsigned int pgno;
        char fileid[20];
        int type;
    } * berk_page_lock;

    /* berkeley page lock */
    if (lklen == 28) {
        berk_page_lock = lkname;
        rc = __dbreg_get_name(dbenv, (u_int8_t *)berk_page_lock->fileid, &file);
        if (rc)
            snprintf(out, outlen, "%s lock unknown file page %d",
                     (berk_page_lock->type == 1) ? "handle" : "page",
                     berk_page_lock->pgno);
        else
            snprintf(out, outlen, "%s lock %s page %d",
                     (berk_page_lock->type == 1) ? "handle" : "page", file,
                     berk_page_lock->pgno);
    }
    /* berkeley recovery lock */
    else if (lklen == 4) {
        snprintf(out, outlen, "berkley rep lock");
    }
    /* lsn lock */
    else if (lklen == 8) {
        DB_LSN lsn;
        memcpy(&lsn, lkname, 8);
        snprintf(out, outlen, "lsn lock %u:%u", lsn.file, lsn.offset);
    }
    /* hashed row lock */
    else if (lklen == 24) {
        snprintf(out, outlen, "hashed row lock?");
    }
    /* row lock */
    else if (lklen == ROWLOCK_KEY_SIZE) {
        unsigned long long genid;
        memcpy(&genid, ((char *)lkname) + 22, sizeof(unsigned long long));
        rc = __dbreg_get_name(dbenv, (u_int8_t *)lkname, &file);
        if (rc)
            snprintf(out, outlen, "rowlock, unknown file, genid %016llx",
                     genid);
        else
            snprintf(out, outlen, "rowlock %s %016llx", file, genid);
    }
    /* minmax lock */
    else if (lklen == MINMAX_KEY_SIZE) {
        char minmax = 0;
        memcpy(&minmax, ((char *)lkname) + 30, sizeof(char));
        rc = __dbreg_get_name(dbenv, (u_int8_t *)lkname, &file);
        if (rc)
            snprintf(out, outlen, "minmax, unknown file %s",
                     minmax == 0 ? "min" : "max");
        else
            snprintf(out, outlen, "minmax %s %s", file,
                     minmax == 0 ? "min" : "max");
    }
    /* stripe lock */
    else if (lklen == STRIPELOCK_KEY_SIZE) {
        rc = __dbreg_get_name(dbenv, (u_int8_t *)lkname, &file);
        if (rc)
            snprintf(out, outlen, "stripelock, unknown file");
        else
            snprintf(out, outlen, "stripelock %s", file);
    }
    /* table lock */
    else if (lklen == TABLELOCK_KEY_SIZE) {
        rc = __dbreg_get_name(dbenv, (u_int8_t *)lkname, &file);
        if (rc)
            snprintf(out, outlen, "tablelock, unknown file");
        else
            snprintf(out, outlen, "tablelock %s", file);
    }
    /* sequence lock */
    else if (lklen == SEQLOCK_KEY_SIZE) {
        snprintf(out, outlen, "sequencelock, %*s", 30, (char *)lkname);
    } else {
        snprintf(out, outlen, "unknown lock %d\n", lklen);
        abort();
    }

    return 0;
}

/* Describe a DB_LOCK object */
int bdb_describe_lock(DB_ENV *dbenv, DB_LOCK *lk, char *out, int outlen)
{
    DBT lkdbt = {0};
    char mem[33];
    int ret;

    lkdbt.data = mem;
    lkdbt.ulen = sizeof(mem);
    lkdbt.flags = DB_DBT_USERMEM;

    if ((ret = dbenv->lock_to_dbt(dbenv, lk, &lkdbt)) != 0) {
        return ret;
    }

    return bdb_describe_lock_dbt(dbenv, &lkdbt, out, outlen);
}

static char *lock_mode_to_str(int mode)
{
    switch (mode) {
    case DB_LOCK_NG:
        return "DB_LOCK_NG";
    case DB_LOCK_READ:
        return "DB_LOCK_READ";
    case DB_LOCK_WRITE:
        return "DB_LOCK_WRITE";
    case DB_LOCK_WAIT:
        return "DB_LOCK_WAIT";
    case DB_LOCK_IWRITE:
        return "DB_LOCK_IWRITE";
    case DB_LOCK_IREAD:
        return "DB_LOCK_IREAD";
    case DB_LOCK_IWR:
        return "DB_LOCK_IWR";
    case DB_LOCK_DIRTY:
        return "DB_LOCK_DIRTY";
    case DB_LOCK_WWRITE:
        return "DB_LOCK_WWRITE";
    case DB_LOCK_WRITEADD:
        return "DB_LOCK_WRITEADD";
    case DB_LOCK_WRITEDEL:
        return "DB_LOCK_WRITEDEL";
    default:
        return "???";
    }
}

extern int gbl_rep_lockid;

/* Throw a rowlock deadlock once every 1000 records or so */
extern int gbl_simulate_rowlock_deadlock_interval;

/* Wrapper around berkeley lock call.  Makes debugging easier. */
int berkdb_lock(DB_ENV *dbenv, int lid, int flags, DBT *lkname, int mode,
                DB_LOCK *lk)
{
    int rc;
    char lock_description[100];

#if 0
    bdb_describe_lock(dbenv, lkname->data, lkname->size, lock_description,
                      sizeof(lock_description));
    printf("get: %s\n", lock_description);
    if (lid && lid == gbl_rep_lockid && lkname->size == 30) {
        printf("Replication thread getting row lock?\n");
        abort();
    }
#endif

    if (lid == gbl_rep_lockid && (lkname->size == 30 || lkname->size > 32)) {
        logmsg(LOGMSG_WARN, "replication thread getting logical locks!\n");
    }

    if (gbl_simulate_rowlock_deadlock_interval &&
        !(rand() % gbl_simulate_rowlock_deadlock_interval)) {
        logmsg(LOGMSG_USER, "Simulating a deadlock!!\n");
        rc = DB_LOCK_DEADLOCK;
    } else {
        rc = dbenv->lock_get(dbenv, lid, flags, lkname, mode, lk);
    }

    if (rc == DB_LOCK_DEADLOCK)
        return BDBERR_DEADLOCK;

    return rc;
}

#include <time.h>

extern int gbl_random_rowlocks;
extern int gbl_disable_rowlocks;
extern int gbl_disable_rowlocks_sleepns;

extern void comdb2_nanosleep(struct timespec *req);

int berkdb_lock_random_rowlock(bdb_state_type *bdb_state, int lid, int flags,
                               void *inlkname, int mode, void *lk)
{
    DBT *lkname = (DBT *)inlkname;
    static __thread int rowlock_rand_seq = 0;
    int *p = (int *)lkname->data;

    (*p) = pthread_self();
    p = (int *)(&((char *)lkname->data)[24]);
    (*p) = rowlock_rand_seq++;

    lkname->size = 30;

    if (gbl_disable_rowlocks) {
        if (gbl_disable_rowlocks_sleepns) {
            struct timespec ts = {0};
            ts.tv_nsec = gbl_disable_rowlocks_sleepns;
            comdb2_nanosleep(&ts);
        }
        return 0;
    }

    return berkdb_lock(bdb_state->dbenv, lid, flags, lkname, mode,
                       (DB_LOCK *)lk);
}

int berkdb_lock_rowlock(bdb_state_type *bdb_state, int lid, int flags,
                        void *lkname, int mode, void *lk)
{
    if (gbl_random_rowlocks && lkname) {
        return berkdb_lock_random_rowlock(bdb_state, lid, flags, lkname, mode,
                                          lk);
    }

    if (gbl_disable_rowlocks) {
        if (gbl_disable_rowlocks_sleepns) {
            struct timespec ts = {0};
            ts.tv_nsec = gbl_disable_rowlocks_sleepns;
            comdb2_nanosleep(&ts);
        }
        return 0;
    }

    return berkdb_lock(bdb_state->dbenv, lid, flags, (DBT *)lkname, mode,
                       (DB_LOCK *)lk);
}

int form_stripelock_keyname(bdb_state_type *bdb_state, int stripe,
                            char *keynamebuf, DBT *dbt_out)
{
    bzero(keynamebuf, STRIPELOCK_KEY_SIZE);
    bzero(dbt_out, sizeof(DBT));

    memcpy(keynamebuf, bdb_state->dbp_data[0][stripe]->fileid, FILEID_LEN);

    dbt_out->data = keynamebuf;
    dbt_out->size = STRIPELOCK_KEY_SIZE;

    return 0;
}

int form_seqlock_keyname(const char *name, char *keynamebuf, DBT *dbt_out)
{
    int len;
    u_int32_t cksum;

    bzero(keynamebuf, SEQLOCK_KEY_SIZE);
    bzero(dbt_out, sizeof(DBT));

    len = strlen(name);

    memcpy(keynamebuf, name, MIN(len, SHORT_SEQNAME_LEN));

    if (len > SHORT_SEQNAME_LEN) {
        cksum = crc32c(name, len);
        memcpy(keynamebuf + SHORT_SEQNAME_LEN, &cksum, sizeof(u_int32_t));
    }

    dbt_out->data = keynamebuf;
    dbt_out->size = SEQLOCK_KEY_SIZE;

    return 0;
}

int form_tablelock_keyname(const char *name, char *keynamebuf, DBT *dbt_out)
{
    int len;
    u_int32_t cksum;

    bzero(keynamebuf, TABLELOCK_KEY_SIZE);
    bzero(dbt_out, sizeof(DBT));

    len = strlen(name);

    memcpy(keynamebuf, name, MIN(len, SHORT_TABLENAME_LEN));

    if (len > SHORT_TABLENAME_LEN) {
        cksum = crc32c(name, len);
        memcpy(keynamebuf + SHORT_TABLENAME_LEN, &cksum, sizeof(u_int32_t));
    }

    dbt_out->data = keynamebuf;
    dbt_out->size = TABLELOCK_KEY_SIZE;

    return 0;
}

static int bdb_lock_seq_int(DB_ENV *dbenv, const char *seqname, int lid,
                            DB_LOCK *dblk, int how)
{
    DBT lk;
    int rc;
    int lockmode;
    char name[SEQLOCK_KEY_SIZE];

    rc = form_seqlock_keyname(seqname, name, &lk);
    if (rc)
        return rc;

    if (how == BDB_LOCK_READ)
        lockmode = DB_LOCK_READ;
    else if (how == BDB_LOCK_WRITE)
        lockmode = DB_LOCK_WRITE;
    else {
        logmsg(LOGMSG_ERROR, "%s unknown lock mode %d requested\n", __func__,
               how);
        return BDBERR_BADARGS;
    }

    rc = berkdb_lock(dbenv, lid, 0, &lk, lockmode, dblk);

#ifdef DEBUG_LOCKS
    fprintf(stderr, "%llx:%s: mode %d, name %s, lid=%x\n", pthread_self(),
            __func__, how, name, lid);
#endif

    return rc;
}

/* FNV-1a */
static inline unsigned long long hash_key(DBT *key)
{
    unsigned char *p = (unsigned char *)key->data;
    unsigned long long h = 0xcbf29ce484222325;
    int ii;

    for (ii = 0; ii < key->size; ii++) {
        h ^= p[ii];
        h = (h * 0x100000001b3);
    }

    return h;
}

/* Same format as a normal rowlock */
int form_ixhash_keyname(bdb_state_type *bdb_state, int ixnum, DBT *key,
                        unsigned long long *outkhash, DBT *dbt_out)
{
    unsigned long long keyhash;
    char *keynamebuf = dbt_out->data;
    keyhash = hash_key(key);
    assert(ixnum != -1);
    memcpy(keynamebuf, bdb_state->dbp_ix[ixnum]->fileid, FILEID_LEN);
    bzero(keynamebuf + FILEID_LEN, sizeof(short));
    memcpy(keynamebuf + FILEID_LEN + sizeof(short), &keyhash,
           sizeof(unsigned long long));
    dbt_out->size = IXHASH_KEY_SIZE;
    if (outkhash)
        *outkhash = keyhash;
    return 0;
}

/* Doesn't need to differentiate stripe (it's in the genid).
 * Masking the genid allows us to get only one rowlock for inplace updates. */
int form_rowlock_keyname(bdb_state_type *bdb_state, int ixnum,
                         unsigned long long genid, char *keynamebuf,
                         DBT *dbt_out)
{
    unsigned long long mgenid = get_search_genid(bdb_state, genid);
    bzero(keynamebuf, ROWLOCK_KEY_SIZE);
    bzero(dbt_out, sizeof(DBT));

#if defined ROWLOCKS_ONELOCK
    int stripe = get_dtafile_from_genid(genid);
    memcpy(keynamebuf, bdb_state->dbp_data[0][stripe]->fileid, FILEID_LEN);
#else
    if (ixnum == -1) {
        int stripe = get_dtafile_from_genid(genid);
        memcpy(keynamebuf, bdb_state->dbp_data[0][stripe]->fileid, FILEID_LEN);
    } else {
        memcpy(keynamebuf, bdb_state->dbp_ix[ixnum]->fileid, FILEID_LEN);
    }
#endif

    memcpy(keynamebuf + FILEID_LEN + sizeof(short), &mgenid,
           sizeof(unsigned long long));
    dbt_out->data = keynamebuf;
    dbt_out->size = ROWLOCK_KEY_SIZE;

    return 0;
}

/* Use the stripe argument to resolve the correct fileid */
int form_minmaxlock_keyname(bdb_state_type *bdb_state, int ixnum, int stripe,
                            int minmax, char *keynamebuf, DBT *dbt_out)
{
    unsigned char minmaxvar;

    if (minmax)
        minmaxvar = 1;
    else
        minmaxvar = 0;

    bzero(keynamebuf, MINMAX_KEY_SIZE);
    bzero(dbt_out, sizeof(DBT));

    if (ixnum == -1)
        memcpy(keynamebuf, bdb_state->dbp_data[0][stripe]->fileid, FILEID_LEN);
    else
        memcpy(keynamebuf, bdb_state->dbp_ix[ixnum]->fileid, FILEID_LEN);

    memcpy(keynamebuf + FILEID_LEN + MINMAXFLUFF_LEN, &minmaxvar,
           sizeof(unsigned char));

    dbt_out->data = keynamebuf;
    dbt_out->size = MINMAX_KEY_SIZE;

    return 0;
}

/* fileid (20) =  20 byte names */
static int bdb_lock_stripe_int(bdb_state_type *bdb_state, tran_type *tran,
                               int stripe, int how)
{
    DB_LOCK dblk;
    DBT lk;
    int rc;
    int lockmode;
    char name[STRIPELOCK_KEY_SIZE];

    /* parent transaction inherits the locks */
    if (tran->parent) tran = tran->parent;

    /* bdb_state is the table that we're locking */
    assert(bdb_state->parent);
    /*
    if (!bdb_state->parent) {
       return BDBERR_BADARGS;
    }
    */

    rc = form_stripelock_keyname(bdb_state, stripe, name, &lk);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s form name %d\n", __func__, rc);
        return rc;
    }

    if (how == BDB_LOCK_READ)
        lockmode = DB_LOCK_READ;
    else if (how == BDB_LOCK_WRITE)
        lockmode = DB_LOCK_WRITE;
    else {
        logmsg(LOGMSG_ERROR, "%s unknown lock mode %d requested\n", __func__,
               how);
        return BDBERR_BADARGS;
    }

    rc = berkdb_lock(bdb_state->dbenv, resolve_locker_id(tran), 0, &lk,
                     lockmode, &dblk);

    if (rc != 0 && rc != BDBERR_DEADLOCK) {
        logmsg(LOGMSG_ERROR, "berkdb_lock %d\n", rc);
    }

    return rc;
}

/* first 28 bytes of table tablename(28) + optional crc32c(4) = 32 byte names */
static int bdb_lock_table_int(DB_ENV *dbenv, const char *tblname, int lid,
                              int how)
{
    DB_LOCK dblk;
    DBT lk;
    int rc;
    int lockmode;
    char name[TABLELOCK_KEY_SIZE];

    rc = form_tablelock_keyname(tblname, name, &lk);
    if (rc)
        return rc;

    if (how == BDB_LOCK_READ)
        lockmode = DB_LOCK_READ;
    else if (how == BDB_LOCK_WRITE)
        lockmode = DB_LOCK_WRITE;
    else {
        logmsg(LOGMSG_ERROR, "%s unknown lock mode %d requested\n", __func__,
               how);
        return BDBERR_BADARGS;
    }

    rc = berkdb_lock(dbenv, lid, 0, &lk, lockmode, &dblk);

#ifdef DEBUG_LOCKS
    fprintf(stderr, "%llx:%s: mode %d, name %s, lid=%x\n", pthread_self(),
            __func__, how, name, lid);
#endif

    return rc;
}

/* fileid (20) + fluff (20) + min/max (1)  =  31 byte names */
int bdb_lock_minmax(bdb_state_type *bdb_state, int ixnum, int stripe,
                    int minmax, int how, DB_LOCK *dblk, DBT *lkname, int lid,
                    int trylock)
{
    DBT lk, *lkptr;
    int rc;
    int lockmode;
    char name[MINMAX_KEY_SIZE];
    char *nameptr;
    int tryflags = 0;

    if (lkname) {
        nameptr = lkname->data;
        lkptr = lkname;
    } else {
        lkptr = &lk;
        nameptr = name;
    }

    /* bdb_state is the table that we're locking */
    assert(bdb_state->parent);
    /*
    if (!bdb_state->parent) {
       return BDBERR_BADARGS;
    }
    */

    rc = form_minmaxlock_keyname(bdb_state, ixnum, stripe, minmax, nameptr,
                                 lkptr);
    if (rc)
        return rc;

    if (how == BDB_LOCK_READ)
        lockmode = DB_LOCK_READ;
    else if (how == BDB_LOCK_WRITE)
        lockmode = DB_LOCK_WRITE;
    else {
        logmsg(LOGMSG_ERROR, "bdb_lock_row_int unknown lock mode %d requested\n",
                how);
        return BDBERR_BADARGS;
    }
    if (trylock)
        tryflags = DB_LOCK_NOWAIT;

    rc = berkdb_lock_rowlock(bdb_state, lid, tryflags, lkptr, lockmode, dblk);

    if (BDBERR_DEADLOCK == rc)
        rc = BDBERR_DEADLOCK_ROWLOCK;

    return rc;
}

static int bdb_lock_ix_value_fromlid(bdb_state_type *bdb_state, int lid,
                                     unsigned long long *hashval, int idx,
                                     DBT *key, DB_LOCK *dblk, DBT *lkname)
{
    int rc;

    /* bdb_state is the table that we're locking */
    assert(bdb_state->parent);

    rc = form_ixhash_keyname(bdb_state, idx, key, hashval, lkname);
    if (rc)
        return rc;

    rc = berkdb_lock_rowlock(bdb_state, lid, 0, lkname, DB_LOCK_WRITE, dblk);
    if (BDBERR_DEADLOCK == rc)
        rc = BDBERR_DEADLOCK_ROWLOCK;

    return rc;
}

/* fileid (20) + fluff (2) + genid (8)  =  30 byte names */
int bdb_lock_row_fromlid_int(bdb_state_type *bdb_state, int lid, int idx,
                             unsigned long long genid, int how, DB_LOCK *dblk,
                             DBT *lkname, int trylock, int flags)
{
    DBT lk, *lkptr;
    int rc;
    int lockmode;
    char name[ROWLOCK_KEY_SIZE];
    char *nameptr;
    int tryflags;

    /* bdb_state is the table that we're locking */
    assert(bdb_state->parent);

    if (lkname) {
        lkptr = lkname;
        nameptr = lkname->data;
    } else {
        lkptr = &lk;
        nameptr = name;
    }

    rc = form_rowlock_keyname(bdb_state, idx, genid, nameptr, lkptr);
    if (rc)
        return rc;

    if (how == BDB_LOCK_READ)
        lockmode = DB_LOCK_READ;
    else if (how == BDB_LOCK_WRITE)
        lockmode = DB_LOCK_WRITE;
    else {
        logmsg(LOGMSG_ERROR, "bdb_lock_row_int unknown lock mode %d requested\n",
                how);
        return BDBERR_BADARGS;
    }

    if (trylock)
        tryflags = flags | DB_LOCK_NOWAIT;
    else
        tryflags = flags;

    rc = berkdb_lock_rowlock(bdb_state, lid, tryflags, lkptr, lockmode, dblk);
    if (BDBERR_DEADLOCK == rc)
        rc = BDBERR_DEADLOCK_ROWLOCK;

    return rc;
}

int bdb_lock_row_fromlid(bdb_state_type *bdb_state, int lid, int idx,
                         unsigned long long genid, int how, DB_LOCK *dblk,
                         DBT *lkname, int trylock)
{
    return bdb_lock_row_fromlid_int(bdb_state, lid, idx, genid, how, dblk,
                                    lkname, trylock, 0);
}

static int bdb_lock_row_int(bdb_state_type *bdb_state, tran_type *tran, int idx,
                            unsigned long long genid, int how, DB_LOCK *dblk,
                            DBT *lkname, int trylock)
{
    if (tran->tranclass != TRANCLASS_LOGICAL)
        return BDBERR_BADARGS;

    assert(NULL == tran->tid);
    return bdb_lock_row_fromlid(bdb_state, tran->logical_lid, idx, genid, how,
                                dblk, lkname, trylock);
}

int bdb_lock_stripe_read(bdb_state_type *bdb_state, int stripe, tran_type *tran)
{
    return bdb_lock_stripe_int(bdb_state, tran, stripe, BDB_LOCK_READ);
}

int bdb_lock_stripe_write(bdb_state_type *bdb_state, int stripe,
                          tran_type *tran)
{
    return bdb_lock_stripe_int(bdb_state, tran, stripe, BDB_LOCK_WRITE);
}

int bdb_lock_table_read_fromlid(bdb_state_type *bdb_state, int lid)
{
    return bdb_lock_table_int(bdb_state->dbenv, bdb_state->name, lid,
                              BDB_LOCK_READ);
}

int bdb_lock_seq_read_fromlid(bdb_state_type *bdb_state, const char *seq,
                              void *dblk, int lid)
{
    return bdb_lock_seq_int(bdb_state->dbenv, seq, lid, dblk, BDB_LOCK_READ);
}
int bdb_lock_seq_write_fromlid(bdb_state_type *bdb_state, const char *seq,
                               int lid)
{
    DB_LOCK dblk;
    return bdb_lock_seq_int(bdb_state->dbenv, seq, lid, &dblk, BDB_LOCK_WRITE);
}

int bdb_lock_table_read(bdb_state_type *bdb_state, tran_type *tran)
{
    int rc;

    /* Readlocks on tables & stripes must be owned by the parent, 
     * as they are quietly dropped if the child aborts */
    if (tran->parent)
        tran = tran->parent;

    rc = bdb_lock_table_int(bdb_state->dbenv, bdb_state->name,
                            resolve_locker_id(tran), BDB_LOCK_READ);

    return rc;
}

int bdb_lock_table_write(bdb_state_type *bdb_state, tran_type *tran)
{
    int rc;

    if (tran->parent)
        tran = tran->parent;

    rc = bdb_lock_table_int(bdb_state->dbenv, bdb_state->name,
                            resolve_locker_id(tran), BDB_LOCK_WRITE);

    return rc;
}

int bdb_lock_tablename_write(DB_ENV *dbenv, const char *name, tran_type *tran)
{
    int rc;

    if (tran->parent) tran = tran->parent;

    rc = bdb_lock_table_int(dbenv, name, resolve_locker_id(tran),
                            BDB_LOCK_WRITE);
    return rc;
}

int bdb_lock_ix_value_write(bdb_state_type *bdb_state, tran_type *tran, int idx,
                            DBT *key, DB_LOCK *dblk, DBT *lkname)
{
    if (tran->tranclass != TRANCLASS_LOGICAL)
        return BDBERR_BADARGS;

    assert(NULL == tran->tid);
    return bdb_lock_ix_value_fromlid(bdb_state, tran->logical_lid, NULL, idx,
                                     key, dblk, lkname);
}

int bdb_lock_row_write_getlock(bdb_state_type *bdb_state, tran_type *tran,
                               int idx, unsigned long long genid, DB_LOCK *dblk,
                               DBT *lkname)
{
    int rc;

    rc = bdb_lock_row_int(bdb_state, tran, idx, genid, BDB_LOCK_WRITE, dblk,
                          lkname, 0);

    return rc;
}

int bdb_lock_row_write_getlock_fromlid(bdb_state_type *bdb_state, int lid,
                                       int idx, unsigned long long genid,
                                       DB_LOCK *lk, DBT *lkname)
{
    int rc;
    rc = bdb_lock_row_fromlid(bdb_state, lid, idx, genid, BDB_LOCK_WRITE, lk,
                              lkname, 0);
    return rc;
}

extern int __lock_to_dbt(DB_ENV *dbenv, DB_LOCK *lock, void **ptr, int *sz);

int bdb_release_lock(bdb_state_type *bdb_state, DB_LOCK *lk)
{
    int rc;

    rc = bdb_state->dbenv->lock_put(bdb_state->dbenv, lk);
    if (rc != 0) {
        logmsg(LOGMSG_ERROR, "%s:%d cant release lock rc %d\n", __FILE__, __LINE__,
                rc);
    }

    return rc;
}

int bdb_release_row_lock(bdb_state_type *bdb_state, DB_LOCK *lk)
{
    if (gbl_disable_rowlocks)
        return 0;
    return bdb_release_lock(bdb_state, lk);
}

extern int __nlocks_for_locker(DB_ENV *dbenv, u_int32_t id);
int bdb_nlocks_for_locker(bdb_state_type *bdb_state, int lid)
{
    return __nlocks_for_locker(bdb_state->dbenv, lid);
}
