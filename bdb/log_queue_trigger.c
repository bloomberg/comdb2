/* Rebuild queue-payload from transaction log & invoke callback */
#include <log_trigger.h>
#include <log_queue_trigger.h>
#include <bdb_int.h>
#include <assert.h>
#include <db_int.h>
#include <dbinc/btree.h>
#include <build/db.h>
#include <epochlib.h>
#include <dbinc_auto/btree_auto.h>

#define MAX_QUEUE_FILES 64

#define FLAGS_BIG 1
#define FLAGS_UNPACKED 2

static comdb2ma qtlogqma;

static void logqm_create(void)
{
    qtlogqma = comdb2ma_create(0, 0, "log_queue_triggers", 1);
}

static comdb2ma logqma(void)
{
    static pthread_once_t once = PTHREAD_ONCE_INIT;
    pthread_once(&once, logqm_create);
    assert(qtlogqma);
    return qtlogqma;
}

struct big_segment {
    LINKC_T(struct big_segment) lnk;
    u_int32_t size;
    u_int8_t data[1];
};

struct log_queue_record {
    u_int64_t txnid;
    u_int8_t key[12];
    u_int8_t *record;
    LISTC_T(struct big_segment) big;
    void *freeptr;
    struct odh odh;
    int record_len;
    int offset;
    int bigcnt;
    u_int32_t flags;
    DB_LSN commit_lsn;
    int qtime;
    LINKC_T(struct log_queue_record) lnk;
};

struct log_queue_file {
    char *filename;
    bdb_state_type *bdb_state;
    pthread_mutex_t hash_lk;
    pthread_mutex_t queue_lk;
    pthread_cond_t queue_cd;
    hash_t *transactions;
    logqueue_trigger_callback func;
    u_int32_t flags;
    void *userptr;
    int lastpr;
    u_int64_t longq_cnt;
    bdb_state_type *(*gethndl)(const char *q);
    pthread_t consumer;
    size_t cursz;
    size_t maxsz;
    size_t droppedbytes;
    size_t dropcnt;
    int lastwarn;
    LISTC_T(struct log_queue_record) outqueue;
};

static int current_queue_files = 0;
struct log_queue_file log_queue_files[MAX_QUEUE_FILES] = {0};

static void *qmalloc(size_t sz)
{
    return comdb2_malloc(logqma(), sz);
}

static inline void qfree(void *ptr)
{
    comdb2_free(ptr);
}

static inline void *qrealloc(void *ptr, size_t sz)
{
    return comdb2_realloc(logqma(), ptr, sz);
}

static inline void *qcalloc(size_t nmemb, size_t size)
{
    return comdb2_calloc(logqma(), nmemb, size);
}

static inline char *qstrdup(const char *str)
{
    return comdb2_strdup(logqma(), str);
}

/* Lockless file lookup */
static inline struct log_queue_file *retrieve_log_queue(const char *filename)
{
    for (int i = 0; i < current_queue_files; i++) {
        if (strcmp(log_queue_files[i].filename, filename) == 0) {
            return &log_queue_files[i];
        }
    }
    return NULL;
}

static struct log_queue_file *add_log_queue(const char *filename, bdb_state_type *(*gethndl)(const char *q),
                                            logqueue_trigger_callback func, void *userptr, int maxsz, u_int32_t flags)
{
    struct log_queue_file *file = retrieve_log_queue(filename);
    if (!file) {
        if (current_queue_files >= MAX_QUEUE_FILES) {
            logmsg(LOGMSG_ERROR, "%s out of queue-files\n", __func__);
            return NULL;
        }

        log_queue_files[current_queue_files].filename = qstrdup(filename);
        log_queue_files[current_queue_files].transactions = hash_setalloc_init(qmalloc, qfree, 0, sizeof(u_int64_t));
        listc_init(&log_queue_files[current_queue_files].outqueue, offsetof(struct log_queue_record, lnk));

        Pthread_mutex_init(&log_queue_files[current_queue_files].hash_lk, NULL);
        Pthread_mutex_init(&log_queue_files[current_queue_files].queue_lk, NULL);
        Pthread_cond_init(&log_queue_files[current_queue_files].queue_cd, NULL);
        file = &log_queue_files[current_queue_files++];
    } else {
        logmsg(LOGMSG_ERROR, "%s log-queue callback for %s already exists\n", __func__, filename);
        return NULL;
    }

    if (file) {
        file->gethndl = gethndl;
        file->func = func;
        file->userptr = userptr;
        file->maxsz = maxsz;
        file->flags = flags;
    }

    return file;
}

static void log_queue_record_free(struct log_queue_record *txn)
{
    struct big_segment *seg;
    while ((seg = listc_rtl(&txn->big)) != NULL) {
        qfree(seg);
    }
    if (txn->freeptr != NULL)
        comdb2_free(txn->freeptr);
    if (txn->record != NULL)
        comdb2_free(txn->record);
    comdb2_free(txn);
}

static int log_queue_record_free_hash(void *obj, void *arg)
{
    struct log_queue_record *txn = obj;
    log_queue_record_free(txn);
    return 0;
}

extern int bdb_unpack_updateid(bdb_state_type *bdb_state, const void *from, size_t fromlen, void *to, size_t tolen,
                               struct odh *odh, int updateid, void **freeptr, int verify_updateid, int force_odh,
                               void *(*fn_malloc)(size_t), void (*fn_free)(void *));

static int add_to_outqueue(struct log_queue_file *file, struct log_queue_record *txn)
{
    int dropped = 0;
    Pthread_mutex_lock(&file->queue_lk);
    txn->qtime = comdb2_time_epoch();
    listc_abl(&file->outqueue, txn);
    file->cursz += txn->record_len;
    while (file->maxsz > 0 && file->cursz > file->maxsz) {
        struct log_queue_record *rm = listc_rtl(&file->outqueue);
        file->droppedbytes += rm->record_len;
        file->dropcnt++;
        file->cursz -= rm->record_len;
        log_queue_record_free(rm);
        dropped = 1;
    }
    Pthread_cond_signal(&file->queue_cd);
    Pthread_mutex_unlock(&file->queue_lk);
    if (dropped && comdb2_time_epoch() - file->lastwarn) {
        logmsg(LOGMSG_WARN, "%s log-queue %s dropped %zu bytes\n", __func__, file->filename, file->droppedbytes);
        file->lastwarn = comdb2_time_epoch();
    }
    return 0;
}

static int handle_big(const DB_LSN *commit_lsn, struct log_queue_file *file, u_int64_t txnid, const __db_big_args *argp)
{
    Pthread_mutex_lock(&file->hash_lk);
    struct log_queue_record *txn = hash_find(file->transactions, &txnid);
    if (!txn) {
        abort();
    }
    assert(txn);
    assert(!log_compare(&txn->commit_lsn, commit_lsn));
    txn->bigcnt++;
    txn->flags |= FLAGS_BIG;
    struct big_segment *seg = qmalloc(sizeof(struct big_segment) + argp->dbt.size);
    seg->size = argp->dbt.size;
    memcpy(seg->data, argp->dbt.data, argp->dbt.size);
    listc_abl(&txn->big, seg);
    txn->record_len += argp->dbt.size;
    Pthread_mutex_unlock(&file->hash_lk);
    return 0;
}

static int handle_addrem(const DB_LSN *commit_lsn, struct log_queue_file *file, u_int64_t txnid,
                         const __db_addrem_args *argp)
{
    if (argp->hdr.size > 0) {
        return 0;
    }
    Pthread_mutex_lock(&file->hash_lk);
    struct log_queue_record *txn = hash_find(file->transactions, &txnid);
    if (txn == NULL) {

        assert(argp->indx % 2 == 0);
        assert(argp->dbt.size == 12);

        txn = qcalloc(sizeof(struct log_queue_record), 1);
        txn->txnid = txnid;
        txn->commit_lsn = *commit_lsn;
        listc_init(&txn->big, offsetof(struct big_segment, lnk));
        memcpy(txn->key, argp->dbt.data, argp->dbt.size);
        hash_add(file->transactions, txn);
        Pthread_mutex_unlock(&file->hash_lk);
        return 0;
    }

    assert(!log_compare(&txn->commit_lsn, commit_lsn));
    assert(argp->indx % 2 == 1);
    if (!(txn->flags & FLAGS_BIG)) {
        txn->record_len = argp->dbt.size;
        txn->record = qmalloc(txn->record_len);
        memcpy(txn->record, argp->dbt.data, argp->dbt.size);
    }
    hash_del(file->transactions, txn);
    Pthread_mutex_unlock(&file->hash_lk);

    return add_to_outqueue(file, txn);
}

static inline int nameboundry(char c)
{
    switch (c) {
    case '/':
    case '.':
        return 1;
    default:
        return 0;
    }
}

static int log_queue_trigger_callback(const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *infilename,
                                      u_int32_t rectype, const void *log)
{
    char *mfile = alloca(strlen(infilename) + 1);
    strcpy(mfile, infilename);

    char *start = (char *)&mfile[0];
    char *end = (char *)&mfile[strlen(infilename) - 1];

    while (end > start && *end != '\0' && *end != '.') {
        end--;
    }
    end -= 17;
    if (end <= start) {
        logmsg(LOGMSG_ERROR, "%s invalid file %s\n", __func__, infilename);
        return -1;
    }

    char *fstart = end, *filename = NULL;
    while (fstart > start && (end - fstart) <= MAXTABLELEN) {
        if (nameboundry(*(fstart - 1))) {
            end[0] = '\0';
            filename = fstart;
            break;
        }
        fstart--;
    }

    struct log_queue_file *file;
    if (!filename || (file = retrieve_log_queue(filename)) == NULL) {
        return -1;
    }

    int rc = normalize_rectype(&rectype);
    if (rectype > 1000 && rectype < 10000) {
        rectype = rectype - 1000;
    }

    switch (rectype) {
    case DB___db_addrem: {
        const __db_addrem_args *argp = log;
        u_int64_t txnid = rc ? argp->txnid->utxnid : argp->txnid->txnid;
        if (argp->opcode == DB_ADD_DUP) {
            return handle_addrem(commit_lsn, file, txnid, argp);
        }
    }
    case DB___db_big: {
        const __db_big_args *argp = log;
        u_int64_t txnid = rc ? argp->txnid->utxnid : argp->txnid->txnid;
        if (argp->opcode == DB_ADD_BIG) {
            return handle_big(commit_lsn, file, txnid, argp);
        }
    }
    default:
        return 0;
    }
}

static int unpack_queue_record(struct log_queue_file *file, struct log_queue_record *txn)
{
    if (file->bdb_state == NULL && file->gethndl) {
        file->bdb_state = file->gethndl(file->filename);
    }

    if (!(txn->flags & FLAGS_UNPACKED) && file->func && file->bdb_state) {
        if (file->flags & LOGQTRIGGER_MASTER_ONLY) {
            if (!bdb_amimaster(file->bdb_state->parent)) {
                return 0;
            }
        }
        if (txn->flags & FLAGS_BIG) {
            txn->record = qmalloc(txn->record_len);
            int offset = 0;
            struct big_segment *seg;
            while ((seg = listc_rtl(&txn->big)) != NULL) {
                memcpy(txn->record + offset, seg->data, seg->size);
                offset += seg->size;
                qfree(seg);
            }
            assert(offset == txn->record_len);
        }
        bdb_unpack_updateid(file->bdb_state, txn->record, txn->record_len, NULL, 0, &txn->odh, 0, &txn->freeptr, 0, 0,
                            qmalloc, qfree);
        txn->flags |= FLAGS_UNPACKED;
    }
    return (txn->flags & FLAGS_UNPACKED);
}

void *log_queue_consumer(void *arg)
{
    struct log_queue_file *file = arg;
    while (1) {
        Pthread_mutex_lock(&file->queue_lk);
        while (listc_size(&file->outqueue) == 0) {
            Pthread_cond_wait(&file->queue_cd, &file->queue_lk);
        }
        struct log_queue_record *txn = listc_rtl(&file->outqueue);
        if (comdb2_time_epoch() - txn->qtime > 0) {
            file->longq_cnt++;
            if (comdb2_time_epoch() - file->lastpr) {
                logmsg(LOGMSG_ERROR, "%s long-queue %d, cnt=%llu\n", __func__, comdb2_time_epoch() - txn->qtime,
                       file->longq_cnt);
                file->lastpr = comdb2_time_epoch();
            }
        }
        file->cursz -= txn->record_len;
        Pthread_mutex_unlock(&file->queue_lk);
        if (unpack_queue_record(file, txn)) {
            DBT key = {.data = txn->key, .size = 12};
            DBT data = {.data = txn->odh.recptr, .size = txn->odh.length};
            file->func(file->bdb_state, &txn->commit_lsn, file->filename, &key, &data, file->userptr);
        }
        log_queue_record_free(txn);
    }
}

static inline void logqueue_wait(struct log_queue_file *file, int timeoutms)
{
    while (listc_size(&file->outqueue) == 0) {
        if (timeoutms < 0) {
            Pthread_cond_wait(&file->queue_cd, &file->queue_lk);
        } else if (timeoutms > 0) {
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            if (timeoutms >= 1000) {
                timeoutms -= 1000;
                ts.tv_sec++;
            } else {
                ts.tv_nsec += timeoutms * 1000000;
                if (ts.tv_nsec >= 1000000000) {
                    ts.tv_sec++;
                    ts.tv_nsec -= 1000000000;
                }
                timeoutms = 0;
            }
            pthread_cond_timedwait(&file->queue_cd, &file->queue_lk, &ts);
        } else {
            break;
        }
    }
}

int logqueue_trigger_get(void *qfile, DBT *key, DBT *data, int timeoutms)
{
    struct log_queue_file *file = qfile;
    int rc = -1;
    Pthread_mutex_lock(&file->queue_lk);
    logqueue_wait(file, timeoutms);
    if (listc_size(&file->outqueue) > 0) {
        struct log_queue_record *txn = listc_rtl(&file->outqueue);
        file->cursz -= txn->record_len;
        Pthread_mutex_unlock(&file->queue_lk);
        if (unpack_queue_record(file, txn)) {
            key->size = 12;
            key->data = malloc(12);
            memcpy(key->data, txn->key, 12);
            data->size = txn->odh.length;
            data->data = malloc(data->size);
            memcpy(data->data, txn->odh.recptr, data->size);
            rc = 0;
        }
        log_queue_record_free(txn);
    } else {
        Pthread_mutex_unlock(&file->queue_lk);
    }
    return rc;
}

/* Register log-queue trigger */
void *register_logqueue_trigger(const char *filename, bdb_state_type *(*gethndl)(const char *q),
                                logqueue_trigger_callback func, void *userptr, size_t maxsz, u_int32_t flags)
{
    struct log_queue_file *file = add_log_queue(filename, gethndl, func, userptr, maxsz, flags);
    if (file) {
        if (flags & LOGQTRIGGER_PUSH) {
            Pthread_create(&file->consumer, NULL, log_queue_consumer, file);
        }
        log_trigger_register(filename, log_queue_trigger_callback, 0);
        return file;
    }

    return NULL;
}
