#include <pthread.h>
#include <sys/queue.h>

#include "thread_util.h"
#include "locks_wrap.h"
#include "cheapstack.h"

static pthread_once_t once = PTHREAD_ONCE_INIT;
static pthread_key_t rwlocker_key;
static pthread_mutex_t lklk = PTHREAD_MUTEX_INITIALIZER;
static int lockid = 1;

struct locker_thread {
    int id;
    arch_tid tid;
    SLIST_ENTRY(locker_thread) lnk;
};

static SLIST_HEAD(, locker_thread) available_locker_threads = SLIST_HEAD_INITIALIZER();

static void release_lockid(void *locker_thread_p) {
    struct locker_thread *t = (struct locker_thread*) locker_thread_p;
    SLIST_INSERT_HEAD(&available_locker_threads, t, lnk);
}

static void debug_locks_init(void) {
    int rc = pthread_key_create(&rwlocker_key, release_lockid);
    if (rc)
        abort();
}

static struct locker_thread *getlocker(void) {
    struct locker_thread *t;
    t = (struct locker_thread*) pthread_getspecific(rwlocker_key);
    if (t == NULL) {
        Pthread_mutex_lock(&lklk);
        if (SLIST_EMPTY(&available_locker_threads)) {
            t = calloc(1, sizeof(struct locker_thread));
            if (t == NULL)
                abort();
            t->id = lockid++;
        }
        else {
            t = SLIST_FIRST(&available_locker_threads);
            SLIST_REMOVE_HEAD(&available_locker_threads, lnk);
        }
        Pthread_mutex_unlock(&lklk);
        pthread_setspecific(rwlocker_key, t);
        t->tid = getarchtid();
    }
    return t;
}

static void rwlock_resize_readers(struct debug_rwlock *rwlock, struct locker_thread *t) {
    if (rwlock->nreadrefs <= t->id) {
        rwlock->readrefs = realloc(rwlock->readrefs, (t->id+1) * sizeof(struct debug_rw_ref));
        for (int i = rwlock->nreadrefs; i <= t->id; i++) {
            memset(&rwlock->readrefs[i], 0, sizeof(struct debug_rw_ref));
        }
        rwlock->nreadrefs = t->id+1;
    }
}

static void rwlock_reset_locker(struct debug_rwlock *rwlock, int id) {
    rwlock->readrefs[id].file = NULL;
    rwlock->readrefs[id].line = 0;
}


int debug_rwlock_destroy(struct debug_rwlock *lk) {
    pthread_once(&once, debug_locks_init);
    int rc = pthread_rwlock_destroy(&lk->lk);
    free(lk->readrefs);
    pthread_mutex_destroy(&lk->lklk);
    return rc;
}

int debug_rwlock_init(struct debug_rwlock *rwlock, const pthread_rwlockattr_t *attr) {
    pthread_once(&once, debug_locks_init);
    int rc = pthread_rwlock_init(&rwlock->lk, NULL);
    if (rc)
        return rc;
    rwlock->nreadrefs = 0;
    rwlock->readrefs = NULL;
    rwlock->writeref.file = NULL;
    rwlock->writeref.line = 0;
    rwlock->writeref.tid = 0;
    Pthread_mutex_init(&rwlock->lklk, NULL);
    return 0;
}

int debug_rwlock_rdlock(struct debug_rwlock *rwlock, const char *file, int line) {
    pthread_once(&once, debug_locks_init);
    struct locker_thread *t = getlocker();
    if (t == NULL)
        abort();
    int rc =  pthread_rwlock_rdlock(&rwlock->lk);

    if (rc == 0) {
        Pthread_mutex_lock(&rwlock->lklk);
        rwlock_resize_readers(rwlock, t);
        rwlock->readrefs[t->id].file = file;
        rwlock->readrefs[t->id].line = line;
        rwlock->readrefs[t->id].tid = t->tid;
        rwlock->readrefs[t->id].ref++;
        Pthread_mutex_unlock(&rwlock->lklk);
    }
    return rc;
}

int debug_rwlock_unlock(struct debug_rwlock *rwlock) {
    pthread_once(&once, debug_locks_init);
    struct locker_thread *t = getlocker();
    if (t == NULL)
        abort();

    Pthread_mutex_lock(&rwlock->lklk);
    if (rwlock->writeref.tid == t->tid) {
        memset(&rwlock->writeref, 0, sizeof(struct debug_rw_ref));
    }
    else {
        rwlock->readrefs[t->id].ref--;
        if (rwlock->readrefs[t->id].ref < 0 && !rwlock->warn_deref) {
            fprintf(stderr, "lock dereferenced when not held?\n");
            cheap_stack_trace();
            rwlock->warn_deref = 1;
        }
        if (rwlock->readrefs[t->id].ref == 0)
            rwlock_reset_locker(rwlock, t->id);
    }
    Pthread_mutex_unlock(&rwlock->lklk);
    return pthread_rwlock_unlock(&rwlock->lk);
}

int debug_rwlock_wrlock(struct debug_rwlock *rwlock, const char *file, int line) {
    pthread_once(&once, debug_locks_init);
    struct locker_thread *t = getlocker();
    if (t == NULL)
        abort();

    int ret = pthread_rwlock_wrlock(&rwlock->lk);
    if (ret == 0) {
        Pthread_mutex_lock(&rwlock->lklk);
        rwlock->writeref.file = file;
        rwlock->writeref.line = line;
        rwlock->writeref.ref = 1;
        rwlock->writeref.tid = t->tid;
        Pthread_mutex_unlock(&rwlock->lklk);
    }
    return ret;
}

int debug_rwlock_tryrdlock(struct debug_rwlock *rwlock, const char *file, int line) {
    pthread_once(&once, debug_locks_init);
    struct locker_thread *t = getlocker();
    if (t == NULL)
        abort();
    int rc = pthread_rwlock_tryrdlock(&rwlock->lk);
    Pthread_mutex_lock(&rwlock->lklk);
    if (rc == 0) {
        rwlock_resize_readers(rwlock, t);
        rwlock->readrefs[t->id].file = file;
        rwlock->readrefs[t->id].line = line;
        rwlock->readrefs[t->id].tid = t->tid;
        rwlock->readrefs[t->id].ref++;
    }
    Pthread_mutex_unlock(&rwlock->lklk);
    return rc;
}

int debug_rwlock_trywrlock(struct debug_rwlock *rwlock, const char *file, int line) {
    pthread_once(&once, debug_locks_init);
    int rc = pthread_rwlock_trywrlock(&rwlock->lk);
    if (rc == 0) {
        struct locker_thread *t = getlocker();
        if (t == NULL)
            abort();
        Pthread_mutex_lock(&rwlock->lklk);
        rwlock->writeref.file = file;
        rwlock->writeref.line = line;
        rwlock->writeref.ref = 1;
        rwlock->writeref.tid = t->tid;
        Pthread_mutex_unlock(&rwlock->lklk);
    }
    return rc;
}
