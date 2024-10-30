#include <log_trigger.h>
#include <cdb2_constants.h>

struct log_trigger {
    char *tablename;
    u_int32_t flags;
    int (*cb)(const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *filename, u_int32_t rectype, const void *log);
};
static hash_t *trigger_hash = NULL;
static pthread_mutex_t trigger_lk = PTHREAD_MUTEX_INITIALIZER;

static hash_t *log_trigger_hash()
{
    if (trigger_hash == NULL) {
        trigger_hash = hash_init_strptr(offsetof(struct log_trigger, tablename));
    }
    return trigger_hash;
}

void *log_trigger(const char *tablename)
{
    struct log_trigger *lt;
    pthread_mutex_lock(&trigger_lk);
    hash_t *th = log_trigger_hash();
    lt = hash_find(th, &tablename);
    pthread_mutex_unlock(&trigger_lk);
    return lt;
}

/* Either XXX.<tablename> or full path */
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

void *log_trigger_btree_trigger(const char *btree)
{
    char *start = (char *)&btree[0];
    char *end = (char *)&btree[strlen(btree) - 1];

    while (end > start && *end != '\0' && *end != '.') {
        end--;
    }
    end -= 17;
    if (end <= start) {
        return 0;
    }
    char *fstart = end;
    while (fstart > start && (end - fstart) <= MAXTABLELEN) {
        if (nameboundry(*(fstart - 1))) {
            char t[MAXTABLELEN + 1] = {0};
            memcpy(t, fstart, (end - fstart));
            return log_trigger(t);
        }
        fstart--;
    }
    return NULL;
}

int log_trigger_callback(void *trigger, const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *filename,
                         u_int32_t rectype, const void *log)
{
    struct log_trigger *lt = trigger;
    int rc = 0;
    if (lt && lt->cb) {
        rc = lt->cb(lsn, commit_lsn, filename, rectype, log);
    }
    if (lt && (lt->flags & LOG_TRIGGER_VERBOSE)) {
        logmsg(LOGMSG_INFO, "%s %d:%d %d:%d %s %u\n", __func__, lsn->file, lsn->offset, commit_lsn->file,
               commit_lsn->offset, filename, rectype);
    }
    return rc;
}

int log_trigger_register(const char *tablename,
                         int (*cb)(const DB_LSN *lsn, const DB_LSN *commit_lsn, const char *filename, u_int32_t rectype,
                                   const void *log),
                         u_int32_t flags)
{
    struct log_trigger *lt;
    pthread_mutex_lock(&trigger_lk);
    hash_t *th = log_trigger_hash();
    if ((lt = hash_find(th, &tablename)) == NULL) {
        lt = malloc(sizeof(struct log_trigger));
        lt->tablename = strdup(tablename);
        hash_add(th, lt);
    }
    lt->cb = cb;
    lt->flags = flags;
    pthread_mutex_unlock(&trigger_lk);
    return 0;
}

int log_trigger_add_table(char *tablename)
{
    return log_trigger_register(tablename, NULL, LOG_TRIGGER_VERBOSE);
}
