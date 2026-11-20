#include "truncate_log.h"
#include "comdb2.h"
#include "bdb_int.h"
#include <time.h>
#include <phys_rep_lsn.h>

#include <parse_lsn.h>

extern int gbl_physrep_debug;
extern struct dbenv *thedb;

static pthread_mutex_t physrep_truncate_lock = PTHREAD_MUTEX_INITIALIZER;

/* Skip the rewind when the source already agrees with our end-of-log. */
int gbl_physrep_skip_noop_truncation = 1;

/* Seconds to wait for a source that agrees with us but is behind before rewinding to it (0: rewind at once). */
int gbl_physrep_behind_source_max_wait = 300;

static LOG_INFO behind_end;
static time_t behind_since;

/* True once we have waited max_wait at this end-of-log for sources to catch up. */
static int behind_wait_expired(LOG_INFO my_end)
{
    time_t now = time(NULL);
    if (behind_end.file != my_end.file || behind_end.offset != my_end.offset) {
        behind_end = my_end;
        behind_since = now;
    }
    return (now - behind_since) >= gbl_physrep_behind_source_max_wait;
}

int truncate_trylock(void)
{
    return pthread_mutex_trylock(&physrep_truncate_lock);
}

void truncate_unlock(void)
{
    Pthread_mutex_unlock(&physrep_truncate_lock);
}

LOG_INFO handle_truncation(cdb2_hndl_tp *repl_db, LOG_INFO latest_info, int source_gen_changed, int source_rolled_back)
{
    LOG_INFO match_lsn = find_match_lsn(thedb->bdb_env, repl_db, latest_info);

    if (match_lsn.file == 0) {
        if (gbl_physrep_debug)
            logmsg(LOGMSG_USER, "%s: unable to find match-lsn\n", __func__);
        return match_lsn;
    }

    /* Only the rewind's recovery + rep_start lift rep->gen and broadcast a new
     * source generation to our replicants; until then we ignore their messages
     * and they sit at their current LSN.  A standalone physrep has none, so the
     * skew is harmless.  The worker flags the gen change before applying the
     * record that carries it, so logged-gen is not ahead yet -- check both. */
    int clustered = (thedb->nsiblings > 1);
    int rewind_for_gen = clustered && (source_gen_changed || physrep_logged_gen_ahead(thedb->bdb_env));

    /* The anchor is only the last commit; the tail past it normally matches the
     * source too.  Compare forward: a full match means nothing to unwind, and a
     * needless rewind takes the write lock, runs recovery, and bumps
     * log_cursor_gen -- cascading a truncation to every tier below us.  On
     * divergence rewind to the newest agreed record, at worst the anchor. */
    if (gbl_physrep_skip_noop_truncation && !rewind_for_gen) {
        LOG_INFO last_match;
        int rc = physrep_find_last_match(thedb->bdb_env, repl_db, match_lsn, latest_info, &last_match);
        if (rc == 0) {
            logmsg(LOGMSG_USER, "%s: log matches source through my end-of-log {%u:%u}, skipping truncation\n", __func__,
                   last_match.file, last_match.offset);
            return last_match;
        }
        if (rc == PHYSREP_SOURCE_BEHIND && !source_rolled_back) {
            if (!behind_wait_expired(latest_info)) {
                /* file 0 makes the caller drop this source and retry, preferring one that has caught up. */
                logmsg(LOGMSG_USER, "%s: source agrees through {%u:%u} but is behind my end-of-log {%u:%u}, not "
                       "rewinding\n", __func__, last_match.file, last_match.offset, latest_info.file,
                       latest_info.offset);
                LOG_INFO wait = {0};
                return wait;
            }
            logmsg(LOGMSG_USER, "%s: no source caught up to {%u:%u} in %ds, rewinding to {%u:%u}\n", __func__,
                   latest_info.file, latest_info.offset, gbl_physrep_behind_source_max_wait, last_match.file,
                   last_match.offset);
        }
        match_lsn = last_match;
    } else if (gbl_physrep_skip_noop_truncation) {
        logmsg(LOGMSG_USER, "%s: %s, rewinding to move my cluster to the new generation\n", __func__,
               source_gen_changed ? "source generation changed" : "log_gen ahead of rep-gen");
    }

    if (gbl_physrep_debug) {
        logmsg(LOGMSG_USER, "Rewind to lsn: {%u:%u}\n", match_lsn.file,
               match_lsn.offset);
    }

    Pthread_mutex_lock(&physrep_truncate_lock);
    truncate_log(match_lsn.file, match_lsn.offset, 1);
    Pthread_mutex_unlock(&physrep_truncate_lock);

    return match_lsn;
}

int truncate_timestamp(time_t timestamp)
{
    int rc;
    unsigned int file, offset;
    if ((rc = find_log_timestamp(thedb->bdb_env, timestamp, &file, &offset)) !=
        0) {
        logmsg(LOGMSG_ERROR, "Couldn't find a record older than %ld\n",
               timestamp);
        return 1;
    }
    logmsg(LOGMSG_USER, "Found lsn that works {%u:%u}", file, offset);

    return truncate_log(file, offset, 1);
}

int truncate_log(unsigned int file, unsigned int offset, uint32_t flags)
{
    return truncate_log_lock(thedb->bdb_env, file, offset, flags);
}
