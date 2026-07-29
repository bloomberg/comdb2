#ifndef TRUNCATE_LOG_H
#define TRUNCATE_LOG_H

#include <phys_rep_lsn.h>
#include <time.h>
#include <comdb2.h>

int truncate_log(unsigned int file, unsigned int offset, uint32_t flags);
int truncate_timestamp(time_t timestamp);
int truncate_trylock(void);
void truncate_unlock(void);
LOG_INFO handle_truncation(cdb2_hndl_tp *repl_db, LOG_INFO prev_info);

/* Best-effort probe: does the connected source cover our current log range?
 * Returns 1 (covers), 0 (no overlap), or -1 (unknown/transient). */
int physrep_source_covers_me(void *bdb_state, cdb2_hndl_tp *repl_db);

#endif
