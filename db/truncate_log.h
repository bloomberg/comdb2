#ifndef TRUNCATE_LOG_H
#define TRUNCATE_LOG_H

#include <phys_rep_lsn.h>
#include <time.h>
#include <comdb2.h>

int truncate_log(unsigned int file, unsigned int offset, uint32_t flags);
int truncate_timestamp(time_t timestamp);
int truncate_trylock(void);
void truncate_unlock(void);
/* source_rolled_back: the source truncated or changed generation, so a source behind us must not be waited for. */
LOG_INFO handle_truncation(cdb2_hndl_tp *repl_db, LOG_INFO prev_info, int source_gen_changed, int source_rolled_back);

#endif
