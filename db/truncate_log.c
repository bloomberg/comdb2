#include "comdb2.h"
#include "bdb_int.h"
#include "truncate_log.h"
#include <phys_rep_lsn.h>
 
extern struct dbenv* thedb;

int truncate_log(unsigned int file, unsigned int offset)
{

    return truncate_log_lock(thedb->bdb_env, file, offset);
}
