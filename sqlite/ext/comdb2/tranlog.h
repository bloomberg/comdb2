#ifndef INCLUDED_TRANLOG_H
#define INCLUDED_TRANLOG_H

#include "build/db.h"

/* Define flags for the third argument */
enum {
    TRANLOG_FLAGS_BLOCK             = 0x1,
    TRANLOG_FLAGS_DURABLE           = 0x2,
    TRANLOG_FLAGS_DESCENDING        = 0x4,
};

u_int64_t get_timestamp_from_matchable_record(char *data);

#endif
