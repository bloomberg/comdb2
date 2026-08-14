/*
   Copyright 2026 Bloomberg Finance L.P.

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
 * Fast field accessors for packed log record buffers.
 *
 * These retrieve individual fields from raw log record data without
 * fully unpacking the record.  Field offsets are relative to the common
 * prefix, whose width depends on the rectype's tags.
 */

#include "logrecord.h"

#include "build/db.h"
#include "dbinc/db_swap.h"
#include "dbinc_auto/txn_auto.h"
#include "logmsg.h"

uint64_t logrecord_timestamp_regop_gen(char *data)
{
    uint64_t timestamp;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_64(&timestamp, &data[prefix + 4 + 4 + 8]);
    return timestamp;
}

uint32_t logrecord_generation_regop_gen(char *data)
{
    uint32_t generation;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_32(&generation, &data[prefix + 4]);
    return generation;
}

uint64_t logrecord_timestamp_dist_commit(char *data)
{
    uint64_t timestamp;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_64(&timestamp, &data[prefix + 4 + 8]);
    return timestamp;
}

uint32_t logrecord_generation_dist_commit(char *data)
{
    uint32_t generation;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_32(&generation, &data[prefix]);
    return generation;
}

uint64_t logrecord_timestamp_dist_abort(char *data)
{
    uint64_t timestamp;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_64(&timestamp, &data[prefix + 4]);
    return timestamp;
}

uint32_t logrecord_generation_dist_abort(char *data)
{
    uint32_t generation;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_32(&generation, &data[prefix]);
    return generation;
}

uint64_t logrecord_timestamp_regop_rowlocks(char *data)
{
    uint64_t timestamp;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_64(&timestamp, &data[prefix + 4 + 8 + 8 + 8 + 8]);
    return timestamp;
}

uint32_t logrecord_generation_regop_rowlocks(char *data)
{
    uint32_t generation = 0;
    uint32_t lflags;
    off_t loff;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    loff = __rectype_prefix_len(rectype) + 4 + 8 + 8 + 8 + 8 + 8;
    LOGCOPY_32(&lflags, &data[loff]);
    if (lflags & DB_TXN_LOGICAL_GEN) {
        LOGCOPY_32(&generation, &data[loff + 4]);
    }
    return generation;
}

uint32_t logrecord_timestamp_regop(char *data)
{
    uint32_t timestamp;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_32(&timestamp, &data[prefix + 4]);
    return timestamp;
}

uint32_t logrecord_timestamp_ckp(char *data)
{
    uint32_t timestamp;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_32(&timestamp, &data[prefix + 8 + 8]);
    return timestamp;
}

uint32_t logrecord_generation_ckp(char *data)
{
    uint32_t generation;
    uint32_t prefix;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    prefix = __rectype_prefix_len(rectype);
    LOGCOPY_32(&generation, &data[prefix + 8 + 8 + 4]);
    return generation;
}

uint64_t logrecord_timestamp_matchable(char *data)
{
    uint32_t rectype = 0, base = 0;
    if (data) {
        LOGCOPY_32(&rectype, data);
        logmsg(LOGMSG_DEBUG, "%s rec: %u\n", __func__, rectype);
    } else {
        logmsg(LOGMSG_DEBUG, "%s: no data, can't get rectype\n", __func__);
    }

    /* Dispatch on the base type; the accessors handle the tags themselves. */
    (void)__rectype_tags(rectype, &base);

    if (base == DB___txn_regop_gen || base == DB___txn_regop_gen_endianize) {
        return logrecord_timestamp_regop_gen(data);
    }

    if (base == DB___txn_dist_commit) {
        return logrecord_timestamp_dist_commit(data);
    }

    if (base == DB___txn_dist_abort) {
        return logrecord_timestamp_dist_abort(data);
    }

    if (base == DB___txn_regop_rowlocks || base == DB___txn_regop_rowlocks_endianize) {
        return logrecord_timestamp_regop_rowlocks(data);
    }

    if (base == DB___txn_regop) {
        return logrecord_timestamp_regop(data);
    }

    if (base == DB___txn_ckp || base == DB___txn_ckp_recovery) {
        return logrecord_timestamp_ckp(data);
    }

    return (uint64_t)-1;
}
