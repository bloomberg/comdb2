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
 * These retrieve individual fields from raw log record data without fully
 * unpacking the record, avoiding the overhead of a complete unpack.  Each
 * function peeks at the rectype first so it can choose the correct field
 * offset (the layout differs between "normal" and "apprec" record types).
 *
 * The rectype range check:
 *   (rectype < 10000 && rectype > 2000) || rectype > 12000
 * identifies the "apprec" (application-record / endianize) variants whose
 * on-disk layout includes an extra 8-byte field before the normal fields.
 */

#include "logrecord.h"

#include "build/db.h"
#include "dbinc/db_swap.h"
#include "dbinc_auto/txn_auto.h"
#include "logmsg.h"

/* True when the record is an utxnid variant. */
#define IS_UTXNID(rectype) (((rectype) < 10000 && (rectype) > 2000) || (rectype) > 12000)

uint64_t logrecord_timestamp_regop_gen(char *data)
{
    uint64_t timestamp;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_64(&timestamp, &data[4 + 4 + 8 + 8 + 4 + 4 + 8]);
    } else {
        LOGCOPY_64(&timestamp, &data[4 + 4 + 8 + 4 + 4 + 8]);
    }
    return timestamp;
}

uint32_t logrecord_generation_regop_gen(char *data)
{
    uint32_t generation;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_32(&generation, &data[4 + 4 + 8 + 8 + 4]);
    } else {
        LOGCOPY_32(&generation, &data[4 + 4 + 8 + 4]);
    }
    return generation;
}

uint64_t logrecord_timestamp_dist_commit(char *data)
{
    uint64_t timestamp;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_64(&timestamp, &data[4 + 4 + 8 + 4 + 8 + 8]);
    } else {
        LOGCOPY_64(&timestamp, &data[4 + 4 + 8 + 4 + 8]);
    }
    return timestamp;
}

uint32_t logrecord_generation_dist_commit(char *data)
{
    uint32_t generation;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_32(&generation, &data[4 + 4 + 8 + 8]);
    } else {
        LOGCOPY_32(&generation, &data[4 + 4 + 8]);
    }
    return generation;
}

uint64_t logrecord_timestamp_dist_abort(char *data)
{
    uint64_t timestamp;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_64(&timestamp, &data[4 + 4 + 8 + 4 + 8]);
    } else {
        LOGCOPY_64(&timestamp, &data[4 + 4 + 8 + 4]);
    }
    return timestamp;
}

uint32_t logrecord_generation_dist_abort(char *data)
{
    uint32_t generation;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_32(&generation, &data[4 + 4 + 8 + 8]);
    } else {
        LOGCOPY_32(&generation, &data[4 + 4 + 8]);
    }
    return generation;
}

uint64_t logrecord_timestamp_regop_rowlocks(char *data)
{
    uint64_t timestamp;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_64(&timestamp, &data[4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8]);
    } else {
        LOGCOPY_64(&timestamp, &data[4 + 4 + 8 + 4 + 8 + 8 + 8 + 8]);
    }
    return timestamp;
}

uint32_t logrecord_generation_regop_rowlocks(char *data)
{
    uint32_t generation = 0;
    uint32_t lflags;
    off_t loff;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        loff = 4 + 4 + 8 + 8 + 4 + 8 + 8 + 8 + 8 + 8;
    } else {
        loff = 4 + 4 + 8 + 4 + 8 + 8 + 8 + 8 + 8;
    }
    LOGCOPY_32(&lflags, &data[loff]);
    if (lflags & DB_TXN_LOGICAL_GEN) {
        LOGCOPY_32(&generation, &data[loff + 4]);
    }
    return generation;
}

uint32_t logrecord_timestamp_regop(char *data)
{
    uint32_t timestamp;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_32(&timestamp, &data[4 + 4 + 8 + 8 + 4]);
    } else {
        LOGCOPY_32(&timestamp, &data[4 + 4 + 8 + 4]);
    }
    return timestamp;
}

uint32_t logrecord_timestamp_ckp(char *data)
{
    uint32_t timestamp;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_32(&timestamp, &data[4 + 4 + 8 + 8 + 8 + 8]);
    } else {
        LOGCOPY_32(&timestamp, &data[4 + 4 + 8 + 8 + 8]);
    }
    return timestamp;
}

uint32_t logrecord_generation_ckp(char *data)
{
    uint32_t generation;
    uint32_t rectype;
    LOGCOPY_32(&rectype, data);
    if (IS_UTXNID(rectype)) {
        LOGCOPY_32(&generation, &data[4 + 4 + 8 + 8 + 8 + 8 + 4]);
    } else {
        LOGCOPY_32(&generation, &data[4 + 4 + 8 + 8 + 8 + 4]);
    }
    return generation;
}

uint64_t logrecord_timestamp_matchable(char *data)
{
    uint32_t rectype = 0;
    if (data) {
        LOGCOPY_32(&rectype, data);
        logmsg(LOGMSG_DEBUG, "%s rec: %u\n", __func__, rectype);
    } else {
        logmsg(LOGMSG_DEBUG, "%s: no data, can't get rectype\n", __func__);
    }

    if (rectype == DB___txn_regop_gen || (rectype == DB___txn_regop_gen + 2000) ||
        rectype == DB___txn_regop_gen_endianize || (rectype == DB___txn_regop_gen_endianize + 2000)) {
        return logrecord_timestamp_regop_gen(data);
    }

    if (rectype == DB___txn_dist_commit || (rectype == DB___txn_dist_commit + 2000)) {
        return logrecord_timestamp_dist_commit(data);
    }

    if (rectype == DB___txn_dist_abort || (rectype == DB___txn_dist_abort + 2000)) {
        return logrecord_timestamp_dist_abort(data);
    }

    if (rectype == DB___txn_regop_rowlocks || (rectype == DB___txn_regop_rowlocks + 2000) ||
        rectype == DB___txn_regop_rowlocks_endianize || (rectype == DB___txn_regop_rowlocks_endianize + 2000)) {
        return logrecord_timestamp_regop_rowlocks(data);
    }

    if (rectype == DB___txn_regop || (rectype == DB___txn_regop + 2000)) {
        return logrecord_timestamp_regop(data);
    }

    if (rectype == DB___txn_ckp || (rectype == DB___txn_ckp + 2000) || rectype == DB___txn_ckp_recovery ||
        (rectype == DB___txn_ckp_recovery + 2000)) {
        return logrecord_timestamp_ckp(data);
    }

    return (uint64_t)-1;
}
