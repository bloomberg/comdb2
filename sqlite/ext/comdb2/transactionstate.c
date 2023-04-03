/*
   Copyright 2022 Bloomberg Finance L.P.

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

#include <stdlib.h>
#include <string.h>
#include <stddef.h>

#include "comdb2.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "types.h"
#include "transactionstate_systable.h"

sqlite3_module systblTransactionStateModule = {
    .access_flag = CDB2_ALLOW_USER,
};

int get_thread_info(void **data, int *npoints) {
    return get_thd_info((thd_info **) data, npoints);
}

void free_thread_info(void *data, int npoints) {
    free_thd_info((thd_info *) data, npoints);
}

int systblTransactionStateInit(sqlite3 *db) {
    return create_system_table(
        db, "comdb2_transaction_state", &systblTransactionStateModule,
        get_thread_info, free_thread_info, sizeof(thd_info),
        CDB2_CSTRING, "state", -1, offsetof(thd_info, state),
        CDB2_REAL, "time", offsetof(thd_info, isIdle), offsetof(thd_info, time),
        CDB2_CSTRING, "machine", offsetof(thd_info, isIdle), offsetof(thd_info, machine),
        CDB2_CSTRING, "opcode", offsetof(thd_info, isIdle), offsetof(thd_info, opcode),
        CDB2_CSTRING, "function", offsetof(thd_info, isIdle), offsetof(thd_info, function),
        SYSTABLE_END_OF_FIELDS);
}
