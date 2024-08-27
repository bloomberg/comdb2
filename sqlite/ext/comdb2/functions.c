/*
   Copyright 2020 Bloomberg Finance L.P.

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
#include "comdb2systbl.h"
#include "comdb2systblInt.h"
#include "sql.h"
#include "ezsystables.h"
#include "cdb2api.h"
#include "sqliteInt.h"

static int get_functions(void **data, int *npoints)
{
    sqlite3GetAllBuiltinFunctions(data, npoints);
    return 0;
}

static void free_functions(void *p, int n)
{
    sqlite3FreeAllBuiltinFunctions(p, n);
}

sqlite3_module systblFunctionsModule = {
    .access_flag = CDB2_ALLOW_ALL,
};

int systblFunctionsInit(sqlite3 *db)
{
    return create_system_table(
        db, "comdb2_functions", &systblFunctionsModule,
        get_functions, free_functions, sizeof(char *),
        CDB2_CSTRING, "name", -1, 0,
        SYSTABLE_END_OF_FIELDS);
}
