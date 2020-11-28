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



#ifndef _comdb2systbl_h
#define _comdb2systbl_h

#include "sqlite3.h"
#ifndef _SQL_H_
#include <mem_sqlite.h>
#include <mem_override.h>
#endif

#ifdef __cplusplus
extern "C" {
#endif  /* __cplusplus */

int comdb2SystblInit(sqlite3 *db);

#ifdef __cplusplus
}  /* extern "C" */
#endif  /* __cplusplus */

#endif
