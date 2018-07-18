/*
   Copyright 2018 Bloomberg Finance L.P.

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


#ifndef _DB_ACTIVELOCKS_H
#define _DB_ACTIVELOCKS_H
typedef int (*collect_locks_func)(void *args, int64_t threadid, int32_t lockerid,
        char *mode, char *status, char *table, char *rectype);
#endif
