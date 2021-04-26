/*
   Copyright 2019 Bloomberg Finance L.P.

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

#ifndef __FDB_SYSTABLE_H_
#define __FDB_SYSTABLE_H_

typedef struct fdb_systable_ent {
    char *dbname;
    char *location;
    char *tablename;
    char *indexname;
    int64_t rootpage;
    int64_t remoterootpage;
    int64_t version;
} fdb_systable_ent_t;

/* Collect/Free existing fdb information */
int fdb_systable_info_collect(void **data, int *npoints);
void fdb_systable_info_free(void *data, int npoints);

#endif
