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


#ifndef INCLUDED_SYSTABLE_H
#define INCLUDED_SYSTABLE_H

enum {
    SYSTABLE_END_OF_FIELDS = -1
};

enum {
    SYSTABLE_FIELD_NULLABLE  = 0x1000,
};

/* All other types have enough information to determine size.  Blobs need a little help. */
typedef struct {
    void *value;
    size_t size;
} systable_blobtype;

int create_system_table(sqlite3 *db, char *name, sqlite3_module *module,
        int(*init_callback)(void **data, int *npoints),
        void(*release_callback)(void *data, int npoints),
        size_t struct_size,
        // type, name, offset,  type2, name2, offset2, ..., SYSTABLE_END_OF_FIELDS
        ...);

#endif
