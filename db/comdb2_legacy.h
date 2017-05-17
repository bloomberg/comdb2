/*
   Copyright 2015 Bloomberg Finance L.P.

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

#ifndef INCLUDED_COMDB2_LEGACY_H
#define INCLUDED_COMDB2_LEGACY_H

/* Support for legacy request types. */

typedef struct {
    char name[32];    /* name of field as a \0 terminated string */
    int type;         /* one of the types in dynschematypes.h    */
    unsigned int len; /* length of field in bytes                */
    unsigned int off; /* offset of field in record structure     */
    int reserved[5];
} comdb2_field_type;

enum db_api_const {
    DB_MAX_KEYS = 28,

    /* DRQS 8241665 - max key size multiplied by 2. - Johan Nystrom */
    DB_MAX_SERVER_KEY_SIZE = 512,
    DB_MAX_CLIENT_KEY_SIZE = 256,
    DB_MAX_ASK_ARRAY = 1024,
    DB_MAX_ASK_SEGS = (DB_MAX_ASK_ARRAY - 2) / 2,
    DBD_SET_DATA_LENGTH = -1,
    DB_MAX_BLOB_FIELDS = 15
};

enum range_constants {
    DB_RANGE_EXTRACT = 0,
    DB_RANGE_EXTRACT_REVERSE = 1,
    DB_RANGE_INTRL_STATE_WIDTH = 10,
    DB_RANGE_BLOCK_BUFFER_SIZE =
        (16 * 1024 + DB_RANGE_INTRL_STATE_WIDTH * sizeof(int) +
         DB_MAX_CLIENT_KEY_SIZE + 4) /* 16K buffer..this can change.. 
                                               16K data + STATE + max key size + 4 byte rrn */,
    DB_RANGE_NO_COUNT = -99,
    DB_RANGE_COUNT = 0,
    DB_RANGE_END = -999,
    DB_RANGE_MAX_MATCHES = 1000000,
    DB_RANGE_MAX_NOLIMIT = 0
};

#endif
