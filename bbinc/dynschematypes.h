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

#ifndef __DYNSCHEMATYPES_H__
#define __DYNSCHEMATYPES_H__

#if defined __cplusplus
extern "C" {
#endif

enum comdb2_fielddatatypes {
    COMDB2_INT = 1,
    COMDB2_UINT = 2,
    COMDB2_SHORT = 3,
    COMDB2_USHORT = 4,
    COMDB2_FLOAT = 5,
    COMDB2_DOUBLE = 6,
    COMDB2_BYTE = 7,
    COMDB2_CSTR = 8,
    COMDB2_PSTR = 9,
    COMDB2_LONGLONG = 10,
    COMDB2_ULONGLONG = 11,
    COMDB2_BLOB = 12,
    COMDB2_DATETIME = 13,
    COMDB2_UTF8 = 14,
    COMDB2_VUTF8 = 15,
    COMDB2_INTERVALYM = 16,
    COMDB2_INTERVALDS = 17,
    COMDB2_DATETIMEUS = 18,
    COMDB2_INTERVALDSUS = 19,
    COMDB2_LAST_TYPE
};

#if defined __cplusplus
}
#endif

#endif
