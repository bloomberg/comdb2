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

#ifndef INCLUDED_COMDB2_UUID_H
#define INCLUDED_COMDB2_UUID_H

typedef unsigned char uuid_t[16];
typedef char uuidstr_t[37];

void comdb2uuid(uuid_t);
char *comdb2uuidstr(const uuid_t, uuidstr_t);
int comdb2uuidcmp(const uuid_t, const uuid_t);
void comdb2uuidcpy(uuid_t dst, const uuid_t src);
void comdb2uuid_clear(uuid_t);
int comdb2uuid_is_zero(const uuid_t);
void init_zero_uuid(void);

#endif
