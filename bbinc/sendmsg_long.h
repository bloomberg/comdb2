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

#ifndef INCLUDED_SENDMSG_LONG
#define INCLUDED_SENDMSG_LONG

#include <sys/types.h>

#if defined __cplusplus
extern "C" {
#endif

int sendmsg_long(const char *taskname, const char *msg, char *err, size_t errsz,
                 int *rcode, int flags);

#if defined __cplusplus
}
#endif

#endif
