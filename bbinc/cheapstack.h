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

#ifndef INCLUDED_CHEAPSTACK
#define INCLUDED_CHEAPSTACK

#if defined __cplusplus
extern "C" {
#endif

#include <sysutil_compilerdefs.h>
#include <comdb2_walkback.h>
#include <logmsg.h>

/*
 * these wrappers are direct call only
 * do not call these from a signal handler
 */

void comdb2_linux_cheap_stack_trace(void);
#define cheap_stack_trace()                                                                                            \
    do {                                                                                                               \
        pthread_t pid = pthread_self();                                                                                \
        logmsg(LOGMSG_USER, "Comdb2's Linux Cheap Stack Trace :: pthread_self:%p @ %s:%d\n", (void *)pid, __FILE__,    \
               __LINE__);                                                                                              \
        comdb2_linux_cheap_stack_trace();                                                                              \
    } while (0)

#if defined __cplusplus
}
#endif

#endif
