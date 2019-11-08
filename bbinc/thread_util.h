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

#ifndef INCLUDED_THREAD_UTIL_H
#define INCLUDED_THREAD_UTIL_H

enum { RESOURCE_BERKELEY_LOCK };

void thread_started(char *name);
void thread_add_resource(int type, void *resource);
void thread_remove_resource(void *resource, void (*freefunc)(void *));
void thread_util_register_describe_function(int type, void (*func)(void *));
void thread_util_init(void);
void thread_util_enable_debug(void);
void thread_util_disable_debug(void);
void thread_util_dump_on_exit_enable(void);
void thread_util_dump_on_exit_disable(void);
void thread_util_donework(void);

/* define architecture thread identifies for sane debugging */
#if defined(_LINUX_SOURCE)
typedef pid_t arch_tid;
#elif defined(_IBM_SOURCE)
typedef tid_t arch_tid;
#else
typedef pthread_t arch_tid;
#endif

/* getarchtid */
arch_tid getarchtid(void);

#endif
