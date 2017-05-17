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

#ifndef INCLUDED_COMDB2_PTHREAD_CREATE_H
#define INCLUDED_COMDB2_PTHREAD_CREATE_H

#include <pthread.h>
#include "mem.h"

/*
** pthread_create with application managed stack.
**
** PARAMETERS
** allocator - where is stack obtained
** stacksz   - size of stack in bytes, should be a multiple of pagesize
**
** RETURN VALUES
** see pthread_create(3)
*/
int comdb2_pthread_create(pthread_t *thread, pthread_attr_t *attr,
                          void *(*start_routine)(void *), void *arg,
                          comdb2ma allocator, size_t stacksz);

#endif
