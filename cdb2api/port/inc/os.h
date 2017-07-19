/*
   Copyright 2017, Bloomberg Finance L.P.

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

#ifndef _INCLUDED_PORT_OS_H_
#define _INCLUDED_PORT_OS_H_

#if defined(__linux__) || defined(__linux) || defined(linux) /* Linux */
#include <linux.h>
#elif defined(sun) || defined(__sun) /* Solaris */
#include <solaris.h>
#elif defined(_AIX) /* AIX */
#include <aix.h>
#elif defined(_WIN32) /* Windows */
#include <win32.h>
#else /* Untested. Assume it is POSIX-compliant and little-endian. */
#include <posix.h>
#define __LITTLE_ENDIAN__ 1
#endif

#endif
