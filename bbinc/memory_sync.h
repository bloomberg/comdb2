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

#ifndef INCLUDED_MEMORY_SYNC_H
#define INCLUDED_MEMORY_SYNC_H

#if defined(__ppc__) || defined(_ARCH_PPC) || defined(_ARCH_PWR) ||            \
    defined(_ARCH_PWR2) || defined(_POWER)

#if !defined(__GNUC__)

#if defined(_AIX)

/* This is our classic, time-honoured AIX implementation of MEMORY_SYNC.
 * According to http://www.ibm.com/developerworks/systems/articles/powerpc.html,
 * Appendix A, this is a full sync (or just "sync"). */
void machine_memory_sync_aix();
#pragma mc_func machine_memory_sync_aix{"7c0004ac"}
#pragma reg_killed_by machine_memory_sync_aix
#define MEMORY_SYNC machine_memory_sync_aix()

#elif defined(__linux__)

/* Linux/Power, but no GCC - assume xlc */
#define MEMORY_SYNC __sync

#endif

#else

/* GCC implementation - so that we can compile successfully with
 * IS_GCC_WARNINGS_CLEAN=true.  This is stolen from sysutil_membar.h.
 * This is also used on Linux/POWER. */
#define MEMORY_SYNC asm __volatile__("sync" : : : "memory")

#endif

#elif defined(__hpux)
#include <machine/sys/kern_inline.h>
#define MEMORY_SYNC _MF()
#else

#define MEMORY_SYNC
#endif

#endif /* INCLUDED_MEMORY_SYNC_H */
