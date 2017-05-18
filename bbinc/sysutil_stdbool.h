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

#ifndef INCLUDED_SYSUTIL_STDBOOL_H
#define INCLUDED_SYSUTIL_STDBOOL_H

/* sysutil_stdbool.h - a portable <stdbool.h>
 *
 * This file aims to provide portable access to simple boolean values from C89
 * with language extensions, i.e. with a valid _Bool builtin type.
 *
 * Notes:
 *
 * http://www.opengroup.org/onlinepubs/000095399/basedefs/stdbool.h.html (SUSv3)
 * http://www.c-faq.com/bool/booltype.html
 *
 * It is not portable to use bool or _Bool as the type of bitflags in a struct
 * because improper handling by some compilers may result in sign extension.
 * Instead, use unsigned types for bitflags in a struct.
 *
 * C99 provides <stdbool.h>
 * Solaris <= 9 does not provide <stdbool.h>.
 * Solaris 10 provides <stdbool.h> only #if defined(_STDC_C99)
 * AIX 5.3 provides <stdbool.h>
 */

#ifndef __cplusplus

/* Solaris 10 provides <stdbool.h> only #if defined(_STDC_C99) */
/* (fake it out b/c _Bool is valid for all compilations other than strict C89)*/
#ifndef _STDC_C99
#define _STDC_C99
#define NEEDED_STDC_C99
#endif

#include <stdbool.h>

#ifdef NEEDED_STDC_C99
#undef NEEDED_STDC_C99
#undef _STDC_C99
#endif

#endif

#endif
