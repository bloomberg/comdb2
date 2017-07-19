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

#ifndef _INCLUDED_PORT_WIN32_H_
#define _INCLUDED_PORT_WIN32_H_

#define __LITTLE_ENDIAN__ 1

#define WIN32_LEAN_AND_MEAN
#define _CRT_SECURE_NO_WARNINGS
#include <windows.h>

#include <BaseTsd.h>
typedef SSIZE_T ssize_t;

#define inline __inline
#ifndef __func__
#define __func__ __FUNCTION__
#endif
#ifndef PATH_MAX
#define PATH_MAX MAX_PATH
#endif

/* MSVC does not like POSIX. */

#include <string.h>
#undef strdup
#define strdup _strdup
#undef strtok_r
#define strtok_r strtok_s
/* MSVC does not have strndup(). Define our own. */
char *strndup(const char *s, size_t n);

#include <stdio.h>
#undef snprintf
#define snprintf sprintf_s

#include <stdlib.h>
#undef random
#define random() rand()
#undef srandom
#define srandom(seed) srand(seed)

#include <io.h>
#undef access
#define access _access

#define S_IRGRP 0
#define S_IWGRP 0
#define S_IXGRP 0
#define S_IRWXG 0
#define S_IROTH 0
#define S_IWOTH 0
#define S_IXOTH 0
#define S_IRWXO 0

#include "winsockets.h"
#define cdb2_gethostbyname(hp, nm)                                             \
    do {                                                                       \
        hp = gethostbyname(nm);                                                \
    } while (0)

/* Windows-style Paths */
static char CDB2DBCONFIG_NOBBENV[512] =
    "\\opt\\bb\\etc\\cdb2\\config\\comdb2db.cfg";
/* The real path is COMDB2_ROOT + CDB2DBCONFIG_NOBBENV_PATH */
static char CDB2DBCONFIG_NOBBENV_PATH[] = "\\etc\\cdb2\\config.d\\";
static char CDB2DBCONFIG_TEMP_BB_BIN[512] = "\\bb\\bin\\comdb2db.cfg";

/* Sockpool is not ported yet, so temporarily disable the feature. */
#ifdef _WIN32
#define WITH_SOCK_POOL 0
#endif

#endif
