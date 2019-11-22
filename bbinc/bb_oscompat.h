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

#ifndef INCLUDED_BB_OSCOMPAT_H
#define INCLUDED_BB_OSCOMPAT_H

#include <dirent.h>

#ifdef __cplusplus
extern "C" {
#endif

struct in_addr;

typedef int(hostbyname)(char **, struct in_addr *);

hostbyname *get_os_hostbyname();
void set_hostbyname(hostbyname *);

hostbyname comdb2_gethostbyname;
void comdb2_getservbyname(const char *, const char *, short *);
int bb_readdir(DIR *d, void *buf, struct dirent **dent);
char *comdb2_realpath(const char *path, char *resolved_path);

#ifdef __cplusplus
}
#endif

#endif
