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

#ifndef _INCLUDED_PORT_WIN32_UNISTD_H_
#define _INCLUDED_PORT_WIN32_UNISTD_H_
#include <windows.h>

#define R_OK 0x04

typedef DWORD pid_t;
#define getpid() GetCurrentProcessId()

long gethostid(void);

typedef DWORD uid_t;
#define getuid() 0
#define geteuid() 0

#endif
