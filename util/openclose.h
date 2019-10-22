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

#ifndef __COMDB2_CLOSE_H_INCLUDED__
#define __COMDB2_CLOSE_H_INCLUDED__

int comdb2_close_impl(int fd, const char *func, int line);
#define comdb2_close(fd) comdb2_close_impl(fd, __func__, __LINE__)

int comdb2_open_impl(const char *pathname, int flags, mode_t mode, const char *func, int line);
#define comdb2_open(pathname, flags, mode) comdb2_open_impl(pathname, flags, mode, __func__, __LINE__)

#endif
