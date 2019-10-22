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

#include <unistd.h>
#include <stdio.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

int gbl_track_close = 0;
int gbl_track_open = 0;

int comdb2_open_impl(const char *pathname, int flags, mode_t mode, const char *func, int line)
{
    int fd = -1;
    fd = open(pathname, flags, mode);
    if (gbl_track_open) {
        fprintf(stderr, "Opening %s flags 0x%x mode 0x%x fd %d from %s line "
                "%d\n", pathname, flags, mode, fd, func, line);
    }
    return fd;
}

int comdb2_close_impl(int fd, const char *func, int line)
{
    if (gbl_track_close) {
        fprintf(stderr, "Closing fd %d from %s line %d\n", fd, func, line);
    }
    return close(fd);
}
