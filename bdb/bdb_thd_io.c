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

#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include <epochlib.h>
#include <build/db.h>
#include <logmsg.h>

static void (*io_start)() = 0;
static void (*io_cmplt)() = 0;

/* gather some data, reset periodically
   same variable for read/pread and write/pwrite
*/

static int long_reads = 0;
static int norm_reads = 0;
static int long_writes = 0;
static int norm_writes = 0;

static int bdb_pread(int fd, void *buf, size_t nbytes, off_t offset)
{
    int rc;
    int t1, t2;
    int errno_sav;

    io_start();

    t1 = comdb2_time_epochms();
    rc = pread(fd, buf, nbytes, offset);
    errno_sav = errno;

    t2 = comdb2_time_epochms();
    if ((t2 - t1) > 2000) {
        logmsg(LOGMSG_WARN, "LONG PREAD: %d ms\n", t2 - t1);
        long_reads++;
    } else {
        norm_reads++;
    }

    io_cmplt();

    errno = errno_sav;
    return rc;
}

static ssize_t bdb_read(int fd, void *buf, size_t nbytes)
{
    int rc;
    int t1, t2;
    int errno_sav;

    io_start();

    t1 = comdb2_time_epochms();
    rc = read(fd, buf, nbytes);
    errno_sav = errno;
    t2 = comdb2_time_epochms();
    if ((t2 - t1) > 2000) {
        logmsg(LOGMSG_WARN, "LONG READ: %d ms\n", t2 - t1);
        long_reads++;
    } else {
        norm_reads++;
    }
    io_cmplt();

    errno = errno_sav;
    return rc;
}

static int bdb_fsync(int fd)
{
    int rc;
    int t1, t2;
    int errno_sav;

    /* io_start(); */

    t1 = comdb2_time_epochms();
    rc = fsync(fd);
    errno_sav = errno;
    t2 = comdb2_time_epochms();
    if ((t2 - t1) > 2000)
        logmsg(LOGMSG_WARN, "LONG FSYNC: %d ms\n", t2 - t1);

    /* io_cmplt(); */

    errno = errno_sav;
    return rc;
}

static int bdb_pwrite(int fd, const void *buf, size_t nbytes, off_t offset)
{
    int rc;
    int t1, t2;
    int errno_sav;

    io_start();

    t1 = comdb2_time_epochms();
    rc = pwrite(fd, buf, nbytes, offset);
    errno_sav = errno;
    t2 = comdb2_time_epochms();
    if ((t2 - t1) > 2000) {
        logmsg(LOGMSG_WARN, "LONG PWRITE: %d ms\n", t2 - t1);
        long_writes++;
    } else {
        norm_writes++;
    }

    io_cmplt();

    errno = errno_sav;
    return rc;
}

static ssize_t bdb_write(int fd, const void *buf, size_t nbytes)
{
    int rc;
    int t1, t2;
    int errno_sav;

    io_start();

    t1 = comdb2_time_epochms();
    rc = write(fd, buf, nbytes);
    errno_sav = errno;
    t2 = comdb2_time_epochms();
    if ((t2 - t1) > 2000) {
        logmsg(LOGMSG_WARN, "LONG WRITE: %d ms\n", t2 - t1);
        long_writes++;
    } else {
        norm_writes++;
    }

    io_cmplt();

    errno = errno_sav;
    return rc;
}

void bdb_get_iostats(int *n_reads, int *l_reads, int *n_writes, int *l_writes)
{
    *n_reads = norm_reads;
    *l_reads = long_reads;
    *n_writes = norm_writes;
    *l_writes = long_writes;

    norm_reads = long_reads = norm_writes = long_writes = 0;
}
