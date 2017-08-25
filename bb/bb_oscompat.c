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

/**
 * The idea of this module is to (where possible) provide a single interface to
 * library/OS calls that vary from system to system.  If this file gets too big,
 * we may want to split it out into bb_oscompat_<arch>.c.  I'm starting with one
 * common file because I don't think cscheckin/robocop can cope with the above
 *well.
 **/

#if defined(_SUN_SOURCE)
#ifndef _REENTRANT
#define _REENTRANT
#endif
#endif
#if defined(_LINUX_SOURCE)
#define __USE_MISC /* getservbyname_r() */
#ifndef _GNU_SOURCE
#define _GNU_SOURCE /* required for "struct ucred" in GLIBC >= 2.8 */
#endif
#endif

/* get the standard ctime_r() on Sun, among other things
 * gcc defines this */
#ifndef _POSIX_PTHREAD_SEMANTICS
#define _POSIX_PTHREAD_SEMANTICS
#endif

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <sys/types.h>
#include <bb_stdint.h>
#include <sys/stat.h>
#include <dirent.h>
#include <fcntl.h>
#include <time.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <pthread.h>
#include <stdint.h>
#include <inttypes.h>

#if defined(_SUN_SOURCE)
#include <procfs.h>
#elif defined(_IBM_SOURCE)
#include <sys/procfs.h>
#include <procinfo.h>
#elif defined(_HP_SOURCE)
#include <sys/pstat.h>
#elif defined(_LINUX_SOURCE)
#include <sys/time.h>
#include <asm/param.h> /* HZ */
#endif

#if defined(_SUN_SOURCE)
#include <ucred.h>
#elif defined(_LINUX_SOURCE)
#include <linux/socket.h>
#endif

#include <bb_oscompat.h>
#include <sysutil_stdbool.h>
#include "logmsg.h"

#define ARRSZ(arr) (sizeof(arr) / sizeof(arr[0]))

/**
 * /proc/pid/ notes:
 *    - Solaris/AIX:  POSIX, accessed with simple open/read/close
 *                    AIX also has a getprocs() interface.
 *    - Linux:        Plain-text files opened with open/read/close
 *                    contain some of the same info as Solaris/AIX
 *    - HP:           No /proc filesystem.  pstat_*() apis to get
 *                    individual pieces of info.  man pstat.
 **/

#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE)
static int __proc_fill_psinfo(char *caller, pid_t pid, struct psinfo *psinfo,
                              int silent)
{
    char procname[64];
    int fd, rc;
    sprintf(procname, "/proc/%" PRIu64 "/psinfo", (uint64_t)pid);
    fd = open(procname, O_RDONLY);
    if (fd < 0) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s:__proc_fill_psinfo: failed to open %s: %s\n",
                    caller, procname, strerror(errno));
        }
        return -1;
    }

    rc = read(fd, (char *)psinfo, sizeof(struct psinfo));
    if (rc != sizeof(struct psinfo)) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s:__proc_fill_psinfo read error: rc %d\n", caller,
                    rc);
        }
        close(fd);
        return -2;
    }

    close(fd);
    return 0;
}

#ifdef _SUN_SOURCE
static int __proc_fill_pstatus(char *caller, pid_t pid, struct pstatus *pstatus,
                               int silent)
{
    char procname[64];
    int fd, rc;
    sprintf(procname, "/proc/%" PRIu64 "/status", (uint64_t)pid);
    fd = open(procname, O_RDONLY);
    if (fd < 0) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s:__proc_fill_pstatus: failed to open %s: %s\n",
                    caller, procname, strerror(errno));
        }
        return -1;
    }

    rc = read(fd, (char *)pstatus, sizeof(struct pstatus));
    if (rc != sizeof(struct pstatus)) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s:__proc_fill_pstatus read error: rc %d\n",
                    caller, rc);
        }
        close(fd);
        return -2;
    }

    close(fd);
    return 0;
}
#endif
#elif defined(_LINUX_SOURCE)

/* interesting things we can gather from /proc/pid/stat (see 'man proc') */
struct linuxpsinfo {
    int32_t pid;
    char comm[64];
    char state;
    int32_t ppid;
    int32_t pgrp; /*  5 */
    int32_t session;
    int32_t tty_nr;
    int32_t tpgid;
    uint64_t flags;
    uint64_t minflt; /* 10 */
    uint64_t cminflt;
    uint64_t majflt;
    uint64_t cmajflt;
    uint64_t utime;
    uint64_t stime; /* 15 */
    uint64_t cutime;
    uint64_t cstime;
    int64_t priority;
    int64_t nice;
    int32_t num_threads; /* 20 */
    int32_t unused;
    uint64_t starttime;
    uint64_t vsize;
    int64_t rss;
    uint64_t rlim /* 25 */;
    uint64_t startcode;
    uint64_t endcode;
    uint64_t startstack;
    uint64_t endstack;
    uint64_t kstkesp; /* 30 */
    uint64_t kstkeip;
    uint64_t signal;
    uint64_t blocked;
    uint64_t sigignore;
    uint64_t sigcatch; /* 35 */
    uint64_t wchan;
    uint64_t nswap;
    uint64_t cnswap;
    int32_t exit_signal;
    int32_t processor; /* 40 */
};

enum { NSEC_IN_SEC = 1000000000, USEC_IN_SEC = 1000000 };

#if 0 // until needed
static int __linux_get_uptime(struct timeval *uptime_tv, int silent)
{
    static char *procname = "/proc/uptime" ;
    FILE *fp ;
    double uptime ;
    int rc ;

    memset(uptime_tv, 0, sizeof(uptime_tv)) ;
    fp = fopen(procname, "r") ;
    if ( fp == NULL )
    {
        if(!silent)
        {
            fprintf(stderr, "__linux_get_uptime: failed to open %s: %s\n", procname, strerror(errno)) ;
        }
        return -1 ;
    }

    rc = fscanf(fp, "%lf", &uptime) ;
    if ( rc != 1 )
    {
        if(!silent)
        {
            fprintf(stderr, "__linux_get_uptime: failed to read uptime from %s: rc %d\n", procname, rc) ;
        }
        fclose(fp) ;
        return -2 ;
    }

    uptime_tv->tv_sec  = (int)uptime ;
    uptime_tv->tv_usec = (uptime - (int)uptime) * USEC_IN_SEC ;
    fclose(fp) ;
    return 0 ;
}
#endif // until needed

static time_t __linux_get_boottime(void)
{
    static char *procname = "/proc/stat";
    FILE *fp;
    char line[128];
    time_t boottime = -1;

    fp = fopen(procname, "r");
    if (fp == NULL) {
        logmsg(LOGMSG_ERROR, "%s: failed to open %s: %s\n", __func__, procname,
                strerror(errno));
        return -1;
    }

    while (fgets(line, sizeof(line), fp)) {
        if (sscanf(line, "btime %ld", &boottime) == 1)
            break;
    }

    fclose(fp);

    if (boottime == -1)
        logmsg(LOGMSG_ERROR, "%s: failed to read boottime from %s\n", __func__,
                procname);

    return boottime;
}

static void __linux_jiffies_to_timespec(int64_t jiffies, struct timespec *spec)
{
    spec->tv_sec = jiffies / HZ;
    spec->tv_nsec = (jiffies % HZ) * (NSEC_IN_SEC / HZ);
}

/**
 * In the linux kernel, jiffies are literally clock ticks (# of timer interrupts
 *since boot).
 * The interrupt frequency can vary by system so the kernel exports jiffies to
 *userspace
 * by scaling with a constant USER_HZ (HZ in asm/param.h).  This is hardcoded to
 *100.
 **/
static int __linux_jiffies_to_epoch_timespec(int64_t jiffies,
                                             struct timespec *spec, int silent)
{
    time_t boottime;
    struct timespec jiffies_spec;

    /* read boot-time */
    boottime = __linux_get_boottime();

    /* jiffies is an offset from boot-time, scaled by HZ (ticks/second) */
    __linux_jiffies_to_timespec(jiffies, &jiffies_spec);
    spec->tv_sec = boottime + jiffies_spec.tv_sec;
    spec->tv_nsec = jiffies_spec.tv_nsec;
    return 0;
}

/* Documentation errors in manpages:
 *  field 20 is num_threads, field 21 is 0 */
static int __linux_fill_psinfo(const char *caller, pid_t pid,
                               struct linuxpsinfo *p, int silent)
{
    char procname[64];
    char cstate[2];
    FILE *fp;
    int len;
    int rc;

    sprintf(procname, "/proc/%d/stat", pid);
    fp = fopen(procname, "r");
    if (fp == NULL) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s:__linux_fill_psinfo: failed to open %s: %s\n",
                    caller, procname, strerror(errno));
        }
        return -1;
    }

    memset(p, 0, sizeof(struct linuxpsinfo));
    rc = fscanf(fp, "%d %64s %2s"
                    "%d %d %d %d %d"

                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "

                    "%" BBSCNd64 " "
                    "%" BBSCNd64 " "

                    "%d %d"

                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "

                    "%" BBSCNd64 " "

                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "
                    "%" BBSCNu64 " "

                    "%d %d",

                &p->pid, p->comm, cstate,

                &p->ppid, &p->pgrp, &p->session, &p->tty_nr, &p->tpgid,

                &p->flags, &p->minflt, &p->cminflt, &p->majflt, &p->cmajflt,
                &p->utime, &p->stime, &p->cutime, &p->cstime,

                &p->priority, &p->nice,

                &p->num_threads, &p->unused,

                &p->starttime, &p->vsize,

                &p->rss,

                &p->rlim,

                &p->startcode, &p->endcode, &p->startstack, &p->endstack,
                &p->kstkesp, &p->kstkeip, &p->signal, &p->blocked,
                &p->sigignore, &p->sigcatch, &p->wchan, &p->nswap, &p->cnswap,
                &p->exit_signal, &p->processor);

    if (rc != 40) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, 
                    "%s:__linux_fill_psinfo: failed to parse %s: rc %d\n",
                    caller, procname, rc);
        }
        fclose(fp);
        return -2;
    }

    fclose(fp);

    /* REMOVE PARENTHESIS FROM COMM */
    len = strlen(p->comm);
    if (p->comm[0] == '(') {
        memmove(p->comm, &p->comm[1], --len);
        p->comm[len] = '\0';
    }
    if ((len > 0) && (p->comm[len - 1] == ')'))
        p->comm[--len] = '\0';

    p->state = cstate[0];
    return 0;
}

/* /proc/<pid>/stat gives a severely truncated command line.
 * /proc/<pid>/cmdline gives the full command line with nul bytes between
 * each argument. */
static int __linux_get_cmdline(const char *caller, pid_t pid, char *cmdline,
                               int sz, int silent, bool *truncated)
{
    char procname[64];
    snprintf(procname, sizeof(procname), "/proc/%d/cmdline", (int)pid);

    int fd = open(procname, O_RDONLY);
    if (fd == -1) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s:%s: failed to open %s: %s\n", caller, __func__,
                    procname, strerror(errno));
        }
        return -1;
    }

    char *pos = cmdline;
    size_t bytesleft = sz;
    while (bytesleft > 0) {
        ssize_t nbytes = read(fd, pos, bytesleft);
        if (nbytes == -1) {
            if (!silent) {
                logmsg(LOGMSG_ERROR, "%s:%s: failed to read %s: %s\n", caller,
                        __func__, procname, strerror(errno));
            }
            close(fd);
            return -1;
        } else if (nbytes == 0) {
            break;
        } else {
            bytesleft -= nbytes;
            pos += nbytes;
        }
    }

    close(fd);

    /* Find length of data returned. */
    int len = (pos - cmdline);

    while ((len > 0) && (cmdline[len - 1] == 0))
        len--;

    if (len == 0) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s:%s: empty command line read from %s\n", caller,
                    __func__, procname);
        }
        return -1;
    }

    /* Convert intermediate nul bytes to spaces, and make sure the whole thing
     * has a nul at the end.  If it didn't then we know that our buffer wasn't
     * large enough and the result is truncated. */
    if (len == sz) {
        len--;
        (*truncated) = true;
    } else {
        (*truncated) = false;
    }

    for (int ii = 0; ii < len; ii++) {
        if (cmdline[ii] == 0)
            cmdline[ii] = ' ';
    }

    cmdline[len] = 0;

    return 0;
}
#endif

int bb_get_pid_starttime(pid_t pid, struct timespec *starttime)
{
    return bb_get_pid_starttime_silent(pid, starttime, 0);
}

int bb_get_pid_starttime_silent(pid_t pid, struct timespec *starttime,
                                int silent)
{
    int rc;

    memset(starttime, 0, sizeof(struct timespec));

#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE)
    struct psinfo psinfo;
    rc = __proc_fill_psinfo("bb_get_pid_starttime", pid, &psinfo, silent);
    if (rc == 0) {
        starttime->tv_sec = psinfo.pr_start.tv_sec;
        starttime->tv_nsec = psinfo.pr_start.tv_nsec;
        return 0;
    } else
        return rc;
#elif defined(_LINUX_SOURCE)
    struct linuxpsinfo psinfo;
    rc = __linux_fill_psinfo("bb_get_pid_starttime", pid, &psinfo, silent);
    if (rc == 0) {
        __linux_jiffies_to_epoch_timespec(psinfo.starttime, starttime, silent);
        return 0;
    } else
        return rc;
#elif defined(_HP_SOURCE)
    struct pst_status buf;
    rc = pstat_getproc(&buf, sizeof(buf), 0, pid);
    if (rc == 1) {
        starttime->tv_sec = buf.pst_start;
        starttime->tv_nsec = 0;
        return 0;
    } else
        return -1;
#else
#error "unsupported architecture"
#endif
}

int bb_get_pid_numlwps(pid_t pid) { return bb_get_pid_numlwps_silent(pid, 0); }

int bb_get_pid_numlwps_silent(pid_t pid, int silent)
{
    int rc;

#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE)
    struct psinfo psinfo;
    rc = __proc_fill_psinfo("bb_get_pid_numlwps", pid, &psinfo, silent);
    if (rc == 0)
        return psinfo.pr_nlwp;
    else
        return rc;
#elif defined(_LINUX_SOURCE)
    struct linuxpsinfo psinfo;
    rc = __linux_fill_psinfo("bb_get_pid_numlwps", pid, &psinfo, silent);
    if (rc == 0)
        return psinfo.num_threads;
    else
        return rc;
#elif defined(_HP_SOURCE)
    struct pst_status buf;
    rc = pstat_getproc(&buf, sizeof(buf), 0, pid);
    if (rc == 1)
        return buf.pst_nlwps;
    else
        return -1;
#else
#error "unsupported architecture"
#endif
}

#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE) || defined(_LINUX_SOURCE)

static int __proc_get_pid_numfds(pid_t pid, int silent)
{
    char path[64];
    DIR *dir;
    struct dirent *dent;
    int numfds = 0;

    snprintf(path, sizeof(path), "/proc/%" PRIu64 "/fd", (uint64_t)pid);
    dir = opendir(path);
    if (dir == NULL) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "__proc_get_pid_numfds: opendir %s failed: %s\n",
                    path, strerror(errno));
        }
        return -1;
    }

    while ((dent = readdir(dir))) {
        if ((strcmp(dent->d_name, ".") == 0) ||
            (strcmp(dent->d_name, "..") == 0))
            continue;
        numfds++;
    }

    closedir(dir);
    return numfds;
}

#endif

#if defined(_HP_SOURCE)

/* note - pstat_getproc() returns a field for the max file descriptor, but not
 * the number
 *        of open files.  so we settle for getting all file info. */
static int __hp_get_pid_numfds(pid_t pid, int silent)
{
    enum { _CHUNKSZ = 32 };
    struct pst_fileinfo2 fileinfos[_CHUNKSZ];
    int current_index = 0;
    int numfds = 0;
    int count;

    while ((count = pstat_getfile2(fileinfos, sizeof(struct pst_fileinfo2),
                                   ARRSZ(fileinfos), current_index, pid)) > 0) {
        numfds += count;

        /* index should be the next *file-descriptor* to return */
        current_index = fileinfos[count - 1].psf_fd + 1;
    }

    if (count == -1) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, 
                    "__hp_get_pid_numfds: pstat_getfile2 failed for pid %d: %s\n",
                    pid, strerror(errno));
        }
        return -1;
    }

    return numfds;
}

#endif
int bb_get_pid_numfds(pid_t pid) { return bb_get_pid_numfds_silent(pid, 0); }

int bb_get_pid_numfds_silent(pid_t pid, int silent)
{
#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE) || defined(_LINUX_SOURCE)
    return __proc_get_pid_numfds(pid, silent);
#elif defined(_HP_SOURCE)
    return __hp_get_pid_numfds(pid, silent);
#else
#error "unsupported architecture"
#endif
}

#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE) || defined(_LINUX_SOURCE)
static int __proc_get_pid_has_file_open(const pid_t pid, const char *file,
                                        const int silent)
{
    char path[64];
    char entryname[128];
    DIR *dir;
    struct dirent *dentp;
    struct stat statbuf;
    ino_t inode;
    int found, rc;

    found = 0;
    rc = stat(file, &statbuf);
    if (rc) {
        if (!(silent)) {
            logmsg(LOGMSG_ERROR, ":%s:%d:warning:file '%s' can not be stat()ed due to rcode "
                    "%d (unix error = \"%s\")\n",
                    __func__, __LINE__, file, rc,
                    strerror(errno) ? strerror(errno)
                                    : "<no strerror() error>");
        }

        return 0;
    }

    inode = statbuf.st_ino;

    snprintf(path, sizeof(path), "/proc/%" PRIu64 "/fd", (uint64_t)pid);
    dir = opendir(path);
    if (dir == NULL) {
        if (!(silent)) {
            logmsg(LOGMSG_ERROR, ":%s:%d:warning: opendir %s failed: %s\n",
                    __func__, __LINE__, path, strerror(errno));
        }

        return 0;
    }

    found = 0;
    while ((dentp = readdir(dir)) != NULL) {
        if ((strcmp(dentp->d_name, ".") == 0) ||
            (strcmp(dentp->d_name, "..") == 0))
            continue;
        snprintf(entryname, sizeof(entryname), "%s/%s", path, dentp->d_name);
        if (stat(entryname, &statbuf)) {
            if (!(silent)) {
                logmsg(LOGMSG_ERROR, 
                        ":%s:%d:warning: unable to stat file '%s' due to '%s'\n",
                        __func__, __LINE__, entryname, strerror(errno));
            }
            continue;
        }

        if (inode == statbuf.st_ino) {
            found = 1;
            break;
        }
    }

    closedir(dir);

    return found;
}
#endif

#if defined(_HP_SOURCE)
static int __hp_get_pid_has_file_open(const pid_t pid, const char *file,
                                      const int silent)
{
    ino_t inode;
    enum { _CHUNKSZ = 32 };
    struct stat statbuf;
    struct pst_fileinfo2 fileinfos[_CHUNKSZ];
    struct pst_filedetails filedetails;
    int current_index = 0;
    int nxt_file;
    int count, rc;

    rc = stat(file, &statbuf);
    if (rc) {
        if (!(silent)) {
            logmsg(LOGMSG_ERROR, ":%s:%d:warning:file '%s' can not be stat()ed due to rcode "
                    "%d (unix error = \"%s\")\n",
                    __func__, __LINE__, file, rc,
                    strerror(errno) ? strerror(errno)
                                    : "<no strerror() error>");
        }

        return 0;
    }

    inode = statbuf.st_ino;

    while ((count = pstat_getfile2(fileinfos, sizeof(struct pst_fileinfo2),
                                   ARRSZ(fileinfos), current_index, pid)) > 0) {
        for (nxt_file = 0; nxt_file < count; ++nxt_file) {
            rc = pstat_getfiledetails(&filedetails, sizeof(filedetails),
                                      &fileinfos[nxt_file].psf_fid);
            if (1 != rc) {
                if (!(silent)) {
                    logmsg(LOGMSG_ERROR, ":%s:%d:warning:rcode %d from pstat_getfiledetails() "
                        "pid = %" BBPRIu64 "\n",
                        __func__, __LINE__, rc, (uint64_t)pid);
                }

                continue;
            }

            if (filedetails.psfd_ino == inode) {
                return 1;
            }
        }

        /* NEXT BATCH: */
        /* index should be the next *file-descriptor* to return */
        current_index = fileinfos[count - 1].psf_fd + 1;
    }

    if (count == -1) {
        if (!(silent)) {
            logmsg(LOGMSG_ERROR, 
                    ":%s:%d: pstat_getfile2 failed for pid %" BBPRIu64 ": %s\n",
                    __func__, __LINE__, (uint64_t)pid, strerror(errno));
        }
        return 0;
    }

    return 0;
}
#endif

int bb_pid_has_file_open(const pid_t pid, const char *file, const int silent)
{
/* note - 'file' must be either a fullpath or a file that exists in this */
/*        task's cwd. */
/*        our approach is to match inodes instead of matching filenames. */
#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE) || defined(_LINUX_SOURCE)
    return __proc_get_pid_has_file_open(pid, file, silent);
#elif defined(_HP_SOURCE)
    return __hp_get_pid_has_file_open(pid, file, silent);
#else
#error "unsupported architecture"
#endif
}

#if defined(_SUN_SOURCE)

static int __proc_get_pid_cputime(pid_t pid, struct timespec *utime,
                                  struct timespec *stime, int silent)
{
    struct prusage prusage;
    char filename[64];
    int fd;
    int rc;

    snprintf(filename, sizeof(filename), "/proc/%" PRIu64 "/usage",
             (uint64_t)pid);
    fd = open(filename, O_RDONLY);
    if (fd < 0) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "__proc_get_pid_cputime: failed to open %s: %s\n",
                    filename, strerror(errno));
        }
        return -1;
    }

    rc = read(fd, &prusage, sizeof(struct prusage));
    if (rc != sizeof(struct prusage)) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "__proc_get_pid_cputime: read error: %d\n", rc);
        }
        close(fd);
        return -2;
    }

    utime->tv_sec = prusage.pr_utime.tv_sec;
    utime->tv_nsec = prusage.pr_utime.tv_nsec;
    stime->tv_sec = prusage.pr_stime.tv_sec;
    stime->tv_nsec = prusage.pr_stime.tv_nsec;

    close(fd);
    return 0;
}

#endif

#if defined(_IBM_SOURCE)

static int __aix_get_pid_cputime(pid_t pid, struct timespec *utime,
                                 struct timespec *stime, int silent)
{
    struct procsinfo procsinfo;
    struct rusage *ru;
    pid_t lpid = pid;
    int rc;

    rc = getprocs(&procsinfo, sizeof(struct procsinfo), NULL, 0, &lpid, 1);
    if (rc != 1) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, 
                    "__aix_get_pid_cputime: getprocs failed for pid %d: rc %d\n",
                    pid, rc);
        }
        return -1;
    } else if (procsinfo.pi_pid != pid) {
        /* returned PID may not be requested PID */
        if (!silent) {
            logmsg(LOGMSG_ERROR, "__aix_get_pid_cputime: getprocs returned bad pid "
                            "%lld != %lld\n",
                    (long long)procsinfo.pi_pid, (long long)pid);
        }
        return -1;
    }

    /* 'man getprocs' documents that the tv_usec fields actually contains
     * nanoseconds */
    ru = (struct rusage *)&procsinfo.pi_ru;
    utime->tv_sec = ru->ru_utime.tv_sec;
    utime->tv_nsec = ru->ru_utime.tv_usec;
    stime->tv_sec = ru->ru_stime.tv_sec;
    stime->tv_nsec = ru->ru_stime.tv_usec;

    return 0;
}

#endif

#if defined(_HP_SOURCE)

/* hp only provides second-level resolution */
static int __hp_get_pid_cputime(pid_t pid, struct timespec *utime,
                                struct timespec *stime, int silent)
{
    struct pst_status status;
    int rc;

    rc = pstat_getproc(&status, sizeof(struct pst_status), 0, pid);
    if (rc != 1) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "__hp_get_pid_cputime: pstat_getproc failed for "
                            "pid %d: rc %d %s\n",
                    pid, rc, strerror(errno));
        }
        return -1;
    }

    utime->tv_sec = status.pst_utime;
    utime->tv_nsec = 0;
    stime->tv_sec = status.pst_stime;
    stime->tv_nsec = 0;

    return 0;
}

#endif

#if defined(_LINUX_SOURCE)

static int __linux_get_pid_cputime(pid_t pid, struct timespec *utime,
                                   struct timespec *stime, int silent)
{
    struct linuxpsinfo psinfo;
    int rc;

    rc = __linux_fill_psinfo("bb_get_pid_cputime", pid, &psinfo, silent);
    if (rc != 0)
        return rc;

    __linux_jiffies_to_timespec(psinfo.utime, utime);
    __linux_jiffies_to_timespec(psinfo.stime, stime);
    return 0;
}

#endif

int bb_get_pid_cputime(pid_t pid, struct timespec *utime,
                       struct timespec *stime)
{
    return bb_get_pid_cputime_silent(pid, utime, stime, 0);
}

int bb_get_pid_cputime_silent(pid_t pid, struct timespec *utime,
                              struct timespec *stime, int silent)
{
    memset(utime, 0, sizeof(struct timespec));
    memset(stime, 0, sizeof(struct timespec));

#if defined(_SUN_SOURCE)
    return __proc_get_pid_cputime(pid, utime, stime, silent);
#elif defined(_IBM_SOURCE)
    return __aix_get_pid_cputime(pid, utime, stime, silent);
#elif defined(_HP_SOURCE)
    return __hp_get_pid_cputime(pid, utime, stime, silent);
#elif defined(_LINUX_SOURCE)
    return __linux_get_pid_cputime(pid, utime, stime, silent);
#else
#error "unsupported architecture"
#endif
}

pid_t bb_get_pid_ppid(pid_t pid) { return bb_get_pid_ppid_silent(pid, 0); }

pid_t bb_get_pid_ppid_silent(pid_t pid, int silent)
{
    int rc;

#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE)
    struct psinfo psinfo;
    rc = __proc_fill_psinfo("bb_get_pid_ppid", pid, &psinfo, silent);
    if (rc == 0)
        return psinfo.pr_ppid;
    else
        return -1;
#elif defined(_LINUX_SOURCE)
    struct linuxpsinfo psinfo;
    rc = __linux_fill_psinfo("bb_get_pid_ppid", pid, &psinfo, silent);
    if (rc == 0)
        return psinfo.ppid;
    else
        return -1;
#elif defined(_HP_SOURCE)
    struct pst_status buf;
    rc = pstat_getproc(&buf, sizeof(buf), 0, pid);
    if (rc == 1)
        return buf.pst_ppid;
    else
        return -1;
#else
#error "unsupported architecture"
#endif
}

enum {
    ARGS_SILENT = 0x01, /* do not complain (to stderr) */
    ARGS_SINGLE = 0x02  /* only want basename of argv0 */
};

#if defined(_SUN_SOURCE)

static int __sun_get_pid_ppid_args(pid_t pid, pid_t *ppid, char *args, int sz,
                                   int flags)
{
    struct psinfo psinfo;
    int rc, ln;
    char *s;
    rc = __proc_fill_psinfo("__sun_get_pid_ppid_args", pid, &psinfo,
                            flags & ARGS_SILENT);
    if (rc == 0) {
        s = psinfo.pr_psargs;
        if (flags & ARGS_SINGLE) {
            /* use pr_fname unless it was truncated */
            s = psinfo.pr_fname;
            if (strchr(psinfo.pr_fname, '\0') ==
                &psinfo.pr_fname[sizeof(psinfo.pr_fname) - 1]) {
                char *p;
                s = psinfo.pr_psargs;
                for (p = s; *p != '\0'; ++p) {
                    if (*p == '/')
                        s = p + 1;
                    else if (*p == ' ')
                        break;
                }
                if ((ln = p - s) < sizeof(psinfo.pr_fname) ||
                    strncmp(s, psinfo.pr_fname, sizeof(psinfo.pr_fname) - 1) !=
                        0) {
                    s = psinfo.pr_fname;
                } else {
                    memcpy(args, s,
                           ln < sz ? ln : sz); /* null terminated below */
                    if (ln < sz)
                        args[ln] = '\0';
                    goto skip;
                }
            }
        }
        ln = snprintf(args, sz, "%s", s);
        if (ln < 0)
            ln = strlen(s);
    skip:
        ;
        if (ln >= sz && sz >= 5) {
            /* too large - terminate with ... */
            args[sz - 2] = args[sz - 3] = args[sz - 4] = '.';
        }
        args[sz - 1] = '\0';
        *ppid = psinfo.pr_ppid;
        return 0;
    } else
        return -11;
}

#endif

#if defined(_IBM_SOURCE)
#include <procinfo.h>
#include <sys/types.h>

static int __aix_get_pid_ppid_args(pid_t pid, pid_t *ppid, char *args, int sz,
                                   int flags)
{
    struct procsinfo p;
    pid_t idx = pid;
    if (1 == getprocs(&p, sizeof(p), NULL, 0, &idx, 1)
        /* returned PID may not be requested PID */
        &&
        pid == p.pi_pid) {
        int ln;
        if (flags & ARGS_SINGLE) {
            ln = snprintf(args, sz, "%s", p.pi_comm);
            if (ln < 0)
                ln = strlen(p.pi_comm);
        } else {
            int lastnull = 0;
            /*
             * getargs() puts a sequence of null-terminated strings
             * into the target array, followed by an extra null byte,
             * assuming all the argument strings fit.  Replace the
             * intermediate null bytes with a space until we reach
             * the end of args[] or find the second null byte in a
             * row.  (Apparently empty arguments aren't an issue!)
             */
            int rc = getargs(&p, sizeof(p), args, sz);
            if (rc == -1)
                return -1;
            for (ln = 0; ln < sz; ++ln) {
                if ('\0' != args[ln]) {
                    lastnull = 0;
                } else if (!lastnull) {
                    lastnull = 1;
                    args[ln] = ' ';
                } else /* reached the extra null byte at the end */
                {
                    --ln;
                    args[ln] = '\0';
                    break;
                }
            }
        }
        if (ln >= sz && sz >= 5) {
            /* too large - terminate with ... */
            args[sz - 2] = args[sz - 3] = args[sz - 4] = '.';
        }
        args[sz - 1] = '\0';
        *ppid = p.pi_ppid;
        return 0;
    }
    return -1;
}

#endif

#if defined(_LINUX_SOURCE)

static int __linux_get_pid_ppid_args(pid_t pid, pid_t *ppid, char *args, int sz,
                                     int flags)
{
    struct linuxpsinfo psinfo;
    bool truncated;
    int rc1 = __linux_fill_psinfo(__func__, pid, &psinfo, flags & ARGS_SILENT);
    int rc2 = __linux_get_cmdline(__func__, pid, args, sz, flags & ARGS_SILENT,
                                  &truncated);
    if (rc1 == 0 && rc2 == 0) {
        if (flags & ARGS_SINGLE) {
            char *s = args;
            char *p;
            for (p = s; *p != '\0'; ++p) {
                if (*p == '/')
                    s = p + 1;
                else if (*p == ' ') {
                    *p = '\0';
                    break;
                }
            }
            int ln = (p - s);
            if (s != args) {
                memmove(args, s, ln + 1);
            }
        }
        if (truncated && sz >= 5) {
            /* too large - terminate with ... */
            args[sz - 2] = args[sz - 3] = args[sz - 4] = '.';
        }
        args[sz - 1] = '\0';
        *ppid = psinfo.ppid;
        return 0;
    } else
        return -1;
}

#endif

#if defined(_HP_SOURCE)

static int __hp_get_pid_ppid_args(pid_t pid, pid_t *ppid, char *args, int sz,
                                  int flags)
{
    struct pst_status pst;
    int rc, ln;
    rc = pstat_getproc(&pst, sizeof(pst), 0, pid);
    if (rc == 1) {
        char *s = pst.pst_cmd;
        if (flags & ARGS_SINGLE) {
            char *p;
            for (p = s; *p != '\0'; ++p) {
                if (*p == '/')
                    s = p + 1;
                else if (*p == ' ') {
                    *p = '\0';
                    break;
                }
            }
            ln = p - s;
        } else {
            ln = strlen(s);
        }

        if (ln < sz) {
            memcpy(args, s, ln);
            args[ln] = 0;
        } else if (sz > 0) {
            memcpy(args, s, sz - 1);
            if (sz >= 5)
                args[sz - 2] = args[sz - 3] = args[sz - 4] = '.';
            args[sz - 1] = 0;
        }
        *ppid = pst.pst_ppid;
        return 0;
    } else
        return -1;
}

#endif

int bb_get_pid_ppid_args_silent(pid_t pid, pid_t *ppid, char *args, int sz,
                                int silent)
{
    if (silent)
        silent = ARGS_SILENT;
#if defined(_SUN_SOURCE)
    return __sun_get_pid_ppid_args(pid, ppid, args, sz, silent);
#elif defined(_IBM_SOURCE)
    return __aix_get_pid_ppid_args(pid, ppid, args, sz, silent);
#elif defined(_LINUX_SOURCE)
    return __linux_get_pid_ppid_args(pid, ppid, args, sz, silent);
#elif defined(_HP_SOURCE)
    return __hp_get_pid_ppid_args(pid, ppid, args, sz, silent);
#else
#error "unsupported architecture"
#endif
}

int bb_get_pid_ppid_args(pid_t pid, pid_t *ppid, char *args, int sz)
{
    return bb_get_pid_ppid_args_silent(pid, ppid, args, sz, 0);
}

int bb_get_pid_ppid_argv0_silent(pid_t pid, pid_t *ppid, char *argv0, int sz,
                                 int silent)
{
    if (silent)
        silent = ARGS_SILENT;
#if defined(_SUN_SOURCE)
    return __sun_get_pid_ppid_args(pid, ppid, argv0, sz, silent | ARGS_SINGLE);
#elif defined(_IBM_SOURCE)
    return __aix_get_pid_ppid_args(pid, ppid, argv0, sz, silent | ARGS_SINGLE);
#elif defined(_LINUX_SOURCE)
    return __linux_get_pid_ppid_args(pid, ppid, argv0, sz,
                                     silent | ARGS_SINGLE);
#elif defined(_HP_SOURCE)
    return __hp_get_pid_ppid_args(pid, ppid, argv0, sz, silent | ARGS_SINGLE);
#else
#error "unsupported architecture"
#endif
}

/* PLEASE do not change the semantics of what is stored in the argv0 field.
 * Each time this changes it breaks sysmets.tsk (i.e. PQC66, HOGS<go>
 * and people complain.  If this does need to change, please coordinate
 * the change with the sysmets guys. */
int bb_get_pid_ppid_argv0(pid_t pid, pid_t *ppid, char *argv0, int sz)
{
    return bb_get_pid_ppid_argv0_silent(pid, ppid, argv0, sz, 0);
}

int bb_get_pid_argv0(pid_t pid, char *argv0, int sz)
{
    pid_t ppid;
    return bb_get_pid_ppid_argv0_silent(pid, &ppid, argv0, sz, 0);
}

int bb_get_pid_argv0_silent(pid_t pid, char *argv0, int sz, int silent)
{
    pid_t ppid;
    return bb_get_pid_ppid_argv0_silent(pid, &ppid, argv0, sz, silent);
}

int bb_get_pid_args_silent(pid_t pid, char *args, int sz, int silent)
{
    pid_t ppid;
    return bb_get_pid_ppid_args_silent(pid, &ppid, args, sz, silent);
}

int bb_get_pid_args(pid_t pid, char *args, int sz)
{
    pid_t ppid;
    return bb_get_pid_ppid_args_silent(pid, &ppid, args, sz, 0);
}

#if defined(_SUN_SOURCE)

static int __sun_is_pid_stopped(pid_t pid, int silent)
{
    pstatus_t p;
    int rc;
    rc = __proc_fill_pstatus("bb_is_pid_stopped", pid, &p, silent);
    if (rc == 0)
        return ((p.pr_flags & PR_STOPPED) != 0);
    else
        return 0;
}

#endif

#if defined(_IBM_SOURCE)

static int __aix_is_pid_stopped(pid_t pid)
{
    int lpid = pid, rc;
    struct procsinfo p;
    rc = getprocs(&p, sizeof(p), NULL, 0, &lpid, 1);
    if (1 == rc && p.pi_pid == pid) {
        /* returned PID may not be requested PID */
        return (p.pi_state == SSTOP);
    }
    return 0;
}

#endif

#if defined(_LINUX_SOURCE)

static int __linux_is_pid_stopped(pid_t pid, int silent)
{
    struct linuxpsinfo psinfo;
    int rc;
    rc = __linux_fill_psinfo("__linux_is_pid_stopped", pid, &psinfo, silent);
    if (rc == 0)
        return (psinfo.state == 'T');
    else
        return 0;
}

#endif

#if defined(_HP_SOURCE)

static int __hp_is_pid_stopped(pid_t pid)
{
    struct pst_status pst;
    int rc;
    rc = pstat_getproc(&pst, sizeof(pst), 0, pid);
    if (rc == 1)
        return (pst.pst_stat == PS_STOP);
    else
        return 0;
}

#endif

int bb_is_pid_stopped(pid_t pid) { return bb_is_pid_stopped_silent(pid, 0); }

int bb_is_pid_stopped_silent(pid_t pid, int silent)
{
#if defined(_SUN_SOURCE)
    return __sun_is_pid_stopped(pid, silent);
#elif defined(_IBM_SOURCE)
    return __aix_is_pid_stopped(pid);
#elif defined(_LINUX_SOURCE)
    return __linux_is_pid_stopped(pid, silent);
#elif defined(_HP_SOURCE)
    return __hp_is_pid_stopped(pid);
#else
#error "unsupported architecture"
#endif
}

#if defined(_SUN_SOURCE)

/* a non-zero pr_sigtrace appears to indicate that a  process is being traced */
static int __sun_is_pid_traced(pid_t pid, int silent)
{
    struct pstatus pstatus;
    int rc;

    rc = __proc_fill_pstatus("bb_is_pid_traced", pid, &pstatus, silent);
    if (rc == 0) {
        sigset_t empty_set;
        memset(&empty_set, 0, sizeof(sigset_t));
        if (memcmp(&empty_set, &pstatus.pr_sigtrace, sizeof(sigset_t)) != 0)
            return 1;
    }

    return 0;
}

#endif

#if defined(_IBM_SOURCE)

static int __aix_is_pid_traced(pid_t pid)
{
    int lpid = pid, rc;
    struct procsinfo p;
    rc = getprocs(&p, sizeof(p), NULL, 0, &lpid, 1);
    if ((rc == 1) && (p.pi_pid == pid) && (p.pi_flags & STRC))
        /* returned PID may not be requested PID */
        return 1;
    else
        return 0;
}

#endif

#if defined(_LINUX_SOURCE)

/* NOT IMPLEMENTED
 * (the PT_PTRACED bit in task->ptrace is what we want but i can't find this
 *  exposed to userspace) */
static int __linux_is_pid_traced(pid_t pid) { return 0; }

#endif

#if defined(_HP_SOURCE)

static int __hp_is_pid_traced(pid_t pid)
{
    struct pst_status pst;
    int rc;
    rc = pstat_getproc(&pst, sizeof(pst), 0, pid);
    if ((rc == 1) && (pst.pst_flag & PS_TRACE))
        return 1;
    else
        return 0;
}

#endif

int bb_is_pid_traced(pid_t pid) { return bb_is_pid_traced_silent(pid, 0); }

int bb_is_pid_traced_silent(pid_t pid, int silent)
{
#if defined(_SUN_SOURCE)
    return __sun_is_pid_traced(pid, silent);
#elif defined(_IBM_SOURCE)
    return __aix_is_pid_traced(pid);
#elif defined(_LINUX_SOURCE)
    return __linux_is_pid_traced(pid);
#elif defined(_HP_SOURCE)
    return __hp_is_pid_traced(pid);
#else
#error "unsupported architecture"
#endif
}

#if defined(_SUN_SOURCE)

static ssize_t __sun_get_pid_vsz(pid_t pid, int silent)
{
    struct psinfo psinfo;
    int rc;

    rc = __proc_fill_psinfo("bb_get_pid_vsz", pid, &psinfo, silent);
    if (rc == 0)
        return psinfo.pr_size;
    else
        return -1;
}

#endif

#if defined(_IBM_SOURCE)

static ssize_t __aix_get_pid_vsz(pid_t pid, int silent)
{
    int lpid = pid, rc;
    struct procsinfo p;
    rc = getprocs(&p, sizeof(p), NULL, 0, &lpid, 1);
    if ((rc == 1) && (p.pi_pid == pid))
        /* returned PID may not be requested PID */
        return p.pi_size * getpagesize() / 1024;
    else
        return -1;
}

#endif

#if defined(_LINUX_SOURCE)

static ssize_t __linux_get_pid_vsz(pid_t pid, int silent)
{
    struct linuxpsinfo psinfo;
    int rc;
    rc = __linux_fill_psinfo("bb_get_pid_vsz", pid, &psinfo, silent);
    if (rc == 0)
        return psinfo.vsize / 1024;
    else
        return -1;
}

#endif

#if defined(_HP_SOURCE)

static ssize_t __hp_get_pid_vsz(pid_t pid, int silent)
{
    struct pst_status buf;
    int num_pages;
    int rc;

    rc = pstat_getproc(&buf, sizeof(buf), 0, pid);
    if (rc != 1) {
        if (!silent)
            logmsg(LOGMSG_ERROR, "%s: pstat_getproc failed: %s\n", __func__,
                    strerror(errno));
        return -1;
    }

    /* hp doesn't give us a total so add up each individual category available
     * (this winds up being higher than what's reported by 'ps -o vsz' but lower
     *  than what pmap reports) -- no time to investigate right now */
    num_pages = buf.pst_vdsize + buf.pst_tsize + buf.pst_vssize +
                buf.pst_vshmsize + buf.pst_vmmsize + buf.pst_vusize +
                buf.pst_viosize + buf.pst_vrsesize;

    return (num_pages * getpagesize() / 1024);
}

#endif

ssize_t bb_get_pid_vsz(pid_t pid) { return bb_get_pid_vsz_silent(pid, 0); }

ssize_t bb_get_pid_vsz_silent(pid_t pid, int silent)
{
#if defined(_SUN_SOURCE)
    return __sun_get_pid_vsz(pid, silent);
#elif defined(_IBM_SOURCE)
    return __aix_get_pid_vsz(pid, silent);
#elif defined(_LINUX_SOURCE)
    return __linux_get_pid_vsz(pid, silent);
#elif defined(_HP_SOURCE)
    return __hp_get_pid_vsz(pid, silent);
#else
#error "unsupported architecture"
#endif
}

int bb_get_pid_execlink(pid_t pid, char *path, int sizeof_path)
{
    return bb_get_pid_execlink_silent(pid, path, sizeof_path, /*silent:*/ 0);
}

int bb_get_pid_execlink_silent(pid_t pid, char *path, int sizeof_path,
                               int silent)
{
#if defined(_SUN_SOURCE) || defined(_IBM_SOURCE)
    snprintf(path, sizeof_path, "/proc/%" PRIu64 "/object/a.out",
             (uint64_t)pid);
    return 0;
#elif defined(_LINUX_SOURCE)
    snprintf(path, sizeof_path, "/proc/%" PRIu64 "/exe", (uint64_t)pid);
    return 0;
#elif defined(_HP_SOURCE)
    struct pst_status st;
    int rc;

    rc = pstat_getproc(&st, sizeof(struct pst_status), 0, pid);
    if (rc != 1) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: pstat_getproc failed for pid %" PRIu64 ": %s\n",
                    __func__, (uint64_t)pid, strerror(errno));
        }
        return -1;
    }

    rc = pstat_getpathname(path, sizeof_path, &st.pst_fid_text);
    if (rc < 1) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: pstat_getpathname failed for pid %" PRIu64 ": %s\n",
                    __func__, (uint64_t)pid, strerror(errno));
        }
        return -2;
    }
    return 0;
#else
#error "unsupported architecture"
#endif
}

int bb_get_pid_execpath(pid_t pid, char *path, int sizeof_path)
{
    char procname[64];
#if defined(_IBM_SOURCE)
    struct procsinfo p;
    pid_t idx = pid;
    struct stat st;
    dev_t dev;
    ino_t ino;

    /* see if argv[0] happens to be what we're looking for */
    if (1 == getprocs(&p, sizeof(p), NULL, 0, &idx, 1) && pid == p.pi_pid) {
        if (0 != getargs(&p, sizeof(p), path, sizeof_path))
            return -2;
        if (0 != stat(path, &st))
            return -3;
        dev = st.st_dev;
        ino = st.st_ino;
        snprintf(procname, sizeof(procname), "/proc/%" PRIu64 "/object/a.out",
                 (uint64_t)pid);
        if (0 != stat(procname, &st))
            return -4;
        if (dev != st.st_dev || ino != st.st_ino)
            return -5;
        return 0;
    }
    return -1;
#elif defined(_SUN_SOURCE)
    int n;

    snprintf(procname, sizeof(procname), "/proc/%" PRIu64 "/path/a.out",
             (uint64_t)pid);
    if ((n = readlink(procname, path, sizeof_path)) < 0 || n >= sizeof_path)
        return -1;
    path[n] = '\0'; /* ensure null termination */
    return 0;
#elif defined(_LINUX_SOURCE)
    int n;

    snprintf(procname, sizeof(procname), "/proc/%" PRIu64 "/exe",
             (uint64_t)pid);
    if ((n = readlink(procname, path, sizeof_path)) < 0 || n >= sizeof_path)
        return -1;
    path[n] = '\0'; /* ensure null termination */
    return 0;
#else
    return -1;
#error "unsupported architecture"
#endif
}

#if defined(_IBM_SOURCE)

struct t_bb_oscompat_map_snapshot {
    int nmap;
    prmap_t map[];
};

static bb_oscompat_map_snapshot_t *__aix_init_map_snap(pid_t pid, int *code)
{
    enum { CHUNK = 24 };
    bb_oscompat_map_snapshot_t *snap;
    char mapfile[64];
    int fd, i, n, sz;

    snprintf(mapfile, sizeof(mapfile), "/proc/%" PRIu64 "/map", (uint64_t)pid);
    if ((fd = open(mapfile, O_RDONLY)) < 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: open(%s) failed - %s\n", __func__, mapfile,
                    strerror(errno));
        }
        *code = -1;
        return 0;
    }
    n = CHUNK;
    if ((snap = malloc(sizeof(*snap) + n * sizeof(prmap_t))) == 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: failed to allocate map - %s\n", __func__,
                    strerror(errno));
        }
        close(fd);
        *code = -2;
        return 0;
    }
    snap->nmap = -1;
    i = 0;
    while ((sz = read(fd, &snap->map[i], CHUNK * sizeof(prmap_t))) ==
           CHUNK * sizeof(prmap_t)) {
        bb_oscompat_map_snapshot_t *new;

        i = n;
        n += CHUNK;
        if ((new = realloc(snap, sizeof(*snap) + n * sizeof(prmap_t))) == 0) {
            if (!*code) {
                logmsg(LOGMSG_ERROR, "%s: failed to grow map - %s\n", __func__,
                        strerror(errno));
            }
            free(snap);
            close(fd);
            *code = -3;
            return 0;
        }
        snap = new;
    }
    close(fd);
    if (sz < 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: read of mapfile %s failed - %s\n", __func__,
                    mapfile, strerror(errno));
        }
        free(snap);
        *code = -4;
        return 0;
    }
    /* determine how many mappings are present */
    n = i + sz / sizeof(prmap_t);
    for (i = 0; i < n; ++i) {
        if (snap->map[i].pr_size == 0 && snap->map[i].pr_vaddr == 0) {
            snap->nmap = i;
            break;
        }
    }
    if (snap->nmap < 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: no end found for mapfile %s\n", __func__,
                    mapfile);
        }
        free(snap);
        *code = -5;
        return 0;
    }
    return snap;
}

int __aix_get_map_snap(bb_oscompat_map_snapshot_t *snap,
                       bb_oscompat_map_entry_t *mapentry, int mapnum,
                       int silent)
{
    prmap_t *map = &snap->map[mapnum];
    int n;

    mapentry->start = (uint64_t)map->pr_vaddr;
    mapentry->after = mapentry->start + map->pr_size;
    mapentry->offset = map->pr_off;
    mapentry->path = 0;
    mapentry->member = 0;
    if (map->pr_pathoff != 0) {
        mapentry->path = map->pr_pathoff + (char *)snap->map;
        n = 1 + strlen(mapentry->path);
        /* member, if any, is the following null terminated string */
        if (mapentry->path[n] != '\0')
            mapentry->member = &mapentry->path[n];
    }
    mapentry->flags = 0;
    if (map->pr_mflags & MA_READ)
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_READ;
    if (map->pr_mflags & MA_WRITE)
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_WRITE;
    if (map->pr_mflags & MA_EXEC)
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_EXEC;
    if (map->pr_mflags & MA_SHARED)
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_SHARED;
    if (map->pr_mflags & MA_MAINEXEC)
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_MAIN;
    return 0;
}

#elif defined(_SUN_SOURCE)

struct t_bb_oscompat_map_snapshot {
    int nmap;
    pid_t pid;
    int idx;
    char *paths;
    prmap_t map[];
};

static bb_oscompat_map_snapshot_t *__sun_init_map_snap(pid_t pid, int *code)
{
    bb_oscompat_map_snapshot_t *snap;
    struct stat stbuf;
    char mapfile[64];
    int fd, n, sz;

    snprintf(mapfile, sizeof(mapfile), "/proc/%" PRIu64 "/map", (uint64_t)pid);
    if ((fd = open(mapfile, O_RDONLY)) < 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: open(%s) failed - %s\n", __func__, mapfile,
                    strerror(errno));
        }
        *code = -1;
        return 0;
    }
    /* Solaris in nice enough to provide the file size for us */
    if (fstat(fd, &stbuf) < 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: cannot stat mapfile %s - %s\n", __func__,
                    mapfile, strerror(errno));
        }
        close(fd);
        *code = -2;
        return 0;
    }
    n = stbuf.st_size / sizeof(prmap_t);
    sz = n * sizeof(prmap_t);
    if ((snap = malloc(sizeof(*snap) + sz + n * PATH_MAX)) == 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: cannot allocate map array - %s\n", __func__,
                    strerror(errno));
        }
        close(fd);
        *code = -3;
        return 0;
    }
    snap->nmap = n;
    snap->pid = pid;
    snap->idx = 0;
    snap->paths = sz + (char *)snap;
    if (read(fd, snap->map, sz) != sz) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: read of mapfile %s failed - %s\n", __func__,
                    mapfile, strerror(errno));
        }
        *code = -4;
        free(snap);
        snap = 0;
    }
    close(fd);
    return snap;
}

int __sun_get_map_snap(bb_oscompat_map_snapshot_t *snap,
                       bb_oscompat_map_entry_t *mapentry, int mapnum,
                       int silent)
{
    prmap_t *map = &snap->map[mapnum];
    char file[64];
    int n;

    mapentry->start = map->pr_vaddr;
    mapentry->after = map->pr_vaddr + map->pr_size;
    mapentry->offset = map->pr_offset;
    mapentry->path = 0;
    mapentry->member = 0;
    mapentry->flags = 0;
    if (map->pr_mapname[0] != '\0') {
        if (strcmp(map->pr_mapname, "a.out") == 0)
            mapentry->flags |= BB_OSCOMPAT_MAPFLAG_MAIN;
        snprintf(file, sizeof(file), "/proc/%" PRIu64 "/path/%s",
                 (uint64_t)snap->pid, map->pr_mapname);
        if ((n = readlink(file, &snap->paths[snap->idx], PATH_MAX)) > 0) {
            mapentry->path = &snap->paths[snap->idx];
            snap->idx += n;
            snap->paths[snap->idx] = '\0';
            ++snap->idx;
        }
    }
    if (map->pr_mflags & MA_READ)
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_READ;
    if (map->pr_mflags & MA_WRITE)
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_WRITE;
    if (map->pr_mflags & MA_EXEC)
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_EXEC;
    if (map->pr_mflags & MA_SHARED)
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_SHARED;
    return 0;
}

#elif defined(_LINUX_SOURCE)

struct t_bb_oscompat_map_snapshot {
    int nmap;
    int *idx;
    char space[];
};

static bb_oscompat_map_snapshot_t *__linux_init_map_snap(pid_t pid, int *code)
{
    enum { CHUNK = 4096, STEP = 100 };
    bb_oscompat_map_snapshot_t *snap, *new;
    char *p, *q, mapfile[64], exefile[64];
    int fd, x, i, n, sz, len, tot;

    snprintf(mapfile, sizeof(mapfile), "/proc/%" PRIu64 "/maps", (uint64_t)pid);
    snprintf(exefile, sizeof(exefile), "/proc/%" PRIu64 "/exe", (uint64_t)pid);
    if ((fd = open(mapfile, O_RDONLY)) < 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: open(%s) failed - %s\n", __func__, mapfile,
                    strerror(errno));
        }
        *code = -1;
        return 0;
    }
    if ((snap = malloc(sizeof(*snap) + CHUNK)) == 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: failed to allocate map - %s\n", __func__,
                    strerror(errno));
        }
        close(fd);
        *code = -2;
        return 0;
    }
    tot = CHUNK;

    /* try to get pathname of the executable */
    if ((x = readlink(exefile, snap->space, CHUNK)) < 0)
        x = 0;
    snap->space[x] = '\0';
    ++x;

    /* read in the entire maps file contents after the exefile link string */
    snap->nmap = 0;
    snap->idx = 0;
    i = x;
    n = CHUNK - x;
    while ((sz = read(fd, &snap->space[i], n)) == n) {
        i += sz;
        tot += CHUNK;
        n = CHUNK;
        if ((new = realloc(snap, sizeof(*snap) + tot)) == 0) {
            if (!*code) {
                logmsg(LOGMSG_ERROR, "%s: failed to grow map - %s\n", __func__,
                        strerror(errno));
            }
            free(snap);
            close(fd);
            *code = -3;
            return 0;
        }
        snap = new;
    }
    close(fd);
    if (sz < 0) {
        if (!*code) {
            logmsg(LOGMSG_ERROR, "%s: read of mapfile %s failed - %s\n", __func__,
                    mapfile, strerror(errno));
        }
        free(snap);
        *code = -4;
        return 0;
    }

    /* At this point, 0<sz<CHUNK, and thus there's at least one byte
     * available after the end of the maps file contents in space[].
     * Drop in a null byte there so that we can safely use string
     * functions on the file contents.
     * Following the file contents and null byte, use the spare space
     * (appropriately aligned) to hold the index strip.
     * Set up so that:
     *   snap->space[0...x-1]   - exefile link string
     *   snap->space[x...sz-1]  - file contents
     *   snap->space[sz]        - safety null byte
     *   snap->space[i...n-1]   - space for index strip
     * where i>sz, but the current index strip might have no space.
     * Set len to the available number of index strip entries.
     */
    sz += i;
    snap->space[sz] = '\0';
    i = sz + sizeof(int) - (sz % sizeof(int)); /* next int-aligned byte */
    snap->idx = (int *)&snap->space[i];
    len = (n - i) / sizeof(int);

    /* walk through the file contents turning lines into separate strings */
    for (p = &snap->space[x]; p < &snap->space[sz]; p = q) {
        if (snap->nmap >= len) /* grow the index strip */
        {
            tot = p - &snap->space[0];
            n += STEP * sizeof(int);
            if ((new = realloc(snap, sizeof(*snap) + n)) == 0) {
                if (!*code) {
                    logmsg(LOGMSG_ERROR, "%s: failed to grow map index - %s\n",
                            __func__, strerror(errno));
                }
                free(snap);
                *code = -4;
                return 0;
            }
            snap = new;
            snap->idx = (int *)&snap->space[i];
            len = (n - i) / sizeof(int);
            p = &snap->space[tot];
        }
        snap->idx[snap->nmap] = p - &snap->space[0];
        ++snap->nmap;
        if ((q = strchr(p, '\n')) != 0)
            *q++ = '\0';
        else
            q = strchr(p, '\0');
    }
    return snap;
}

int __linux_get_map_snap(bb_oscompat_map_snapshot_t *snap,
                         bb_oscompat_map_entry_t *mapentry, int mapnum,
                         int silent)
{
    char *end, *q, *p = &snap->space[snap->idx[mapnum]];

    q = p;
    mapentry->start = strtoull(q, &end, 16);
    if (end == q || *end != '-') {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: bad start addr in '%s', entry %d\n", __func__,
                    p, mapnum);
        }
        return -2;
    }
    q = end + 1;
    mapentry->after = strtoull(q, &end, 16);
    if (end == q || *end != ' ' || mapentry->after < mapentry->start) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: bad after addr in '%s', entry %d\n", __func__,
                    p, mapnum);
        }
        return -3;
    }
    mapentry->flags = 0;
    q = end;
    if (*++q == 'r')
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_READ;
    else if (*q != '-') {
    badperm:
        ;
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: bad permission chars in '%s', entry %d\n",
                    __func__, p, mapnum);
        }
        return -4;
    }
    if (*++q == 'w')
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_WRITE;
    else if (*q != '-')
        goto badperm;
    if (*++q == 'x')
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_EXEC;
    else if (*q != '-')
        goto badperm;
    if (*++q == 's')
        mapentry->flags |= BB_OSCOMPAT_MAPFLAG_SHARED;
    else if (*q != '-' && *q != 'p')
        goto badperm;
    if (*++q != ' ')
        goto badperm;
    mapentry->offset = strtoull(++q, &end, 16);
    if (end == q || *end != ' ') {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: bad offset in '%s', entry %d\n", __func__, p,
                    mapnum);
        }
        return -5;
    }
    mapentry->path = 0;
    mapentry->member = 0;
    if ((q = strchr(q, '/')) != 0) {
        if (strcmp(q, snap->space) == 0)
            mapentry->flags |= BB_OSCOMPAT_MAPFLAG_MAIN;
        mapentry->path = q;
    }
    return 0;
}

#else
#error "unsupported architecture"
#endif

bb_oscompat_map_snapshot_t *bb_init_pid_map_snapshot(pid_t pid, int *nmap)
{
    return bb_init_pid_map_snapshot_silent(pid, nmap, 0);
}

bb_oscompat_map_snapshot_t *
bb_init_pid_map_snapshot_silent(pid_t pid, int *nmap, int silent)
{
    bb_oscompat_map_snapshot_t *snap;

#if defined(_IBM_SOURCE)
    snap = __aix_init_map_snap(pid, &silent);
#elif defined(_SUN_SOURCE)
    snap = __sun_init_map_snap(pid, &silent);
#elif defined(_LINUX_SOURCE)
    snap = __linux_init_map_snap(pid, &silent);
#else
    snap = 0;
#error "unsupported architecture"
#endif
    if (snap != 0)
        silent = snap->nmap;
    *nmap = silent;
    return snap;
}

void bb_fini_pid_map_snapshot(bb_oscompat_map_snapshot_t *snap) { free(snap); }

int bb_get_pid_map_snapshot(bb_oscompat_map_snapshot_t *snap,
                            bb_oscompat_map_entry_t *mapentry, int mapnum)
{
    return bb_get_pid_map_snapshot_silent(snap, mapentry, mapnum, 0);
}

int bb_get_pid_map_snapshot_silent(bb_oscompat_map_snapshot_t *snap,
                                   bb_oscompat_map_entry_t *mapentry,
                                   int mapnum, int silent)
{
    if (mapnum < 0 || mapnum >= snap->nmap) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: bad mapnum %d should be [0,%d)\n", __func__,
                    mapnum, snap->nmap);
        }
        return -1;
    }
#if defined(_IBM_SOURCE)
    return __aix_get_map_snap(snap, mapentry, mapnum, silent);
#elif defined(_SUN_SOURCE)
    return __sun_get_map_snap(snap, mapentry, mapnum, silent);
#elif defined(_LINUX_SOURCE)
    return __linux_get_map_snap(snap, mapentry, mapnum, silent);
#else
    return -1;
#error "unsupported architecture"
#endif
}

/* helper routine to allocate a thread-specific buffer associated with a given
 * key
 * this would all be simpler if the pthread_once init routine took an argument
 * and the
 * key destructor function got the key as an argument */
static void *__compat_allocate_thread_buffer(pthread_once_t *once,
                                             void (*keyalloc_routine)(void),
                                             pthread_key_t *key, int bufsize)
{
    void *buf;
    int rc;

    rc = pthread_once(once, keyalloc_routine);
    if (rc != 0) {
        logmsgperror("__compat_allocate_thread_buffer:pthread_once");
        return NULL;
    }

    if ((buf = pthread_getspecific(*key)) == NULL) {
        buf = malloc(bufsize);
        if (buf == NULL) {
            logmsgperror("__compat_allocate_thread_buffer:malloc");
            return NULL;
        }

        rc = pthread_setspecific(*key, buf);
        if (rc != 0) {
            logmsgperror("__compat_allocate_thread_buffer:pthread_setspecific");
            free(buf);
            return NULL;
        }
    }

    return buf;
}

/* GETHOSTBYNAME/GETHOSTBYADDR: NATIVELY USES THREAD STORAGE ON IBM+HP */

#if defined(_SUN_SOURCE) || defined(_LINUX_SOURCE)

struct __compat_gethoststar_buffer {
    struct hostent hostent;
    char data[4096];
};

static pthread_once_t gethoststar_once = PTHREAD_ONCE_INIT;
static pthread_key_t gethoststar_key;

static void __compat_free_gethoststar_buffer(void *buf)
{
    int rc;
    rc = pthread_setspecific(gethoststar_key, NULL);
    if (rc != 0)
        logmsgperror("__compat_free_gethoststar_buffer:pthread_setspecific");
    free(buf);
}

static void __compat_allocate_gethoststar_key()
{
    int rc;
    rc = pthread_key_create(&gethoststar_key, __compat_free_gethoststar_buffer);
    if (rc != 0)
        logmsgperror("__compat_allocate_gethoststar_key:pthread_key_create");
}

static struct hostent *__compat_gethostbyname(const char *name)
{
    struct __compat_gethoststar_buffer *buf;
#if defined(_LINUX_SOURCE)
    struct hostent *result;
#endif

    buf = __compat_allocate_thread_buffer(
        &gethoststar_once, __compat_allocate_gethoststar_key, &gethoststar_key,
        sizeof(struct __compat_gethoststar_buffer));
    if (buf == NULL)
        return NULL;

#if defined(_SUN_SOURCE)
    return gethostbyname_r(name, &buf->hostent, buf->data, sizeof(buf->data),
                           &h_errno);
#elif defined(_LINUX_SOURCE)
    gethostbyname_r(name, &buf->hostent, buf->data, sizeof(buf->data), &result,
                    &h_errno);
    return result;
#endif
}

static struct hostent *__compat_gethostbyaddr(const void *addr, int len,
                                              int type)
{
    struct __compat_gethoststar_buffer *buf;
#if defined(_LINUX_SOURCE)
    struct hostent *result;
#endif

    buf = __compat_allocate_thread_buffer(
        &gethoststar_once, __compat_allocate_gethoststar_key, &gethoststar_key,
        sizeof(struct __compat_gethoststar_buffer));
    if (buf == NULL)
        return NULL;

#if defined(_SUN_SOURCE)
    return gethostbyaddr_r(addr, len, type, &buf->hostent, buf->data,
                           sizeof(buf->data), &h_errno);
#elif defined(_LINUX_SOURCE)
    gethostbyaddr_r(addr, len, type, &buf->hostent, buf->data,
                    sizeof(buf->data), &result, &h_errno);
    return result;
#endif
}

#endif /* defined(_SUN_SOURCE) || defined(_LINUX_SOURCE) */

struct hostent *bb_gethostbyname(const char *name)
{
#if defined(_SUN_SOURCE) || defined(_LINUX_SOURCE)
    return __compat_gethostbyname(name);
#elif defined(_IBM_SOURCE) || defined(_HP_SOURCE)
    /* IBM/HP's gethostbyname() is already thread-safe */
    return gethostbyname(name);
#else
#error "unsupported architecture"
#endif
}

struct hostent *bb_gethostbyaddr(const void *addr, int len, int type)
{
#if defined(_SUN_SOURCE) || defined(_LINUX_SOURCE)
    return __compat_gethostbyaddr(addr, len, type);
#elif defined(_IBM_SOURCE) || defined(_HP_SOURCE)
    /* IBM/HP's gethostbyaddr() is already thread-safe */
    return gethostbyaddr(addr, len, type);
#else
#error "unsupported architecture"
#endif
}

/* GETSERVBYNAME: NATIVELY USES THREAD STORAGE ON IBM/HP */

#if defined(_SUN_SOURCE) || defined(_LINUX_SOURCE)

struct __compat_getservbyname_buffer {
    struct servent servent;
    char data[4096];
};

static pthread_once_t getservbyname_once = PTHREAD_ONCE_INIT;
static pthread_key_t getservbyname_key;

static void __compat_free_getservbyname_buffer(void *buf)
{
    int rc;
    rc = pthread_setspecific(getservbyname_key, NULL);
    if (rc != 0)
        logmsgperror("__compat_free_getservbyname_buffer:pthread_setspecific");
    free(buf);
}

static void __compat_allocate_getservbyname_key()
{
    int rc;
    rc = pthread_key_create(&getservbyname_key,
                            __compat_free_getservbyname_buffer);
    if (rc != 0)
        logmsgperror("__compat_allocate_getservbyname_key:pthread_key_create");
}

static struct servent *__compat_getservbyname(const char *name,
                                              const char *proto)
{
    struct __compat_getservbyname_buffer *buf;
#if defined(_LINUX_SOURCE)
    struct servent *result;
#endif

    buf = __compat_allocate_thread_buffer(
        &getservbyname_once, __compat_allocate_getservbyname_key,
        &getservbyname_key, sizeof(struct __compat_getservbyname_buffer));
    if (buf == NULL)
        return NULL;

#if defined(_SUN_SOURCE)
    return getservbyname_r(name, proto, &buf->servent, buf->data,
                           sizeof(buf->data));
#elif defined(_LINUX_SOURCE)
    getservbyname_r(name, proto, &buf->servent, buf->data, sizeof(buf->data),
                    &result);
    return result;
#endif
}

#endif /* defined(_SUN_SOURCE) || defined(_LINUX_SOURCE) */

struct servent *bb_getservbyname(const char *name, const char *proto)
{
#if defined(_SUN_SOURCE) || defined(_LINUX_SOURCE)
    return __compat_getservbyname(name, proto);
#elif defined(_IBM_SOURCE) || defined(_HP_SOURCE)
    /* IBM/HP's getservbyname() is already thread-safe */
    return getservbyname(name, proto);
#else
#error "unsupported architecture"
#endif
}

/* TMPNAM: NATIVELY USES THREAD STORAGE ON SOLARIS */
#if defined(_IBM_SOURCE) || defined(_HP_SOURCE)
struct __compat_tmpnam_buffer {
    char data[L_tmpnam];
};

/* 'PTHREAD_ONCE_INIT' initializer on AIX is missing
 * a set of braces which is making GCC compilation fail. */
/* apparently no longer necessary */
static pthread_once_t tmpnam_once = PTHREAD_ONCE_INIT;
static pthread_key_t tmpnam_key;

static void __compat_free_tmpnam_buffer(void *buf)
{
    int rc;
    rc = pthread_setspecific(tmpnam_key, NULL);
    if (rc != 0)
        logmsgperror("__compat_free_tmpnam_buffer:pthread_setspecific");
    free(buf);
}

static void __compat_allocate_tmpnam_key()
{
    int rc;
    rc = pthread_key_create(&tmpnam_key, __compat_free_tmpnam_buffer);
    if (rc != 0)
        logmsgperror("__compat_allocate_tmpnam_key:pthread_key_create");
}

static char *__compat_tmpnam(char *s)
{
    struct __compat_tmpnam_buffer *buf;

    buf = __compat_allocate_thread_buffer(
        &tmpnam_once, __compat_allocate_tmpnam_key, &tmpnam_key,
        sizeof(struct __compat_tmpnam_buffer));
    if (buf == NULL)
        return NULL;

    return tmpnam(buf->data);
}
#endif

#ifndef _LINUX_SOURCE
char *bb_tmpnam(char *s)
{
    if (s != NULL)
        return tmpnam(s);
    else {
#if defined(_SUN_SOURCE)
        /* Solaris's tmpnam(NULL) is already thread-safe */
        return tmpnam(NULL);
#elif defined(_IBM_SOURCE) || defined(_HP_SOURCE)
        return __compat_tmpnam(NULL);
#else
#error "unsupported architecture"
#endif
    }
}
#endif

/* with #define _POSIX_PTHREAD_SEMANTICS, we get the standard prototype even on
 * Sun */
char *bb_ctime_r(const time_t *clock, char *buf) { return ctime_r(clock, buf); }

/* with #define _POSIX_PTHREAD_SEMANTICS, we get the standard prototype even on
 * Sun */
char *bb_asctime_r(const struct tm *tm, char *buf)
{
    return asctime_r(tm, buf);
}

int bb_cftime(char *s, size_t maxsize, const char *format, const time_t *clock)
{
    struct tm tm;
    localtime_r(clock, &tm);
    return strftime(s, maxsize, format, &tm);
}

int bb_getpeercred_silent(int fd, pid_t *pid, uid_t *euid, gid_t *egid,
                          int silent)
{
#ifdef _SUN_SOURCE
    ucred_t *cred = NULL;
    int rc = getpeerucred(fd, &cred);
    if (rc) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: getpeerucred failed rc %d errno %d: %s\n",
                    __func__, rc, errno, strerror(errno));
        }
        return -1;
    }

    *pid = ucred_getpid(cred);
    *euid = ucred_geteuid(cred);
    *egid = ucred_getegid(cred);
    ucred_free(cred);
#elif defined(_IBM_SOURCE)
    struct peercred_struct cred;
    socklen_t cred_sz = sizeof(cred);
    int rc = getsockopt(fd, SOL_SOCKET, SO_PEERID, &cred, &cred_sz);
    if (rc) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: getsockopt failed rc %d errno %d: %s\n",
                    __func__, rc, errno, strerror(errno));
        }
        return -1;
    }
    *pid = cred.pid;
    *euid = cred.euid;
    *egid = cred.egid;

#elif defined(_LINUX_SOURCE)
    struct ucred cred;
    socklen_t cred_sz = sizeof(cred);
    int rc = getsockopt(fd, SOL_SOCKET, SO_PEERCRED, &cred, &cred_sz);
    if (rc) {
        if (!silent) {
            logmsg(LOGMSG_ERROR, "%s: getsockopt failed rc %d errno %d: %s\n",
                    __func__, rc, errno, strerror(errno));
        }
        return -1;
    }
    *pid = cred.pid;
    *euid = cred.uid;
    *egid = cred.gid;
#else
#error "unsupported architecture"
#endif
    return 0;
}

int bb_getpeercred(int fd, pid_t *pid, uid_t *euid, gid_t *egid)
{
    return bb_getpeercred_silent(fd, pid, euid, egid, /* silent */ 0);
}

int bb_readdir(DIR *d, void *buf, struct dirent **dent) {
#ifdef _LINUX_SOURCE
    int rc;
    *dent = readdir(d);
    if (*dent == NULL)
        return errno;
    memcpy(buf, *dent, sizeof(struct dirent));
    return 0;
#else
    int rc;
    return readdir_r(d, buf, dent);
#endif
}

#ifdef BB_OSCOMPAT_TESTPROGRAM
#include <assert.h>
#include <signal.h>
#include <sys/wait.h>

static int test_bb_get_pid_numfds(int mypid)
{
    char cmd[128];
    int numfds;
    int fds[20];
    int ii;

    for (ii = 0; ii < 20; ii++) {
        fds[ii] = open("/dev/null", O_RDONLY);
        if (fds[ii] == -1)
            logmsgperror("open");
    }

    close(fds[5]);
    close(fds[13]);

    numfds = bb_get_pid_numfds(mypid);
    printf("i have %d open fds\n", numfds);

/* this will be off by one from my result on sun/ibm because
 * my result counts the open DIR* */
#if defined(_SUN_SOURCE) || defined(_HP_SOURCE)
    snprintf(cmd, sizeof(cmd), "pfiles %d | grep '^ [ ]*[0-9][0-9]*:' | wc -l",
             mypid);
#elif defined(_IBM_SOURCE)
    snprintf(cmd, sizeof(cmd),
             "procfiles %d | grep '^ [ ]*[0-9][0-9]*:' | wc -l", mypid);
#elif defined(_LINUX_SOURCE)
    /* LINUX DOESN'T HAVE PFILES */
    cmd[0] = '\0';
#else
#error "unsupported architecture"
#endif

    if (cmd[0])
        system(cmd);

    return 0;
}

static int test_bb_get_pid_cputime(int mypid)
{
    struct timespec utime, stime;
    int rc;
    int ii;

    for (ii = 0; ii < 1000000000; ii++)
        ;

    rc = bb_get_pid_cputime(mypid, &utime, &stime);
    assert(rc == 0);
    printf("utime: %d %9d\n", utime.tv_sec, utime.tv_nsec);
    printf("stime: %d %9d\n", stime.tv_sec, stime.tv_nsec);
    return 0;
}

static int test_bb_is_pid_stopped(int mypid)
{
    pid_t child;
    int rc;

    printf("TEST_BB_IS_PID_STOPPED() : STARTING...\n");

    /* first, test me - i should be running */
    rc = bb_is_pid_stopped(mypid);
    printf("  bb_is_pid_stopped(%d) returned %d\n", mypid, rc);
    assert(rc == 0);

    /* next - fork a child, stop him and test */
    if ((child = fork()) < 0) {
        logmsgperror("fork");
        return -1;
    } else if (child == 0) {
        /* CHILD */
        sleep(5);
        exit(0);
    } else {
        /* PARENT */
        sleep(1);

        rc = kill(child, SIGSTOP);
        if (rc != 0) {
            logmsgperror("kill");
            return -1;
        }

        sleep(1);

        rc = bb_is_pid_stopped(child);
        printf("  bb_is_pid_stopped(%d) returned %d\n", child, rc);
        assert(rc == 1);

        /* RESTART */
        rc = kill(child, SIGCONT);
        if (rc != 0) {
            logmsgperror("kill");
            return -1;
        }

        sleep(1);
        printf("  bb_is_pid_stopped(%d) returned %d\n", child, rc);
        assert(rc == 0);

        wait(0);
        printf("TEST_BB_IS_PID_STOPPED() : TESTS PASSED\n");
    }

    return 0;
}

static int test_bb_get_pid_execlink(int mypid)
{
    char path[PATH_MAX + 1];
    int rc;

    rc = bb_get_pid_execlink(mypid, path, sizeof(path));
    assert(rc == 0);

    printf("  bb_get_pid_execlink(%d) returned %s\n", mypid, path);
    return 0;
}

static int test_bb_get_pid_execpath(int mypid)
{
    char path[PATH_MAX + 1];
    int rc;

    strcpy(path, "-unset-");
    rc = bb_get_pid_execpath(mypid, path, sizeof(path));

    printf("  bb_get_pid_execpath(%d) returned %s\n", mypid, path);
    return 0;
}

static int test_bb_get_pid_map_snapshot(int mypid)
{
    bb_oscompat_map_snapshot_t *snap;
    bb_oscompat_map_entry_t mapentry;
    int i, n;

    if ((snap = bb_init_pid_map_snapshot(mypid, &n)) == 0) {
        printf("  bb_init_pid_map_snapshot(%d) failed with code %d\n", mypid,
               n);
        return 0;
    }
    i = n;
    while (--i >= 0) {
        if ((n = bb_get_pid_map_snapshot(snap, &mapentry, i)) != 0) {
            printf("  bb_get_pid_map_snapshot(%d) failed with code %d\n", i, n);
            break;
        }
        printf(" [%02d] %c%c%c%c%c %#llx-%#llx(+%08llu) ", i,
               mapentry.flags & BB_OSCOMPAT_MAPFLAG_READ ? 'r' : '-',
               mapentry.flags & BB_OSCOMPAT_MAPFLAG_WRITE ? 'w' : '-',
               mapentry.flags & BB_OSCOMPAT_MAPFLAG_EXEC ? 'x' : '-',
               mapentry.flags & BB_OSCOMPAT_MAPFLAG_SHARED ? 's' : '-',
               mapentry.flags & BB_OSCOMPAT_MAPFLAG_MAIN ? '*' : ' ',
               mapentry.start, mapentry.after, mapentry.offset);
        if (mapentry.path == 0)
            printf("-\n");
        else if (mapentry.member == 0)
            printf("%s\n", mapentry.path);
        else
            printf("%s[%s]\n", mapentry.path, mapentry.member);
    }
    bb_fini_pid_map_snapshot(snap);
    return 0;
}

/* expect threads to have different buffers but each thread to always reuse his
 */
static void *test_bb_gethostservstar_thread(void *v)
{
    struct hostent *prev_hentname = NULL, *prev_hentaddr = NULL;
    struct servent *prev_sent = NULL;
    char *fname;
    struct sockaddr_in addr;
    char buf[32];
    int ii;

    for (ii = 0; ii < 2; ii++) {
        struct hostent *hentname, *hentaddr;
        struct servent *sent;

        /* GETHOSTBYNAME */
        hentname = bb_gethostbyname("sundev9");
        if (hentname == NULL) {
            fprintf(stderr, "bb_gethostbyname failed: h_errno=%d\n", h_errno);
            return NULL;
        }

        memcpy((caddr_t)&addr.sin_addr, hentname->h_addr, hentname->h_length);
        inet_ntop(AF_INET, &addr.sin_addr, buf, sizeof(buf));
        fprintf(stderr, "tid %d iter %d:  %s (hent = %p)\n", pthread_self(), ii,
                buf, hentname);
        if (prev_hentname == NULL)
            prev_hentname = hentname;
        assert(prev_hentname == hentname); /* EXPECT SAME ADDRESS EACH TIME */

        /* GETHOSTBYADDR */
        hentaddr =
            bb_gethostbyaddr(&addr.sin_addr.s_addr, sizeof(in_addr_t), AF_INET);
        if (hentaddr == NULL) {
            fprintf(stderr, "bb_gethostbyaddr failed: h_errno=%d\n", h_errno);
            return NULL;
        }
        fprintf(stderr, "tid %d iter %d:  %s (hent = %p)\n", pthread_self(), ii,
                hentaddr->h_name, hentaddr);
        if (prev_hentaddr == NULL)
            prev_hentaddr = hentaddr;
        assert(prev_hentaddr == hentaddr); /* EXPECT SAME ADDRESS EACH TIME */

/* EXPECT THESE TO REUSE THE SAME STORAGE
 * (on HP-UX they don't, which is fine; we document the weaker interface) */
#ifndef __hpux
        assert(hentname == hentaddr);
#endif

        /* GETSERVBYNAME */
        sent = bb_getservbyname("bigrcv", "tcp");
        if (sent == NULL) {
            fprintf(stderr, "bb_getservbyname failed\n");
            return NULL;
        }
        fprintf(stderr, "tid %d iter %d:  %s -> %d (sent = %p)\n",
                pthread_self(), ii, sent->s_name, ntohs(sent->s_port), sent);
        if (prev_sent == NULL)
            prev_sent = sent;
        assert(prev_sent == sent); /* EXPECT SAME ADDRESS EACH TIME */

        /* EXPECT GETSERVBYNAME TO USE DIFFERENT STORAGE */
        assert((void *)hentname != (void *)sent);

#ifndef _LINUX_SOURCE
        fname = bb_tmpnam(NULL);
        fprintf(stderr, "tid %d iter %d: %s (%p)\n", pthread_self(), ii, fname,
                fname);
#endif
    }

    /* prevent buffer free (thread exit) until all threads have started */
    sleep(1);

    return NULL;
}

static int test_bb_gethostservstar()
{
    pthread_t tids[3];
    int rc;
    int ii;

    for (ii = 0; ii < 3; ii++) {
        rc = pthread_create(&tids[ii], NULL, test_bb_gethostservstar_thread,
                            NULL);
        assert(rc == 0);
    }

    for (ii = 0; ii < 3; ii++)
        pthread_join(tids[ii], NULL);

    return 0;
}

static int test_bb_ctime_r()
{
    char buf[26], *p;
    time_t now;

    time(&now);
    p = bb_ctime_r(&now, buf);
    printf("bb_ctime_r returns: %s\n", p);
    return 0;
}

static int test_bb_asctime_r()
{
    char buf[26], *p;
    time_t now;
    struct tm *tm;

    time(&now);
    tm = localtime(&now);
    p = bb_asctime_r(tm, buf);
    printf("bb_asctime_r returns: %s\n", p);
    return 0;
}

static int test_bb_cftime()
{
    char buf[32];
    time_t now;

    time(&now);
    int rc = bb_cftime(buf, sizeof(buf), "%T", &now);
    printf("bb_cftime returns: %s (%d)\n", buf, rc);
    return 0;
}

int main(int argc, char **argv)
{
    int mypid = getpid();
    struct timespec starttime;
    char argv0[64];
    int rc;
    int nlwps;
    pid_t ppid;
    char buffer[1024];

    if (argc > 1) {
        int pid = atoi(argv[1]);
        rc = bb_is_pid_traced(pid);
        printf("bb_is_pid_traced(%d) returns %d\n", pid, rc);
        printf("bb_get_pid_vsz(%d) returns %d\n", pid, bb_get_pid_vsz(pid));
    }

    printf("MY VSZ : %ld\n", bb_get_pid_vsz(mypid));

    rc = bb_get_pid_starttime(mypid, &starttime);
    assert(rc == 0);
    printf("PID STARTTIME: %d sec %ld nsec\n", starttime.tv_sec,
           starttime.tv_nsec);

    nlwps = bb_get_pid_numlwps(mypid);
    assert(nlwps > 0);
    printf("I HAVE %d LWPS\n", nlwps);

    ppid = bb_get_pid_ppid(mypid);
    printf("MY PPID IS %d\n", ppid);

    test_bb_ctime_r();
    test_bb_asctime_r();
    test_bb_cftime();

    rc = bb_get_pid_ppid_argv0(mypid, &ppid, argv0, sizeof(argv0));
    if (rc != 0) {
        fprintf(stderr, "bb_get_pid_ppid_argv0 failed for pid %d: rc %d\n",
                mypid, rc);
        return -1;
    }

    printf("tmpnam(0)   %s\n", tmpnam(NULL));
    printf("tmpnam(buf) %s\n", tmpnam(buffer));

    printf("bb_get_pid_ppid_argv0 reports:  ppid %d argv0 %s\n", ppid, argv0);

    test_bb_is_pid_stopped(mypid);

    test_bb_get_pid_numfds(mypid);

    test_bb_get_pid_cputime(mypid);

    test_bb_get_pid_execlink(mypid);

    test_bb_get_pid_execpath(mypid);

    test_bb_get_pid_map_snapshot(mypid);

    test_bb_gethostservstar();

    return 0;
}

#endif
