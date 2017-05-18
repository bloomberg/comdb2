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

#include <stdint.h>
#include <sys/types.h>
#include <time.h>
#include <limits.h>
#include <dirent.h>


#ifdef __cplusplus
extern "C" {
#endif

/* INPUT INTO THIS IS 'struct ipc_perm' */
#if defined(_LINUX_SOURCE)
#define BB_IPC_PERM_KEY(perm) ((perm).__key)
#else
#define BB_IPC_PERM_KEY(perm) ((perm).key)
#endif

#ifndef PATH_MAX
#ifdef MAXPATHLEN
#define PATH_MAX MAXPATHLEN
#else
#define PATH_MAX 1024
#endif
#endif

enum {
    BB_OSCOMPAT_MAPFLAG_READ = 0x1 << 0,
    BB_OSCOMPAT_MAPFLAG_WRITE = 0x1 << 1,
    BB_OSCOMPAT_MAPFLAG_EXEC = 0x1 << 2,
    BB_OSCOMPAT_MAPFLAG_SHARED = 0x1 << 3,
    BB_OSCOMPAT_MAPFLAG_MAIN = 0x1 << 4
};

typedef struct {
    uint64_t start;  /* starting address */
    uint64_t after;  /* just after the last mapped-in */
    uint64_t offset; /* offset into the mapped object */
    char *path;      /* pathname to the mapped object */
    char *member;    /* only nonnull for AIX */
    uint32_t flags;  /* BB_OSCOMPAT_MAPFLAG_* bits */
} bb_oscompat_map_entry_t;

typedef struct t_bb_oscompat_map_snapshot bb_oscompat_map_snapshot_t;

/**
 * The silent vesions of the following functions take an additional int silent
 * parameter on input and if it is not 0, not trace is printed from these
 *routines.
 **/

/**
 * read the start-time of a process from the /proc filesystem
 * returns 0 on success and fills starttime, non-zero on error
 **/
int bb_get_pid_starttime(pid_t pid, struct timespec *starttime);
int bb_get_pid_starttime_silent(pid_t pid, struct timespec *starttime,
                                int silent);
/**
 * returns the number of threads 'pid' has using /proc
 * returns < 0 on error
 **/
int bb_get_pid_numlwps(pid_t pid);
int bb_get_pid_numlwps_silent(pid_t pid, int silent);

/**
 * returns the number of open file descriptors 'pid' has
 * returns < 0 on error
 **/
int bb_get_pid_numfds(pid_t pid);
int bb_get_pid_numfds_silent(pid_t pid, int silent);

/**
 * returns true if 'pid' has 'file' open. 'file' must be
 * either a full-path name or a file existing in this
 * process' current working directory.
 **/
int bb_pid_has_file_open(const pid_t pid, const char *file, const int silent);

/**
 * returns user/system time used by 'pid'
 * returns 0 on success and fills utime,stime, non-zero on error
 **/
int bb_get_pid_cputime(pid_t pid, struct timespec *utime,
                       struct timespec *stime);
int bb_get_pid_cputime_silent(pid_t pid, struct timespec *utime,
                              struct timespec *stime, int silent);

/**
 * returns parent-pid of 'pid'
 **/
pid_t bb_get_pid_ppid(pid_t pid);
pid_t bb_get_pid_ppid_silent(pid_t pid, int silent);

/**
 * returns basename of argv0 (usually name of task image) of 'pid'
 **/
int bb_get_pid_argv0(pid_t pid, char *argv0, int sz);
int bb_get_pid_argv0_silent(pid_t pid, char *argv0, int sz, int silent);

/**
 * returns parent-pid and basename of argv0 of 'pid'
 **/
int bb_get_pid_ppid_argv0(pid_t pid, pid_t *ppid, char *argv0, int sz);
int bb_get_pid_ppid_argv0_silent(pid_t pid, pid_t *ppid, char *argv0, int sz,
                                 int silent);

/**
 * returns command argument string (usually name of task image) of 'pid'
 **/
int bb_get_pid_args(pid_t pid, char *args, int sz);
int bb_get_pid_args_silent(pid_t pid, char *args, int sz, int silent);

/**
 * returns parent-pid and command argument string of 'pid'
 **/
int bb_get_pid_ppid_args(pid_t pid, pid_t *ppid, char *args, int sz);
int bb_get_pid_ppid_args_silent(pid_t pid, pid_t *ppid, char *args, int sz,
                                int silent);

/**
 * return 1 if target pid is STOPPED
 **/
int bb_is_pid_stopped(pid_t pid);
int bb_is_pid_stopped_silent(pid_t pid, int silent);

/**
 * return 1 if target pid is being traced by a debugger
 **/
int bb_is_pid_traced(pid_t pid);
int bb_is_pid_traced_silent(pid_t pid, int silent);

/**
 * returns virtual address size (in KB) of 'pid'
 */
ssize_t bb_get_pid_vsz(pid_t pid);
ssize_t bb_get_pid_vsz_silent(pid_t pid, int silent);

/**
 * returns the path to a link to the executable file of 'pid'
 * differs from bb_get_pid_execpath in that this pathname will exist,
 * but only while the PID is still executing
 */
int bb_get_pid_execlink(pid_t pid, char *path, int sizeof_path);
int bb_get_pid_execlink_silent(pid_t pid, char *path, int sizeof_path,
                               int silent);

/**
 * returns the full path to the executable file for 'pid' if available
 * differs from bb_get_pid_execlink in that it might not be successful
 * (is silent regardless) but the path does not disappear automatically
 * when the PID exits and might be a relative path.
 * Note that due to symlinks sometimes being followed, different paths
 * for executables in the same location on different architectures can
 * end up with different results.
 */
int bb_get_pid_execpath(pid_t pid, char *path, int sizeof_path);

/**
 * Capture the current mappings for a PID.  End the snapshot lifetime with
 * bb_fini_pid_map_snapshot().  Returns a null pointer on error and
 * sets *nmap to an error code.  Otherwise, it returns a pointer to
 * a snapshot object and *nmap is set to the number of mappings.
 */
bb_oscompat_map_snapshot_t *bb_init_pid_map_snapshot(pid_t pid, int *nmap);
bb_oscompat_map_snapshot_t *
bb_init_pid_map_snapshot_silent(pid_t pid, int *nmap, int silent);

/**
 * Finish using a mappings snapshot.
 * The space referenced by any filled-in bb_oscompat_map_entry_t objects
 * is no longer usable as such.
 */
void bb_fini_pid_map_snapshot(bb_oscompat_map_snapshot_t *snap);

/**
 * Fill in *mapentry with the contents of snapshot entry number mapnum.
 * Returns zero on success; otherwise an error code.  Memory referenced
 * by the mapentry pointers is owned by the snapshot whose lifetime will
 * end once bb_fini_pid_map_snapshot() is called.
 */
int bb_get_pid_map_snapshot(bb_oscompat_map_snapshot_t *snap,
                            bb_oscompat_map_entry_t *mapentry, int mapnum);
int bb_get_pid_map_snapshot_silent(bb_oscompat_map_snapshot_t *snap,
                                   bb_oscompat_map_entry_t *mapentry,
                                   int mapnum, int silent);

/**
 * this routine is thread-safe.  it uses pthread keys and gethostbyname_r
 *internally
 * on systems that don't natively provide a thread-safe gethostbyname().
 * if it returns NULL, the rcode is found in h_errno.
 * NOTE: for a given thread, the same buffer is reused with each call.  you
 *should save
 *       any data that needs to persist.  making a shallow copy is not
 *sufficient.
 **/
struct hostent *bb_gethostbyname(const char *name);

/**
 * Similar semantics to bb_gethostbyname.
 * This reuses *THE SAME BUFFER* as bb_gethostbyname!
 */
struct hostent *bb_gethostbyaddr(const void *addr, int len, int type);

/**
 * Similar semantics to bb_gethostbyname.
 * This uses a different thread-safe buffer than bb_gethost*
 **/
struct servent *bb_getservbyname(const char *name, const char *proto);

#ifndef _LINUX_SOURCE
/**
 * Solaris-like thread-safe semantics on all systems.
 * If 's' is NULL, returns a pointer to thread-local storage.
 * (No Linux implementation because gnu ld gives us an annoying warning:
 *  warning: the use of `tmpnam' is dangerous, better use `mkstemp')
 */
char *bb_tmpnam(char *s);
#endif

/**
 * Uses standard syntax: buf must have at least 26 bytes of space.
 **/
char *bb_ctime_r(const time_t *clock, char *buf);

/**
 * Uses standard syntax: buf must have at least 26 bytes of space.
 **/
char *bb_asctime_r(const struct tm *tm, char *buf);

/**
 * A variant of strftime() that takes a time_t instead of a 'struct tm'.
 * (This is analagous to the cftime() function provided on Sun but also takes a
 * 'maxsize' arg)
 * Returns the number of bytes written if there was enough space, 0 otherwise
 * (in which case the buffer is left in an indeterminate state).
 */
int bb_cftime(char *s, size_t maxsize, const char *format, const time_t *clock);

/**
 * A way to find some details of the client at the other end of a
 * unix domain socket.
 * Uses getsockopt (Linux), getpeereid and getsockopt (AIX),
 * getpeerucred (Sun).
 *
 * Returns 0 on success, non-zero otherwise.
 */
int bb_getpeercred(int fd, pid_t *pid, uid_t *euid, gid_t *egid);
int bb_getpeercred_silent(int fd, pid_t *pid, uid_t *euid, gid_t *egid,
                          int silent);

int bb_readdir(DIR *d, void *buf, struct dirent **dent);

#ifdef __cplusplus
}
#endif

#endif
