/*
   Copyright 2025 Bloomberg Finance L.P.

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

#include <sys/resource.h>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <dirent.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include "resource_use_dumper.h"

/*
 Convert a long value to a string representation.
 - If the value is RLIM_INFINITY, return "unlimited".
 - If the value is -2, return "unknown".
 - Otherwise, convert the value to a string using snprintf.
    - If snprintf fails or the buffer is too small, log an error and return "unknown".
*/
static const char *rlim_long_to_str(long val, char *buf, size_t buflen) {
    if (val == RLIM_INFINITY)
        return "unlimited";
    if (val == -2)
        return "unknown";
    const int rc = snprintf(buf, buflen, "%ld", val);
    if (rc < 0 || (size_t)rc >= buflen) {
        logmsg(LOGMSG_ERROR, "%s: snprintf failed or buffer too small. "
               "val=%ld, buflen=%zu, rc=%d\n",
               __func__, val, buflen, rc);
        return "unknown";
    }
    return buf;
}

static int get_nprocs_from_proc_dir(DIR *proc_dir) {
    long nprocs = 0;
    struct dirent *ent;
    for (ent = readdir(proc_dir); ent != NULL; ent = readdir(proc_dir)) {
        char *endptr;
        (void)strtol(ent->d_name, &endptr, 10);
        if (*endptr != '\0') continue;
        nprocs++;
    }

    return nprocs;
}

static void print_nproc_limits(loglvl logmsg_lvl) {
    struct rlimit rl;
    long nproc_cur = -2, nproc_max = -2;
    if (getrlimit(RLIMIT_NPROC, &rl) == 0) {
        nproc_cur = (long)rl.rlim_cur;
        nproc_max = (long)rl.rlim_max;
    }

    long nprocs = -2;
    DIR *proc_dir = opendir("/proc");
    if (proc_dir) {
        nprocs = get_nprocs_from_proc_dir(proc_dir);
        closedir(proc_dir);
    }

    char buf1[32], buf2[32], buf3[32];
    logmsg(logmsg_lvl, "RLIMIT_NPROC: cur=%s max=%s, utilization=%s\n",
        rlim_long_to_str(nproc_cur, buf1, sizeof(buf1)),
        rlim_long_to_str(nproc_max, buf2, sizeof(buf2)),
        rlim_long_to_str(nprocs, buf3, sizeof(buf3)));
}

static void print_system_thread_info(loglvl logmsg_lvl) {
    long sys_threads_max = -2;
    FILE *f = fopen("/proc/sys/kernel/threads-max", "r");
    if (f) {
        if (fscanf(f, "%ld", &sys_threads_max) != 1) sys_threads_max = -2;
        fclose(f);
    }
    long sys_threads = -2;
    FILE *p = popen("ps -eLf | wc -l", "r");
    if (p) {
        if (fscanf(p, "%ld", &sys_threads) != 1) sys_threads = -2;
        pclose(p);
    }
    char buf1[32], buf2[32];
    logmsg(logmsg_lvl, "System threads: max=%s utilization=%s\n",
        rlim_long_to_str(sys_threads_max, buf1, sizeof(buf1)),
        rlim_long_to_str(sys_threads, buf2, sizeof(buf2)));
}

static void print_stack_info(loglvl logmsg_lvl) {
    struct rlimit rl;
    long stack_max = -2;
    long stack_cur = -2;
    if (getrlimit(RLIMIT_STACK, &rl) == 0) {
        stack_cur = (long)rl.rlim_cur;
        stack_max = (long)rl.rlim_max;
    }

    long stack_util = -2;
    char stack_var;
    void *stack_addr = (void *)&stack_var;
    pthread_attr_t attr;
    if (pthread_getattr_np(pthread_self(), &attr) == 0) {
        void *base;
        size_t size;
        if (pthread_attr_getstack(&attr, &base, &size) == 0)
            stack_util = (char *)base + size - (char *)stack_addr;
        pthread_attr_destroy(&attr);
    }
    char buf1[32], buf2[32], buf3[32];
    logmsg(logmsg_lvl, "RLIMIT_STACK: cur=%s max=%s utilization=%s\n",
        rlim_long_to_str(stack_cur, buf1, sizeof(buf1)),
        rlim_long_to_str(stack_max, buf2, sizeof(buf2)),
        rlim_long_to_str(stack_util, buf3, sizeof(buf3)));
}

static void print_as_info(loglvl logmsg_lvl) {
    struct rlimit rl;
    long as_max = -2;
    long as_cur = -2;
    if (getrlimit(RLIMIT_AS, &rl) == 0) {
        as_max = (long)rl.rlim_max;
        as_cur = (long)rl.rlim_cur;
    } 

    long vm_util = -2;
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0)
        vm_util = (long)usage.ru_maxrss * 1024; // ru_maxrss is in KB

    char buf1[32], buf2[32], buf3[32];
    logmsg(logmsg_lvl, "RLIMIT_AS: cur=%s max=%s utilization: %s\n",
        rlim_long_to_str(as_cur, buf1, sizeof(buf1)),
        rlim_long_to_str(as_max, buf2, sizeof(buf2)),
        rlim_long_to_str(vm_util, buf3, sizeof(buf3)));
}

static void print_sysmem_info(loglvl logmsg_lvl) {
    struct sysinfo info;
    if (sysinfo(&info) == 0) {
        logmsg(logmsg_lvl, "System RAM: total=%lu free=%lu, swap: total=%lu free=%lu\n",
            info.totalram, info.freeram, info.totalswap, info.freeswap);
    }
}

void print_resource_utilization_and_limits(loglvl logmsg_lvl) {
    print_nproc_limits(logmsg_lvl);
    print_system_thread_info(logmsg_lvl);
    print_stack_info(logmsg_lvl);
    print_as_info(logmsg_lvl);
    print_sysmem_info(logmsg_lvl);
}
