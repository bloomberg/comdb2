#include <sys/resource.h>
#include <sys/sysinfo.h>
#include <sys/time.h>
#include <dirent.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>

#include "resource_use_dumper.h"

static const char *long_to_str(long val, char *buf, size_t buflen) {
    if (val == RLIM_INFINITY)
        return "unlimited";
    if (val == -2)
        return "unknown";
    snprintf(buf, buflen, "%ld", val);
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
    long cur = -2, max = -2;
    if (getrlimit(RLIMIT_NPROC, &rl) == 0) {
        cur = (long)rl.rlim_cur;
        max = (long)rl.rlim_max;
    }

    long nprocs = -2;
    DIR *proc_dir = opendir("/proc");
    if (proc_dir) {
        nprocs = get_nprocs_from_proc_dir(proc_dir);
        closedir(proc_dir);
    }

    char buf1[32], buf2[32], buf3[32];
    logmsg(logmsg_lvl, "RLIMIT_NPROC: cur=%s max=%s, utilization=%s\n",
        long_to_str(cur, buf1, sizeof(buf1)),
        long_to_str(max, buf2, sizeof(buf2)),
        long_to_str(nprocs, buf3, sizeof(buf3)));
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
    logmsg(logmsg_lvl, "System threads max: %s\n",
        long_to_str(sys_threads_max, buf1, sizeof(buf1)));
    logmsg(logmsg_lvl, "System threads utilization: %s\n",
        long_to_str(sys_threads, buf2, sizeof(buf2)));
}

static void print_stack_info(loglvl logmsg_lvl) {
    struct rlimit rl;
    long stack_limit = -2;
    if (getrlimit(RLIMIT_STACK, &rl) == 0) stack_limit = (long)rl.rlim_cur;

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
    char buf1[32], buf2[32];
    logmsg(logmsg_lvl, "RLIMIT_STACK: cur=%s\n",
        long_to_str(stack_limit, buf1, sizeof(buf1)));
    logmsg(logmsg_lvl, "Approximate stack utilization: %s\n",
        long_to_str(stack_util, buf2, sizeof(buf2)));
}

static void print_as_info(loglvl logmsg_lvl) {
    struct rlimit rl;
    long as_limit = -2;
    if (getrlimit(RLIMIT_AS, &rl) == 0) as_limit = (long)rl.rlim_max;

    long vm_util = -2;
    struct rusage usage;
    if (getrusage(RUSAGE_SELF, &usage) == 0)
        vm_util = (long)usage.ru_maxrss * 1024; // ru_maxrss is in KB

    char buf1[32], buf2[32];
    logmsg(logmsg_lvl, "RLIMIT_AS: max=%s\n",
        long_to_str(as_limit, buf1, sizeof(buf1)));
    logmsg(logmsg_lvl, "Approximate virtual memory utilization: %s\n",
        long_to_str(vm_util, buf2, sizeof(buf2)));
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
