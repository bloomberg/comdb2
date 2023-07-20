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

/*
 * OPEN TRACE LOG FOR C PROGRAMS.
 *
 * Note on dependencies:
 *
 * ctrace was designed for use in pekludged programs, and so "vanilla" pekludge
 * will call gettsk_() on the first ctrace() call to determine the program's
 * name and thus the log file name.  A non-pekludged program can call
 * ctrace_openlog_taskname() to explicitly provide its name.
 *
 * ctrace also depends on paulbits and time_epoch().  For the latter, we
 * keep track of whether or not we are pekludged and call time() if we are
 * not.  This is preferable to just using bbipc_time_epoch(), as that approach
 * might lead to a program attaching to corp and then pekludging, leading to
 * an error.
 */

#include "ctrace.h"

#include <errno.h>
#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <unistd.h>
#include <ctype.h>

#include <bbhrtime.h>
#include <lockmacros.h>
#include <memory_sync.h>
#include <epochlib.h>
#include <str0.h>

#include "roll_file.h"
#include "logmsg.h"
#include "locks_wrap.h"

enum { DEFAULT_WARNAT = 1 * 1024 * 1024 * 1024 };

int nlogs = 7;
unsigned long long rollat = 0;

static FILE *logf = 0;
static int once = 0;
static unsigned long long warnat = 0, orig_warnat = 0;
static unsigned long long filesz = 0;
static char logfilename[256] = {0};
static char savetaskname[9] = {0};

static ctrace_rollover_callback_t *callback_func = NULL;

static pthread_mutex_t g_mutex = PTHREAD_MUTEX_INITIALIZER;
static int g_mutex_enabled = 1;

#define IOLIMIT_FAST 500
#define IOLIMIT_SLOW 500

/* Safe version of time_epoch().  If we've pekludged then this will use our
 * shmem style of time_epoch(); otherwise this falls back to time() (which
 * I suspect is not all that slow, as it is usually a fast system call). */
static int ctrace_time_epoch(void) { return time(NULL); }

#define CTRACE_TIMER_START(THRESHOLD)                                          \
    do {                                                                       \
        bbhrtime_t timer_start;                                                \
        const int timer_thresh = (THRESHOLD);                                  \
        if (timer_thresh != 0)                                                 \
            getbbhrtime(&timer_start);

#define CTRACE_TIMER_END(opname)                                               \
    if (timer_thresh != 0) {                                                   \
        bbhrtime_t timer_end;                                                  \
        getbbhrtime(&timer_end);                                               \
        bbint64_t elapsed_ns = diff_bbhrtime(&timer_end, &timer_start);        \
        int elapsed_ms = (elapsed_ns / 1000000LL);                             \
        if (elapsed_ms > timer_thresh) {                                       \
            logmsg(LOGMSG_USER,                                                \
                    ":%s():%d:LONG I/O %dms above %dms - %s: %s\n",            \
                    __func__, __LINE__, elapsed_ms, timer_thresh, opname,      \
                    logfilename);                                              \
        }                                                                      \
    }                                                                          \
    }                                                                          \
    while (0)

/* caller MUST hold g_mutex */
static void init_filesz_lk(void)
{
    struct stat st;

    CTRACE_TIMER_START(IOLIMIT_FAST);
    {
        if (fstat(fileno(logf), &st) == -1) {
            logmsg(LOGMSG_ERROR, "Error %d in fstat on %s: %s\n", errno, logfilename,
                    strerror(errno));
            filesz = 0;
        } else {
            filesz = st.st_size;
        }
    }
    CTRACE_TIMER_END("fstat()");
}

/* caller MUST hold g_mutex */
static void wr_lk(int nbytes)
{
    if (nbytes > 0)
        filesz += nbytes;
}

/* See if the file has reached its size warning threshold.
 * To avoid stat'ing the file all the time we use dead reckoning here
 * (stat the file at open time, and then just keep track of how many bytes
 * we write to the file).  This is not thread safe and it won't work
 * accurately if multiple tasks are appending to the same ctrace file.
 * However it will work in most cases, so it's better than nothing.
 * If we think we might be over the limit then we do a proper stat on the
 * file again to determine if we really are.
 * caller MUST hold g_mutex
 */
static void chkwarnsz_lk(void)
{
    init_filesz_lk();
    if (warnat > 0 && filesz >= warnat) {
        logmsg(LOGMSG_WARN, "WARNING: ctrace log file '%s' is now %llu bytes; this "
                        "log needs to be rolled\n",
                logfilename, (unsigned long long)filesz);

        /* Warn again at 10% interval */
        warnat = 1.10 * filesz;
    } else {
        /* reset warnat to original value */
        warnat = orig_warnat;
    }
}

void comdb2_toupi(char *str, int len)
{
    char *p;
    for (p = str; len; p++, len--)
        *p = toupper(*p);
}

/* caller MUST hold g_mutex */
static void ctrace_openlog_taskname_lk(const char *directory,
                                       const char *taskname)
{
    char envvar[32] = {0}, *envstrp;

    if (once)
        return; /*already open*/
    once = 1;

    snprintf(envvar, sizeof(envvar), "%s_TRCLOG_ROLLAT", taskname);
    comdb2_toupi(envvar, sizeof(envvar));
    envstrp = getenv(envvar);
    if (NULL != envstrp && strlen(envstrp) > 0) {
        rollat = atoll(envstrp);
    } else {
        envstrp = getenv("BBTRCLOG_ROLLAT");
        if (NULL != envstrp && strlen(envstrp) > 0) {
            rollat = atoll(envstrp);
        }
    }

    if (warnat == 0)
        warnat = DEFAULT_WARNAT; /* default if not already set */
    snprintf(envvar, sizeof(envvar), "%s_TRCLOG_WARNAT", taskname);
    comdb2_toupi(envvar, sizeof(envvar));
    envstrp = getenv(envvar);
    if (NULL != envstrp && strlen(envstrp) > 0) {
        warnat = atoll(envstrp);
    } else {
        envstrp = getenv("BBTRCLOG_WARNAT");
        if (NULL != envstrp && strlen(envstrp) > 0) {
            warnat = atoll(envstrp);
        }
    }
    orig_warnat = warnat;

    snprintf(logfilename, sizeof(logfilename), "%s/%s.trc.c", directory,
             taskname);
    strncpy(savetaskname, taskname, sizeof(savetaskname) - 1);

    CTRACE_TIMER_START(IOLIMIT_SLOW);
    {
        logf = fopen(logfilename, "a");
        if (logf == 0) {
            logmsg(LOGMSG_ERROR, "%s:failed to create task log %s: %s\n", __func__,
                    logfilename, strerror(errno));
        } else {
            logmsg(LOGMSG_INFO, "%s:open ctrace file %s\n", __func__, logfilename);
            chkwarnsz_lk();

            /* without timestamp - eases searching for this simple header */
            logmsg(LOGMSG_INFO, "### start ctrace file %s by task %s ###\n",
                    logfilename, savetaskname);
        }
    }
    CTRACE_TIMER_END("fopen()+fprintf()");
}

void ctrace_unlink_taskname(const char *tsknm)
{
    /* don't bother with CTRACE_TIMER_...() here as this is called
     * as the process is exiting, so delays shouldn't be an issue.
     */
    if (savetaskname[0] == '\0') {
        /* no ctrace file created */
    } else if (strcmp(tsknm, savetaskname) == 0) {
        unlink(logfilename);
    }
}

void ctrace_openlog_taskname(const char *directory, const char *taskname)
{
    int mutex_enabled = g_mutex_enabled;

    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        ctrace_openlog_taskname_lk(directory, taskname);
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

/* caller MUST hold g_mutex */
static void ctrace_closelog_lk(void)
{
    if (logf == 0)
        return;
    CTRACE_TIMER_START(IOLIMIT_SLOW);
    {
        fclose(logf);
    }
    CTRACE_TIMER_END("fclose()");
    logf = NULL;
    once = 0;
}

void ctrace_closelog(void)
{
    int mutex_enabled = g_mutex_enabled;

    LOCKIFNZ(&g_mutex, mutex_enabled) { ctrace_closelog_lk(); }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

void ctrace_roll_lk(void)
{
    time_t now;
    enum { ROLLSEC = 10 };

    now = ctrace_time_epoch();
    ctrace_closelog_lk();
    roll_file(logfilename, nlogs);
    now = ctrace_time_epoch() - now;

    CTRACE_TIMER_START(IOLIMIT_SLOW);
    {
        logf = fopen(logfilename, "a");
    }
    CTRACE_TIMER_END("fopen()");

    if (logf == 0) {
        logmsg(LOGMSG_ERROR, "%s:failed to create task log %s: %s\n", __func__,
                logfilename, strerror(errno));
    }
}

void ctrace_roll(void)
{
    int mutex_enabled = g_mutex_enabled;

    LOCKIFNZ(&g_mutex, mutex_enabled) { ctrace_roll_lk(); }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

/* caller MUST hold g_mutex */
static void chkrollover_lk(void)
{
    enum { CHK_PERIOD = 10 };
    static time_t lastchk = 0;
    struct stat st;
    time_t now;
    int rc;

    if (!rollat) /* not set up for auto rolling */
        return;

    /* I dont want to stat() too often */
    now = ctrace_time_epoch();
    if (now - lastchk < CHK_PERIOD)
        return;
    else
        lastchk = now;

    CTRACE_TIMER_START(IOLIMIT_FAST);
    {
        rc = fstat(fileno(logf), &st);
        if (rc) {
            logmsg(LOGMSG_ERROR, "chklog:failed to fstat '%s'\n", logfilename);
            return;
        }
    }
    CTRACE_TIMER_END("fstat()");

    if (st.st_size < rollat) /* does not need to be rolled */
        return;

    /* check if a callback func was registered to handle rollover */
    if (callback_func) {
        callback_func();
        if (logf)
            chkwarnsz_lk();
        return;
    }

    ctrace_roll_lk();
}

/* usage in ctrace.h */
void ctrace_enable_threading(void)
{
    g_mutex_enabled = 1;
    MEMORY_SYNC;
}

int ctrace_set_rollat(void *unused, void *value)
{
    int mutex_enabled = g_mutex_enabled;
    int rollat_sz = *((int *)value);

    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        rollat = rollat_sz;
        if (logf)
            chkrollover_lk();
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
    return 0;
}

void ctrace_set_nlogs(int n) { nlogs = n; }

void ctrace_rollover_register_callback(ctrace_rollover_callback_t *func)
{
    int mutex_enabled = g_mutex_enabled;

    LOCKIFNZ(&g_mutex, mutex_enabled) { callback_func = func; }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

const char *ctrace_get_logfilename(void)
{
    /* don't grab lock because this address can't change, if the logfile isn't
     * yet open the filename itself can change */
    return logfilename;
}

/**************ctrace******************************************************
 *
 * Part 2: ctrace functions
 *
 *******************************************************************/

/* Caller must hold g_mutex if enabled.
 * Make sure that our logfile logf is open and ok to write to.
 */
static int ctrace_ok_ll(void)
{
    if (logf == 0) {
        return 0;
    } else {
        chkrollover_lk();
    }
    if (logf == 0) {
        return 0; /*failed to open log*/
    }
    return 1;
}

/* Returns a NUL terminated string which contains the current date/time prefix
 * to log with.  We cache the result so that if we call ctrace() many times a
 * second (which happens) we don't continuously call localtime(), as this is
 * expensive.
 *
 * The implementation here might look scary and lockless, however it should be
 * ok.  Thie following things make it ok:
 *
 * - Applications which use ctrace_enable_threading() will be calling this
 *   under lock anyway.
 *
 * - MT applications which don't enable locking should still be ok in here.
 *   Note that strftime is only given the size of tstr minus one.  This ensures
 *   that the final char of tstr will never be anything other than a NUL.
 *   Therefore, no matter what tearing effects there might be from two threads
 *   both calling ctrace() at exactly the same moment, the returned pointer
 *   will always be a NUl terminated string.
 *
 * Astute observers will realize that tearing effects are possible that may
 * cause weird results.  For example, two threads log simultaneously during
 * the transition from 10:59:59 to 11:00:00.  Thread 1 calls this, which
 * returns a pointer to tstr containing "10:59:59".  Thread 1 starts doing its
 * printf call and buffers "10:" in its output.  *then* thread 2 steps in, sees
 * that the epoch time has changed, and overwrites tstr with "11:00:00".
 * Thread 1 resumes execution and buffers out "00:00".  As a result thread 1
 * logs a timestamp of 10:00:00, which is an hour out.
 *
 * To reduce the likelihood of this ever happening I use an alternating two
 * buffer scheme described below in the function.  This does not make it
 * impossible, but very unlikely (I think one of the participating threads
 * would have to be descheduled at a critical moment for a full second for
 * it to happen).
 *
 * "This lockless Tom-foolery is terrible!"  I hear you cry.
 * But wait...
 * The old ctrace code, which this replaces, called localtime() *not*
 * localtime_r() and was therefore vulnerable to similar tearing effects
 * inside the static tm structure which that call presumably uses internally.
 * So really we're no worse off than we ever have been in programs that don't
 * call ctrace_enable_threading(), and entirely correct in programs that do.
 */
static char *ctrace_tstr(void)
{
    enum { LENGTH = 32 };
    static volatile time_t last_time;
    static char tstr1[LENGTH + 1] = {0};
    static char tstr2[LENGTH + 1] = {0};

    time_t now = ctrace_time_epoch();

    /*
     * To reduce the likelihood of tearing effects we alternate between two
     * static buffers.  This reduces (not eliminates!) the possibility that
     * while one thread is busy reading from the tstr buffer returned to it,
     * another thread is overwriting that buffer with a different date/time.
     * If that were to happen the first thread would likely output a weird date
     */
    char *tstr = (now & 1) ? tstr1 : tstr2;

    if (now != last_time) {
        struct tm tm;
        localtime_r(&now, &tm);

        strftime(tstr, LENGTH, "%D %T", &tm);

        last_time = now;
    }

    return tstr;
}

__thread int gbl_logmsg_ctrace = 0;

int ctracev(const char *format, va_list ap)
{
    int a, b;
    int mutex_enabled = g_mutex_enabled;
    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        if (!ctrace_ok_ll()) {
            errUNLOCKIFNZ(&g_mutex, mutex_enabled);
            return -1;
        }
        char *tstr = ctrace_tstr();
        CTRACE_TIMER_START(IOLIMIT_FAST);
        {
            int old_value = gbl_logmsg_ctrace;
            gbl_logmsg_ctrace = 0;
            a = logmsgf(LOGMSG_USER, logf, "%s:", tstr);
            wr_lk(a);
            b = logmsgvf(LOGMSG_USER, logf, format, ap);
            wr_lk(b);
            fflush(logf);
            gbl_logmsg_ctrace = old_value;
        }
        CTRACE_TIMER_END("fprintf()+vfprintf()+fflush()");
        if (warnat > 0 && filesz >= warnat)
            chkwarnsz_lk();
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
    if (a < 0 || b < 0) return -1;
    return a + b;
}

void ctrace(const char *format, ...)
{
    va_list ap;
    va_start(ap, format);
    ctracev(format, ap);
    va_end(ap);
}

void ctracef(FILE *tee, const char *format, ...)
{
    int mutex_enabled = g_mutex_enabled;
    va_list ap;

    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        if (!ctrace_ok_ll()) {
            errUNLOCKIFNZ(&g_mutex, mutex_enabled);
            return;
        }
        char *tstr = ctrace_tstr();
        CTRACE_TIMER_START(IOLIMIT_FAST);
        {
            wr_lk(logmsgf(LOGMSG_USER, logf, "%s:", tstr));
            va_start(ap, format);
            wr_lk(logmsgvf(LOGMSG_USER, logf, format, ap));
            fflush(logf);
        }
        CTRACE_TIMER_END("fprintf()+vfprintf()+fflush()");
        if (tee) {
            CTRACE_TIMER_START(IOLIMIT_FAST);
            {
                logmsgf(LOGMSG_USER, tee, "%s:", tstr);
                logmsgvf(LOGMSG_USER, tee, format, ap);
                fflush(tee);
            }
            CTRACE_TIMER_END("fprintf()+vfprintf()+fflush()");
        }
        va_end(ap);
        if (warnat > 0 && filesz >= warnat)
            chkwarnsz_lk();
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

void vctrace(const char *format, va_list ap)
{
    int mutex_enabled = g_mutex_enabled;

    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        if (!ctrace_ok_ll()) {
            errUNLOCKIFNZ(&g_mutex, mutex_enabled);
            return;
        }
        char *tstr = ctrace_tstr();
        CTRACE_TIMER_START(IOLIMIT_FAST);
        {
            wr_lk(logmsgf(LOGMSG_USER, logf, "%s:", tstr));
            wr_lk(logmsgvf(LOGMSG_USER, logf, format, ap));
            fflush(logf);
        }
        CTRACE_TIMER_END("fprintf()+vfprintf()+fflush()");
        if (warnat > 0 && filesz >= warnat)
            chkwarnsz_lk();
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

void ctracenf(const char *format, ...)
{
    int mutex_enabled = g_mutex_enabled;
    va_list ap;

    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        if (!ctrace_ok_ll()) {
            errUNLOCKIFNZ(&g_mutex, mutex_enabled);
            return;
        }
        char *tstr = ctrace_tstr();
        CTRACE_TIMER_START(IOLIMIT_FAST);
        {
            wr_lk(logmsgf(LOGMSG_USER, logf, "%s:", tstr));
            va_start(ap, format);
            wr_lk(logmsgvf(LOGMSG_USER, logf, format, ap));
        }
        CTRACE_TIMER_END("fprintf()+vfprintf()");
        va_end(ap);
        if (warnat > 0 && filesz >= warnat)
            chkwarnsz_lk();
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

void ctracent(const char *format, ...)
{
    int mutex_enabled = g_mutex_enabled;
    va_list ap;

    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        if (!ctrace_ok_ll()) {
            errUNLOCKIFNZ(&g_mutex, mutex_enabled);
            return;
        }
        va_start(ap, format);
        CTRACE_TIMER_START(IOLIMIT_FAST);
        {
            wr_lk(logmsgvf(LOGMSG_USER, logf, format, ap));
        }
        CTRACE_TIMER_END("vfprintf()");
        va_end(ap);
        if (warnat > 0 && filesz >= warnat)
            chkwarnsz_lk();
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

void vctracent(const char *format, va_list ap)
{
    int mutex_enabled = g_mutex_enabled;

    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        if (!ctrace_ok_ll()) {
            errUNLOCKIFNZ(&g_mutex, mutex_enabled);
            return;
        }
        CTRACE_TIMER_START(IOLIMIT_FAST);
        {
            wr_lk(logmsgvf(LOGMSG_USER, logf, format, ap));
            fflush(logf);
        }
        CTRACE_TIMER_END("vfprintf()+fflush()");
        if (warnat > 0 && filesz >= warnat)
            chkwarnsz_lk();
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

void ctrace_(const char *str, int len)
{
    int mutex_enabled = g_mutex_enabled;

    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        if (!ctrace_ok_ll()) {
            errUNLOCKIFNZ(&g_mutex, mutex_enabled);
            return;
        }

        /* adjust for FORTRAN space padded string */
        while ((len > 0) && (str[len - 1] == ' '))
            len--;

        char *tstr = ctrace_tstr();
        CTRACE_TIMER_START(IOLIMIT_FAST);
        {
            wr_lk(logmsgf(LOGMSG_USER, logf, "%s:%.*s\n", tstr, len, str));
            fflush(logf);
        }
        CTRACE_TIMER_END("fprintf()+fflush()");
        if (warnat > 0 && filesz >= warnat)
            chkwarnsz_lk();
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

void ctrace_flushlog(void)
{
    int mutex_enabled = g_mutex_enabled;

    LOCKIFNZ(&g_mutex, mutex_enabled)
    {
        if (logf) {
            CTRACE_TIMER_START(IOLIMIT_FAST);
            {
                fflush(logf);
            }
            CTRACE_TIMER_END("fflush()");
        }
    }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);
}

FILE *ctrace_get_stream(void)
{
    int mutex_enabled = g_mutex_enabled;
    FILE *logf_local;

    LOCKIFNZ(&g_mutex, mutex_enabled) { logf_local = logf; }
    UNLOCKIFNZ(&g_mutex, mutex_enabled);

    return logf_local;
}
