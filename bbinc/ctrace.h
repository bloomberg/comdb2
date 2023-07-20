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

#ifndef INCLUDED_CTRACE
#define INCLUDED_CTRACE

#include <stdio.h>
#include <stdarg.h>

#include <sysutil_compilerdefs.h>

#if defined __cplusplus
extern "C" {
#endif

/* use this to create /bb/data/taskname.trc.c and write printf
   comments.  ie, ctrace("big bug at line %d",__LINE__);

   The following environment variables can be used:-

   BBTRCLOG_ROLLAT=#, <TASKNAME>_TRCLOG_ROLLAT=#
    - the ctrace api will automatically roll the log file using the
      newoccurence script when it exceeds # bytes in size

   Note that the auto-rollat feature is not thread safe (unless
   ctrace_enable_threading() is called, see below), so only single threaded
   clients can currently use it.

   BBTRCLOG_WARNAT=#, <TASKNAME>s_TRCLOG_WARNAT=#
    - the ctrace api will warn when it opens the log file if it
      exceeds the size given.

   If BBTRCLOG_WARNAT or <TASKNAME>s_TRCLOG_WARNAT are not in the environment
   then the ctrace api will warn if the log file is larger than 1GB when
   it opens it.
   */

/* Enable locking, this should be called before any other ctrace api calls.
   This makes the auto-roll (ie rollat) feature safe in multithreaded
   applications.

   The rest of the ctrace api is relatively threadsafe so it
   isn't strictly necessary for a multithreaded application to call this unless
   it wants to safely use that feature, but it doesn't really hurt (under the
   hood the logging calls will contend over a file lock anyway).
   Similarly it isn't necessary to call this in a singly threaded application
   (or an application where only one thread uses ctrace), but it's not going to
   hurt anything.
   */
void ctrace_enable_threading(void);

/* Be careful with a rollover callback if ctrace_enable_threading() has been
   called, calling any ctrace api call except ctrace_get_logfilename() will
   deadlock.  This really eliminates any working rollover callback.
   If someone would like to use a rollover callback with
   ctrace_enable_threading() we can add callback-safe versions of other
   functions like ctrace_closelog() etc.
   */
typedef void ctrace_rollover_callback_t(void);
void ctrace_rollover_register_callback(ctrace_rollover_callback_t *func);

/* Set the ctrace rollat threshold in bytes.
 * If you call these before the log file is opened then the values set here
 * can be overridden by the env variables above.
 * Calling these after the log file is open will override the existing
 * values. */
int ctrace_set_rollat(void *, void *);

/* specify which directory trace log goes in. default is /bb/data.
   also specify taskname to be used in the trc.c filename
   can only be called once.  must be called before any ctrace calls */
void ctrace_openlog_taskname(const char *directory, const char *taskname);

/* Remove the active ctrace logfile.  Expects that the taskname passed
   matches the name used when creating the ctrace logfile, and will NOT
   remove the logfile the names do not match (and complains to stderr) */
void ctrace_unlink_taskname(const char *tsknm);

/* specify which directory trace log goes in. default is /bb/data.
   can only be called once.  must be called before any ctrace calls */
void ctrace_openlog(const char *directory);

/* close ctrace log file; log file will need to be re-opened
 * either by explicit openlog apis above; or will occur automatically
 * at next ctrace output w/ default dir/filename */
void ctrace_closelog(void);

/* output to ctrace log */
void ctrace(const char *format, ...) __attribute_format__((printf, 1, 2));

/* output to ctrace log, and also to specified FILE. */
void ctracef(FILE *tee, const char *format, ...)
    __attribute_format__((printf, 2, 3));

/* also output to ctrace log */
void vctrace(const char *format, va_list ap)
    __attribute_format__((printf, 1, 0));

void vctracent(const char *format, va_list ap)
    __attribute_format__((printf, 1, 0));

/* output to ctrace log, do not flush log after write */
void ctracenf(const char *format, ...) __attribute_format__((printf, 1, 2));

/* output to ctrace log, do not prefix with the time */
void ctracent(const char *format, ...) __attribute_format__((printf, 1, 2));

/* flush out ctrace log file */
void ctrace_flushlog(void);

/* get the filename of the log opened by ctrace module
 * the filename will be empty if log has not yet been opened
 * inorder to determine the filename, ctrace log file needs
 * to be opened - either by openlog apis above or writing
 * to the log */
const char *ctrace_get_logfilename(void);

/* Fortran wrapper (I hope!) */
void ctrace_(const char *str, int len);

/* Return the stdio stream object to which ctrace writes its output. */
FILE *ctrace_get_stream(void);

/* Set number of rolled copies to gzip (passed to newoccurence) */
void ctrace_set_gzip(int n);

/* Roll ctrace log now */
void ctrace_roll(void);

/* Set max ctrace logs to keep */
void ctrace_set_nlogs(int n);

int ctracev(const char *format, va_list args)
    __attribute_format__((printf, 1, 0));

extern __thread int gbl_logmsg_ctrace;

#if defined __cplusplus
}
#endif

#endif
