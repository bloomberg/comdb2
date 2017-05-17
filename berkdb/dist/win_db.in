/*-
 * $Id: win_db.in,v 11.1 2003/03/20 15:08:21 bostic Exp $
 *
 * The following provides the information necessary to build Berkeley
 * DB on native Windows, and other Windows environments such as MinGW.
 */

#include <sys/types.h>
#include <sys/stat.h>

#include <direct.h>
#include <fcntl.h>
#include <io.h>
#include <limits.h>
#include <memory.h>
#include <process.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <errno.h>

/*
 * To build Tcl interface libraries, the include path must be configured to
 * use the directory containing <tcl.h>, usually the include directory in
 * the Tcl distribution.
 */
#ifdef DB_TCL_SUPPORT
#include <tcl.h>
#endif

#define	WIN32_LEAN_AND_MEAN
#include <windows.h>

/*
 * All of the necessary includes have been included, ignore the #includes
 * in the Berkeley DB source files.
 */
#define	NO_SYSTEM_INCLUDES

/*
 * Win32 has getcwd, snprintf and vsnprintf, but under different names.
 */
#define	getcwd(buf, size)	_getcwd(buf, size)
#define	snprintf		_snprintf
#define	vsnprintf		_vsnprintf

/*
 * Win32 does not define getopt and friends in any header file, so we must.
 */
#if defined(__cplusplus)
extern "C" {
#endif
extern int optind;
extern char *optarg;
extern int getopt(int, char * const *, const char *);
#if defined(__cplusplus)
}
#endif
