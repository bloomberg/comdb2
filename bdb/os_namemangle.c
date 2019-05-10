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

/* This is very, very verbose even at level 1.  Default to level 0. */
int gbl_namemangle_loglevel = 0;

#ifndef COMPILING_FOR_DB_TOOLS

/*
   this file overrides os_namemangle.o in the berkdb library with a version
   that intercepts all the calls dealing file filenames to call to
   bdb_trans().

   the purpose of this is to provide name mangling that
   allows us to   1) support clusters where databases live in different
                     directories on different nodes in the cluster
                  2) allow us to freely move databases from one directory
                     to another using tools no more advanced than "mv."

   */

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <limits.h>
#include <stdarg.h>

#include <build/db.h>

#include <ctrace.h>

char *bdb_trans(const char infile[], char outfile[], size_t outsz);

/*
** error: conflicting types for built-in function 'clogf'
** #include <complex.h>
** float complex clogf(float complex z);
**/
#define clogf comdb2_clogf
/* Log all file operations in ctrace file.  Filter out log file ops
 * since we do soooo many of those.  Also temp table opens/closes. */
static void clogf(const char *fmt, ...)
{
    va_list args;
    char line[1024];
    if (gbl_namemangle_loglevel > 0) {
        va_start(args, fmt);
        vsnprintf(line, sizeof(line), fmt, args);
        va_end(args);
        if (gbl_namemangle_loglevel > 1 ||
            (!strstr(line, "log.0") && !strstr(line, ".tmpdbs/"))) {
            ctrace("%s", line);
        }
    }
}

char *___db_rpath(const char *path);
char *__db_rpath(const char *path) { return ___db_rpath(path); }

int ___os_abspath(const char *path);
int __os_abspath(const char *path)
{
    int rc;
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(path, buf, sizeof(buf));
    rc = ___os_abspath(pbuf);
    if (gbl_namemangle_loglevel > 1)
        clogf("__os_abspath(%s:%s) = %d\n", path, pbuf, rc);
    return rc;
}

int ___os_dirlist(DB_ENV *dbenv, const char *dir, char ***namesp, int *cntp);
int __os_dirlist(DB_ENV *dbenv, const char *dir, char ***namesp, int *cntp)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(dir, buf, sizeof(buf));
    int rc = ___os_dirlist(dbenv, pbuf, namesp, cntp);
    clogf("___os_dirlist(%s:%s) = %d\n", dir, pbuf, rc);
    return rc;
}

#if defined(BERKDB_4_2) || defined(BERKDB_4_3)
int ___os_exists(const char *path, int *isdirp);
int __os_exists(const char *path, int *isdirp)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(path, buf, sizeof(buf));
    int rc = ___os_exists(pbuf, isdirp);
    if (gbl_namemangle_loglevel > 1)
        clogf("___os_exists(%s:%s) = %d\n", path, pbuf, rc);
    return rc;
}
#endif

#ifdef BERKDB_4_5
int ___os_exists(DB_ENV *dbenv, const char *path, int *isdirp);
int __os_exists(DB_ENV *dbenv, const char *path, int *isdirp)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(path, buf, sizeof(buf));
    int rc = ___os_exists(dbenv, pbuf, isdirp);
    if (gbl_namemangle_loglevel > 1)
        clogf("___os_exists(%s:%s) = %d\n", path, pbuf, rc);
    return rc;
}
#endif

int ___os_open(DB_ENV *dbenv, const char *name, u_int32_t flags, int mode,
               DB_FH **fhpp);
int __os_open(DB_ENV *dbenv, const char *name, u_int32_t flags, int mode,
              DB_FH **fhpp)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(name, buf, sizeof(buf));
    int rc = ___os_open(dbenv, pbuf, flags, 0664, fhpp);
    clogf("___os_open(%s:%s) = %d\n", name, pbuf, rc);
    return rc;
}

#if defined(BERKDB_4_2) || defined(BERKDB_4_3)
int ___os_open_extend(DB_ENV *dbenv, const char *name, u_int32_t log_size,
                      u_int32_t page_size, u_int32_t flags, int mode,
                      DB_FH **fhpp);
int __os_open_extend(DB_ENV *dbenv, const char *name, u_int32_t log_size,
                     u_int32_t page_size, u_int32_t flags, int mode,
                     DB_FH **fhpp)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(name, buf, sizeof(buf));
    int rc =
        ___os_open_extend(dbenv, pbuf, log_size, page_size, flags, 0664, fhpp);
    clogf("___os_open_extend(%s:%s) = %d\n", name, pbuf, rc);
    return rc;
}
#endif

#ifdef BERKDB_4_5
int ___os_open_extend(DB_ENV *dbenv, const char *name, u_int32_t page_size,
                      u_int32_t flags, int mode, DB_FH **fhpp);
int __os_open_extend(DB_ENV *dbenv, const char *name, u_int32_t page_size,
                     u_int32_t flags, int mode, DB_FH **fhpp)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(name, buf, sizeof(buf));
    int rc = ___os_open_extend(dbenv, pbuf, page_size, flags, 0664, fhpp);
    clogf("___os_open_extend(%s:%s) = %d\n", name, pbuf, rc);
    return rc;
}
#endif

int ___os_openhandle(DB_ENV *dbenv, const char *name, int flags, int mode,
                     DB_FH **fhpp);
int __os_openhandle(DB_ENV *dbenv, const char *name, int flags, int mode,
                    DB_FH **fhpp)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(name, buf, sizeof(buf));
    int rc = ___os_openhandle(dbenv, pbuf, flags, mode, fhpp);
    clogf("___os_openhandle(%s:%s) = %d\n", name, pbuf, rc);
    return rc;
}

int ___os_fileid(DB_ENV *dbenv, const char *fname, int unique_okay,
                 u_int8_t *fidp);
int __os_fileid(DB_ENV *dbenv, const char *fname, int unique_okay,
                u_int8_t *fidp)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(fname, buf, sizeof(buf));
    int rc = ___os_fileid(dbenv, pbuf, unique_okay, fidp);
    clogf("___os_fileid(%s:%s) = %d (*fidp = %u)\n", fname, pbuf, rc,
          (unsigned)*fidp);
    return rc;
}

int ___os_rename(DB_ENV *dbenv, const char *old, const char *new,
                 u_int32_t flags);
int __os_rename(DB_ENV *dbenv, const char *old, const char *new,
                u_int32_t flags)
{
    char old_path[PATH_MAX];
    char new_path[PATH_MAX];
    const char *pold = bdb_trans(old, old_path, sizeof(old_path));
    const char *pnew = bdb_trans(new, new_path, sizeof(new_path));
    int rc = ___os_rename(dbenv, pold, pnew, flags);
    clogf("___os_rename(%s:%s => %s:%s) = %d\n", old, pold, new, pnew, rc);
    return rc;
}

int ___os_ioinfo(DB_ENV *dbenv, const char *path, DB_FH *fhp,
                 u_int32_t *mbytesp, u_int32_t *bytesp, u_int32_t *iosizep);
int __os_ioinfo(DB_ENV *dbenv, const char *path, DB_FH *fhp, u_int32_t *mbytesp,
                u_int32_t *bytesp, u_int32_t *iosizep)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(path, buf, sizeof(buf));
    int rc = ___os_ioinfo(dbenv, pbuf, fhp, mbytesp, bytesp, iosizep);
    clogf("___os_ioinfo(%s:%s) = %d\n", path, pbuf, rc);
    return rc;
}

int ___os_region_unlink(DB_ENV *dbenv, const char *path);
int __os_region_unlink(DB_ENV *dbenv, const char *path)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(path, buf, sizeof(buf));
    int rc = ___os_region_unlink(dbenv, pbuf);
    clogf("___os_region_unlink(%s:%s) = %d\n", path, pbuf, rc);
    return rc;
}

int ___os_unlink(DB_ENV *dbenv, const char *path);
int __os_unlink(DB_ENV *dbenv, const char *path)
{
    char buf[PATH_MAX];
    const char *pbuf = bdb_trans(path, buf, sizeof(buf));
    int rc = ___os_unlink(dbenv, pbuf);
    clogf("__os_unlink(%s:%s) = %d\n", path, pbuf, rc);
    return rc;
}

int ___os_mapfile(DB_ENV *dbenv, char *path, DB_FH *fhp, size_t len,
                  int is_rdonly, void **addrp);
int __os_mapfile(DB_ENV *dbenv, char *path, DB_FH *fhp, size_t len,
                 int is_rdonly, void **addrp)
{
    char buf[PATH_MAX];
    char *pbuf = bdb_trans(path, buf, sizeof(buf));
    int rc = ___os_mapfile(dbenv, pbuf, fhp, len, is_rdonly, addrp);
    clogf("__os_mapfile(%s:%s) = %d\n", path, pbuf, rc);
    return rc;
}

#endif
