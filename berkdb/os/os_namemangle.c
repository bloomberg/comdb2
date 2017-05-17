/*
  the purpose of this file is to allow berkdb to link and run "normally"
  when it is built as a standalone library.

  when we link with comdb2, we override this file in the berkdb library
  with a local comdb2 version.  the one we provide in comdb2 does name
  mangling to allow us to support clusters where databases are in different
  directories on different nodes, and allow us to freely move databases
  between directories using tools no more advanced than "mv"

  when berkdb is not linked against comdb2, there will be no change in
  behaviour from the expected berkdb functionality.
  */

#include "db_config.h"

#ifndef lint
static const char revid[] = "$Id: os_open.c,v 11.48 2003/09/10 00:27:29 bostic Exp $";
#endif /* not lint */

#ifndef NO_SYSTEM_INCLUDES
#include <sys/types.h>

#ifdef HAVE_SYS_FCNTL_H
#include <sys/fcntl.h>
#endif

#include <fcntl.h>
#include <string.h>
#endif

#include "db_int.h"


char *___db_rpath(const char *path);
char *
__db_rpath(const char *path)
{
	return ___db_rpath(path);
}

int ___os_abspath(const char *path);
int
__os_abspath(const char *path)
{
	return ___os_abspath(path);
}

int ___os_dirlist(DB_ENV *dbenv, const char *dir, char ***namesp, int *cntp);
int
__os_dirlist(DB_ENV *dbenv, const char *dir, char ***namesp, int *cntp)
{
	return ___os_dirlist(dbenv, dir, namesp, cntp);
}

int ___os_exists(const char *path, int *isdirp);
int
__os_exists(const char *path, int *isdirp)
{
	return ___os_exists(path, isdirp);
}

int ___os_open(DB_ENV *dbenv, const char *name, u_int32_t flags,
    int mode, DB_FH ** fhpp);
int
__os_open(DB_ENV *dbenv, const char *name, u_int32_t flags,
    int mode, DB_FH ** fhpp)
{
	return ___os_open(dbenv, name, flags, 0664, fhpp);
}

int ___os_open_extend(DB_ENV *dbenv, const char *name,
    u_int32_t log_size, u_int32_t page_size, u_int32_t flags,
    int mode, DB_FH ** fhpp);
int
__os_open_extend(DB_ENV *dbenv, const char *name,
    u_int32_t log_size, u_int32_t page_size, u_int32_t flags,
    int mode, DB_FH ** fhpp)
{
	return ___os_open_extend(dbenv, name, log_size, page_size,
	    flags, 0664, fhpp);
}

int ___os_openhandle(DB_ENV *dbenv, const char *name, int flags,
    int mode, DB_FH ** fhpp);
int
__os_openhandle(DB_ENV *dbenv, const char *name, int flags,
    int mode, DB_FH ** fhpp)
{
	return ___os_openhandle(dbenv, name, flags, mode, fhpp);
}


int ___os_fileid(DB_ENV *dbenv, const char *fname, int unique_okay,
    u_int8_t *fidp);
int
__os_fileid(DB_ENV *dbenv, const char *fname, int unique_okay, u_int8_t *fidp)
{
	return ___os_fileid(dbenv, fname, unique_okay, fidp);
}

int ___os_rename(DB_ENV *dbenv, const char *old, const char *new,
    u_int32_t flags);
int
__os_rename(DB_ENV *dbenv, const char *old, const char *new, u_int32_t flags)
{
	return ___os_rename(dbenv, old, new, flags);
}

int ___os_ioinfo(DB_ENV *dbenv, const char *path, DB_FH * fhp,
    u_int32_t *mbytesp, u_int32_t *bytesp, u_int32_t *iosizep);
int
__os_ioinfo(DB_ENV *dbenv, const char *path, DB_FH * fhp,
    u_int32_t *mbytesp, u_int32_t *bytesp, u_int32_t *iosizep)
{
	return ___os_ioinfo(dbenv, path, fhp, mbytesp, bytesp, iosizep);
}

int ___os_region_unlink(DB_ENV *dbenv, const char *path);
int
__os_region_unlink(DB_ENV *dbenv, const char *path)
{
	return ___os_region_unlink(dbenv, path);
}

int ___os_unlink(DB_ENV *dbenv, const char *path);
int
__os_unlink(DB_ENV *dbenv, const char *path)
{
	return ___os_unlink(dbenv, path);
}

int ___os_mapfile(DB_ENV *dbenv, char *path, DB_FH * fhp, size_t len,
    int is_rdonly, void **addrp);
int
__os_mapfile(DB_ENV *dbenv, char *path, DB_FH * fhp, size_t len,
    int is_rdonly, void **addrp)
{
	return ___os_mapfile(dbenv, path, fhp, len, is_rdonly, addrp);
}
