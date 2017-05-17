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

/* use bdb routines, but without path translation */

char *___db_rpath(const char *path);
char *__db_rpath(const char *path)
{
    /*fprintf(stderr, "___db_rpath\n");*/
    return ___db_rpath(path);
}

static char *trans(const char infile[], char outfile[]) { return infile; }

int ___os_abspath(const char *path);
int __os_abspath(const char *path)
{
    char buf[256];
    /*fprintf(stderr, "___os_abspath\n");*/
    return ___os_abspath(trans(path, buf));
}

int ___os_dirlist(DB_ENV *dbenv, const char *dir, char ***namesp, int *cntp);
int __os_dirlist(DB_ENV *dbenv, const char *dir, char ***namesp, int *cntp)
{
    char buf[256];
    /*fprintf(stderr, "___os_dirlist\n");*/
    return ___os_dirlist(dbenv, trans(dir, buf), namesp, cntp);
}

int ___os_exists(const char *path, int *isdirp);
int __os_exists(const char *path, int *isdirp)
{
    char buf[256];
    /*fprintf(stderr, "___os_exists\n");*/
    return ___os_exists(trans(path, buf), isdirp);
}

int ___os_open(DB_ENV *dbenv, const char *name, u_int32_t flags, int mode,
               DB_FH **fhpp);
int __os_open(DB_ENV *dbenv, const char *name, u_int32_t flags, int mode,
              DB_FH **fhpp)
{
    char buf[256];
    /*fprintf(stderr, "___os_open\n");*/
    /*return ___os_open(dbenv, trans(name, buf), flags, mode, fhpp);*/
    return ___os_open(dbenv, trans(name, buf), flags, 0664, fhpp);
}

int ___os_open_extend(DB_ENV *dbenv, const char *name, u_int32_t log_size,
                      u_int32_t page_size, u_int32_t flags, int mode,
                      DB_FH **fhpp);
int __os_open_extend(DB_ENV *dbenv, const char *name, u_int32_t log_size,
                     u_int32_t page_size, u_int32_t flags, int mode,
                     DB_FH **fhpp)
{
    char buf[256];
    /*fprintf(stderr, "___os_open_extend\n");*/
    /*
    return ___os_open_extend(dbenv, trans(name, buf), log_size, page_size,
       flags, mode, fhpp);
       */
    return ___os_open_extend(dbenv, trans(name, buf), log_size, page_size,
                             flags, 0664, fhpp);
}

int ___os_openhandle(DB_ENV *dbenv, const char *name, int flags, int mode,
                     DB_FH **fhpp);
int __os_openhandle(DB_ENV *dbenv, const char *name, int flags, int mode,
                    DB_FH **fhpp)
{
    char buf[256];
    /*fprintf(stderr, "___os_openhandle\n");*/
    return ___os_openhandle(dbenv, trans(name, buf), flags, mode, fhpp);
}

int ___os_fileid(DB_ENV *dbenv, const char *fname, int unique_okay,
                 u_int8_t *fidp);
int __os_fileid(DB_ENV *dbenv, const char *fname, int unique_okay,
                u_int8_t *fidp)
{
    char buf[256];
    /*fprintf(stderr, "___os_fileid\n");*/
    return ___os_fileid(dbenv, trans(fname, buf), unique_okay, fidp);
}

int ___os_rename(DB_ENV *dbenv, const char *old, const char *new,
                 u_int32_t flags);
int __os_rename(DB_ENV *dbenv, const char *old, const char *new,
                u_int32_t flags)
{
    char buf1[256];
    char buf2[256];
    /*fprintf(stderr, "___os_rename\n");*/
    return ___os_rename(dbenv, trans(old, buf1), trans(new, buf2), flags);
}

int ___os_ioinfo(DB_ENV *dbenv, const char *path, DB_FH *fhp,
                 u_int32_t *mbytesp, u_int32_t *bytesp, u_int32_t *iosizep);
int __os_ioinfo(DB_ENV *dbenv, const char *path, DB_FH *fhp, u_int32_t *mbytesp,
                u_int32_t *bytesp, u_int32_t *iosizep)
{
    char buf[256];
    /*fprintf(stderr, "___os_ioinfo\n");*/
    return ___os_ioinfo(dbenv, trans(path, buf), fhp, mbytesp, bytesp, iosizep);
}

int ___os_region_unlink(DB_ENV *dbenv, const char *path);
int __os_region_unlink(DB_ENV *dbenv, const char *path)
{
    char buf[256];
    /*fprintf(stderr, "___os_region_unlink\n");*/
    return ___os_region_unlink(dbenv, trans(path, buf));
}

int ___os_unlink(DB_ENV *dbenv, const char *path);
int __os_unlink(DB_ENV *dbenv, const char *path)
{
    char buf[256];
    /*fprintf(stderr, "___os_unlink <%s>\n", trans(path, buf));*/
    return ___os_unlink(dbenv, trans(path, buf));
}

int ___os_mapfile(DB_ENV *dbenv, char *path, DB_FH *fhp, size_t len,
                  int is_rdonly, void **addrp);
int __os_mapfile(DB_ENV *dbenv, char *path, DB_FH *fhp, size_t len,
                 int is_rdonly, void **addrp)
{
    char buf[256];
    /*fprintf(stderr, "___os_mapfile\n");*/
    return ___os_mapfile(dbenv, trans(path, buf), fhp, len, is_rdonly, addrp);
}
