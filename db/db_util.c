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

#include <alloca.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <stddef.h>
#include <pthread.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/statvfs.h>
#ifdef __linux__
#include <sys/vfs.h>
#endif
#include <sys/msg.h>
#include <sys/resource.h>
#include <stdarg.h>
#include <sys/time.h>
#include <segstr.h>
#include <comdb2buf.h>
#include <str0.h>

#include "comdb2.h"
#include "util.h" /* the .h file for this .c */

#include <limits.h>
#include <float.h>
#include <ctype.h>
#include <sys/resource.h>

#include <logmsg.h>


/* like perror(), but takes the error number as an argument (useful for
 * pthreads) */
void perror_errnum(const char *s, int errnum)
{
    /*
            char errmsg[NL_TEXTMAX];
            (void)strerror_r(errnum, errmsg, sizeof(errmsg));
    */
    char *errmsg;

/* we use the deprecated sys_errlist, as strerror_r isnt available
   on the version of sunos we use (5.9) */

#ifdef _LINUX_SOURCE
    errmsg = strerror(errnum);
#elif _SUN_SOURCE
    char errmsgb[100];
    strerror_r(errnum, errmsgb, sizeof(errmsgb));
    errmsg = errmsgb;
#else
    errmsg =
        (unsigned int)errnum < sys_nerr ? sys_errlist[errnum] : "Unknown error";
#endif

    if (s != NULL)
        (void)logmsg(LOGMSG_ERROR, "%s: %s\n", s, errmsg);
    else
        (void)logmsg(LOGMSG_ERROR, "%s\n", errmsg);

    fflush(stderr);
}

void xorbufcpy(char *dest, const char *src, size_t len)
{
    while (len > 4) {
        *((uint32_t *)dest) = ~(*((const uint32_t *)src));
        len -= 4;
        dest += 4;
        src += 4;
    }
    while (len > 0) {
        *dest = ~(*src);
        len--;
        dest++;
        src++;
    }
}

void split_genid(unsigned long long genid, unsigned int *rrn1,
                 unsigned int *rrn2)
{
    unsigned int *ptr;

    ptr = (unsigned int *)(&genid);

    *rrn1 = ptr[0];
    *rrn2 = ptr[1];
}

unsigned long long merge_genid(unsigned int rrn1, unsigned int rrn2)
{
    unsigned long long genid;
    unsigned int *ptr;

    ptr = (unsigned int *)(&genid);

    ptr[0] = rrn1;
    ptr[1] = rrn2;

    return genid;
}

/* load a text file.  returns NULL and prints an error if it fails.
 * caller is responsible for free()ing the returned memory. */
char *load_text_file(const char *filename)
{
    int fd;
    char tempbuf[1024];
    char *buf = NULL;
    size_t buflen = 1024; /* we actually have one more char - room for \0 */
    size_t pos = 0;

    buf = malloc(buflen + 1);
    if (!buf) {
        logmsg(LOGMSG_ERROR, "load_text_file: '%s' out of memory, need %zu\n",
               filename, buflen);
        return NULL;
    }

    fd = open(filename, O_RDONLY);
    if (fd < 0) {
        logmsg(LOGMSG_ERROR, "load_text_file: '%s' cannot open: %s\n", filename,
                strerror(errno));
        free(buf);
        return NULL;
    }

    while (1) {
        size_t bytesread;
        bytesread = read(fd, tempbuf, sizeof(tempbuf));
        if (bytesread == 0)
            break;

        if (pos + bytesread > buflen) {
            char *newbuf;
            buflen += bytesread;
            newbuf = realloc(buf, buflen + 1);
            if (!newbuf) {
                logmsg(LOGMSG_ERROR,
                       "load_text_file: '%s' out of memory, need %zu\n",
                       filename, buflen);
                free(buf);
                Close(fd);
                return NULL;
            }
            buf = newbuf;
        }

        memcpy(buf + pos, tempbuf, bytesread);
        pos += bytesread;
    }

    Close(fd);
    buf[pos] = '\0';

    return buf;
}

/* Remove all table definitions from the lrl file and append use_llmeta */
int rewrite_lrl_remove_tables(const char *lrlname)
{
    int fdnew, fdold;
    COMDB2BUF *sbnew;
    COMDB2BUF *sbold;
    char newlrlname[256];
    char savlrlname[256];
    char line[1024];
    int err = 0;

    if (!lrlname)
        return 0;

    snprintf0(newlrlname, sizeof(newlrlname), "%s.newlrl", lrlname);
    snprintf0(savlrlname, sizeof(savlrlname), "%s.sav", lrlname);

    fdold = open(lrlname, O_RDONLY);
    if (fdold == -1) {
        logmsg(LOGMSG_ERROR, 
                "rewrite_lrl_remove_tables: cannot open '%s' for reading "
                "%d %s\n",
                lrlname, errno, strerror(errno));
        return -1;
    }

    fdnew = open(newlrlname, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fdnew == -1) {
        logmsg(LOGMSG_ERROR, 
                "rewrite_lrl_remove_tables: cannot open '%s' for writing "
                "%d %s\n",
                lrlname, errno, strerror(errno));
        Close(fdold);
        return -1;
    }

    sbnew = cdb2buf_open(fdnew, 0);
    if (!sbnew) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_remove_tables: cdb2buf_open fdnew failed\n");
        Close(fdnew);
        Close(fdold);
    }
    sbold = cdb2buf_open(fdold, 0);
    if (!sbold) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_remove_tables: cdb2buf_open fdold failed\n");
        cdb2buf_close(sbnew);
        Close(fdold);
    }

    /* phew!  ready to go */
    while (cdb2buf_gets(line, sizeof(line), sbold) > 0) {
        int st = 0, ltok;
        char *tok;

        /* get the first token on the line */
        tok = segtok(line, sizeof(line), &st, &ltok);

        /* if this line is a table def, skip it */
        if (ltok && tokcmp(tok, ltok, "table") == 0) {
            continue;
        }

        /* if this line is a spfile def, skip it */
        if (ltok && tokcmp(tok, ltok, "spfile") == 0) {
            continue;
        }

        /* if this line is a version_spfile def, skip it */
        if (ltok && tokcmp(tok, ltok, "version_spfile") == 0) {
            continue;
        }

        if (ltok && tokcmp(tok, ltok, "sfuncs") == 0)
            continue;
        if (ltok && tokcmp(tok, ltok, "afuncs") == 0)
            continue;
        if (ltok && tokcmp(tok, ltok, "queuedb") == 0)
            continue;
        if (ltok && tokcmp(tok, ltok, "timepartitions") == 0)
            continue;

        /* echo the line back out unchanged */
        cdb2buf_printf(sbnew, "%s", line);
    }

    cdb2buf_close(sbold);
    cdb2buf_close(sbnew);

    if (rename(lrlname, savlrlname) == -1) {
        logmsg(LOGMSG_ERROR, 
                "rewrite_lrl_remove_tables: rename %s -> %s failed %d %s\n",
                lrlname, savlrlname, errno, strerror(errno));
        err++;
    }
    if (rename(newlrlname, lrlname) == -1) {
        logmsg(LOGMSG_ERROR, 
                "rewrite_lrl_remove_tables: rename %s -> %s failed %d %s\n",
                newlrlname, lrlname, errno, strerror(errno));
        err++;
    }

    return err ? -1 : 0;
}

#define print_lua_funcs(pfx)                                                   \
    do {                                                                       \
        int num = listc_size(&thedb->lua_##pfx##funcs);                        \
        if (num) {                                                             \
            cdb2buf_printf(sb_out, #pfx "funcs %d", num);                         \
            struct lua_func_t * func;                                          \
            LISTC_FOR_EACH(&thedb->lua_##pfx##funcs, func, lnk) {              \
                cdb2buf_printf(sb_out, " %s", func->name);                        \
            }                                                                  \
            cdb2buf_printf(sb_out, "\n");                                         \
        }                                                                      \
    } while (0)

/* Create a new lrl file with the table defs added back in (the reverse of
 * llmeta'ing an lrl file */
int rewrite_lrl_un_llmeta(const char *p_lrl_fname_in, const char *p_lrl_fname_out, char *p_table_names[],
                          char *p_csc2_paths[], int table_nums[], size_t num_tables, char *out_lrl_dir, int has_sp,
                          int has_user_vers_sp, int has_timepartitions)
{
    unsigned i;
    int fd_out;
    int fd_in;
    COMDB2BUF *sb_out;
    COMDB2BUF *sb_in;
    char line[1024];

    fd_in = open(p_lrl_fname_in, O_RDONLY);
    if (fd_in == -1) {
        logmsg(LOGMSG_ERROR, "%s: cannot open '%s' for reading %d %s\n", __func__,
                p_lrl_fname_in, errno, strerror(errno));
        return -1;
    }

    fd_out = open(p_lrl_fname_out, O_WRONLY | O_CREAT | O_EXCL, 0666);
    if (fd_out == -1) {
        logmsg(LOGMSG_ERROR, "%s: cannot open '%s' for writing %d %s\n", __func__,
                p_lrl_fname_out, errno, strerror(errno));
        Close(fd_in);
        return -1;
    }

    sb_out = cdb2buf_open(fd_out, 0);
    if (!sb_out) {
        logmsg(LOGMSG_ERROR, "%s: cdb2buf_open fd_out failed\n", __func__);
        Close(fd_out);
        Close(fd_in);
    }
    sb_in = cdb2buf_open(fd_in, 0);
    if (!sb_in) {
        logmsg(LOGMSG_ERROR, "%s: cdb2buf_open fd_in failed\n", __func__);
        cdb2buf_close(sb_out);
        Close(fd_in);
    }

    /* phew!  ready to go, copy the contents of the in lrl file */
    while (cdb2buf_gets(line, sizeof(line), sb_in) > 0) {
        int st = 0, ltok;
        char *tok;
        tok = segtok(line, sizeof(line), &st, &ltok);
        if (ltok && tokcmp(tok, ltok, "table") == 0) {
            logmsg(LOGMSG_ERROR, "%s: skipping line containing table def: %s\n",
                    __func__, line);
            continue;
        }
        cdb2buf_printf(sb_out, "%s", line);
    }

    /* add table definitions */
    for (i = 0; i < num_tables; ++i) {
        cdb2buf_printf(sb_out, "table %s %s", p_table_names[i], p_csc2_paths[i]);

        if (table_nums[i])
            cdb2buf_printf(sb_out, " %d", table_nums[i]);

        cdb2buf_printf(sb_out, "\n");
    }

    if (has_sp) {
        cdb2buf_printf(sb_out, "spfile %s/%s_%s", out_lrl_dir, thedb->envname,
                    SP_FILE_NAME);
        cdb2buf_printf(sb_out, "\n");
    }

    if (has_user_vers_sp) {
        cdb2buf_printf(sb_out, "version_spfile %s/%s_%s", out_lrl_dir, thedb->envname, SP_VERS_FILE_NAME);
        cdb2buf_printf(sb_out, "\n");
    }

    if (has_timepartitions) {
        cdb2buf_printf(sb_out, "timepartitions %s/%s_%s", out_lrl_dir, thedb->envname, TIMEPART_FILE_NAME);
        cdb2buf_printf(sb_out, "\n");
    }

    for (int i = 0;
         i < thedb->num_qdbs && thedb->qdbs[i]->dbtype == DBTYPE_QUEUEDB; ++i)
        cdb2buf_printf(sb_out, "queuedb " REPOP_QDB_FMT "\n", out_lrl_dir,
                    thedb->envname, i);

    print_lua_funcs(s);
    print_lua_funcs(a);

    cdb2buf_close(sb_in);
    cdb2buf_close(sb_out);
    return 0;
}

/* Load the given lrl file and change or add a line for the given table. */
int rewrite_lrl_table(const char *lrlname, const char *tablename,
                      const char *csc2path)
{
    int fdnew, fdold;
    COMDB2BUF *sbnew;
    COMDB2BUF *sbold;
    char newlrlname[256];
    char savlrlname[256];
    char line[1024];
    int ntables = 0;
    int err = 0;

    snprintf0(newlrlname, sizeof(newlrlname), "%s.newlrl", lrlname);
    snprintf0(savlrlname, sizeof(savlrlname), "%s.sav", lrlname);

    fdold = open(lrlname, O_RDONLY);
    if (fdold == -1) {
        logmsg(LOGMSG_ERROR, 
                "rewrite_lrl_table: cannot open '%s' for reading %d %s\n",
                lrlname, errno, strerror(errno));
        return -1;
    }

    fdnew = open(newlrlname, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fdnew == -1) {
        logmsg(LOGMSG_ERROR, 
                "rewrite_lrl_table: cannot open '%s' for writing %d %s\n",
                lrlname, errno, strerror(errno));
        Close(fdold);
        return -1;
    }

    sbnew = cdb2buf_open(fdnew, 0);
    if (!sbnew) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_table: cdb2buf_open fdnew failed\n");
        Close(fdnew);
        Close(fdold);
    }
    sbold = cdb2buf_open(fdold, 0);
    if (!sbold) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_table: cdb2buf_open fdold failed\n");
        cdb2buf_close(sbnew);
        Close(fdold);
    }

    /* phew!  ready to go */
    while (cdb2buf_gets(line, sizeof(line), sbold) > 0) {
        int st = 0, ltok;
        char *tok;
        tok = segtok(line, sizeof(line), &st, &ltok);
        if (ltok && tokcmp(tok, ltok, "table") == 0) {
            tok = segtok(line, sizeof(line), &st, &ltok);
            if (ltok && tokcmp(tok, ltok, (char *)tablename) == 0) {
                /* skip past the current csc2 part */
                tok = segtok(line, sizeof(line), &st, &ltok);

                /* Rewrite this table line, preserving the database number
                 * field if present. */
                cdb2buf_printf(sbnew, "table %s %s%s", tablename, csc2path,
                            tok + ltok);
                ntables++;
                continue;
            }
        }
        cdb2buf_printf(sbnew, "%s", line);
    }

    /* If no tables written then add it */
    if (ntables == 0) {
        cdb2buf_printf(sbnew, "table %s %s\n", tablename, csc2path);
        ntables++;
    }

    cdb2buf_close(sbold);
    cdb2buf_close(sbnew);

    if (ntables != 1) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_table: something confused me because I "
                        "wrote %d lines instead of 1\n",
                ntables);
        return -1;
    }

    if (rename(lrlname, savlrlname) == -1) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_table: rename %s -> %s failed %d %s\n",
                lrlname, savlrlname, errno, strerror(errno));
        err++;
    }
    if (rename(newlrlname, lrlname) == -1) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_table: rename %s -> %s failed %d %s\n",
                newlrlname, lrlname, errno, strerror(errno));
        err++;
    }

    return err ? -1 : 0;
}

/* Just what the world needed.  This, and another Tetris clone. */
char *fmt_size(char *buf, size_t bufsz, uint64_t bytes)
{
    if (bytes > 1024ULL * 1024ULL * 1024ULL)
        snprintf(buf, bufsz, "%.2fGB",
                 (double)bytes / (1024.00 * 1024.00 * 1024.00));
    else if (bytes > 1024ULL * 1024ULL)
        snprintf(buf, bufsz, "%.2fMB", (double)bytes / (1024.00 * 1024.00));
    else if (bytes > 1024ULL)
        snprintf(buf, bufsz, "%.2fKB", (double)bytes / (1024.00));
    else
        snprintf(buf, bufsz, "%lluB", (long long unsigned)bytes);
    return buf;
}

/* NOTE: isn't threadsafe */
char *get_full_filename(char *path, int pathlen, enum dirtype type, char *name,
                        ...)
{
    int len;
    va_list args;
    char *ret = path;
    if (gbl_nonames) {
        switch (type) {
        case DIR_DB:
            snprintf(path, pathlen, "%s/", thedb->basedir);
            break;
        case DIR_TMP:
            snprintf(path, pathlen, "%s/tmp/", thedb->basedir);
            break;
        case DIR_TRANSACTION:
            snprintf(path, pathlen, "%s/logs/", thedb->basedir);
            break;
        default:
            return NULL;
        }
    } else {
        switch (type) {
        case DIR_DB:
            snprintf(path, pathlen, "%s/", thedb->basedir);
            break;
        case DIR_TMP:
            snprintf(path, pathlen, "%s/%s.tmpdbs/", thedb->basedir,
                     thedb->envname);
            break;
        case DIR_TRANSACTION:
            snprintf(path, pathlen, "%s/%s.txn/", thedb->basedir,
                     thedb->envname);
            break;
        default:
            return NULL;
        }
    }
    len = strlen(path);
    pathlen -= len;
    path += len;
    va_start(args, name);
    vsnprintf(path, pathlen, name, args);
    va_end(args);
    return ret;
}

