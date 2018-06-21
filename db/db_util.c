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
#include <string.h>
#include <strings.h>
#include <pthread.h>
#include <sys/types.h>
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

#include <plbitlib.h>
#include <segstr.h>
#include <sbuf2.h>
#include <str0.h>

#include <plhash.h>
#include "comdb2.h"
#include "util.h" /* the .h file for this .c */

#include <limits.h>
#include <float.h>
#include <ctype.h>
#include <sys/resource.h>

#include <logmsg.h>

#define TOUPPER(x) (((x >= 'a') && (x <= 'z')) ? x - 32 : x)

#ifdef _IBM_SOURCE
extern char *sys_errlist[];
extern int sys_nerr;
#endif

/* like perror(), but takes the error number as an argument (useful for
 * pthreads) */
void perror_errnum(const char *s, int errnum)
{
    /*
            char errmsg[NL_TEXTMAX];
            (void)strerror_r(errnum, errmsg, sizeof(errmsg));
    */
    char *errmsg;
    char errmsgb[100];

/* we use the deprecated sys_errlist, as strerror_r isnt available
   on the version of sunos we use (5.9) */

#ifdef _LINUX_SOURCE
    errmsg = strerror(errnum);
#elif _SUN_SOURCE
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

/* case-insensitive compare */
int strcmpfunc(char **a, char **b, int len)
{
    int cmp;
    cmp = strcasecmp(*a, *b);
    return cmp;
}

u_int strhashfunc(u_char **keyp, int len)
{
    unsigned hash;
    int jj;
    u_char *key = *keyp;
    for (hash = 0; *key; key++)
        hash = ((hash % 8388013) << 8) + (TOUPPER(*key));
    return hash;
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

void timeval_diff(struct timeval *before, struct timeval *after,
                  struct timeval *diff)
{
    diff->tv_sec = after->tv_sec - before->tv_sec;
    if (diff->tv_sec) {
        --diff->tv_sec;
        diff->tv_usec = 1000000 - before->tv_usec;
        diff->tv_usec += after->tv_usec;
        if (diff->tv_usec == 1000000) {
            ++diff->tv_sec;
            diff->tv_usec = 0;
        }
    } else {
        diff->tv_usec = after->tv_usec - before->tv_usec;
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
                close(fd);
                return NULL;
            }
            buf = newbuf;
        }

        memcpy(buf + pos, tempbuf, bytesread);
        pos += bytesread;
    }

    close(fd);
    buf[pos] = '\0';

    return buf;
}

/* Remove all table definitions from the lrl file and append use_llmeta */
int rewrite_lrl_remove_tables(const char *lrlname)
{
    int fdnew, fdold;
    SBUF2 *sbnew;
    SBUF2 *sbold;
    char newlrlname[256];
    char savlrlname[256];
    char line[1024];
    int ntables = 0;
    int err = 0;
    int have_use_llmeta = 0;
    int have_table_comment = 0;

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
        close(fdold);
        return -1;
    }

    sbnew = sbuf2open(fdnew, 0);
    if (!sbnew) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_remove_tables: sbuf2open fdnew failed\n");
        close(fdnew);
        close(fdold);
    }
    sbold = sbuf2open(fdold, 0);
    if (!sbold) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_remove_tables: sbuf2open fdold failed\n");
        sbuf2close(sbnew);
        close(fdold);
    }

    /* phew!  ready to go */
    while (sbuf2gets(line, sizeof(line), sbold) > 0) {
        int st = 0, ltok;
        char *tok;

        /* get the first token on the line */
        tok = segtok(line, sizeof(line), &st, &ltok);

        /* if this line is a table def, skip it */
        if (ltok && tokcmp(tok, ltok, "table") == 0) {
            ++ntables;
            continue;
        }

        /* if this line is a spfile def, skip it */
        if (ltok && tokcmp(tok, ltok, "spfile") == 0) {
            continue;
        }

        if (ltok && tokcmp(tok, ltok, "sfuncs") == 0)
            continue;
        if (ltok && tokcmp(tok, ltok, "afuncs") == 0)
            continue;
        if (ltok && tokcmp(tok, ltok, "queuedb") == 0)
            continue;

        /* echo the line back out unchanged */
        sbuf2printf(sbnew, "%s", line);
    }

    sbuf2close(sbold);
    sbuf2close(sbnew);

    /* If we didn't see as many definitions as we have tables */
#if 0
    if (ntables != thedb->num_dbs) {
        fprintf(stderr,
                "rewrite_lrl_remove_tables: something confused me because"
                " I found %d table definitions instead of %d\n",
                ntables, thedb->num_dbs);
        return -1;
    }
#endif

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
        int num = thedb->num_lua_##pfx##funcs;                                 \
        if (num) {                                                             \
            sbuf2printf(sb_out, #pfx "funcs %d", num);                         \
            for (i = 0; i < num; ++i) {                                        \
                sbuf2printf(sb_out, " %s", thedb->lua_##pfx##funcs[i]);        \
            }                                                                  \
            sbuf2printf(sb_out, "\n");                                         \
        }                                                                      \
    } while (0)

/* Create a new lrl file with the table defs added back in (the reverse of
 * llmeta'ing an lrl file */
int rewrite_lrl_un_llmeta(const char *p_lrl_fname_in,
                          const char *p_lrl_fname_out, char *p_table_names[],
                          char *p_csc2_paths[], int table_nums[],
                          size_t num_tables, char *out_lrl_dir, int has_sp)
{
    unsigned i;
    int fd_out;
    int fd_in;
    SBUF2 *sb_out;
    SBUF2 *sb_in;
    char line[1024];
    int ntables = 0;

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
        close(fd_in);
        return -1;
    }

    sb_out = sbuf2open(fd_out, 0);
    if (!sb_out) {
        logmsg(LOGMSG_ERROR, "%s: sbuf2open fd_out failed\n", __func__);
        close(fd_out);
        close(fd_in);
    }
    sb_in = sbuf2open(fd_in, 0);
    if (!sb_in) {
        logmsg(LOGMSG_ERROR, "%s: sbuf2open fd_in failed\n", __func__);
        sbuf2close(sb_out);
        close(fd_in);
    }

    /* phew!  ready to go, copy the contents of the in lrl file */
    while (sbuf2gets(line, sizeof(line), sb_in) > 0) {
        int st = 0, ltok;
        char *tok;
        tok = segtok(line, sizeof(line), &st, &ltok);
        if (ltok && tokcmp(tok, ltok, "table") == 0) {
            logmsg(LOGMSG_ERROR, "%s: skipping line containing table def: %s\n",
                    __func__, line);
            continue;
        }
        sbuf2printf(sb_out, "%s", line);
    }

    /* add table definitions */
    for (i = 0; i < num_tables; ++i) {
        if (strncmp(p_table_names[i], "sqlite_stat",
                    sizeof("sqlite_stat") - 1) == 0)
            continue;

        sbuf2printf(sb_out, "table %s %s", p_table_names[i], p_csc2_paths[i]);

        if (table_nums[i])
            sbuf2printf(sb_out, " %d", table_nums[i]);

        sbuf2printf(sb_out, "\n");
    }

    if (has_sp) {
        sbuf2printf(sb_out, "spfile %s/%s_%s", out_lrl_dir, thedb->envname,
                    SP_FILE_NAME);
        sbuf2printf(sb_out, "\n");
    }

    for (int i = 0;
         i < thedb->num_qdbs && thedb->qdbs[i]->dbtype == DBTYPE_QUEUEDB; ++i)
        sbuf2printf(sb_out, "queuedb " REPOP_QDB_FMT "\n", out_lrl_dir,
                    thedb->envname, i);

    print_lua_funcs(s);
    print_lua_funcs(a);

    sbuf2close(sb_in);
    sbuf2close(sb_out);
    return 0;
}

/* Load the given lrl file and change or add a line for the given table. */
int rewrite_lrl_table(const char *lrlname, const char *tablename,
                      const char *csc2path)
{
    int fdnew, fdold;
    SBUF2 *sbnew;
    SBUF2 *sbold;
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
        close(fdold);
        return -1;
    }

    sbnew = sbuf2open(fdnew, 0);
    if (!sbnew) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_table: sbuf2open fdnew failed\n");
        close(fdnew);
        close(fdold);
    }
    sbold = sbuf2open(fdold, 0);
    if (!sbold) {
        logmsg(LOGMSG_ERROR, "rewrite_lrl_table: sbuf2open fdold failed\n");
        sbuf2close(sbnew);
        close(fdold);
    }

    /* phew!  ready to go */
    while (sbuf2gets(line, sizeof(line), sbold) > 0) {
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
                sbuf2printf(sbnew, "table %s %s%s", tablename, csc2path,
                            tok + ltok);
                ntables++;
                continue;
            }
        }
        sbuf2printf(sbnew, "%s", line);
    }

    /* If no tables written then add it */
    if (ntables == 0) {
        sbuf2printf(sbnew, "table %s %s\n", tablename, csc2path);
        ntables++;
    }

    sbuf2close(sbold);
    sbuf2close(sbnew);

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

static inline char hex(unsigned char a)
{
    if (a < 10)
        return '0' + a;
    return 'a' + (a - 10);
}

void hexdumpbuf(char *key, int keylen, char **buf)
{
    char *mem;
    char *output;

    mem = malloc((2 * keylen) + 2);
    output = util_tohex(mem, key, keylen);

    *buf = output;
}

/* Return a hex string
 * output buffer should be appropriately sized */
char *util_tohex(char *out, const char *in, size_t len)
{
    char *beginning = out;
    char hex[] = "0123456789abcdef";
    const char *end = in + len;
    while (in != end) {
        char i = *(in++);
        *(out++) = hex[(i & 0xf0) >> 4];
        *(out++) = hex[i & 0x0f];
    }
    *out = 0;

    return beginning;
}

void hexdump(loglvl lvl, unsigned char *key, int keylen)
{
    if (key == NULL || keylen == 0) {
        logmsg(LOGMSG_ERROR, "NULL(%d)\n", keylen);
        return;
    }
    char *mem = alloca((2 * keylen) + 2);
    char *output = util_tohex(mem, key, keylen);

    logmsg(lvl, "%s\n", output);
}

/* printf directly (for printlog) */
void hexdumpdbt(DBT *dbt)
{
    unsigned char *s = dbt->data;
    int len = dbt->size;

    while (len) {
        printf("%02x", *s);
        s++;
        len--;
    }
}

void hexdumpfp(FILE *fp, unsigned char *key, int keylen)
{
    int i = 0;
    for (i = 0; i < keylen; i++) {
        if (fp) {
            fprintf(fp, "%c%c", hex(((unsigned char)key[i]) / 16),
                    hex(((unsigned char)key[i]) % 16));
        } else {
            logmsg(LOGMSG_USER, "%c%c", hex(((unsigned char)key[i]) / 16),
                   hex(((unsigned char)key[i]) % 16));
        }
    }
}
