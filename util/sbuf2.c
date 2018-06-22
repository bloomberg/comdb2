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

/* simple buffering for stream */

#include <sbuf2.h>

#include <errno.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/uio.h>
#include <unistd.h>
#include <netdb.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#if SBUF2_SERVER
#  ifndef SBUF2_DFL_SIZE
#    define SBUF2_DFL_SIZE 1024ULL
#  endif
#  include "mem_util.h"
#  define calloc comdb2_calloc_util
#  define malloc(size) comdb2_malloc(sb->allocator, size)
#  define free comdb2_free
#else /* SBUF2_SERVER */
#  ifndef SBUF2_DFL_SIZE
#    define SBUF2_DFL_SIZE (1024ULL * 128ULL)
#  endif
#endif /* !SBUF2_SERVER */

#if SBUF2_UNGETC
#  define SBUF2UNGETC_BUF_MAX 4
#endif

struct sbuf2 {
    int fd;
    int flags;

    int readtimeout;
    int writetimeout;

    int rhd, rtl;
    int whd, wtl;

#if SBUF2_UNGETC
    /* Server always has these. */
    int ungetc_buf[SBUF2UNGETC_BUF_MAX];
    int ungetc_buf_len;
#endif

    sbuf2writefn write;
    sbuf2readfn read;

    unsigned int lbuf;
    unsigned char *rbuf;
    unsigned char *wbuf;

    char *dbgout, *dbgin;

    void *userptr;

#if SBUF2_SERVER
    comdb2ma allocator;
#endif

#if WITH_SSL
    /* Server always supports SSL. */
    SSL *ssl;
    X509 *cert;
#endif
};

int SBUF2_FUNC(sbuf2fileno)(SBUF2 *sb)
{
    if (sb == NULL)
        return -1;
    return sb->fd;
}

/*just free SBUF2.  don't flush or close fd*/
int SBUF2_FUNC(sbuf2free)(SBUF2 *sb)
{
    if (sb == 0)
        return -1;

#if WITH_SSL
    /* Gracefully shutdown SSL to make the
       fd re-usable. */
    sslio_close(sb, 1);
#endif
    sb->fd = -1;
    if (sb->rbuf) {
        free(sb->rbuf);
        sb->rbuf = NULL;
    }
    if (sb->wbuf) {
        free(sb->wbuf);
        sb->wbuf = NULL;
    }
    if (sb->dbgin) {
        free(sb->dbgin);
        sb->dbgin = NULL;
    }
    if (sb->dbgout) {
        free(sb->dbgout);
        sb->dbgout = NULL;
    }
#if SBUF2_SERVER
    comdb2ma alloc = sb->allocator;
#endif
    free(sb);
#if SBUF2_SERVER
    comdb2ma_destroy(alloc);
#endif
    return 0;
}

/* flush output, close fd, and free SBUF2.*/
int SBUF2_FUNC(sbuf2close)(SBUF2 *sb)
{
    if (sb == 0)
        return -1;
    if (sb->fd < 0)
        return -1;

    if (!(sb->flags & SBUF2_NO_FLUSH))
        sbuf2flush(sb);

#if WITH_SSL
    /* We need to send "close notify" alert
       before closing the underlying fd. */
    sslio_close(sb, (sb->flags & SBUF2_NO_CLOSE_FD));
#endif

    if (!(sb->flags & SBUF2_NO_CLOSE_FD))
        close(sb->fd);

    return sbuf2free(sb);
}

/* flush output */
int SBUF2_FUNC(sbuf2flush)(SBUF2 *sb)
{
    int cnt = 0, rc, len;
    void *ssl;

    if (sb == 0)
        return -1;
    while (sb->whd != sb->wtl) {
        if (sb->wtl > sb->whd) {
            len = sb->lbuf - sb->wtl;
        } else {
            len = sb->whd - sb->wtl;
        }

#if SBUF2_SERVER && WITH_SSL
ssl_downgrade:
        ssl = sb->ssl;
        rc = sb->write(sb, (char *)&sb->wbuf[sb->wtl], len);
        if (rc == 0 && sb->ssl != ssl) {
            /* Fall back to plaintext if client donates
               the socket to sockpool. */
            goto ssl_downgrade;
        }
#else
        rc = sb->write(sb, (char *)&sb->wbuf[sb->wtl], len);
#endif
        if (rc <= 0)
            return -1 + rc;
        cnt += rc;
        sb->wtl += rc;
        if (sb->wtl >= sb->lbuf)
            sb->wtl = 0;
    }

    /* this reduces fragmentation for Nagle-disabled sockets*/
    sb->whd = sb->wtl = 0;
    return cnt;
}

int SBUF2_FUNC(sbuf2putc)(SBUF2 *sb, char c)
{
    int rc;
    if (sb == 0)
        return -1;
    if ((sb->whd == sb->lbuf - 1 && sb->wtl == 0) || (sb->whd == sb->wtl - 1)) {
        rc = sbuf2flush(sb);
        if (rc < 0)
            return rc;
    }
    sb->wbuf[sb->whd] = c;
    sb->whd++;
    if (sb->whd >= sb->lbuf)
        sb->whd = 0;
    if ((sb->flags & SBUF2_WRITE_LINE) && c == '\n') {
        rc = sbuf2flush(sb);
        if (rc < 0)
            return rc;
    }
    return 1;
}

int SBUF2_FUNC(sbuf2puts)(SBUF2 *sb, char *string)
{
    int rc, ii;
    if (sb == 0)
        return -1;
    for (ii = 0; string[ii]; ii++) {
        rc = sbuf2putc(sb, string[ii]);
        if (rc < 0)
            return rc;
    }
    if (sb->flags & SBUF2_DEBUG_LAST_LINE) {
        if (sb->dbgout)
            free(sb->dbgout);
        sb->dbgout = strdup(string);
    }
    return ii;
}

/* returns num items written || <0 for error*/
int SBUF2_FUNC(sbuf2write)(char *ptr, int nbytes, SBUF2 *sb)
{
    int rc, ii, off, left, written = 0;
    if (sb == 0)
        return -1;
    off = 0;
    left = nbytes;
    while (left > 0) {
        int towrite = 0;

        if ((sb->whd == sb->lbuf - 1 && sb->wtl == 0) ||
            (sb->whd == sb->wtl - 1)) {
            rc = sbuf2flush(sb);
            if (rc < 0)
                return written;
        }

        if (sb->whd < sb->wtl) {
            towrite = sb->wtl - sb->whd - 1;
            if (towrite > left)
                towrite = left;

        } else {
            towrite = sb->lbuf - sb->whd - 1;
            if (sb->wtl != 0)
                towrite++;
            if (towrite > left)
                towrite = left;
        }

        memcpy(&sb->wbuf[sb->whd], &ptr[off], towrite);
        sb->whd += towrite;
        off += towrite;
        left -= towrite;
        written += towrite;
        if (sb->wtl == 0 && sb->whd >= (sb->lbuf - 1)) {
            continue;
        } else if (sb->whd >= sb->lbuf)
            sb->whd = 0;
    }

    return nbytes;
}

/* returns num items written || <0 for error*/
int SBUF2_FUNC(sbuf2fwrite)(char *ptr, int size, int nitems, SBUF2 *sb)
{
    int rc, ii, jj, off;
    if (sb == 0)
        return -1;
    off = 0;
    if (!(sb->flags & SBUF2_WRITE_LINE))
        sbuf2write(ptr, size * nitems, sb);
    else {
        for (ii = 0; ii < nitems; ii++) {
            for (jj = 0; jj < size; jj++) {
                rc = sbuf2putc(sb, ptr[off++]);
                if (rc < 0)
                    return ii;
            }
        }
    }
    return nitems;
}

int SBUF2_FUNC(sbuf2getc)(SBUF2 *sb)
{
    int rc, cc;
    void *ssl;
    if (sb == 0)
        return -1;

#if SBUF2_UNGETC
    if (sb->ungetc_buf_len > 0) {
        sb->ungetc_buf_len--;
        return sb->ungetc_buf[sb->ungetc_buf_len];
    }
#endif

    if (sb->rtl == sb->rhd) {
        /*nothing buffered*/
        sb->rtl = 0;
        sb->rhd = 0;
#if SBUF2_SERVER && WITH_SSL
ssl_downgrade:
        ssl = sb->ssl;
        rc = sb->read(sb, (char *)sb->rbuf, sb->lbuf - 1);
        if (rc == 0 && sb->ssl != ssl)
            goto ssl_downgrade;
#else
        rc = sb->read(sb, (char *)sb->rbuf, sb->lbuf - 1);
#endif
        if (rc <= 0)
            return -1 + rc;
        sb->rhd = rc;
    }
    cc = sb->rbuf[sb->rtl];
    sb->rtl++;
    if (sb->rtl >= sb->lbuf)
        sb->rtl = 0;
    return cc;
}

#if SBUF2_UNGETC
int SBUF2_FUNC(sbuf2ungetc)(char c, SBUF2 *sb)
{
    int i;
    if (sb == NULL)
        return -1;

    i = c;
    if (i == EOF || (sb->ungetc_buf_len == SBUF2UNGETC_BUF_MAX))
        return EOF;

    sb->ungetc_buf[sb->ungetc_buf_len] = c;
    sb->ungetc_buf_len++;
    return c;
}
#endif

/*return null terminated string and len (or <0 if error)*/
int SBUF2_FUNC(sbuf2gets)(char *out, int lout, SBUF2 *sb)
{
    int cc, ii;
    if (sb == 0)
        return -1;
    lout--;
    for (ii = 0; ii < lout;) {
        cc = sbuf2getc(sb);
        if (cc < 0) {
            if (ii == 0)
                return cc; /*return error if first char*/
            break;
        }
        out[ii] = cc;
        ii++;
        if (cc == '\n')
            break;
    }
    out[ii] = 0;
    if (sb->flags & SBUF2_DEBUG_LAST_LINE) {
        if (sb->dbgin)
            free(sb->dbgin);
        sb->dbgin = strdup(out);
    }
    return ii; /*return string len*/
}

/* returns num items read || <0 for error*/
static int sbuf2fread_int(char *ptr, int size, int nitems,
                          SBUF2 *sb, int *was_timeout)
{
    int need = size * nitems;
    int done = 0;
    void *ssl;

#if SBUF2_UNGETC
    if (sb->ungetc_buf_len > 0) {
        int from = sb->ungetc_buf_len;
        while (from && (done < need)) {
            --from;
            ptr[done] = sb->ungetc_buf[from];
            ++done;
            --need;
        }
        sb->ungetc_buf_len = from;
    }
#endif

    while (1) {
        /* if data available in buffer */
        if (sb->rtl != sb->rhd) {
            int buffered = sb->rhd - sb->rtl;
            int amt = need < buffered ? need : buffered;
            void *to = ptr + done;
            void *from = sb->rbuf + sb->rtl;
            memcpy(to, from, amt);
            need -= amt;
            done += amt;
            sb->rtl += amt;
        }

        /* if still need more data */
        if (need > 0) {
            int rc;
            sb->rtl = 0;
            sb->rhd = 0;
#if SBUF2_SERVER && WITH_SSL
ssl_downgrade:
            ssl = sb->ssl;
            rc = sb->read(sb, (char *)sb->rbuf, sb->lbuf - 1);
            if (rc == 0 && sb->ssl != ssl)
                goto ssl_downgrade;
#else
            rc = sb->read(sb, (char *)sb->rbuf, sb->lbuf - 1);
#endif
            if (rc <= 0) {
                if (rc == 0) { /* this is a timeout */
                    if (was_timeout)
                        *was_timeout = 1;
                }
                return (done / size);
            }
            sb->rhd = rc;
            continue;
        }
        break;
    }
    return nitems;
}

/* returns num items read || <0 for error*/
int SBUF2_FUNC(sbuf2fread)(char *ptr, int size, int nitems, SBUF2 *sb)
{
    return sbuf2fread_int(ptr, size, nitems, sb, NULL);
}

/* returns num items read || <0 for error*/
int SBUF2_FUNC(sbuf2fread_timeout)(char *ptr, int size, int nitems, SBUF2 *sb,
                                   int *was_timeout)
{
    return sbuf2fread_int(ptr, size, nitems, sb, was_timeout);
}

int SBUF2_FUNC(sbuf2printf)(SBUF2 *sb, const char *fmt, ...)
{
    /*just do sprintf to local buf (limited to 1k),
      and then emit through sbuf2*/
    char lbuf[1024];
    va_list ap;
    if (sb == 0)
        return -1;
    va_start(ap, fmt);
    vsnprintf(lbuf, sizeof(lbuf), fmt, ap);
    va_end(ap);
    return sbuf2puts(sb, lbuf);
}

int SBUF2_FUNC(sbuf2printfx)(SBUF2 *sb, char *buf, int lbuf, char *fmt, ...)
{
    /*do sprintf to user supplied buffer*/
    int rc;
    va_list ap;
    if (sb == 0)
        return -1;
    va_start(ap, fmt);
    rc = vsnprintf(buf, lbuf, fmt, ap);
    va_end(ap);
    if (rc < 0)
        return rc;
    return sbuf2puts(sb, buf);
}

/* default read/write functions for sbuf, which implement timeouts and
 * retry on EINTR. */
static int swrite_unsecure(SBUF2 *sb, const char *cc, int len)
{
    int rc;
    struct pollfd pol;
    if (sb == 0)
        return -1;

    if (sb->writetimeout > 0) {
        do {
            pol.fd = sb->fd;
            pol.events = POLLOUT;
            rc = poll(&pol, 1, sb->writetimeout);
        } while (rc == -1 && errno == EINTR);

        if (rc <= 0)
            return rc; /*timed out or error*/
        if ((pol.revents & POLLOUT) == 0)
            return -100000 + pol.revents;
        /*can write*/
    }
    return write(sb->fd, cc, len);
}

static int swrite(SBUF2 *sb, const char *cc, int len)
{
    int rc;
#if WITH_SSL
    if (sb->ssl == NULL)
        rc = swrite_unsecure(sb, cc, len);
    else
        rc = sslio_write(sb, cc, len);
#else /* WITH_SSL */
    rc = swrite_unsecure(sb, cc, len);
#endif /* !WITH_SSL */
    return rc;
}

int SBUF2_FUNC(sbuf2unbufferedwrite)(SBUF2 *sb, const char *cc, int len)
{
    int n, ioerr;
#if !WITH_SSL
    n = write(sb->fd, cc, len);
#else
ssl_downgrade:
    if (sb->ssl == NULL)
        n = write(sb->fd, cc, len);
    else {
        ERR_clear_error();
        n = SSL_write(sb->ssl, cc, len);
        if (n <= 0) {
            ioerr = SSL_get_error(sb->ssl, n);
            switch (ioerr) {
            case SSL_ERROR_WANT_READ:
                errno = EAGAIN;
                break;
            case SSL_ERROR_WANT_WRITE:
                errno = EAGAIN;
                break;
            case SSL_ERROR_ZERO_RETURN:
                /* Peer has done a clean shutdown. */
                SSL_shutdown(sb->ssl);
                SSL_free(sb->ssl);
                sb->ssl = NULL;
                if (sb->cert) {
                    X509_free(sb->cert);
                    sb->cert = NULL;
                }
                goto ssl_downgrade;
            case SSL_ERROR_SYSCALL:
                if (n == 0)
                    errno = ECONNRESET;
                break;
            default:
                errno = EIO;
                break;
            }
        }
    }
#endif
    return n;
}

static int sread_unsecure(SBUF2 *sb, char *cc, int len)
{
    int rc;
    struct pollfd pol;
    if (sb == 0)
        return -1;
    if (sb->readtimeout > 0) {
        do {
            pol.fd = sb->fd;
            pol.events = POLLIN;
            rc = poll(&pol, 1, sb->readtimeout);
        } while (rc == -1 && errno == EINTR);

        if (rc <= 0)
            return rc; /*timed out or error*/
        if ((pol.revents & POLLIN) == 0)
            return -100000 + pol.revents;
        /*something to read*/
    }
    return read(sb->fd, cc, len);
}

static int sread(SBUF2 *sb, char *cc, int len)
{
    int rc;
#if WITH_SSL
    if (sb->ssl == NULL)
        rc = sread_unsecure(sb, cc, len);
    else
        rc = sslio_read(sb, cc, len);
#else /* WITH_SSL */
    rc = sread_unsecure(sb, cc, len);
#endif /* !WITH_SSL */
    return rc;
}

int SBUF2_FUNC(sbuf2unbufferedread)(SBUF2 *sb, char *cc, int len)
{
    int n, ioerr;
#if !WITH_SSL
    n = read(sb->fd, cc, len);
#else
ssl_downgrade:
    if (sb->ssl == NULL)
        n = read(sb->fd, cc, len);
    else {
        ERR_clear_error();
        n = SSL_read(sb->ssl, cc, len);
        if (n <= 0) {
            ioerr = SSL_get_error(sb->ssl, n);
            switch (ioerr) {
            case SSL_ERROR_WANT_READ:
                errno = EAGAIN;
                break;
            case SSL_ERROR_WANT_WRITE:
                errno = EAGAIN;
                break;
            case SSL_ERROR_ZERO_RETURN:
                /* Peer has done a clean shutdown. */
                SSL_shutdown(sb->ssl);
                SSL_free(sb->ssl);
                sb->ssl = NULL;
                if (sb->cert) {
                    X509_free(sb->cert);
                    sb->cert = NULL;
                }
                goto ssl_downgrade;
            case SSL_ERROR_SYSCALL:
                if (n == 0)
                    errno = ECONNRESET;
                break;
            default:
                errno = EIO;
                break;
            }
        }
    }
#endif
    return n;
}

void SBUF2_FUNC(sbuf2settimeout)(SBUF2 *sb, int readtimeout, int writetimeout)
{
    sb->readtimeout = readtimeout;
    sb->writetimeout = writetimeout;
}

void SBUF2_FUNC(sbuf2gettimeout)(SBUF2 *sb, int *readtimeout, int *writetimeout)
{
    *readtimeout = sb->readtimeout;
    *writetimeout = sb->writetimeout;
}

void SBUF2_FUNC(sbuf2setrw)(SBUF2 *sb, sbuf2readfn read, sbuf2writefn write)
{
    sb->read = read;
    sb->write = write;
}

void SBUF2_FUNC(sbuf2setr)(SBUF2 *sb, sbuf2readfn read)
{
    sb->read = read;
}

void SBUF2_FUNC(sbuf2setw)(SBUF2 *sb, sbuf2writefn write)
{
    sb->write = write;
}

sbuf2readfn SBUF2_FUNC(sbuf2getr)(SBUF2 *sb)
{
    return sb->read;
}

sbuf2writefn SBUF2_FUNC(sbuf2getw)(SBUF2 *sb)
{
    return sb->write;
}

int SBUF2_FUNC(sbuf2setbufsize)(SBUF2 *sb, unsigned int size)
{
    if (size < 1024)
        size = 1024;
    if (sb->rbuf)
        free(sb->rbuf);
    if (sb->wbuf)
        free(sb->wbuf);
    sb->rbuf = sb->wbuf = 0;
    sb->rhd = sb->rtl = 0;
    sb->whd = sb->wtl = 0;
    sb->lbuf = size;
    sb->rbuf = malloc(size);
    sb->wbuf = malloc(size);
    if (sb->rbuf == NULL || sb->wbuf == NULL) {
        free(sb->rbuf);
        free(sb->wbuf);
        free(sb);
        return ENOMEM;
    }
    return 0;
}

void SBUF2_FUNC(sbuf2setflags)(SBUF2 *sb, int flags)
{
    sb->flags |= flags;
}

SBUF2 *SBUF2_FUNC(sbuf2open)(int fd, int flags)
{
    if (fd < 0) {
        return NULL;
    }
    SBUF2 *sb = NULL;
#if SBUF2_SERVER
    comdb2ma alloc = comdb2ma_create(0, 0, "sbuf2", 0);
    if (alloc == NULL) {
        goto error;
    }
    /* get malloc to work in server-mode */
    SBUF2 dummy = {.allocator = alloc};
    sb = &dummy;
#endif
    sb = malloc(sizeof(SBUF2));
    if (sb == NULL) {
        goto error;
    }
    memset(sb, 0, sizeof(SBUF2));
    sb->fd = fd;
    sb->flags = flags;
#if SBUF2_SERVER
    sb->allocator = alloc;
#endif

#if SBUF2_UNGETC
    sb->ungetc_buf_len = 0;
    memset(sb->ungetc_buf, EOF, SBUF2UNGETC_BUF_MAX);
#endif
    /* default writer/reader */
    sb->write = swrite;
    sb->read = sread;
    if (sbuf2setbufsize(sb, SBUF2_DFL_SIZE) == 0) {
        return sb;
    }
error:
    if (sb) {
        free(sb);
    }
#if SBUF2_SERVER
    if (alloc) {
        comdb2ma_destroy(alloc);
    }
#endif
    return NULL;
}

char *SBUF2_FUNC(sbuf2dbgin)(SBUF2 *sb)
{
    if (sb->dbgin != 0)
        return sb->dbgin;
    return "";
}

char *SBUF2_FUNC(sbuf2dbgout)(SBUF2 *sb)
{
    if (sb->dbgout != 0)
        return sb->dbgout;
    return "";
}

#if SBUF2_UNGETC
int SBUF2_FUNC(sbuf2eof)(SBUF2 *sb)
{
    int i;

    if (sb == NULL)
        return -2;

    errno = 0;
    i = sbuf2getc(sb);

    if (i >= 0) {
        sbuf2ungetc(i, sb);
        return 0;
    } else {
        if (errno == 0)
            return 1;
        else
            return -1;
    }
}
#endif

void SBUF2_FUNC(sbuf2setuserptr)(SBUF2 *sb, void *userptr)
{
    sb->userptr = userptr;
}

void *SBUF2_FUNC(sbuf2getuserptr)(SBUF2 *sb)
{
    return sb->userptr;
}

#if SBUF2_SERVER
#include <lockmacro.h> /* LOCK & UNLOCK */
#include <plhash.h>    /* hash_t */
#include <logmsg.h>    /* logmsg */
#define logi(...) logmsg(LOGMSG_INFO, ##__VA_ARGS__)
#define loge(...) logmsg(LOGMSG_ERROR, ##__VA_ARGS__)
static pthread_mutex_t peer_lk = PTHREAD_MUTEX_INITIALIZER;
static hash_t *peer_hash = NULL;
struct peer_info {
    /* Key (must come first in struct) */
    struct in_addr addr;
    short family;
    /* (don't forget to bzero the key area as the member fields may not
     * be aligned) */

    /* Data */
    char *host;
};
#else
/* Keep quiet in client mode. */
#define logi(...)
#define loge(...)
/* Don't cache host info in client mode. */
#ifdef LOCK
#undef LOCK
#endif /* LOCK */
#define LOCK(arg)
#ifdef UNLOCK
#undef UNLOCK
#endif /* UNLOCK */
#define UNLOCK(arg)
#endif /* SBUF2_SERVER */

char *SBUF2_FUNC(get_origin_mach_by_buf)(SBUF2 *sb)
{
    int fd;
    char *funcname;
    struct sockaddr_in peeraddr;
    socklen_t len = sizeof(peeraddr);
    char *host;
#if SBUF2_SERVER
    struct peer_info *info;
    struct peer_info key;
#else
    void *info = NULL;
#endif

    if (sb == NULL)
        return "???";

    fd = sb->fd;

    if (fd == -1)
        return "???";

    bzero(&peeraddr, sizeof(peeraddr));
    if (getpeername(fd, (struct sockaddr *)&peeraddr, &len) < 0) {
        loge("%s:getpeername failed fd %d: %d %s\n", __func__, fd, errno,
             strerror(errno));
        return "???";
    }

#if SBUF2_SERVER
    bzero(&key, offsetof(struct peer_info, host));
    memcpy(&key.addr, &peeraddr.sin_addr, sizeof(key.addr));
    key.family = peeraddr.sin_family;
#endif

    LOCK(&peer_lk)
    {
#if SBUF2_SERVER
        if (!peer_hash) {
            peer_hash = hash_init(offsetof(struct peer_info, host));
            if (!peer_hash) {
                loge("%s:hash_init failed\n", __func__);
                errUNLOCK(&peer_lk);
                return "???";
            }
        }

        info = hash_find(peer_hash, &key);
#endif
        if (!info) {
            /* Do a slow lookup of this internet address in the host database
             * to get a hostname, and then search the bigsnd node list to
             * map this to a node number. */
            struct hostent *hp = NULL, rslt;
            char hnm[256] = {0};
            char *h_name = NULL;
            int node, rc;
            int error_num = 0;
            int goodrc = 0;

#ifdef _LINUX_SOURCE
            funcname = "getnameinfo";
            rc = getnameinfo((struct sockaddr *)&peeraddr, sizeof(peeraddr),
                             hnm, sizeof(hnm), NULL, 0, 0);

            if (0 == rc) {
                goodrc = 1;
                h_name = hnm;
            } else {
                error_num = errno;
            }
#else
            funcname = "getipnodebyaddr";
            hp = getipnodebyaddr(&peeraddr.sin_addr, sizeof(peeraddr.sin_addr),
                                 peeraddr.sin_family, &error_num);
            if (hp) {
                goodrc = 1;
                h_name = hp->h_name;
            }
#endif

            if (0 == goodrc) {
                char addrstr[64] = "";
                inet_ntop(peeraddr.sin_family, &peeraddr.sin_addr, addrstr,
                          sizeof(addrstr));
                loge("%s:%s failed fd %d (%s): error_num %d", __func__,
                     funcname, fd, addrstr, error_num);
                switch (error_num) {
                case HOST_NOT_FOUND:
                    loge(" HOST_NOT_FOUND\n");
                    break;
                case NO_DATA:
                    loge(" NO_DATA\n");
                    break;
                case NO_RECOVERY:
                    loge(" NO_RECOVERY\n");
                    break;
                case TRY_AGAIN:
                    loge(" TRY_AGAIN\n");
                    break;
                default:
                    loge(" ???\n");
                    break;
                }
                host = strdup(addrstr);
            } else {
                host = strdup(h_name);
            }

#if SBUF2_SERVER
            info = calloc(1, sizeof(struct peer_info));
            if (!info) {
                errUNLOCK(&peer_lk);
                loge("%s: out of memory\n", __func__);
                free(host);
                host = NULL;
                return "???";
            }

            memcpy(info, &key, sizeof(key));
            info->host = host;
            if (hash_add(peer_hash, info) != 0) {
                errUNLOCK(&peer_lk);
                loge("%s: hash_add failed\n", __func__);
                free(info);
                return host;
            }
#endif
        }
    }
    UNLOCK(&peer_lk);

#if SBUF2_SERVER
    return (info != NULL) ? info->host : "???";
#else
    return (host != NULL) ? host : "???";
#endif
}

void SBUF2_FUNC(cleanup_peer_hash)()
{
#if SBUF2_SERVER
    if (peer_hash) {
        hash_clear(peer_hash);
        hash_free(peer_hash);
        peer_hash = NULL;
    }
#endif
}

#if WITH_SSL
#  ifdef my_ssl_println
#    undef my_ssl_println
#  endif
#  ifdef my_ssl_eprintln
#    undef my_ssl_eprintln
#  endif
#  define my_ssl_println(fmt, ...)    \
      ssl_println("SBUF2", fmt, ##__VA_ARGS__)
#  define my_ssl_eprintln(fmt, ...)   \
      ssl_eprintln("SBUF2", "%s: " fmt, __func__, ##__VA_ARGS__)
#  include "ssl_io.c"
#endif
