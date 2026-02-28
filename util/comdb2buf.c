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

#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <poll.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/uio.h>
#include <unistd.h>

#include <hostname_support.h>
#include <comdb2buf.h>
#if SBUF2_SERVER
#include <sys_wrap.h>
#endif

#if SBUF2_SERVER
#  ifndef CDB2BUF_DFL_SIZE
#    define CDB2BUF_DFL_SIZE 1024ULL
#  endif
#  ifdef PER_THREAD_MALLOC
#    include "mem_util.h"
#    define calloc comdb2_calloc_util
#    define malloc(size) comdb2_malloc(sb->allocator, size)
#    define free comdb2_free
#  endif
#else /* SBUF2_SERVER */
#  ifndef CDB2BUF_DFL_SIZE
#    define CDB2BUF_DFL_SIZE (1024ULL * 128ULL)
#  endif
#endif /* !SBUF2_SERVER */

#if CDB2BUF_UNGETC
#  define CDB2BUF_UNGETC_BUF_MAX 8 /* see also net/net_evbuffer.c */
#endif

#ifdef my_ssl_println
#undef my_ssl_println
#endif
#ifdef my_ssl_eprintln
#undef my_ssl_eprintln
#endif
#define my_ssl_println(fmt, ...) ssl_println("COMDB2BUF", fmt, ##__VA_ARGS__)
#define my_ssl_eprintln(fmt, ...)                                              \
    ssl_eprintln("COMDB2BUF", "%s: " fmt, __func__, ##__VA_ARGS__)

struct comdb2buf {
    int fd;
    int flags;

    int readtimeout;
    int writetimeout;
    int nowait;

    int rhd, rtl;
    int whd, wtl;

#if CDB2BUF_UNGETC
    /* Server always has these. */
    int ungetc_buf[CDB2BUF_UNGETC_BUF_MAX];
    int ungetc_buf_len;
#endif

    cdb2buf_writefn write;
    cdb2buf_readfn read;

    unsigned int lbuf;
    unsigned char *rbuf;
    unsigned char *wbuf;

    char *dbgout, *dbgin;

    void *userptr;

#if SBUF2_SERVER
#   ifdef PER_THREAD_MALLOC
    comdb2ma allocator;
#   endif
    struct sqlclntstate *clnt;
#endif

    /* Server always supports SSL. */
    SSL *ssl;
    X509 *cert;
    int protocolerr;
    char sslerr[SSL_ERRSTR_LEN];
};

int CDB2BUF_FUNC(cdb2buf_fileno)(COMDB2BUF *sb)
{
    if (sb == NULL)
        return -1;
    return sb->fd;
}

/*just free COMDB2BUF.  don't flush or close fd*/
int CDB2BUF_FUNC(cdb2buf_free)(COMDB2BUF *sb)
{
    if (sb == 0)
        return -1;

    int rc = 0;
    /* Gracefully shutdown SSL to make the
       fd re-usable. Close the fd if it fails. */
    if (!(sb->flags & CDB2BUF_NO_SSL_CLOSE)) {
        rc = sslio_close(sb, 1);
        if (rc) {
#if SBUF2_SERVER
            Close(sb->fd);
#else
            close(sb->fd);
#endif
        }
    }

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
#if SBUF2_SERVER && defined(PER_THREAD_MALLOC)
    comdb2ma alloc = sb->allocator;
#endif
    free(sb);
#if SBUF2_SERVER && defined(PER_THREAD_MALLOC)
    comdb2ma_destroy(alloc);
#endif
    return rc;
}

/* flush output, close fd, and free COMDB2BUF.*/
int CDB2BUF_FUNC(cdb2buf_close)(COMDB2BUF *sb)
{
    if (sb == 0)
        return -1;
    if (sb->fd < 0)
        return -1;

    if (!(sb->flags & CDB2BUF_NO_FLUSH))
        cdb2buf_flush(sb);

    /* We need to send "close notify" alert
       before closing the underlying fd. */
    if (!(sb->flags & CDB2BUF_NO_SSL_CLOSE))
        sslio_close(sb, (sb->flags & CDB2BUF_NO_CLOSE_FD));

    if (!(sb->flags & CDB2BUF_NO_CLOSE_FD)) {
#if SBUF2_SERVER
        Close(sb->fd);
#else
        close(sb->fd);
#endif
    }

    return cdb2buf_free(sb);
}

/* flush output */
int CDB2BUF_FUNC(cdb2buf_flush)(COMDB2BUF *sb)
{
    int cnt = 0, rc, len;

    if (sb == 0)
        return -1;
    while (sb->whd != sb->wtl) {
        if (sb->wtl > sb->whd) {
            len = sb->lbuf - sb->wtl;
        } else {
            len = sb->whd - sb->wtl;
        }

#if SBUF2_SERVER
        void *ssl;
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

int CDB2BUF_FUNC(cdb2buf_putc)(COMDB2BUF *sb, char c)
{
    int rc;
    if (sb == 0)
        return -1;

    if (sb->wbuf == NULL) {
        /* lazily establish write buffer */
        sb->wbuf = malloc(sb->lbuf);
        if (sb->wbuf == NULL)
            return -1;
    }

    if ((sb->whd == sb->lbuf - 1 && sb->wtl == 0) || (sb->whd == sb->wtl - 1)) {
        rc = cdb2buf_flush(sb);
        if (rc < 0)
            return rc;
    }
    sb->wbuf[sb->whd] = c;
    sb->whd++;
    if (sb->whd >= sb->lbuf)
        sb->whd = 0;
    if ((sb->flags & CDB2BUF_WRITE_LINE) && c == '\n') {
        rc = cdb2buf_flush(sb);
        if (rc < 0)
            return rc;
    }
    return 1;
}

int CDB2BUF_FUNC(cdb2buf_puts)(COMDB2BUF *sb, char *string)
{
    int rc, ii;
    if (sb == 0)
        return -1;
    for (ii = 0; string[ii]; ii++) {
        rc = cdb2buf_putc(sb, string[ii]);
        if (rc < 0)
            return rc;
    }
    if (sb->flags & CDB2BUF_DEBUG_LAST_LINE) {
        if (sb->dbgout)
            free(sb->dbgout);
        sb->dbgout = strdup(string);
    }
    return ii;
}

/* returns num items written || <0 for error*/
int CDB2BUF_FUNC(cdb2buf_write)(char *ptr, int nbytes, COMDB2BUF *sb)
{
    int rc, off, left, written = 0;
    if (sb == 0)
        return -1;
    if (sb->wbuf == NULL) {
        /* lazily establish write buffer */
        sb->wbuf = malloc(sb->lbuf);
        if (sb->wbuf == NULL)
            return -1;
    }
    off = 0;
    left = nbytes;
    while (left > 0) {
        int towrite = 0;

        if ((sb->whd == sb->lbuf - 1 && sb->wtl == 0) ||
            (sb->whd == sb->wtl - 1)) {
            rc = cdb2buf_flush(sb);
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
int CDB2BUF_FUNC(cdb2buf_fwrite)(char *ptr, int size, int nitems, COMDB2BUF *sb)
{
    int rc, ii, jj, off;
    if (sb == 0)
        return -1;
    off = 0;
    if (!(sb->flags & CDB2BUF_WRITE_LINE))
        cdb2buf_write(ptr, size * nitems, sb);
    else {
        for (ii = 0; ii < nitems; ii++) {
            for (jj = 0; jj < size; jj++) {
                rc = cdb2buf_putc(sb, ptr[off++]);
                if (rc < 0)
                    return ii;
            }
        }
    }
    return nitems;
}

int CDB2BUF_FUNC(cdb2buf_getc)(COMDB2BUF *sb)
{
    int rc, cc;
    if (sb == 0)
        return -1;

    if (sb->rbuf == NULL) {
        /* lazily establish read buffer */
        sb->rbuf = malloc(sb->lbuf);
        if (sb->rbuf == NULL)
            return -1;
    }
#if CDB2BUF_UNGETC
    if (sb->ungetc_buf_len > 0) {
        sb->ungetc_buf_len--;
        return sb->ungetc_buf[sb->ungetc_buf_len];
    }
#endif

    if (sb->rtl == sb->rhd) {
        /*nothing buffered*/
        sb->rtl = 0;
        sb->rhd = 0;
#if SBUF2_SERVER
        void *ssl;
ssl_downgrade:
        ssl = sb->ssl;
        rc = sb->read(sb, (char *)sb->rbuf, sb->lbuf - 1);
        if (rc == 0 && sb->ssl != ssl) {
            goto ssl_downgrade;
        }
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

#if CDB2BUF_UNGETC
int CDB2BUF_FUNC(cdb2buf_ungetc)(char c, COMDB2BUF *sb)
{
    int i;
    if (sb == NULL)
        return -1;

    i = c;
    if (i == EOF || (sb->ungetc_buf_len == CDB2BUF_UNGETC_BUF_MAX))
        return EOF;

    sb->ungetc_buf[sb->ungetc_buf_len] = c;
    sb->ungetc_buf_len++;
    return c;
}
#endif

/*return null terminated string and len (or <0 if error)*/
int CDB2BUF_FUNC(cdb2buf_gets)(char *out, int lout, COMDB2BUF *sb)
{
    int cc, ii;
    if (sb == 0)
        return -1;
    lout--;
    for (ii = 0; ii < lout;) {
        cc = cdb2buf_getc(sb);
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
    if (sb->flags & CDB2BUF_DEBUG_LAST_LINE) {
        if (sb->dbgin)
            free(sb->dbgin);
        sb->dbgin = strdup(out);
    }
    return ii; /*return string len*/
}

int CDB2BUF_FUNC(cdb2buf_rd_pending)(COMDB2BUF *sb)
{
    if (!sb || !sb->ssl)
        return 0;
    return sslio_pending(sb);
}

/* returns num items read || <0 for error*/
static int cdb2buf_fread_int(char *ptr, int size, int nitems,
                          COMDB2BUF *sb, int *was_timeout)
{
    int need = size * nitems;
    int done = 0;

    if (sb == 0)
        return -1;

    if (sb->rbuf == NULL) {
        /* lazily establish read buffer */
        sb->rbuf = malloc(sb->lbuf);
        if (sb->rbuf == NULL)
            return -1;
    }

#if CDB2BUF_UNGETC
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
#if SBUF2_SERVER
            void *ssl;
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
int CDB2BUF_FUNC(cdb2buf_fread)(char *ptr, int size, int nitems, COMDB2BUF *sb)
{
    return cdb2buf_fread_int(ptr, size, nitems, sb, NULL);
}

/* returns num items read || <0 for error*/
int CDB2BUF_FUNC(cdb2buf_fread_timeout)(char *ptr, int size, int nitems, COMDB2BUF *sb,
                                   int *was_timeout)
{
    return cdb2buf_fread_int(ptr, size, nitems, sb, was_timeout);
}

int CDB2BUF_FUNC(cdb2buf_printf)(COMDB2BUF *sb, const char *fmt, ...)
{
    /*just do sprintf to local buf (limited to 1k),
      and then emit through comdb2buf*/
    char lbuf[1024];
    va_list ap;
    if (sb == 0)
        return -1;
    va_start(ap, fmt);
    vsnprintf(lbuf, sizeof(lbuf), fmt, ap);
    va_end(ap);
    return cdb2buf_puts(sb, lbuf);
}

int CDB2BUF_FUNC(cdb2buf_printfx)(COMDB2BUF *sb, char *buf, int lbuf, char *fmt, ...)
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
    return cdb2buf_puts(sb, buf);
}

/* default read/write functions for sbuf, which implement timeouts and
 * retry on EINTR. */
static int swrite_unsecure(COMDB2BUF *sb, const char *cc, int len)
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
#if 0
    char buf[100] = {0};
    memcpy(buf, cc, (len < 99) ? len : 99);
    printf("%s:%d writing data of size %d '%s'\n", __func__, __LINE__, len, buf);
#endif
    return write(sb->fd, cc, len);
}

static int swrite(COMDB2BUF *sb, const char *cc, int len)
{
    int rc;
    if (sb->ssl == NULL)
        rc = swrite_unsecure(sb, cc, len);
    else
        rc = sslio_write(sb, cc, len);
    return rc;
}

int CDB2BUF_FUNC(cdb2buf_unbufferedwrite)(COMDB2BUF *sb, const char *cc, int len)
{
    int n;
ssl_downgrade:
    if (sb->ssl == NULL)
        n = write(sb->fd, cc, len);
    else {
        ERR_clear_error();
        n = SSL_write(sb->ssl, cc, len);
        if (n <= 0) {
            int ioerr = SSL_get_error(sb->ssl, n);
            switch (ioerr) {
            case SSL_ERROR_WANT_READ:
                sb->protocolerr = 0;
                errno = EAGAIN;
                break;
            case SSL_ERROR_WANT_WRITE:
                sb->protocolerr = 0;
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
                sb->protocolerr = 0;
                if (n == 0) {
                    ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr),
                                 my_ssl_eprintln, "Unexpected EOF observed.");
                    errno = ECONNRESET;
                } else {
                    ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr),
                                 my_ssl_eprintln, "IO error. errno %d.", errno);
                }
                break;
            case SSL_ERROR_SSL:
                errno = EIO;
                sb->protocolerr = 1;
                ssl_sfliberrprint(sb->sslerr, sizeof(sb->sslerr),
                                  my_ssl_eprintln,
                                  "A failure in SSL library occured");
                break;
            default:
                errno = EIO;
                sb->protocolerr = 1;
                ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                             "Failed to establish connection with peer. "
                             "SSL error = %d.",
                             ioerr);
                break;
            }
        }
    }
    return n;
}

static int sread_unsecure(COMDB2BUF *sb, char *cc, int len)
{
    int rc;
    struct pollfd pol;
    if (sb == 0)
        return -1;
    if (sb->nowait || sb->readtimeout > 0) {
        do {
            pol.fd = sb->fd;
            pol.events = POLLIN;
            rc = poll(&pol, 1, sb->nowait ? 0 : sb->readtimeout);
        } while (rc == -1 && errno == EINTR);

        if (rc <= 0)
            return rc; /*timed out or error*/
        if ((pol.revents & POLLIN) == 0)
            return -100000 + pol.revents;
        /*something to read*/
    }
    return read(sb->fd, cc, len);
}

static int sread(COMDB2BUF *sb, char *cc, int len)
{
    int rc;
    if (sb->ssl == NULL)
        rc = sread_unsecure(sb, cc, len);
    else
        rc = sslio_read(sb, cc, len);
    return rc;
}

int CDB2BUF_FUNC(cdb2buf_unbufferedread)(COMDB2BUF *sb, char *cc, int len)
{
    int n;
ssl_downgrade:
    if (sb->ssl == NULL)
        n = read(sb->fd, cc, len);
    else {
        ERR_clear_error();
        n = SSL_read(sb->ssl, cc, len);
        if (n <= 0) {
            int ioerr = SSL_get_error(sb->ssl, n);
            switch (ioerr) {
            case SSL_ERROR_WANT_READ:
                sb->protocolerr = 0;
                errno = EAGAIN;
                break;
            case SSL_ERROR_WANT_WRITE:
                sb->protocolerr = 0;
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
                sb->protocolerr = 0;
                if (n == 0) {
                    ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr),
                                 my_ssl_eprintln, "Unexpected EOF observed.");
                    errno = ECONNRESET;
                } else {
                    ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr),
                                 my_ssl_eprintln, "IO error. errno %d.", errno);
                }
                break;
            case SSL_ERROR_SSL:
                errno = EIO;
                sb->protocolerr = 1;
                ssl_sfliberrprint(sb->sslerr, sizeof(sb->sslerr),
                                  my_ssl_eprintln,
                                  "A failure in SSL library occured");
                break;
            default:
                errno = EIO;
                sb->protocolerr = 1;
                ssl_sfeprint(sb->sslerr, sizeof(sb->sslerr), my_ssl_eprintln,
                             "Failed to establish connection with peer. "
                             "SSL error = %d.",
                             ioerr);
                break;
            }
        }
    }
    return n;
}

void CDB2BUF_FUNC(cdb2buf_setnowait)(COMDB2BUF *sb, int value)
{
    sb->nowait = value;
}

void CDB2BUF_FUNC(cdb2buf_settimeout)(COMDB2BUF *sb, int readtimeout, int writetimeout)
{
    sb->readtimeout = readtimeout;
    sb->writetimeout = writetimeout;
    sb->nowait = 0;
}

void CDB2BUF_FUNC(cdb2buf_gettimeout)(COMDB2BUF *sb, int *readtimeout, int *writetimeout)
{
    *readtimeout = sb->readtimeout;
    *writetimeout = sb->writetimeout;
}

void CDB2BUF_FUNC(cdb2buf_setrw)(COMDB2BUF *sb, cdb2buf_readfn read, cdb2buf_writefn write)
{
    sb->read = read;
    sb->write = write;
}

void CDB2BUF_FUNC(cdb2buf_setr)(COMDB2BUF *sb, cdb2buf_readfn read)
{
    sb->read = read;
}

void CDB2BUF_FUNC(cdb2buf_setw)(COMDB2BUF *sb, cdb2buf_writefn write)
{
    sb->write = write;
}

cdb2buf_readfn CDB2BUF_FUNC(cdb2buf_getr)(COMDB2BUF *sb)
{
    return sb->read;
}

cdb2buf_writefn CDB2BUF_FUNC(cdb2buf_getw)(COMDB2BUF *sb)
{
    return sb->write;
}

int CDB2BUF_FUNC(cdb2buf_setbufsize)(COMDB2BUF *sb, unsigned int size)
{
    if (size < 1024)
        size = 1024;
    free(sb->rbuf);
    free(sb->wbuf);
    sb->rbuf = sb->wbuf = 0;
    sb->rhd = sb->rtl = 0;
    sb->whd = sb->wtl = 0;
    sb->lbuf = size;
    return 0;
}

void CDB2BUF_FUNC(cdb2buf_setflags)(COMDB2BUF *sb, int flags)
{
    sb->flags |= flags;
}

void CDB2BUF_FUNC(cdb2buf_setisreadonly)(COMDB2BUF *sb)
{
    sb->flags |= CDB2BUF_IS_READONLY;
}

int CDB2BUF_FUNC(cdb2buf_getisreadonly)(COMDB2BUF *sb)
{
    return (sb->flags & CDB2BUF_IS_READONLY) ? 1 : 0;
}

COMDB2BUF *CDB2BUF_FUNC(cdb2buf_open)(int fd, int flags)
{
    if (fd < 0) {
        return NULL;
    }
    COMDB2BUF *sb = NULL;
#if SBUF2_SERVER && defined(PER_THREAD_MALLOC)
    comdb2ma alloc = comdb2ma_create(0, 0, "sbuf2", 0);
    if (alloc == NULL) {
        goto error;
    }
    /* get malloc to work in server-mode */
    COMDB2BUF dummy = {.allocator = alloc};
    sb = &dummy;
#endif
    sb = calloc(1, sizeof(COMDB2BUF));
    if (sb == NULL) {
        goto error;
    }
    sb->fd = fd;
    sb->flags = flags;
#if SBUF2_SERVER
#   ifdef PER_THREAD_MALLOC
    sb->allocator = alloc;
#   endif
    sb->clnt = NULL;
#endif

#if CDB2BUF_UNGETC
    sb->ungetc_buf_len = 0;
    memset(sb->ungetc_buf, EOF, sizeof(sb->ungetc_buf));
#endif
    /* default writer/reader */
    sb->write = swrite;
    sb->read = sread;
    if (cdb2buf_setbufsize(sb, CDB2BUF_DFL_SIZE) == 0) {
        return sb;
    }
error:
    if (sb) {
        free(sb);
    }
#if SBUF2_SERVER && defined(PER_THREAD_MALLOC)
    if (alloc) {
        comdb2ma_destroy(alloc);
    }
#endif
    return NULL;
}

char *CDB2BUF_FUNC(cdb2buf_dbgin)(COMDB2BUF *sb)
{
    if (sb->dbgin != 0)
        return sb->dbgin;
    return "";
}

char *CDB2BUF_FUNC(cdb2buf_dbgout)(COMDB2BUF *sb)
{
    if (sb->dbgout != 0)
        return sb->dbgout;
    return "";
}

#if CDB2BUF_UNGETC
int CDB2BUF_FUNC(cdb2buf_eof)(COMDB2BUF *sb)
{
    int i;

    if (sb == NULL)
        return -2;

    errno = 0;
    i = cdb2buf_getc(sb);

    if (i >= 0) {
        cdb2buf_ungetc(i, sb);
        return 0;
    } else {
        if (errno == 0)
            return 1;
        else
            return -1;
    }
}
#endif

#if SBUF2_SERVER
void CDB2BUF_FUNC(cdb2buf_setclnt)(COMDB2BUF *sb, struct sqlclntstate *clnt)
{
    sb->clnt = clnt;
}

struct sqlclntstate *CDB2BUF_FUNC(cdb2buf_getclnt)(COMDB2BUF *sb)
{
    return sb->clnt;
}
#endif

void CDB2BUF_FUNC(cdb2buf_setuserptr)(COMDB2BUF *sb, void *userptr)
{
    sb->userptr = userptr;
}

void *CDB2BUF_FUNC(cdb2buf_getuserptr)(COMDB2BUF *sb)
{
    return sb->userptr;
}

void CDB2BUF_FUNC(cdb2buf_nextline)(COMDB2BUF *sb)
{
    char c;
    while ((c = cdb2buf_getc(sb)) >= 0 && c != '\n')
        ;
}

char *CDB2BUF_FUNC(get_origin_mach_by_buf)(COMDB2BUF *sb)
{
    if (sb == NULL || sb->fd == -1) {
        return NULL;
    }
    return get_hostname_by_fileno(sb->fd);
}

int CDB2BUF_FUNC(cdb2buf_lasterror)(COMDB2BUF *sb, char *err, size_t n)
{
    if (err != NULL)
        strncpy(err, sb->sslerr,
                n > sizeof(sb->sslerr) ? sizeof(sb->sslerr) : n);
    return sb->protocolerr;
}

#include "ssl_io.c"
