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

#ifndef INCLUDED_SBUF2
#define INCLUDED_SBUF2

#include <stddef.h> /* for size_t */

/* comdb2buf.h -  simple buffering for stream. stupid fopen can't handle fd>255 */

/* Server comdb2buf uses dlmalloc. Client does not. The simplest approach
   to avoid adding dlmalloc dependency to client API is to compile
   server and client version separately. */
#ifndef SBUF2_SERVER
#  define SBUF2_SERVER 1
#  ifndef CDB2BUF_UNGETC
#    define CDB2BUF_UNGETC 1
#  endif
#endif

/* Name mangling */
#ifndef CDB2BUF_FUNC
#  if SBUF2_SERVER
#    define CDB2BUF_FUNC(func) SERVER_ ## func
#  else
#    define CDB2BUF_FUNC(func) CLIENT_ ## func
#  endif
#endif

#if defined __cplusplus
extern "C" {
#endif

typedef struct comdb2buf COMDB2BUF;

enum CDB2BUF_FLAGS {
    /* FLUSH OUTPUT ON \n */
    CDB2BUF_WRITE_LINE = 1,
    /*STORE LAST LINE IN/OUT IN DEBUG BUFFER*/
    CDB2BUF_DEBUG_LAST_LINE = 2,
    /* cdb2buf_close() will not close the underlying fd */
    CDB2BUF_NO_CLOSE_FD = 4,
    /* cdb2buf_close() will not flush the underlying fd */
    CDB2BUF_NO_FLUSH = 8,
    /* adjust read/write calls to write on non-blocking socket */
    CDB2BUF_NO_BLOCK = 16,
    /* the underlying connection has been marked 'READONLY' */
    CDB2BUF_IS_READONLY = 32,
    /* do not close ssl */
    CDB2BUF_NO_SSL_CLOSE = 64,
};

typedef int (*cdb2buf_readfn)(COMDB2BUF *sb, char *buf, int nbytes);
typedef int (*cdb2buf_writefn)(COMDB2BUF *sb, const char *buf, int nbytes);

/* retrieve underlying fd */
int CDB2BUF_FUNC(cdb2buf_fileno)(COMDB2BUF *sb);
#define cdb2buf_fileno CDB2BUF_FUNC(cdb2buf_fileno)

/* set flags on an COMDB2BUF after opening */
void CDB2BUF_FUNC(cdb2buf_setflags)(COMDB2BUF *sb, int flags);
#define cdb2buf_setflags CDB2BUF_FUNC(cdb2buf_setflags)

void CDB2BUF_FUNC(cdb2buf_setisreadonly)(COMDB2BUF *sb);
#define cdb2buf_setisreadonly CDB2BUF_FUNC(cdb2buf_setisreadonly)

int CDB2BUF_FUNC(cdb2buf_getisreadonly)(COMDB2BUF *sb);
#define cdb2buf_getisreadonly CDB2BUF_FUNC(cdb2buf_getisreadonly)

/* open COMDB2BUF for file descriptor.  returns COMDB2BUF handle or 0 if error.*/
COMDB2BUF *CDB2BUF_FUNC(cdb2buf_open)(int fd, int flags);
#define cdb2buf_open CDB2BUF_FUNC(cdb2buf_open)

/* flush output, close fd, and free COMDB2BUF. 0==success */
int CDB2BUF_FUNC(cdb2buf_close)(COMDB2BUF *sb);
#define cdb2buf_close CDB2BUF_FUNC(cdb2buf_close)

/* flush output, close fd, and free COMDB2BUF.*/
int CDB2BUF_FUNC(cdb2buf_free)(COMDB2BUF *sb);
#define cdb2buf_free CDB2BUF_FUNC(cdb2buf_free)

/* flush output.  returns # of bytes written or <0 for error */
int CDB2BUF_FUNC(cdb2buf_flush)(COMDB2BUF *sb);
#define cdb2buf_flush CDB2BUF_FUNC(cdb2buf_flush)

/* sets buffer size. this does NOT flush for you. if you
   change the buffer size, it will lose any buffered writes.
   default buffer size is 1K */
int CDB2BUF_FUNC(cdb2buf_setbufsize)(COMDB2BUF *sb, unsigned int size);
#define cdb2buf_setbufsize CDB2BUF_FUNC(cdb2buf_setbufsize)

/* put character. returns # of bytes written (always 1) or <0 for err */
int CDB2BUF_FUNC(cdb2buf_putc)(COMDB2BUF *sb, char c);
#define cdb2buf_putc CDB2BUF_FUNC(cdb2buf_putc)

/* put \0 terminated string. returns # of bytes written or <0 for err */
int CDB2BUF_FUNC(cdb2buf_puts)(COMDB2BUF *sb, char *string);
#define cdb2buf_puts CDB2BUF_FUNC(cdb2buf_puts)

/* write to COMDB2BUF. returns number of bytes written or <0 for error */
int CDB2BUF_FUNC(cdb2buf_write)(char *ptr, int nbytes, COMDB2BUF *sb);
#define cdb2buf_write CDB2BUF_FUNC(cdb2buf_write)

/* fwrite to COMDB2BUF. returns # of items written or <0 for error */
int CDB2BUF_FUNC(cdb2buf_fwrite)(char *ptr, int size, int nitems, COMDB2BUF *sb);
#define cdb2buf_fwrite CDB2BUF_FUNC(cdb2buf_fwrite)

/* get character. returns character read */
int CDB2BUF_FUNC(cdb2buf_getc)(COMDB2BUF *sb);
#define cdb2buf_getc CDB2BUF_FUNC(cdb2buf_getc)

/* returns string len (not including \0) and string. <0 means err*/
int CDB2BUF_FUNC(cdb2buf_gets)(char *out, int lout, COMDB2BUF *sb);
#define cdb2buf_gets CDB2BUF_FUNC(cdb2buf_gets)

int CDB2BUF_FUNC(cdb2buf_rd_pending)(COMDB2BUF *sb);
#define cdb2buf_rd_pending CDB2BUF_FUNC(cdb2buf_rd_pending)

/* fread from COMDB2BUF. returns # of items read or <0 for error */
int CDB2BUF_FUNC(cdb2buf_fread)(char *ptr, int size, int nitems, COMDB2BUF *sb);
#define cdb2buf_fread CDB2BUF_FUNC(cdb2buf_fread)

/* returns # of bytes written or <0 for err*/
int CDB2BUF_FUNC(cdb2buf_printf)(COMDB2BUF *sb, const char *fmt, ...);
#define cdb2buf_printf CDB2BUF_FUNC(cdb2buf_printf)

/* returns # of bytes written or <0 for err*/
int CDB2BUF_FUNC(cdb2buf_printfx)(COMDB2BUF *sb, char *buf, int lbuf, char *fmt, ...);
#define cdb2buf_printfx CDB2BUF_FUNC(cdb2buf_printfx)

/* set custom read/write routines */
void CDB2BUF_FUNC(cdb2buf_setrw)(COMDB2BUF *sb, cdb2buf_readfn read, cdb2buf_writefn write);
#define cdb2buf_setrw CDB2BUF_FUNC(cdb2buf_setrw)
void CDB2BUF_FUNC(cdb2buf_setr)(COMDB2BUF *sb, cdb2buf_readfn read);
#define cdb2buf_setr CDB2BUF_FUNC(cdb2buf_setr)
void CDB2BUF_FUNC(cdb2buf_setw)(COMDB2BUF *sb, cdb2buf_writefn write);
#define cdb2buf_setw CDB2BUF_FUNC(cdb2buf_setw)
cdb2buf_readfn CDB2BUF_FUNC(cdb2buf_getr)(COMDB2BUF *sb);
#define cdb2buf_getr CDB2BUF_FUNC(cdb2buf_getr)
cdb2buf_writefn CDB2BUF_FUNC(cdb2buf_getw)(COMDB2BUF *sb);
#define cdb2buf_getw CDB2BUF_FUNC(cdb2buf_getw)

/* Set no wait value */
void CDB2BUF_FUNC(cdb2buf_setnowait)(COMDB2BUF *sb, int value);
#define cdb2buf_setnowait CDB2BUF_FUNC(cdb2buf_setnowait)

/* set up poll timeout on file descriptor*/
void CDB2BUF_FUNC(cdb2buf_settimeout)(COMDB2BUF *sb, int readtimeout, int writetimeout);
#define cdb2buf_settimeout CDB2BUF_FUNC(cdb2buf_settimeout)

void CDB2BUF_FUNC(cdb2buf_gettimeout)(COMDB2BUF *sb,
                                 int *readtimeout, int *writetimeout);
#define cdb2buf_gettimeout CDB2BUF_FUNC(cdb2buf_gettimeout)

int CDB2BUF_FUNC(cdb2buf_fread_timeout)(char *ptr, int size, int nitems, COMDB2BUF *sb,
                                   int *was_timeout);
#define cdb2buf_fread_timeout CDB2BUF_FUNC(cdb2buf_fread_timeout)

/* return last line read*/
char *CDB2BUF_FUNC(cdb2buf_dbgin)(COMDB2BUF *sb);
#define cdb2buf_dbgin CDB2BUF_FUNC(cdb2buf_dbgin)

/* return last line written*/
char *CDB2BUF_FUNC(cdb2buf_dbgout)(COMDB2BUF *sb);
#define cdb2buf_dbgout CDB2BUF_FUNC(cdb2buf_dbgout)

#if SBUF2_SERVER
struct sqlclntstate;

/* set the sqlclntstate pointer associated with this sbuf */
void CDB2BUF_FUNC(cdb2buf_setclnt)(COMDB2BUF *sb, struct sqlclntstate *clnt);
#define cdb2buf_setclnt CDB2BUF_FUNC(cdb2buf_setclnt)

/* get the sqlclntstate pointer associated with this sbuf */
struct sqlclntstate *CDB2BUF_FUNC(cdb2buf_getclnt)(COMDB2BUF *sb);
#define cdb2buf_getclnt CDB2BUF_FUNC(cdb2buf_getclnt)
#endif

/* set the userptr associated with this sbuf - use this for whatever your
 * application desires. */
void CDB2BUF_FUNC(cdb2buf_setuserptr)(COMDB2BUF *sb, void *userptr);
#define cdb2buf_setuserptr CDB2BUF_FUNC(cdb2buf_setuserptr)

/* get the user pointer associated with this sbuf */
void *CDB2BUF_FUNC(cdb2buf_getuserptr)(COMDB2BUF *sb);
#define cdb2buf_getuserptr CDB2BUF_FUNC(cdb2buf_getuserptr)

/* advance the comdb2buf to the next newline */
void CDB2BUF_FUNC(cdb2buf_nextline)(COMDB2BUF *sb);
#define cdb2buf_nextline CDB2BUF_FUNC(cdb2buf_nextline)

#if CDB2BUF_UNGETC
int CDB2BUF_FUNC(cdb2buf_ungetc)(char c, COMDB2BUF *sb);
#  define cdb2buf_ungetc CDB2BUF_FUNC(cdb2buf_ungetc)
int CDB2BUF_FUNC(cdb2buf_eof)(COMDB2BUF *sb);
#  define cdb2buf_eof CDB2BUF_FUNC(cdb2buf_eof)
#endif

/* Unbuffered IO. If SSL is used, the SSL error codes
   will be converted to UNIX error codes:

   SSL_ERROR_WANT_READ   -> EAGAIN
   SSL_ERROR_WANT_WRITE  -> EAGAIN
   SSL_ERROR_ZERO_RETURN -> ECONNRESET
   SSL_SYS_CALL          -> ECONNRESET
   everything else       -> EIO
*/
int CDB2BUF_FUNC(cdb2buf_unbufferedread)(COMDB2BUF *sb, char *cc, int len);
#define cdb2buf_unbufferedread CDB2BUF_FUNC(cdb2buf_unbufferedread)
int CDB2BUF_FUNC(cdb2buf_unbufferedwrite)(COMDB2BUF *sb, const char *cc, int len);
#define cdb2buf_unbufferedwrite CDB2BUF_FUNC(cdb2buf_unbufferedwrite)

/* Given an fd, work out which machine the connection is from.
   Cache hostname info only in server mode. */
char *CDB2BUF_FUNC(get_origin_mach_by_buf)(COMDB2BUF *);
#define get_origin_mach_by_buf CDB2BUF_FUNC(get_origin_mach_by_buf)

/* Returns the error of a preceding call to cdb2buf_flush(), cdb2buf_putc(),
   cdb2buf_puts(), cdb2buf_write(), cdb2buf_fwrite(), cdb2buf_getc(), cdb2buf_gets(),
   cdb2buf_fread(), cdb2buf_unbufferedread() or cdb2buf_unbufferedwrite().

   When compiled with SSL, the function returns non-zero if the last error
   is an SSL protocol error, and 0 otherwise. The caller should not attempt
   to retry on a non-zero return code; When not compiled with SSL, the
   function always returns 0. */
int CDB2BUF_FUNC(cdb2buf_lasterror)(COMDB2BUF *sb, char *err, size_t n);
#define cdb2buf_lasterror CDB2BUF_FUNC(cdb2buf_lasterror)

/* SSL routines. */
#include <ssl_support.h>
#include <ssl_io.h>

#if defined __cplusplus
}
#endif

#endif
