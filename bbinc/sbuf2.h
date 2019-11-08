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

/* sbuf2.h -  simple buffering for stream. stupid fopen can't handle fd>255 */

/* Server sbuf2 uses dlmalloc. Client does not. The simplest approach
   to avoid adding dlmalloc dependency to client API is to compile
   server and client version separately. */
#ifndef SBUF2_SERVER
#  define SBUF2_SERVER 1
#  ifndef SBUF2_UNGETC
#    define SBUF2_UNGETC 1
#  endif
#endif

/* Name mangling */
#ifndef SBUF2_FUNC
#  if SBUF2_SERVER
#    define SBUF2_FUNC(func) SERVER_ ## func
#  else
#    define SBUF2_FUNC(func) CLIENT_ ## func
#  endif
#endif

#if defined __cplusplus
extern "C" {
#endif

typedef struct sbuf2 SBUF2;

enum SBUF2_FLAGS {
    /* FLUSH OUTPUT ON \n */
    SBUF2_WRITE_LINE = 1,
    /*STORE LAST LINE IN/OUT IN DEBUG BUFFER*/
    SBUF2_DEBUG_LAST_LINE = 2,
    /* sbuf2close() will not close the underlying fd */
    SBUF2_NO_CLOSE_FD = 4,
    /* sbuf2close() will not flush the underlying fd */
    SBUF2_NO_FLUSH = 8,
    /* adjust read/write calls to write on non-blocking socket */
    SBUF2_NO_BLOCK = 16
};

typedef int (*sbuf2readfn)(SBUF2 *sb, char *buf, int nbytes);
typedef int (*sbuf2writefn)(SBUF2 *sb, const char *buf, int nbytes);

/* retrieve underlying fd */
int SBUF2_FUNC(sbuf2fileno)(SBUF2 *sb);
#define sbuf2fileno SBUF2_FUNC(sbuf2fileno)

/* set flags on an SBUF2 after opening */
void SBUF2_FUNC(sbuf2setflags)(SBUF2 *sb, int flags);
#define sbuf2setflags SBUF2_FUNC(sbuf2setflags)

/* open SBUF2 for file descriptor.  returns SBUF2 handle or 0 if error.*/
SBUF2 *SBUF2_FUNC(sbuf2open)(int fd, int flags);
#define sbuf2open SBUF2_FUNC(sbuf2open)

/* flush output, close fd, and free SBUF2. 0==success */
int SBUF2_FUNC(sbuf2close)(SBUF2 *sb);
#define sbuf2close SBUF2_FUNC(sbuf2close)

/* flush output, close fd, and free SBUF2.*/
int SBUF2_FUNC(sbuf2free)(SBUF2 *sb);
#define sbuf2free SBUF2_FUNC(sbuf2free)

/* flush output.  returns # of bytes written or <0 for error */
int SBUF2_FUNC(sbuf2flush)(SBUF2 *sb);
#define sbuf2flush SBUF2_FUNC(sbuf2flush)

/* sets buffer size. this does NOT flush for you. if you
   change the buffer size, it will lose any buffered writes.
   default buffer size is 1K */
int SBUF2_FUNC(sbuf2setbufsize)(SBUF2 *sb, unsigned int size);
#define sbuf2setbufsize SBUF2_FUNC(sbuf2setbufsize)

/* put character. returns # of bytes written (always 1) or <0 for err */
int SBUF2_FUNC(sbuf2putc)(SBUF2 *sb, char c);
#define sbuf2putc SBUF2_FUNC(sbuf2putc)

/* put \0 terminated string. returns # of bytes written or <0 for err */
int SBUF2_FUNC(sbuf2puts)(SBUF2 *sb, char *string);
#define sbuf2puts SBUF2_FUNC(sbuf2puts)

/* write to SBUF2. returns number of bytes written or <0 for error */
int SBUF2_FUNC(sbuf2write)(char *ptr, int nbytes, SBUF2 *sb);
#define sbuf2write SBUF2_FUNC(sbuf2write)

/* fwrite to SBUF2. returns # of items written or <0 for error */
int SBUF2_FUNC(sbuf2fwrite)(char *ptr, int size, int nitems, SBUF2 *sb);
#define sbuf2fwrite SBUF2_FUNC(sbuf2fwrite)

/* get character. returns character read */
int SBUF2_FUNC(sbuf2getc)(SBUF2 *sb);
#define sbuf2getc SBUF2_FUNC(sbuf2getc)

/* returns string len (not including \0) and string. <0 means err*/
int SBUF2_FUNC(sbuf2gets)(char *out, int lout, SBUF2 *sb);
#define sbuf2gets SBUF2_FUNC(sbuf2gets)

/* fread from SBUF2. returns # of items read or <0 for error */
int SBUF2_FUNC(sbuf2fread)(char *ptr, int size, int nitems, SBUF2 *sb);
#define sbuf2fread SBUF2_FUNC(sbuf2fread)

/* returns # of bytes written or <0 for err*/
int SBUF2_FUNC(sbuf2printf)(SBUF2 *sb, const char *fmt, ...);
#define sbuf2printf SBUF2_FUNC(sbuf2printf)

/* returns # of bytes written or <0 for err*/
int SBUF2_FUNC(sbuf2printfx)(SBUF2 *sb, char *buf, int lbuf, char *fmt, ...);
#define sbuf2printfx SBUF2_FUNC(sbuf2printfx)

/* set custom read/write routines */
void SBUF2_FUNC(sbuf2setrw)(SBUF2 *sb, sbuf2readfn read, sbuf2writefn write);
#define sbuf2setrw SBUF2_FUNC(sbuf2setrw)
void SBUF2_FUNC(sbuf2setr)(SBUF2 *sb, sbuf2readfn read);
#define sbuf2setr SBUF2_FUNC(sbuf2setr)
void SBUF2_FUNC(sbuf2setw)(SBUF2 *sb, sbuf2writefn write);
#define sbuf2setw SBUF2_FUNC(sbuf2setw)
sbuf2readfn SBUF2_FUNC(sbuf2getr)(SBUF2 *sb);
#define sbuf2getr SBUF2_FUNC(sbuf2getr)
sbuf2writefn SBUF2_FUNC(sbuf2getw)(SBUF2 *sb);
#define sbuf2getw SBUF2_FUNC(sbuf2getw)

/* set up poll timeout on file descriptor*/
void SBUF2_FUNC(sbuf2settimeout)(SBUF2 *sb, int readtimeout, int writetimeout);
#define sbuf2settimeout SBUF2_FUNC(sbuf2settimeout)

void SBUF2_FUNC(sbuf2gettimeout)(SBUF2 *sb,
                                 int *readtimeout, int *writetimeout);
#define sbuf2gettimeout SBUF2_FUNC(sbuf2gettimeout)

int SBUF2_FUNC(sbuf2fread_timeout)(char *ptr, int size, int nitems, SBUF2 *sb,
                                   int *was_timeout);
#define sbuf2fread_timeout SBUF2_FUNC(sbuf2fread_timeout)

/* return last line read*/
char *SBUF2_FUNC(sbuf2dbgin)(SBUF2 *sb);
#define sbuf2dbgin SBUF2_FUNC(sbuf2dbgin)

/* return last line written*/
char *SBUF2_FUNC(sbuf2dbgout)(SBUF2 *sb);
#define sbuf2dbgout SBUF2_FUNC(sbuf2dbgout)

#if SBUF2_SERVER
struct sqlclntstate;

/* set the sqlclntstate pointer associated with this sbuf */
void SBUF2_FUNC(sbuf2setclnt)(SBUF2 *sb, struct sqlclntstate *clnt);
#define sbuf2setclnt SBUF2_FUNC(sbuf2setclnt)

/* get the sqlclntstate pointer associated with this sbuf */
struct sqlclntstate *SBUF2_FUNC(sbuf2getclnt)(SBUF2 *sb);
#define sbuf2getclnt SBUF2_FUNC(sbuf2getclnt)
#endif

/* set the userptr associated with this sbuf - use this for whatever your
 * application desires. */
void SBUF2_FUNC(sbuf2setuserptr)(SBUF2 *sb, void *userptr);
#define sbuf2setuserptr SBUF2_FUNC(sbuf2setuserptr)

/* get the user pointer associated with this sbuf */
void *SBUF2_FUNC(sbuf2getuserptr)(SBUF2 *sb);
#define sbuf2getuserptr SBUF2_FUNC(sbuf2getuserptr)

/* advance the sbuf2 to the next newline */
void SBUF2_FUNC(sbuf2nextline)(SBUF2 *sb);
#define sbuf2nextline SBUF2_FUNC(sbuf2nextline)

#if SBUF2_UNGETC
int SBUF2_FUNC(sbuf2ungetc)(char c, SBUF2 *sb);
#  define sbuf2ungetc SBUF2_FUNC(sbuf2ungetc)
int SBUF2_FUNC(sbuf2eof)(SBUF2 *sb);
#  define sbuf2eof SBUF2_FUNC(sbuf2eof)
#endif

/* Unbuffered IO. If SSL is used, the SSL error codes
   will be converted to UNIX error codes:

   SSL_ERROR_WANT_READ   -> EAGAIN
   SSL_ERROR_WANT_WRITE  -> EAGAIN
   SSL_ERROR_ZERO_RETURN -> ECONNRESET
   SSL_SYS_CALL          -> ECONNRESET
   everything else       -> EIO
*/
int SBUF2_FUNC(sbuf2unbufferedread)(SBUF2 *sb, char *cc, int len);
#define sbuf2unbufferedread SBUF2_FUNC(sbuf2unbufferedread)
int SBUF2_FUNC(sbuf2unbufferedwrite)(SBUF2 *sb, const char *cc, int len);
#define sbuf2unbufferedwrite SBUF2_FUNC(sbuf2unbufferedwrite)

/* Given an fd, work out which machine the connection is from.
   Cache hostname info only in server mode. */
char *SBUF2_FUNC(get_origin_mach_by_buf)(SBUF2 *);
#define get_origin_mach_by_buf SBUF2_FUNC(get_origin_mach_by_buf)

void SBUF2_FUNC(cleanup_peer_hash)();
#define cleanup_peer_hash SBUF2_FUNC(cleanup_peer_hash)

#ifndef WITH_SSL
#  define WITH_SSL 1
#endif

/* SSL routines. */
#if WITH_SSL
#  include <ssl_support.h>
#  include <ssl_io.h>
#endif

#if defined __cplusplus
}
#endif

#endif
