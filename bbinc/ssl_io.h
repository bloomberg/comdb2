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

#ifndef _INCLUDED_SSL_IO_H_
#define _INCLUDED_SSL_IO_H_

#include <stddef.h>
#include <ssl_support.h>

/* Gracefully shutdown an SSL connection. The fd remains resuable.
   Return 0 upon success. */
int SBUF2_FUNC(sslio_close)(SBUF2 *, int reuse);
#define sslio_close SBUF2_FUNC(sslio_close)

int SBUF2_FUNC(sslio_read)(SBUF2 *, char *cc, int len);
#define sslio_read SBUF2_FUNC(sslio_read)
int SBUF2_FUNC(sslio_write)(SBUF2 *, const char *cc, int len);
#define sslio_write SBUF2_FUNC(sslio_write)

/* Return the associated SSL object. */
SSL *SBUF2_FUNC(sslio_get_ssl)(SBUF2 *);
#define sslio_get_ssl SBUF2_FUNC(sslio_get_ssl)

/* Return 1 if ssl is on. This function is slightly 
   faster than sslio_get_ssl if we just want to
   check the ssl status. */
int SBUF2_FUNC(sslio_has_ssl)(SBUF2 *);
#define sslio_has_ssl SBUF2_FUNC(sslio_has_ssl)

/* Return 1 if the connection came with an X509 cert. 
   The function makes sense only in server mode, because
   server always sends its certificate to clients. */
int SBUF2_FUNC(sslio_has_x509)(SBUF2 *);
#define sslio_has_x509 SBUF2_FUNC(sslio_has_x509)

/* Perform an SSL handshake.
   Return 1 upon success. */
#if SBUF2_SERVER
int SBUF2_FUNC(sslio_connect)(SBUF2 *, SSL_CTX *, ssl_mode, const char *dbname,
                              int nid, char *err, size_t n,
                              int close_on_verify_error);
#else
int SBUF2_FUNC(sslio_connect)(SBUF2 *, SSL_CTX *, ssl_mode, const char *dbname,
                              int nid, char *err, size_t n, SSL_SESSION *,
                              int *unrecoverable);
#endif
#define sslio_connect SBUF2_FUNC(sslio_connect)

/* Perform an SSL handshake.
   Return 1 upon success. */
int SBUF2_FUNC(sslio_accept)(SBUF2 *, SSL_CTX *, ssl_mode, const char *dbname,
                             int nid, char *err, size_t n,
                             int close_on_verify_error);
#define sslio_accept SBUF2_FUNC(sslio_accept)

/* Given an NID, return the attribute in the X509 certificate in `out'. */
int SBUF2_FUNC(sslio_x509_attr)(SBUF2 *sb, int nid, char *out, size_t len);
#define sslio_x509_attr SBUF2_FUNC(sslio_x509_attr)
#endif
