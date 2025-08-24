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
int CDB2BUF_FUNC(sslio_close)(COMDB2BUF *, int reuse);
#define sslio_close CDB2BUF_FUNC(sslio_close)

/* Does not shut down, simply frees. Use this for fatal SSL errors
   such as SSL_ERROR_SYSCALL or SSL_ERROR_SSL */
void CDB2BUF_FUNC(sslio_free)(COMDB2BUF *);
#define sslio_free CDB2BUF_FUNC(sslio_free)

int CDB2BUF_FUNC(sslio_read)(COMDB2BUF *, char *cc, int len);
#define sslio_read CDB2BUF_FUNC(sslio_read)
int CDB2BUF_FUNC(sslio_write)(COMDB2BUF *, const char *cc, int len);
#define sslio_write CDB2BUF_FUNC(sslio_write)

/* Return the associated SSL object. */
SSL *CDB2BUF_FUNC(sslio_get_ssl)(COMDB2BUF *);
#define sslio_get_ssl CDB2BUF_FUNC(sslio_get_ssl)

/* Return 1 if ssl is on. This function is slightly 
   faster than sslio_get_ssl if we just want to
   check the ssl status. */
int CDB2BUF_FUNC(sslio_has_ssl)(COMDB2BUF *);
#define sslio_has_ssl CDB2BUF_FUNC(sslio_has_ssl)

/* Return 1 if the connection came with an X509 cert. 
   The function makes sense only in server mode, because
   server always sends its certificate to clients. */
int CDB2BUF_FUNC(sslio_has_x509)(COMDB2BUF *);
#define sslio_has_x509 CDB2BUF_FUNC(sslio_has_x509)

/* Perform an SSL handshake.
   Return 1 upon success. */
#if SBUF2_SERVER
int CDB2BUF_FUNC(sslio_connect)(COMDB2BUF *, SSL_CTX *, ssl_mode, const char *dbname,
                              int nid, int close_on_verify_error);
#else
int CDB2BUF_FUNC(sslio_connect)(COMDB2BUF *, SSL_CTX *, ssl_mode, const char *dbname,
                              int nid, SSL_SESSION *);
#endif
#define sslio_connect CDB2BUF_FUNC(sslio_connect)

/* Perform an SSL handshake.
   Return 1 upon success. */
int CDB2BUF_FUNC(sslio_accept)(COMDB2BUF *, SSL_CTX *, ssl_mode, const char *dbname,
                             int nid, int close_on_verify_error);
#define sslio_accept CDB2BUF_FUNC(sslio_accept)

/* Given an NID, return the attribute in the X509 certificate in `out'. */
int CDB2BUF_FUNC(sslio_x509_attr)(COMDB2BUF *sb, int nid, char *out, size_t len);
#define sslio_x509_attr CDB2BUF_FUNC(sslio_x509_attr)

int CDB2BUF_FUNC(sslio_pending)(COMDB2BUF *sb);
#define sslio_pending CDB2BUF_FUNC(sslio_pending)
#endif
