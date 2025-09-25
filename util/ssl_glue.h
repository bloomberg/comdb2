/*
   Copyright 2022 Bloomberg Finance L.P.

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

#ifndef INCLUDED_SSL_GLUE_H
#define INCLUDED_SSL_GLUE_H

/* XXX Don't change the order of the enum types */
typedef enum ssl_mode {
    SSL_DISABLE, /* invisible to users */
    SSL_UNKNOWN, /* invisible to users */
    SSL_ALLOW,
    SSL_PREFER,
    SSL_PREFER_VERIFY_CA,       /* implies PREFER */
    SSL_PREFER_VERIFY_HOSTNAME, /* implies PREFER_VERIFY_CA */
    SSL_PREFER_VERIFY_DBNAME,   /* implies PREFER_VERIFY_DBNAME */
    SSL_REQUIRE,
    SSL_VERIFY_CA,       /* It implies REQUIRE. */
    SSL_VERIFY_HOSTNAME, /* It impiles VERIFY_CA. */
    SSL_VERIFY_DBNAME    /* It impiles VERIFY_HOSTNAME. */
} ssl_mode;

#include <openssl/ssl.h>
int ssl_verify_dbname(X509 *, const char *, int);
int ssl_x509_get_attr(const X509 *, int, char *, size_t);
int ssl_verify_hostname(X509 *, int);

#ifndef SBUF2_SERVER
#  define SBUF2_SERVER 1
#endif

#if SBUF2_SERVER /* visible to server only */
struct ssl_data;
int ssl_whitelisted(const char *); /* returns 1 if the connection is whitelisted */
int verify_ssl_evbuffer(struct ssl_data *, enum ssl_mode);
#endif

#endif /* INCLUDED_SSL_GLUE_H */
