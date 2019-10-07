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

/*************************************************
   SSL Miscellaneous
 *************************************************/

#ifndef _INCLUDED_SSL_SUPPORT_H_
#define _INCLUDED_SSL_SUPPORT_H_

#include <alloca.h>
#include <stddef.h>
#include <openssl/ssl.h>
#include <openssl/err.h>

#ifndef SBUF2_SERVER
#  define SBUF2_SERVER 1
#  ifndef SBUF2_UNGETC
#    define SBUF2_UNGETC 1
#  endif
#endif

#ifndef SBUF2_FUNC
#  if SBUF2_SERVER
#    define SBUF2_FUNC(func) SERVER_ ## func
#  else
#    define SBUF2_FUNC(func) CLIENT_ ## func
#  endif
#endif

#if OPENSSL_VERSION_NUMBER >= 0x00907000L
#define HAVE_CRL 1
#else
#define HAVE_CRL 0
#endif

/* Common SSL options and values */
#define SSL_CERT_PATH_OPT       "ssl_cert_path"
#define SSL_CERT_OPT            "ssl_cert"
#define SSL_KEY_OPT             "ssl_key"
#define SSL_CA_OPT              "ssl_ca"
#if HAVE_CRL
#define SSL_CRL_OPT "ssl_crl"
#endif
#define SSL_MIN_TLS_VER_OPT "ssl_min_tls_ver"

#define SSL_MODE_ALLOW          "ALLOW"
#define SSL_MODE_REQUIRE        "REQUIRE"
#define SSL_MODE_VERIFY_CA      "VERIFY_CA"
#define SSL_MODE_VERIFY_HOST    "VERIFY_HOSTNAME"
#define SSL_MODE_VERIFY_DBNAME "VERIFY_DBNAME"
#define SSL_MODE_OPTIONAL       "OPTIONAL"

/* Default file names */
#define DEFAULT_SERVER_KEY "server.key"
#define DEFAULT_SERVER_CERT "server.crt"
#define DEFAULT_CLIENT_KEY "client.key"
#define DEFAULT_CLIENT_CERT "client.crt"
#if SBUF2_SERVER
#define DEFAULT_KEY DEFAULT_SERVER_KEY
#define DEFAULT_CERT DEFAULT_SERVER_CERT
#else
#define DEFAULT_KEY DEFAULT_CLIENT_KEY
#define DEFAULT_CERT DEFAULT_CLIENT_CERT
#endif
#define DEFAULT_CA "root.crt"
#if HAVE_CRL
#define DEFAULT_CRL "root.crl"
#endif

/* logmsg: when compiled with cdb2api, do nothing. */
#if SBUF2_SERVER
#  include <logmsg.h>
#  define logi(...) logmsg(LOGMSG_INFO, ##__VA_ARGS__)
#  define loge(...) logmsg(LOGMSG_ERROR, ##__VA_ARGS__)
#else
#define logi(...)
#define loge(...)
#endif

/* Helper functions */
#define ssl_println(pfx, fmt, ...)  \
    logi(pfx " SSL Info: " fmt "\n", ##__VA_ARGS__)
#define ssl_eprintln(pfx, fmt, ...) \
    loge(pfx " SSL Error: " fmt "\n", ##__VA_ARGS__)

#define SSL_ERRSTR() ERR_reason_error_string(ERR_get_error())
#define SSL_ERRSTR_MT(buf) ERR_error_string(ERR_get_error(), buf)

#define PRINT_SSL_ERRSTR_MT(cb, msg)            \
do {                                            \
    char *__b = alloca(120);                    \
    ERR_error_string(ERR_get_error(), __b);     \
    cb(msg ": %s", __b);                        \
} while (0)

#define ssl_sfeprint(err, n, cb, fmt, ...)                      \
    do {                                                        \
        if (err != NULL)                                        \
            snprintf(err, n, "SSL Error: " fmt, ##__VA_ARGS__); \
        else                                                    \
            cb(fmt, ##__VA_ARGS__);                             \
    } while (0)

#define ssl_sfliberrprint(err, n, cb, msg)                      \
    do {                                                        \
        if (err != NULL)                                        \
            snprintf(err, n,                                    \
                     "SSL Error: %s: (%lu) %s",                 \
                     msg, ERR_get_error(), SSL_ERRSTR());       \
        else                                                    \
            PRINT_SSL_ERRSTR_MT(cb, msg);                       \
    } while (0)

typedef enum {
    SSL_DISABLE, /* invisible to users */
    SSL_UNKNOWN, /* invisible to users */
    SSL_ALLOW,
    SSL_REQUIRE,
    SSL_VERIFY_CA,       /* It implies REQUIRE. */
    SSL_VERIFY_HOSTNAME, /* It impiles VERIFY_CA. */
    SSL_VERIFY_DBNAME    /* It impiles VERIFY_HOSTNAME. */
} ssl_mode;

typedef enum {
    PEER_SSL_UNSUPPORTED,
    PEER_SSL_ALLOW,
    PEER_SSL_REQUIRE
} peer_ssl_mode;

const static struct ssl_protocol {
    double tlsver;
    long opensslver;
    const char *name;
} SSL_NO_PROTOCOLS[] = {{-2.0, SSL_OP_NO_SSLv2, "SSLv2"},
                        {-1.0, SSL_OP_NO_SSLv3, "SSLv3"},
                        {1.0, SSL_OP_NO_TLSv1, "TLSv1"},
                        {1.1, SSL_OP_NO_TLSv1_1, "TLSv1.1"},
                        {1.2, SSL_OP_NO_TLSv1_2, "TLSv1.2"}};

/*
 * Initialize SSL library.
 *
 * PARAMETERS
 * init_openssl - set to non-zero to initialize libssl
 * init_crypto  - set to non-zero to initialize libcrypto
 * locking      - set to non-zero to mutex lock the initialization
 * err          - set to NULL to print to stderr
 * n            - length of the error string buffer
 *
 * RETURN VALUES
 * 0 upon success
 */
int SBUF2_FUNC(ssl_init)(int init_openssl, int init_crypto, int locking,
                       char *err, size_t n);
#define ssl_init SBUF2_FUNC(ssl_init)

/*
 * Create an SSL context.
 *
 * PARAMETERS
 * pctx        - return an SSL_CTX in pctx
 * dir         - the path to SSL certificate directory. the function looks up
 *               certificate, key and CA under the directory only if
 *               (*cert), (*key) and (*ca) are NULL, respectively
 * cert        - if *cert is null, return path to certificate in cert;
 *               else use *cert as the certificate
 * key         - if *key is null, return path to the key in key;
 *               else use *key as the key
 * ca          - if *ca is null, return path to trusted CA in ca;
 *               else use *ca as the trusted CA
 * sesssz      - SSL session cache size
 * ciphers     - cipher suites. ignored in client mode.
 * mintlsver   - minimum acceptable TLS protocol version.
 *
 * RETURN VALUES
 * 0 upon success
 */
int SBUF2_FUNC(ssl_new_ctx)(SSL_CTX **pctx, ssl_mode mode, const char *dir,
                            char **cert, char **key, char **ca, char **crl,
                            long sesssz, const char *ciphers, double mintlsver,
                            char *err, size_t n);
#define ssl_new_ctx SBUF2_FUNC(ssl_new_ctx)

#endif
