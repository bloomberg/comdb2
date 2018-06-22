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

/* myself */
#include <ssl_support.h>

/* sys */
#include <errno.h>
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <pthread.h>

/* openssl */
#define OPENSSL_THREAD_DEFINES
#include <openssl/opensslconf.h>

#include <openssl/conf.h>
#include <openssl/opensslv.h>
#include <openssl/crypto.h>

/* bb */
#if SBUF2_SERVER
#  include <openssl/rand.h> /* RAND_pseudo_bytes() */
#  include "mem_util.h" /* subsystem malloc routines. */
#  include <mem_override.h> /* override malloc routines. */
#endif

#ifdef my_ssl_println
#  undef my_ssl_println
#endif
#ifdef my_ssl_eprintln
#  undef my_ssl_eprintln
#endif
#define my_ssl_println(fmt, ...)     \
    ssl_println("Generic", fmt, ##__VA_ARGS__)
#define my_ssl_eprintln(fmt, ...)    \
    ssl_eprintln("Generic", "%s: " fmt, __func__, ##__VA_ARGS__)

static int is_ssl_initialized = 0;
static pthread_mutex_t ssl_init_lock = PTHREAD_MUTEX_INITIALIZER;

#ifndef CRYPTO_num_locks
/* Callbacks for OpenSSL locking. OpenSSL >= 1.1.0 has its own locking.
   CRYPTO_num_locks is a function in OpenSSL < 1.1.0 but is made a macro
   in OpenSSL >= 1.1.0. So we assume that if CRYPTO_num_locks is not
   defined, we still need to implement our own locking. */
static pthread_mutex_t *ssl_locks = NULL;

static void ssl_threadid(CRYPTO_THREADID *thd)
{
    CRYPTO_THREADID_set_numeric(thd, (intptr_t)pthread_self());
}

/* For OpenSSL < 1.0.0. */
static unsigned long ssl_threadid_deprecated()
{
    return (unsigned long)pthread_self();
}

static void ssl_lock(int mode, int type, const char *file, int line)
{
    int rc;
    if (mode & CRYPTO_LOCK) {
        if ((rc = pthread_mutex_lock(&ssl_locks[type])) != 0)
            my_ssl_eprintln("Failed to lock pthread mutex: rc = %d.", rc);
    } else {
        if ((rc = pthread_mutex_unlock(&ssl_locks[type])) != 0)
            my_ssl_eprintln("Failed to unlock pthread mutex: rc = %d.", rc);
    }
}
#endif /* CRYPTO_num_locks */

int SBUF2_FUNC(ssl_init)(int init_openssl, int init_crypto,
                       int lock, char *err, size_t n)
{
    int ii, nlocks, rc;

    if (init_openssl) {
#ifndef OPENSSL_THREADS
        /* OpenSSL is not configured for threaded applications. */
        ssl_sfeprint(err, n, my_ssl_eprintln,
                     "OpenSSL is not configured with thread support.");
        return EPERM;
#endif /* OPENSSL_THREADS */
    }

	rc = lock ? pthread_mutex_lock(&ssl_init_lock) : 0;
    if (rc != 0)
        return rc;

    /* Initialize OpenSSL only once. */
    if (!is_ssl_initialized) {
#ifndef CRYPTO_num_locks
        if (init_crypto) {
            /* Configure SSL locking.
               This is only required for OpenSSL < 1.1.0. */
            nlocks = CRYPTO_num_locks();
            ssl_locks = malloc(sizeof(pthread_mutex_t) * nlocks);
            if (ssl_locks == NULL) {
                ssl_sfeprint(err, n, my_ssl_eprintln,
                             "Failed to allocate SSL locks.");
                rc = ENOMEM;
                goto error;
            }

            for (ii = 0; ii < nlocks; ++ii) {
                if ((rc = pthread_mutex_init(&ssl_locks[ii], NULL)) != 0) {
                    /* Whoops - roll back all we have done. */
                    while (ii > 0) {
                        --ii;
                        pthread_mutex_destroy(&ssl_locks[ii]);
                    }
                    free(ssl_locks);
                    ssl_locks = NULL;
                    ssl_sfeprint(err, n, my_ssl_eprintln,
                                 "Failed to initialize mutex: %s", strerror(rc));
                    goto error;
                }
            }
#  ifdef OPENSSL_NO_DEPRECATED
            CRYPTO_THREADID_set_callback(ssl_threadid);
#  else
            /* Use deprecated functions for OpenSSL < 1.0.0. */
            CRYPTO_set_id_callback(ssl_threadid_deprecated);
#  endif /* OPENSSL_NO_DEPRECATED */
            CRYPTO_set_locking_callback(ssl_lock);
        }
#endif /* CRYPTO_num_locks */

       /* Configure the library. */
        if (init_openssl) {
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
            OPENSSL_init_ssl(OPENSSL_INIT_LOAD_CONFIG, NULL);
#else
            OPENSSL_config(NULL);
            SSL_library_init();
            SSL_load_error_strings();
#endif /* OPENSSL_VERSION_NUMBER */
        }
        is_ssl_initialized = 1;
    }

error:
    if (lock) {
        if (rc == 0)
            rc = pthread_mutex_unlock(&ssl_init_lock);
        else
            pthread_mutex_unlock(&ssl_init_lock);
    }

    return rc;
}

#if SBUF2_SERVER
/* server-side session id context */
static unsigned char sid_ctx[8];
#endif

int SBUF2_FUNC(ssl_new_ctx)(SSL_CTX **pctx, ssl_mode mode, const char *dir,
                            char **pcert, char **pkey, char **pca, char **pcrl,
                            long sess_sz, const char *ciphers, char *err,
                            size_t n)
{
    SSL_CTX *myctx;
    char *buffer, *cert, *key, *ca, *crl;
    int rc, servermode;
    struct stat buf;
    STACK_OF(X509_NAME) *cert_names;

#if SBUF2_SERVER
    servermode = 1;
#else
    servermode = 0;
#endif
    myctx = NULL;
    cert = *pcert;
    key = *pkey;
    ca = *pca;
    crl = *pcrl;
    cert_names = NULL;

    /* If we are told to verify peer, and cacert file is NULL,
       we explicitly make one with the default name so that
       ssl_new_ctx() would fail if it could not load the CA. */
    if (mode >= SSL_VERIFY_CA && *pca == NULL) {
        if (dir == NULL) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "A trusted CA certificate is required "
                         "to verify server certificates.");
            goto error;
        }
        ca = malloc(strlen(dir) + sizeof("/" DEFAULT_CA));
        if (ca == NULL) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Failed to allocate memory for cacert: %s.",
                         strerror(errno));
            goto error;
        }
        /* overflow-safe */
        sprintf(ca, "%s/%s", dir, DEFAULT_CA);
    }

    /* If we're given a directory, find files under it. */
    if (dir != NULL) {
        if (cert == NULL) {
            buffer = alloca(strlen(dir) + sizeof("/" DEFAULT_CERT));
            sprintf(buffer, "%s/%s", dir, DEFAULT_CERT);

            if ((rc = access(buffer, R_OK)) == 0) {
                cert = strdup(buffer);
                if (cert == NULL) {
                    ssl_sfeprint(err, n, my_ssl_eprintln,
                            "Failed to duplicate string: %s.",
                            strerror(errno));
                    goto error;
                }
            } else if (servermode) {
                ssl_sfeprint(err, n, my_ssl_eprintln,
                        "Could not find server certificate:%s.",
                        buffer);
                goto error;
            }
        }

        if (key == NULL) {
            buffer = alloca(strlen(dir) + sizeof("/" DEFAULT_KEY));
            sprintf(buffer, "%s/%s", dir, DEFAULT_KEY);

            if ((rc = access(buffer, R_OK)) == 0) {
                key = strdup(buffer);
                if (key == NULL) {
                    ssl_sfeprint(err, n, my_ssl_eprintln,
                            "Failed to duplicate string: %s.",
                            strerror(errno));
                    goto error;
                }
            } else if (servermode) {
                ssl_sfeprint(err, n, my_ssl_eprintln,
                        "Could not find server key:%s.",
                        buffer);
                goto error;
            }
        }

        if (ca == NULL) {
            buffer = alloca(strlen(dir) + sizeof("/" DEFAULT_CA));
            sprintf(buffer, "%s/%s", dir, DEFAULT_CA);

            if ((rc = access(buffer, R_OK)) == 0) {
                ca = strdup(buffer);
                if (ca == NULL) {
                    ssl_sfeprint(err, n, my_ssl_eprintln,
                                 "Failed to duplicate string: %s.",
                                 strerror(errno));
                    goto error;
                }
            }
        }

#if HAVE_CRL
        if (crl == NULL) {
            buffer = alloca(strlen(dir) + sizeof("/" DEFAULT_CRL));
            sprintf(buffer, "%s/%s", dir, DEFAULT_CRL);

            if ((rc = access(buffer, R_OK)) == 0) {
                crl = strdup(buffer);
                if (crl == NULL) {
                    ssl_sfeprint(err, n, my_ssl_eprintln,
                                 "Failed to duplicate string: %s.",
                                 strerror(errno));
                    goto error;
                }
            }
        }
#endif /* HAVE_CRL */
    }

    /* Test permissions. */

    /* Test read permission on certificate. */
    if (cert != NULL && (rc = access(cert, R_OK)) != 0) {
        ssl_sfeprint(err, n, my_ssl_eprintln,
                     "Failed to read certificate %s: %s.",
                     cert, strerror(rc));
        goto error;
    }

    if (key != NULL) {
        if ((rc = stat(key, &buf)) != 0) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Failed to access key %s: %s.",
                         key, strerror(rc));
            goto error;
        }

        /* Key must to be owned by either us or root. */
        if (buf.st_uid != geteuid() && buf.st_uid != 0) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Key %s must be owned by root or "
                         "the effective user of the database process.",
                         key);
            rc = EACCES;
            goto error;
        }

        /* Key must be 0 to O/G if owned by us. */
        if (buf.st_uid == geteuid() && buf.st_mode & (S_IRWXG | S_IRWXO)) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Permissions for key %s are too open.", key);
            rc = EACCES;
            goto error;
        }

        /* Key must be 4 to G and 0 to O, if owned by root. */
        if (geteuid() != 0 &&
            buf.st_uid == 0 &&
            buf.st_mode & (S_IWGRP | S_IXGRP | S_IRWXO)) {
            ssl_sfeprint(err, n, my_ssl_eprintln,
                         "Permissions for key %s are too open.", key);
            rc = EACCES;
            goto error;
        }
    }

    /* Test read permission on cacert. */
    if (ca != NULL && (rc = access(ca, R_OK)) != 0) {
        /* User has provided us with root CA. */
        ssl_sfeprint(err, n, my_ssl_eprintln,
                     "Could not read cacert %s: %s.",
                     ca, strerror(rc));
        goto error;
    }

#if HAVE_CRL
    /* Test read permission on crl. */
    if (crl != NULL && (rc = access(crl, R_OK)) != 0) {
        /* User has provided us with root CA. */
        ssl_sfeprint(err, n, my_ssl_eprintln, "Could not read CRL %s: %s.", crl,
                     strerror(rc));
        goto error;
    }
#endif /* HAVE_CRL */

    /* Create SSL context. */
    rc = 1;
    ERR_clear_error();
#if OPENSSL_VERSION_NUMBER >= 0x10100000L
    myctx = SSL_CTX_new(TLS_method());
#else
    myctx = SSL_CTX_new(SSLv23_method());
#endif
    if (myctx == NULL) {
        ssl_sfliberrprint(err, n, my_ssl_eprintln,
                          "Failed to create SSL context");
        rc = ERR_get_error();
        goto error;
    }

    /* Disable SSL protocols to prevent POODLE attack (CVE-2014-3566). */
    SSL_CTX_set_options(myctx, SSL_OP_NO_SSLv2 | SSL_OP_NO_SSLv3);

    /* We need the flag to be able to write as fast as possible.
       We let sbuf2/comdb2buf take care of uncomplete writes. */
    SSL_CTX_set_mode(myctx, SSL_MODE_ENABLE_PARTIAL_WRITE |
                            SSL_MODE_ACCEPT_MOVING_WRITE_BUFFER);

    /* Adjust session cache size. */
    if (sess_sz < 0)
        sess_sz = SSL_SESSION_CACHE_MAX_SIZE_DEFAULT;
    SSL_CTX_sess_set_cache_size(myctx, sess_sz);

#if SBUF2_SERVER
    /* Set up session id context in server mode. */
    if (sess_sz > 0) {
        if (RAND_bytes(sid_ctx, sizeof(sid_ctx)) != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln,
                              "Failed to get random bytes");
            rc = ERR_get_error();
            goto error;
        }
        SSL_CTX_set_session_id_context(myctx, sid_ctx, sizeof(sid_ctx));
    }

    rc = SSL_CTX_set_cipher_list(myctx, ciphers);
    if (rc != 1) {
        ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to set cipher list");
        goto error;
    }
#endif

    if (cert != NULL || servermode == 1) {
        /* Use certificate. Force error if no cert in server mode */
        rc = SSL_CTX_use_certificate_chain_file(myctx, cert);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln,
                              "Failed to use certificate");
            goto error;
        }
    }

    if (key != NULL || servermode == 1) {
        /* Use private key. Force error if no cert in server mode */
        rc = SSL_CTX_use_PrivateKey_file(myctx,
                                         key, SSL_FILETYPE_PEM);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln,
                              "Failed to use private key");
            goto error;
        }
    }

    if (key != NULL || cert != NULL) {
        rc = SSL_CTX_check_private_key(myctx);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln,
                              "Failed to validate private key");
            goto error;
        }
    }

    /* Load CA cert. */
    if (ca != NULL) {
        rc = SSL_CTX_load_verify_locations(myctx, ca, NULL);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln,
                              "Failed to load cacert");
            goto error;
        }

        cert_names = SSL_load_client_CA_file(ca);
        if (cert_names == NULL) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln,
                              "Failed to load names from cacert");
            goto error;
        }

        SSL_CTX_set_verify(myctx,
                           SSL_VERIFY_PEER | SSL_VERIFY_CLIENT_ONCE, NULL);

        if (servermode)
            SSL_CTX_set_client_CA_list(myctx, cert_names);
    }

#if HAVE_CRL
    /* Load the certificate revocation list (CRL). */
    if (crl != NULL) {
        X509_STORE *cvs = SSL_CTX_get_cert_store(myctx);
        if (cvs == NULL) { /* Unlikely but just in case */
            ssl_sfliberrprint(err, n, my_ssl_eprintln,
                              "Failed to get cert store.");
            goto error;
        }
        rc = X509_STORE_load_locations(cvs, crl, NULL);
        if (rc != 1) {
            ssl_sfliberrprint(err, n, my_ssl_eprintln, "Failed to load CRL.");
            goto error;
        }
        rc = X509_STORE_set_flags(cvs, X509_V_FLAG_CRL_CHECK |
                                           X509_V_FLAG_CRL_CHECK_ALL);
    }
#endif /* HAVE_CRL */

    /* SSL success is 1. We want to return 0 upon success. */
    if (rc != 1) {
error:  if (myctx != NULL) {
            SSL_CTX_free(myctx);
            myctx = NULL;
        }

        /* Free strdup()'d memory. */
        if (cert != *pcert)
            free(cert);
        if (key != *pkey)
            free(key);
        if (ca != *pca)
            free(ca);
        if (crl != *pcrl)
            free(crl);

        if (rc == 0)
            rc = 1;
    } else {
        rc = 0;
        *pctx = myctx;

        if (*pcert == NULL)
            *pcert = cert;
        if (*pkey == NULL)
            *pkey = key;
        if (*pca == NULL)
            *pca = ca;
        if (*pcrl == NULL)
            *pcrl = crl;
    }

    return rc;
}
