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

/******************************************
            SSL backend support
 ******************************************/

/******************
 Headers.
 ******************/
/* myself */
#include "ssl_bend.h"

/* sys */
#include <errno.h>
#include <openssl/crypto.h>
#include <openssl/objects.h>
#include <string.h>

/* bb */
#include <util.h>
#include <segstr.h> /* tokcmp(), segtok(), tokdup() and etc. */
#include "sql.h"    /* struct sqlclntstate */

/******************
 Helpers.
 ******************/
#define my_ssl_printf(fmt, ...)     \
    ssl_println("Backend", fmt, ##__VA_ARGS__)

#define my_ssl_eprintln(fmt, ...)    \
    ssl_eprintln("Backend", "%s: " fmt, __func__, ##__VA_ARGS__)

/*****************************
 Backend SSL variables.
 *****************************/
static const char *gbl_cert_dir = NULL;
char *gbl_cert_file = NULL;
char *gbl_key_file = NULL;
char *gbl_ca_file = NULL;
char *gbl_crl_file = NULL;
int gbl_ssl_allow_remsql = 0;
/* negative -> OpenSSL default */
long gbl_sess_cache_sz = -1;
/* Default cipher suites comdb2 uses. */
const char *gbl_ciphers = "HIGH:!aNULL:!eNULL";
int gbl_nid_user = NID_undef;
#ifdef NID_host /* available as of RFC 4524 */
int gbl_nid_dbname = NID_host;
#else
int gbl_nid_dbname = NID_commonName;
#endif
ssl_mode gbl_client_ssl_mode = SSL_UNKNOWN;
ssl_mode gbl_rep_ssl_mode = SSL_UNKNOWN;
SSL_CTX *gbl_ssl_ctx = NULL;

/******************
 Functions.
 ******************/
int ssl_process_lrl(char *line, size_t len)
{
    int st, ltok;
    char *tok;

    /* Reset the offset and parse the line from the beginning. */
    st = 0;
    tok = segtok(line, len, &st, &ltok);

    /* Should not happen. But just in case. */
    if (ltok == 0 || tok[0] == '#')
        return 0;

    if (tokcmp(line, ltok, "ssl_client_mode") == 0) {
        /* SSL client mode */
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Expected SSL mode for `ssl_client_mode`.");
            return EINVAL;
        }
        if (tokcmp(tok, ltok, SSL_MODE_ALLOW) == 0)
            gbl_client_ssl_mode = SSL_ALLOW;
        else if (tokcmp(tok, ltok, SSL_MODE_REQUIRE) == 0)
            gbl_client_ssl_mode = SSL_REQUIRE;
        else if (tokcmp(tok, ltok, SSL_MODE_VERIFY_CA) == 0)
            gbl_client_ssl_mode = SSL_VERIFY_CA;
        else if (tokcmp(tok, ltok, SSL_MODE_VERIFY_HOST) == 0)
            gbl_client_ssl_mode = SSL_VERIFY_HOSTNAME;
        else if (tokcmp(tok, ltok, SSL_MODE_OPTIONAL) == 0)
            gbl_client_ssl_mode = SSL_UNKNOWN;
        else if (tokcmp(tok, ltok, SSL_MODE_VERIFY_DBNAME) == 0)
            gbl_client_ssl_mode = SSL_VERIFY_DBNAME;
        else {
            my_ssl_eprintln("Unrecognized SSL mode `%s`.", tok);
            return EINVAL;
        }
    } else if (tokcmp(line, ltok, "ssl_replicant_mode") == 0) {
        /* SSL client mode */
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Expected SSL mode for `ssl_replicant_mode`.");
            return EINVAL;
        }
        if (tokcmp(tok, ltok, SSL_MODE_ALLOW) == 0)
            gbl_rep_ssl_mode = SSL_ALLOW;
        else if (tokcmp(tok, ltok, SSL_MODE_REQUIRE) == 0)
            gbl_rep_ssl_mode = SSL_REQUIRE;
        else if (tokcmp(tok, ltok, SSL_MODE_VERIFY_CA) == 0)
            gbl_rep_ssl_mode = SSL_VERIFY_CA;
        else if (tokcmp(tok, ltok, SSL_MODE_VERIFY_HOST) == 0)
            gbl_rep_ssl_mode = SSL_VERIFY_HOSTNAME;
        else if (tokcmp(tok, ltok, SSL_MODE_VERIFY_DBNAME) == 0)
            gbl_rep_ssl_mode = SSL_VERIFY_DBNAME;
        else {
            my_ssl_eprintln("Unrecognized SSL mode `%s`.", tok);
            return EINVAL;
        }
    } else if (tokcmp(line, ltok, SSL_CERT_PATH_OPT) == 0) {
        /* Get key store directory. */
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Expected directory for `%s`.",
                            SSL_CERT_PATH_OPT);
            return EINVAL;
        }

        gbl_cert_dir = tokdup(tok, ltok);
        if (gbl_cert_dir == NULL) {
            my_ssl_eprintln("Failed to duplicate string: %s.",
                           strerror(errno));
            return errno;
        }
    } else if (tokcmp(line, ltok, SSL_CERT_OPT) == 0) {
        /* Get server certificate. */
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Expected server certificate for `%s`.",
                            SSL_CERT_OPT);
            return EINVAL;
        }

        gbl_cert_file = tokdup(tok, ltok);
        if (gbl_cert_file == NULL) {
            my_ssl_eprintln("Failed to duplicate string: %s.",
                           strerror(errno));
            return errno;
        }
    } else if (tokcmp(line, ltok, SSL_KEY_OPT) == 0) {
        /* Get server private key. */
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Expected server private key for `%s`.",
                            SSL_KEY_OPT);
            return EINVAL;
        }

        gbl_key_file = tokdup(tok, ltok);
        if (gbl_key_file == NULL) {
            my_ssl_eprintln("Failed to duplicate string: %s.",
                           strerror(errno));
            return errno;
        }
    } else if (tokcmp(line, ltok, SSL_CA_OPT) == 0) {
        /* Get trusted CA. */
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Expected trusted certificate "
                            "authorities for `%s`.", SSL_CA_OPT);
            return EINVAL;
        }

        gbl_ca_file = tokdup(tok, ltok);
        if (gbl_ca_file == NULL) {
            my_ssl_eprintln("Failed to duplicate string: %s.",
                           strerror(errno));
            return errno;
        }
#if HAVE_CRL
    } else if (tokcmp(line, ltok, SSL_CRL_OPT) == 0) {
        /* Get CRL. */
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Expected certificate revocation list file"
                            "for `%s`.",
                            SSL_CRL_OPT);
            return EINVAL;
        }

        gbl_crl_file = tokdup(tok, ltok);
        if (gbl_crl_file == NULL) {
            my_ssl_eprintln("Failed to duplicate string: %s.", strerror(errno));
            return errno;
        }
#endif /* HAVE_CRL */
    } else if (tokcmp(line, ltok, "ssl_sess_cache_size") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Expected # for `ssl_sess_cache_size`.");
            return EINVAL;
        }
        gbl_sess_cache_sz = toknum(tok, ltok);
    } else if (tokcmp(line, ltok, "ssl_allow_remsql") == 0) {
        tok = segtok(line, len, &st, &ltok);
        gbl_ssl_allow_remsql = (ltok <= 0) ? 1 : toknum(tok, ltok);
        logmsg(LOGMSG_WARN, "POTENTIAL SECURITY ISSUE: "
               "Plaintext remote SQL is permitted. Please make sure that "
               "the databases are in a secure environment.\n");
    } else if (tokcmp(line, ltok, "ssl_cipher_suites") == 0) {
        /* Get cipher suites. */
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Expected ciphers for `ssl_cipher_suites'.");
            return EINVAL;
        }

        gbl_ciphers = tokdup(tok, ltok);
        if (gbl_ciphers == NULL) {
            my_ssl_eprintln("Failed to duplicate string: %s.", strerror(errno));
            return errno;
        }
    } else if (tokcmp(line, ltok, "ssl_map_cert_to_user") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
#ifdef NID_userId
            gbl_nid_user = NID_userId; /* becomes official in RFC 4514 */
#else
            gbl_nid_user = NID_commonName;
#endif
        } else {
            char *nidtext = tokdup(tok, ltok);
            if (nidtext == NULL) {
                my_ssl_eprintln("Failed to duplicate string: %s.",
                                strerror(errno));
                return errno;
            }
            gbl_nid_user = OBJ_txt2nid(nidtext);
            free(nidtext);
        }
    } else if (tokcmp(line, ltok, "ssl_dbname_field") == 0) {
        /* Specify dbname field in certificates. The setting
           applies to both clients and replicants. */
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            my_ssl_eprintln("Missing certificate field for "
                            "`ssl_dbname_field`.");
            return EINVAL;
        }

        char *nidtext = tokdup(tok, ltok);
        if (nidtext == NULL) {
            my_ssl_eprintln("Failed to duplicate string: %s.", strerror(errno));
            return errno;
        }
        gbl_nid_dbname = OBJ_txt2nid(nidtext);
        free(nidtext);
    }
    return 0;
}

#ifndef USE_SYS_ALLOC
#include <mem.h>
static comdb2ma sslm;
#  include <openssl/opensslv.h>
#  if OPENSSL_VERSION_NUMBER >= 0x10100000L
static void *ssl_malloc(size_t sz, const char *file, int line)
{
    return comdb2_malloc(sslm, sz);
}

static void *ssl_realloc(void *p, size_t sz, const char *file, int line)
{
    return comdb2_realloc(sslm, p, sz);
}

static void ssl_free(void *p, const char *file, int line)
{
    comdb2_free(p);
}
#  else
static void *ssl_malloc(size_t sz)
{
    return comdb2_malloc(sslm, sz);
}

static void *ssl_realloc(void *p, size_t sz)
{
    return comdb2_realloc(sslm, p, sz);
}

static void ssl_free(void *p)
{
    comdb2_free(p);
}
#  endif
#endif

int ssl_bend_init(const char *default_certdir)
{
    const char *ks;
    int rc;
    char errmsg[512];
    ks = (gbl_cert_dir == NULL) ? default_certdir : gbl_cert_dir;

#ifndef USE_SYS_ALLOC
    sslm = comdb2ma_create(0, 0, "ssl", 1);
    if (sslm == NULL)
        return ENOMEM;
    CRYPTO_set_mem_functions(ssl_malloc, ssl_realloc, ssl_free);
#endif

    rc = ssl_init(1, 1, 1, NULL, 0);
    if (rc != 0)
        return rc;

    if (gbl_client_ssl_mode >= SSL_UNKNOWN ||
        gbl_rep_ssl_mode >= SSL_UNKNOWN) {
        rc = ssl_new_ctx(
            &gbl_ssl_ctx,
            gbl_client_ssl_mode > gbl_rep_ssl_mode ? gbl_client_ssl_mode
                                                   : gbl_rep_ssl_mode,
            ks, &gbl_cert_file, &gbl_key_file, &gbl_ca_file, &gbl_crl_file,
            gbl_sess_cache_sz, gbl_ciphers, errmsg, sizeof(errmsg));
        if (rc == 0) {
            if (gbl_client_ssl_mode == SSL_UNKNOWN)
                gbl_client_ssl_mode = SSL_ALLOW;
            if (gbl_rep_ssl_mode == SSL_UNKNOWN)
                gbl_rep_ssl_mode = SSL_ALLOW;
        } else if (gbl_client_ssl_mode == SSL_UNKNOWN &&
                   gbl_rep_ssl_mode == SSL_UNKNOWN) {
            gbl_client_ssl_mode = SSL_DISABLE;
            gbl_rep_ssl_mode = SSL_DISABLE;
            rc = 0;
        } else {
            /* Have user-defined SSL modes.
               Print the error message and return an error. */
            logmsg(LOGMSG_FATAL, "%s\n", errmsg);
        }
    }
    return rc;
}

static const char *ssl_mode_to_string(ssl_mode mode)
{
    switch (mode) {
    case SSL_DISABLE:
        return "DISABLE";
    case SSL_ALLOW:
        return SSL_MODE_ALLOW;
    case SSL_REQUIRE:
        return SSL_MODE_REQUIRE;
    case SSL_VERIFY_CA:
        return SSL_MODE_VERIFY_CA;
    case SSL_VERIFY_HOSTNAME:
        return SSL_MODE_VERIFY_HOST;
    case SSL_VERIFY_DBNAME:
        return SSL_MODE_VERIFY_DBNAME;
    default:
        return "UNKNOWN";
    }
}

void ssl_set_clnt_user(struct sqlclntstate *clnt)
{
    int rc =
        sslio_x509_attr(clnt->sb, gbl_nid_user, clnt->user, sizeof(clnt->user));
    if (rc != 0)
        return;
    clnt->have_user = 1;
    clnt->is_x509_user = 1;
}

void ssl_stats(void)
{
    logmsg(LOGMSG_INFO, "Client SSL mode: %s\n",
           ssl_mode_to_string(gbl_client_ssl_mode));
    if (gbl_client_ssl_mode >= SSL_VERIFY_DBNAME)
        logmsg(LOGMSG_INFO,
               "Verify database name in client certificate: YES (%s)\n",
               OBJ_nid2ln(gbl_nid_dbname));

    logmsg(LOGMSG_INFO, "Replicant SSL mode: %s\n",
           ssl_mode_to_string(gbl_rep_ssl_mode));
    if (gbl_client_ssl_mode >= SSL_VERIFY_DBNAME)
        logmsg(LOGMSG_INFO,
               "Verify database name in replicant certificate: YES (%s)\n",
               OBJ_nid2ln(gbl_nid_dbname));

    logmsg(LOGMSG_INFO, "Certificate: %s\n",
           gbl_cert_file ? gbl_cert_file : "N/A");
    logmsg(LOGMSG_INFO, "Key: %s\n",
           gbl_key_file ? gbl_key_file : "N/A");
    logmsg(LOGMSG_INFO, "CA: %s\n",
           gbl_ca_file ? gbl_ca_file : "N/A");
    logmsg(LOGMSG_INFO, "CRL: %s\n", gbl_ca_file ? gbl_crl_file : "N/A");
    logmsg(LOGMSG_INFO, "Allow remote SQL: %s\n",
           gbl_ssl_allow_remsql ? "YES" : "no");

    if (gbl_sess_cache_sz == 0)
        logmsg(LOGMSG_INFO, "Session Cache Size: unlimited\n");
    else if (gbl_sess_cache_sz < 0)
        logmsg(LOGMSG_INFO, "Session Cache Size: %d\n",
               SSL_SESSION_CACHE_MAX_SIZE_DEFAULT);
    else
        logmsg(LOGMSG_INFO, "Session Cache Size: %ld\n", gbl_sess_cache_sz);

    logmsg(LOGMSG_INFO, "Cipher suites: %s\n", gbl_ciphers);

    if (gbl_nid_user == NID_undef)
        logmsg(LOGMSG_INFO,
               "Mapping client certificates to database users: no\n");
    else
        logmsg(LOGMSG_INFO,
               "Mapping client certificates to database users: YES (%s)\n",
               OBJ_nid2ln(gbl_nid_user));
}
