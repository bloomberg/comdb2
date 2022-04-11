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

#include <ctype.h>
#include <errno.h>
#include <netdb.h>
#include <netinet/in.h>
#include <string.h>
#include <time.h>

#include <openssl/asn1.h>
#include <openssl/x509v3.h>

#include <hostname_support.h>
#include <ssl_glue.h>

static int dbname_wildcard_match(const char *s, const char *p)
{
    const char *asterisk = NULL;
    const char *ts = s;
    const char *pc = p;

    /* A pattern can't be all wildcard characters */
    for (; *pc && (*pc == '?' || *pc == '*'); ++pc)
        ;
    if (*pc == 0)
        return 1;

    while (*s) {
        if ((*p == '?') || (tolower(*p) == tolower(*s))) {
            ++s;
            ++p;
        } else if (*p == '*') {
            asterisk = p++;
            ts = s;
        } else if (asterisk) {
            p = asterisk + 1;
            s = ++ts;
        } else {
            return 1;
        }
    }
    for (; *p == '*'; ++p)
        ;
    return (*p == 0) ? 0 : 1;
}

/* This is rougly in line with RFC6125, Section 6.4.3.
   (https://tools.ietf.org/html/rfc6125#section-6.4.3) */
static int hostname_wildcard_match(const char *s, const char *p)
{
    const char *asterisk = NULL;
    const char *ts;
    const char *dotasterisk;

    /* Use optimized libc function first.
       If no exact match, we use wildcard matching
       and accept the overhead. */
    if (strcasecmp(s, p) == 0)
        return 0;

    /* RFC6125 Rule 1 */
    dotasterisk = strstr(p, "*");
    if (strcspn(p, ".") < dotasterisk - p)
        return 1;

    ts = s;
    while (*s) {
        if (tolower(*p) == tolower(*s)) {
            ++s;
            ++p;
        } else if (*p == '*') {
            asterisk = p++;
            ts = s;
        } else if (asterisk && *s != '.') {
            p = asterisk + 1;
            s = ++ts;
        } else {
            return 1;
        }
    }
    for (; *p == '*'; ++p)
        ;

    /* No wildcard match */
    return (*p != 0);
}

#if (OPENSSL_VERSION_NUMBER < 0x10100000L) || defined(LIBRESSL_VERSION_NUMBER)
#    define ASN1_STRING_get0_data ASN1_STRING_data
#endif

/* 0: okay. -1: no san. 1: no match. */
static int ssl_verify_san(const char *hostname, const X509 *cert)
{
    STACK_OF(GENERAL_NAME) *peersan;
    const GENERAL_NAME *name;
    const char *dnsname;
    int rc, ii, len;

    peersan = (STACK_OF(GENERAL_NAME) *)
        X509_get_ext_d2i((X509 *)cert, NID_subject_alt_name, NULL, NULL);
    if (peersan == NULL)
        return -1;

    len = sk_GENERAL_NAME_num(peersan);
    for (ii = 0, rc = 1; ii != len; ++ii) {
        name = sk_GENERAL_NAME_value(peersan, ii);
        if (name->type != GEN_DNS)
            continue;
        dnsname = (const char *)ASN1_STRING_get0_data(name->d.dNSName);

        /* CVE-2009-4034 */
        if (ASN1_STRING_length(name->d.dNSName) != strlen(dnsname)) {
            rc = 1;
            break;
        }

        if (hostname_wildcard_match(hostname, dnsname) == 0) {
            rc = 0;
            break;
        }
    }

    sk_GENERAL_NAME_pop_free(peersan, GENERAL_NAME_free);
    return rc;
}

static int ssl_verify_cn(const char *hostname, const X509 *cert)
{
    int rc;
    /* RFC 2181 */
    char cn[256];
    rc = ssl_x509_get_attr(cert, NID_commonName, cn, sizeof(cn));
    if (rc != 0)
        return 1;
    cn[255] = 0;
    return hostname_wildcard_match(hostname, cn);
}

int ssl_x509_get_attr(const X509 *cert, int nid, char *out, size_t len)
{
    int fldindx;
    X509_NAME  *certname;
    X509_NAME_ENTRY *fld;
    ASN1_STRING *fldasn1;
    const char *fldstr;

    /* Fast return if nid is undefined */
    if (nid == NID_undef)
        return EINVAL;

    certname = X509_get_subject_name((X509 *)cert);
    if (certname == NULL)
        return EINVAL;

    fldindx = X509_NAME_get_index_by_NID(certname, nid, -1);
    if (fldindx < 0)
        return EINVAL;

    fld = X509_NAME_get_entry(certname, fldindx);
    if (fld == NULL)
        return EINVAL;

    fldasn1 = X509_NAME_ENTRY_get_data(fld);
    if (fldasn1 == NULL)
        return EINVAL;

    fldstr = (const char *)ASN1_STRING_get0_data(fldasn1);
    /* CVE-2009-4034 */
    if (ASN1_STRING_length(fldasn1) != strlen(fldstr))
        return EINVAL;

    strncpy(out, fldstr, len);
    return 0;
}

int ssl_verify_dbname(X509 *cert, const char *dbname, int nid)
{
    size_t sz = strlen(dbname) + 1;
    char dbname_in_cert[sz];
    int rc = ssl_x509_get_attr(cert, nid, dbname_in_cert, sz);
    if (rc != 0)
        return rc;
    if (strncasecmp(dbname_in_cert, dbname, sz) == 0)
        return 0;
    dbname_in_cert[sz - 1] = 0;
    return dbname_wildcard_match(dbname, dbname_in_cert);
}

int ssl_verify_hostname(X509 *cert, int fd)
{
    /*
    ** 1) Perform a reverse DNS lookup to get the hostname
    **    associated with the source address.
    ** 2) Perform a forward DNS lookup to get a list of addresses
    **    associated with the hostname.
    ** 3) If the source address is in the list, proceed;
    **    otherwise, return 1 immediately.
    ** 4) Perform SAN/CN validation.
    **
    ** The forward DNS lookup is necessary in case an attacker is
    ** in control of reverse DNS for the source IP.
    */
    struct sockaddr_in peeraddr;
    struct in_addr *peer_in_addr, **p_fwd_in_addr;
    socklen_t len = sizeof(struct sockaddr_in);
    int rc, found_addr;
    struct hostent *hp = NULL;

    /* Reverse lookup the hostname */
    char peerhost[NI_MAXHOST];
    if (get_hostname_by_fileno_v2(fd, peerhost, sizeof(peerhost))) {
        return 1;
    }

    /* Should always succeed as get_origin_mach_by_buf()
       returns a valid hostname. */
    getpeername(fd, (struct sockaddr *)&peeraddr, &len);

    /* Forward lookup the IPs */

#   if defined(__APPLE__)
    hp = gethostbyname(peerhost);
#   elif defined(_LINUX_SOURCE)
    int herr;
    char buf[8192];
    struct hostent hostbuf;
    gethostbyname_r(peerhost, &hostbuf, buf, sizeof(buf), &hp, &herr);
#   elif defined(_SUN_SOURCE)
    int herr;
    char buf[8192];
    struct hostent hostbuf;
    hp = gethostbyname_r(peerhost, &hostbuf, buf, sizeof(buf), &herr);
#   else
    hp = gethostbyname(peerhost);
#   endif

    if (hp == NULL) {
        return 1;
    }

    /* Find the source address in the address list returned
       by the forward DNS lookup. */
    for (found_addr = 0, peer_in_addr = &peeraddr.sin_addr,
        p_fwd_in_addr = (struct in_addr **)hp->h_addr_list;
         *p_fwd_in_addr != NULL; ++p_fwd_in_addr) {
        if (peer_in_addr->s_addr == (*p_fwd_in_addr)->s_addr) {
            found_addr = 1;
            break;
        }
    }

    /* Suspicious PTR record. Reject it. */
    if (!found_addr) return 1;

    /* Trust localhost */
    if (strcasecmp(peerhost, "localhost") == 0 ||
        strcasecmp(peerhost, "localhost.localdomain") == 0) {
        return 0;
    }

    /* Per RFC 6125, If SANs are presented, they must be used and
       the Comman Name must be ignored. */
    rc = ssl_verify_san(peerhost, cert);
    if (rc == -1) {
        rc = ssl_verify_cn(peerhost, cert);
    }

    return rc;
}

#if SBUF2_SERVER
extern int gbl_ssl_allow_localhost;
int ssl_whitelisted(const char *host)
{
    if (gbl_ssl_allow_localhost) {
        if (host != NULL) {
            if (strcasecmp(host, "localhost") || strcasecmp(host, "localhost.localdomain")) {
                return 1;
            }
        }
    }
    return 0;
}

void ssl_extend_session_lifetime(SSL *ssl)
{
    /* If a session is reused, extend its lifetime. OpenSSL internally has
       a session TTL of 300 seconds. We modify the session's creation time
       to the current time so that the session can stay around for another
       300 seconds. Essentially, as long as a session is getting reused,
       it'll never expire. Otherwise it'll age out after 300 seconds (or more,
       depending on when OpenSSL checks for expired sessions) */
    if (ssl != NULL && SSL_session_reused(ssl))
        SSL_SESSION_set_time(SSL_get0_session(ssl), time(NULL));
}
#endif
