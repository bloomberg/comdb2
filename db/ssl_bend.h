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
            SSL backend support

 - It reads SSL configuration from the LRL file.
 - It initializes OpenSSL libray.
 - It creates an SSL context.
 *************************************************/

#ifndef _INCLUDED_SSL_BEND_H_
#define _INCLUDED_SSL_BEND_H_

#include <stddef.h>
#include <ssl_support.h>

/* Path to the server certificate. */
extern char *gbl_cert_file;

/* Path to the server private key. */
extern char *gbl_key_file;

/* Path to the trusted CA (certificate authorities). */
extern char *gbl_ca_file;

/* Path to the CRL file (certificate revocation list). */
extern char *gbl_crl_file;

/* SSL session cache size. Special cases are:
   0 -> OpenSSL default;
   negative -> unlimited */
extern long gbl_sess_cache_sz;

/* Allow plaintext remsql even if client SSL is required.
   It makes littile sense to support SSL remote SQL.
   First of all, it is infeasible for a client to verify
   the remote databasae. Second of all, it is also infeasible
   for a remote database to verify the actual client of
   a remsql request. It'd be possible only if the peer database
   had the client private key. Therefore we decide not to
   support SSL remote SQL, but instead to provide this simple knob
   to permit remote SQL when client SSL is required.
   Turn it on at your own risk. */
extern int gbl_ssl_allow_remsql;

/* OpenSSL cipher suites. */
extern const char *gbl_ciphers;

/* Client SSL mode */
extern ssl_mode gbl_client_ssl_mode;

/* Replicant SSL mode */
extern ssl_mode gbl_rep_ssl_mode;

/* SSL context. */
extern SSL_CTX *gbl_ssl_ctx;

/* Process SSL LRL. Return 0 upon success.
   The function only reads SSL configuration.
   It does not create SSL context. */
int ssl_process_lrl(char *line, size_t len);

/* Initialize SSL backend. Return 0 upon success. */
int ssl_bend_init(const char *);

/* NID to be used as user */
extern int gbl_nid_user;

/* NID to be used as database name */
extern int gbl_nid_dbname;

/* Set user to the given nid field in the certificate. */
struct sqlclntstate;
void ssl_set_clnt_user(struct sqlclntstate *clnt);

/* Print SSL information. */
void ssl_stats(void);
#endif
