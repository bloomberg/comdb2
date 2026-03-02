#ifndef INCLUDED_CDB2API_SSL_TEST_H
#define INCLUDED_CDB2API_SSL_TEST_H

#ifdef CDB2API_TEST
#include "cdb2api_test.h"
#define DECLARE_CDB2API_TEST_SWITCH(name)                                                                              \
    extern int name;                                                                                                   \
    void set_##name(int num);

/* fail the pthread_mutex_lock call in cdb2_add_ssl_session().
   this shouldn't cause any failures. */
DECLARE_CDB2API_TEST_SWITCH(fail_mutex_lock_in_add_ssl_session);
/* ignore the Subject Alternative Names (SAN) check in ssl_verify_ca.
   code should check the Common Name (CN) instead. */
DECLARE_CDB2API_TEST_SWITCH(ignore_san);
/* fail SSL_CTX_new() */
DECLARE_CDB2API_TEST_SWITCH(fail_ssl_ctx_new);
/* fail sslio_connect() */
DECLARE_CDB2API_TEST_SWITCH(fail_sslio_connect);
/* fail sslio_write() */
DECLARE_CDB2API_TEST_SWITCH(fail_sslio_write);
/* fail sslio_read() */
DECLARE_CDB2API_TEST_SWITCH(fail_sslio_read);
/* override the rcode from an openssl call to SSL_ERROR_ZERO_RETURN */
DECLARE_CDB2API_TEST_SWITCH(fail_sslio_zero_return);
/* override the rcode from an openssl call to SSL_ERROR_SYSCALL */
DECLARE_CDB2API_TEST_SWITCH(fail_sslio_syscall);
/* override the rcode from an openssl call to SSL_ERROR_NONE */
DECLARE_CDB2API_TEST_SWITCH(fail_sslio_others);
/* override the owner of the private key to id 1 */
DECLARE_CDB2API_TEST_SWITCH(fail_key_ownership);
/* override the owner of the private key to id 0 */
DECLARE_CDB2API_TEST_SWITCH(fake_root_key);
/* override dbname in server certificate to 'cd*b*' to exercise the wildcard matching code. */
DECLARE_CDB2API_TEST_SWITCH(override_dbname_in_cert);
/* override hostname in server certificate to 'hello'. verification should then fail */
DECLARE_CDB2API_TEST_SWITCH(override_hostname_in_cert);
/* fail getnameinfo() */
DECLARE_CDB2API_TEST_SWITCH(fail_reverse_dns);
/* fail gethostbyname() */
DECLARE_CDB2API_TEST_SWITCH(fail_forward_dns);
/* fail SSL_get_peer_certificate() */
DECLARE_CDB2API_TEST_SWITCH(fail_null_server_cert);
/* override the ssl context argument passed to sslio_connect to NULL */
DECLARE_CDB2API_TEST_SWITCH(fail_null_ssl_ctx);
/* ssl_accept twice */
DECLARE_CDB2API_TEST_SWITCH(fail_ssl_accept_twice);
/* fail SSL_new() */
DECLARE_CDB2API_TEST_SWITCH(fail_ssl_new);
/* fail sslio_poll() */
DECLARE_CDB2API_TEST_SWITCH(fail_ssl_poll);

#endif /* CDB2API_TEST */

#endif /* INCLUDED_CDB2API_SSL_TEST_H */
