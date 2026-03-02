#include "cdb2api_ssl_test.h"

#ifdef CDB2API_TEST
#define DEFINE_CDB2API_TEST_SWITCH(name)                                                                               \
    int name;                                                                                                          \
    void set_##name(int num)                                                                                           \
    {                                                                                                                  \
        name = num;                                                                                                    \
    }

DEFINE_CDB2API_TEST_SWITCH(fail_mutex_lock_in_add_ssl_session);
DEFINE_CDB2API_TEST_SWITCH(ignore_san);
DEFINE_CDB2API_TEST_SWITCH(fail_ssl_ctx_new);
DEFINE_CDB2API_TEST_SWITCH(fail_sslio_connect);
DEFINE_CDB2API_TEST_SWITCH(fail_sslio_write);
DEFINE_CDB2API_TEST_SWITCH(fail_sslio_read);
DEFINE_CDB2API_TEST_SWITCH(fail_sslio_zero_return);
DEFINE_CDB2API_TEST_SWITCH(fail_sslio_syscall);
DEFINE_CDB2API_TEST_SWITCH(fail_sslio_others);
DEFINE_CDB2API_TEST_SWITCH(fail_key_ownership);
DEFINE_CDB2API_TEST_SWITCH(fake_root_key);
DEFINE_CDB2API_TEST_SWITCH(override_dbname_in_cert);
DEFINE_CDB2API_TEST_SWITCH(override_hostname_in_cert);
DEFINE_CDB2API_TEST_SWITCH(fail_reverse_dns);
DEFINE_CDB2API_TEST_SWITCH(fail_forward_dns);
DEFINE_CDB2API_TEST_SWITCH(fail_null_server_cert);
DEFINE_CDB2API_TEST_SWITCH(fail_null_ssl_ctx);
DEFINE_CDB2API_TEST_SWITCH(fail_ssl_accept_twice);
DEFINE_CDB2API_TEST_SWITCH(fail_ssl_new);
DEFINE_CDB2API_TEST_SWITCH(fail_ssl_poll);

#endif
