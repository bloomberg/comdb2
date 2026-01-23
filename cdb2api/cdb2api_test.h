#ifndef INCLUDED_CDB2API_TEST_H
#define INCLUDED_CDB2API_TEST_H

#ifdef __cplusplus
extern "C" {
#endif /* __cplusplus */

int get_dbinfo_state(void);
void set_dbinfo_state(int);

int get_num_get_dbhosts(void);
int get_num_skip_dbinfo(void);
int get_num_sockpool_fd(void);
int get_num_sql_connects(void);
int get_num_tcp_connects(void);
int get_num_sockpool_recv(void);
int get_num_sockpool_send(void);
int get_num_sockpool_send_timeouts(void);
int get_num_sockpool_recv_timeouts(void);

void set_fail_dbhosts_invalid_response(int);
void set_fail_dbhosts_bad_response(int);
void set_fail_dbhosts_cant_read_response(int);
void set_fail_dbinfo_invalid_header(int);
void set_fail_dbinfo_invalid_response(int);
void set_fail_dbinfo_no_response(int);

int get_num_cache_lru_evicts(void);
int get_num_cache_hits(void);
int get_num_cache_misses(void);

void set_fail_next(int);
void set_fail_read(int);
void set_fail_reject(int);
void set_fail_sb(int);
void set_fail_send(int);
void set_fail_sockpool(int);
void set_fail_tcp(int);
void set_fail_timeout_sockpool_recv(int);
void set_fail_timeout_sockpool_send(int);

void set_allow_pmux_route(int allow);
void set_atexit_delay(int on);

void set_local_connections_limit(int lim);
int get_num_cached_connections_for(const char *dbname);
void dump_cached_connections(void);
int get_cached_connection_index(const char *dbname);
void test_process_env_vars(void);
int get_max_local_connection_cache_entries(void);
void reset_once(void);
void local_connection_cache_clear(int);

void set_fail_mutex_lock_in_add_ssl_session(int);
void set_ignore_san(int);
void set_fail_ssl_ctx_new(int);
void set_fail_sslio_connect(int);
void set_fail_sslio_read(int);
void set_fail_sslio_write(int);
void set_fail_sslio_zero_return(int);
void set_fail_sslio_syscall(int);
void set_fail_sslio_others(int);
void set_fail_key_ownership(int);
void set_fake_root_key(int);
void set_override_dbname_in_cert(int);
// void set_override_hostname_in_cert(int);
void set_fail_reverse_dns(int);
void set_fail_forward_dns(int);
void set_fail_null_server_cert(int);
void set_fail_null_ssl_ctx(int);
void set_fail_ssl_accept_twice(int);
void set_fail_ssl_new(int);
void set_fail_ssl_negotiation_once(int);
void set_fail_ssl_poll(int);
void set_fail_sslio_close_in_local_cache(int);

void set_cdb2api_test_comdb2db_cfg(const char *);
void set_cdb2api_test_single_cfg(const char *);
void set_cdb2api_test_dbname_cfg(const char *);

struct cdb2_hndl;
const char *get_default_cluster(void);
const char *get_default_cluster_hndl(struct cdb2_hndl *);
int get_gbl_event_version(void);

void cdb2_set_max_retries(int max_retries);
void cdb2_set_min_retries(int min_retries);

void cdb2_cluster_info(cdb2_hndl_tp *hndl, char **cluster, int *ports, int max, int *count);
const char *cdb2_cnonce(cdb2_hndl_tp *hndl);
int cdb2_snapshot_file(cdb2_hndl_tp *hndl, int *file, int *offset);
void cdb2_dump_ports(cdb2_hndl_tp *hndl, FILE *out);

#if defined __cplusplus
}
#endif /* __cplusplus */

#endif /* INCLUDED_CDB2API_TEST_H */
