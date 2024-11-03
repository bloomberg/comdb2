/*
   Copyright 2023 Bloomberg Finance L.P.

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

#include <event2/buffer.h>
#include <poll.h>
#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "bdb_int.h"
#include "cdb2_constants.h"
#include "comdb2.h"
#include "cdb2api.h"
#include "list.h"
#include "logmsg.h"
#include "net_int.h"
#include "reverse_conn.h"
#include "phys_rep.h"
#include "machclass.h"
#include "net_appsock.h"

#define revconn_logmsg(lvl, ...)                                               \
    do {                                                                       \
        logmsg(lvl, "revconn: " __VA_ARGS__);                                  \
    } while (0)

typedef struct reverse_conn_host_st {
    char *dbname;
    char *host;

    pthread_t thd; // Worker thread handle
    pthread_mutex_t mu;
    int worker_state;    // State of the worker thread
    LINKC_T(struct reverse_conn_host_st) lnk;
} reverse_conn_host_tp;

typedef LISTC_T(reverse_conn_host_tp) reverse_conn_host_list_tp;

extern char *gbl_myhostname;
extern char *gbl_physrep_metadb_name;
extern char *gbl_physrep_metadb_host;
extern char gbl_dbname[MAX_DBNAME_LENGTH];

int gbl_revsql_allow_command_exec;
int gbl_revsql_debug = 0;
int gbl_revsql_cdb2_debug;
// 'reverse-connection host' list refresh frequency
int gbl_revsql_host_refresh_freq_sec = 5;
// 'reverse-connection' worker's new connection attempt frequency
int gbl_revsql_connect_freq_sec = 5;

static pthread_t reverse_conn_manager;
static int reverse_conn_manager_running;
static int stop_reverse_conn_manager;

static pthread_mutex_t reverse_conn_hosts_mu = PTHREAD_MUTEX_INITIALIZER;

reverse_conn_host_list_tp reverse_conn_hosts;

int db_is_exiting();
int read_stream(netinfo_type *netinfo_ptr, host_node_type *host_node_ptr,
                SBUF2 *sb, void *inptr, int maxbytes);

enum {
    REVERSE_CONN_WORKER_NEW = 0,
    REVERSE_CONN_WORKER_RUNNING,
    REVERSE_CONN_WORKER_EXITING,
    REVERSE_CONN_WORKER_EXITED,
};

int gbl_revsql_force_rte = 1;

int send_reversesql_request(const char *dbname, const char *host, const char *command)
{
    int rc = 0;

    if (gbl_revsql_debug == 1) {
        revconn_logmsg(LOGMSG_USER, "%s:%d Sending reversesql request to %s@%s\n", __func__, __LINE__, dbname, host);
    }

    SBUF2 *sb = connect_remote_db(NULL, dbname, NULL, (char *)host, 0, gbl_revsql_force_rte);
    if (!sb) {
        revconn_logmsg(LOGMSG_ERROR, "%s:%d Failed to connect to %s:%s\n", __func__, __LINE__, dbname, host);
        return 1;
    }

    int new_fd = sbuf2fileno(sb);
    make_server_socket(new_fd);

    if (db_is_exiting()) {
        if (gbl_revsql_debug == 1) {
            revconn_logmsg(LOGMSG_USER, "%s:%d Comdb2 is exiting\n", __func__, __LINE__);
        }
        rc = 0;
        goto cleanup;
    }

    char msg[512];
    size_t len = snprintf(msg, sizeof(msg), "reversesql\n%s\n%s\n%s\n", gbl_dbname, gbl_myhostname, command);
    if (sbuf2write(msg, len, sb) != len || sbuf2flush(sb) != len) {
        revconn_logmsg(LOGMSG_USER, "%s:%d Failed to send reversesql request fd:%d len:%zu\n",
                       __func__, __LINE__, new_fd, len);
        rc = -1;
        goto cleanup;
    }

    if (gbl_revsql_debug == 1) {
        revconn_logmsg(LOGMSG_USER, "%s:%d Sent '%s' through fd:%d\n", __func__, __LINE__, msg, new_fd);
    }

    struct timeval timeout = {.tv_usec = 100 * 1000};
    return event_base_once(get_main_event_base(), new_fd, EV_READ, do_revconn_evbuffer, NULL, &timeout);

cleanup:
    sbuf2close(sb);
    return rc;
}

int replace_tier_by_hostname(reverse_conn_host_list_tp *new_reverse_conn_hosts) {
    reverse_conn_host_tp *new_host;
    LISTC_FOR_EACH(new_reverse_conn_hosts, new_host, lnk) {
        if (is_valid_mach_class(new_host->host)) {
            cdb2_hndl_tp *hndl;
            int rc;

            if ((rc = cdb2_open(&hndl, new_host->dbname, new_host->host, 0)) != 0) {
                revconn_logmsg(LOGMSG_ERROR, "%s:%d Failed to connect to %s@%s (rc: %d)\n", __func__, __LINE__, new_host->dbname, new_host->host, rc);
                free(new_host->dbname);
                free(new_host->host);
                free(listc_rfl(&new_reverse_conn_hosts, new_host));
                continue;
            }

            const char *cmd = "SELECT host FROM comdb2_cluster WHERE is_master='Y'";
            if ((rc = cdb2_run_statement(hndl, cmd)) == CDB2_OK) {
                while ((rc = cdb2_next_record(hndl)) == CDB2_OK) {
                    free(new_host->host);
                    new_host->host = strdup((char *)cdb2_column_value(hndl, 0));
                }
            }

            cdb2_close(hndl);
        }
    }
    return 0;
}

// Work performed by a 'reverse-connection' worker thread.
static void *reverse_connection_worker(void *args) {
    reverse_conn_host_tp *host = args;
    time_t last_conn_attempt = 0;
    backend_thread_event(thedb, COMDB2_THR_EVENT_START_RDONLY);

    host->worker_state = REVERSE_CONN_WORKER_RUNNING;
    if (gbl_revsql_debug == 1) {
        revconn_logmsg(LOGMSG_USER, "%s:%d 'reverse-connection' worker thread started for %s@%s\n",
                       __func__, __LINE__, host->dbname, host->host);
    }

    while (!db_is_exiting()) {
        time_t now = time(NULL);

        if ((now - last_conn_attempt) < gbl_revsql_connect_freq_sec) {
            sleep(1);
            continue;
        }

        if (!bdb_am_i_coherent(thedb->bdb_env)) {
            if (gbl_revsql_debug == 1) {
                revconn_logmsg(LOGMSG_USER, "%s:%d not starting connection, not coherent\n", __func__, __LINE__);
            }
            sleep(1);
            continue;
        }

        last_conn_attempt = now;

        pthread_mutex_lock(&reverse_conn_hosts_mu);
        {
            pthread_mutex_lock(&host->mu);
            {
                if (host->worker_state == REVERSE_CONN_WORKER_EXITING) {
                    host->worker_state = REVERSE_CONN_WORKER_EXITED;
                    pthread_mutex_unlock(&host->mu);
                    pthread_mutex_unlock(&reverse_conn_hosts_mu);
                    // Exit out of the main while loop
                    break;
                }
            }
            pthread_mutex_unlock(&host->mu);
        }
        pthread_mutex_unlock(&reverse_conn_hosts_mu);

        int rc = send_reversesql_request(host->dbname, host->host, "");
        if (rc != 0) {
          revconn_logmsg(LOGMSG_ERROR, "%s:%d Failed to send 'reversesql' request to %s@%s\n",
                         __func__, __LINE__, host->dbname, host->host);
        } else if (gbl_revsql_debug == 1) {
            revconn_logmsg(LOGMSG_USER, "%s:%d 'reversesql' request sent to %s@%s\n",
                           __func__, __LINE__, host->dbname, host->host);
        }
        sleep(1);
    }
    backend_thread_event(thedb, COMDB2_THR_EVENT_DONE_RDONLY);
    return 0;
}

// Refresh the 'reverse connection host' list
static int refresh_reverse_conn_hosts() {
    reverse_conn_host_tp *old_host;
    reverse_conn_host_tp *new_host;
    reverse_conn_host_tp *tmp;
    cdb2_hndl_tp *repl_metadb = NULL;
    int rc = 0;
    char cmd[400];

    // Remove the 'EXITED' reverse-connection hosts from the main list.
    pthread_mutex_lock(&reverse_conn_hosts_mu);
    {
        LISTC_FOR_EACH_SAFE(&reverse_conn_hosts, old_host, tmp, lnk) {
            if (old_host->worker_state == REVERSE_CONN_WORKER_EXITED) {
                if (gbl_revsql_debug == 1) {
                    revconn_logmsg(LOGMSG_USER, "%s:%d %s@%s removed from 'reverse-connection' hosts list\n",
                                   __func__, __LINE__, old_host->dbname, old_host->host);
                }
                free(old_host->dbname);
                free(old_host->host);
                free(listc_rfl(&reverse_conn_hosts, old_host));
            }
        }
    }
    pthread_mutex_unlock(&reverse_conn_hosts_mu);

    if ((rc = physrep_get_metadb_or_local_hndl(&repl_metadb)) != 0) {
        revconn_logmsg(LOGMSG_ERROR, "%s:%d Failed to get a connection handle for 'replication metadb' (rc: %d)\n",
                       __func__, __LINE__, rc);
        return 1;
    }

    rc = snprintf(cmd, sizeof(cmd),
                  "exec procedure sys.physrep.get_reverse_hosts('%s', '%s')",
                  gbl_dbname, gbl_myhostname);
    if (rc < 0 || rc >= sizeof(cmd)) {
        revconn_logmsg(LOGMSG_ERROR, "Insufficient buffer size!\n");
        rc = 1;
        goto err;
    }

    if (gbl_revsql_debug == 1) {
        revconn_logmsg(LOGMSG_USER, "%s:%d Executing %s\n", __func__, __LINE__, cmd);
    }

    if (gbl_revsql_cdb2_debug == 1) {
        cdb2_set_debug_trace(repl_metadb);
    }

    reverse_conn_host_list_tp new_reverse_conn_hosts;
    listc_init(&new_reverse_conn_hosts, offsetof(reverse_conn_host_tp, lnk));

    if ((rc = cdb2_run_statement(repl_metadb, cmd)) == CDB2_OK) {
        while ((rc = cdb2_next_record(repl_metadb)) == CDB2_OK) {
            char *dbname = (char *)cdb2_column_value(repl_metadb, 0);
            char *host = (char *)cdb2_column_value(repl_metadb, 1);

            new_host = malloc(sizeof(reverse_conn_host_tp));
            if (!new_host) {
                revconn_logmsg(LOGMSG_ERROR, "%s:%d Failed to allocate memory\n", __func__, __LINE__);

                // Free the items added to the list
                LISTC_FOR_EACH_SAFE(&new_reverse_conn_hosts, new_host, tmp, lnk) {
                    free(new_host->dbname);
                    free(new_host->host);
                    free(listc_rfl(&new_reverse_conn_hosts, new_host));
                }

                rc = 1;
                goto err;
            }
            new_host->dbname = strdup(dbname);
            new_host->host = strdup(host);
            new_host->worker_state = REVERSE_CONN_WORKER_NEW;
            pthread_mutex_init(&new_host->mu, NULL);

            if (gbl_revsql_debug == 1) {
                revconn_logmsg(LOGMSG_USER, "%s:%d Adding %s/%s to revconn list\n", __func__, __LINE__, dbname, host);
            }

            // Add to the list
            listc_abl(&new_reverse_conn_hosts, new_host);
        }

        // Close the connection
        cdb2_close(repl_metadb);
    }

    replace_tier_by_hostname(&new_reverse_conn_hosts);

    // Refresh the main 'reverse-connection host' list
    pthread_mutex_lock(&reverse_conn_hosts_mu);

    // Mark all existing 'reverse-connection hosts' as 'EXITING' that are not
    // in the new list.
    LISTC_FOR_EACH(&reverse_conn_hosts, old_host, lnk) {
        int found = 0;
        LISTC_FOR_EACH(&new_reverse_conn_hosts, new_host, lnk) {
            if (strcmp(old_host->dbname, new_host->dbname) == 0 &&
                strcmp(old_host->host, new_host->host) == 0) {
                found = 1;
                break;
            }
        }

        // If the 'old' host is not found in the new list, notify the worker.
        if (found == 0) {
            old_host->worker_state = REVERSE_CONN_WORKER_EXITING;
        }
    }

    // Add all new 'reverse-connection hosts' to the main list.
    LISTC_FOR_EACH_SAFE(&new_reverse_conn_hosts, new_host, tmp, lnk) {
        int found = 0;
        LISTC_FOR_EACH(&reverse_conn_hosts, old_host, lnk) {
            if (strcmp(old_host->dbname, new_host->dbname) == 0 &&
                strcmp(old_host->host, new_host->host) == 0) {
                found = 1;
                break;
            }
        }

        // *Move* from new list to the main list
        if (found == 0) {
            if (gbl_revsql_debug == 1) {
                revconn_logmsg(LOGMSG_USER, "%s:%d %s@%s added to the main 'reverse-connection' hosts list\n",
                               __func__, __LINE__, new_host->dbname, new_host->host);
            }
            listc_abl(&reverse_conn_hosts,
                      listc_rfl(&new_reverse_conn_hosts, new_host));
        } else {
            listc_rfl(&new_reverse_conn_hosts, new_host);
        }
    }
    pthread_mutex_unlock(&reverse_conn_hosts_mu);

    return 0;

err:
    cdb2_close(repl_metadb);
    return rc;
}

static void *reverse_connection_manager(void *args) {
    int rc = 0;
    static time_t last_refreshed = 0;

    listc_init(&reverse_conn_hosts, offsetof(reverse_conn_host_tp, lnk));

    while (!db_is_exiting() && stop_reverse_conn_manager == 0) {
        time_t now = time(NULL);

        if ((now - last_refreshed) < gbl_revsql_host_refresh_freq_sec) {
            sleep(1);
            continue;
        }

        last_refreshed = now;

        // Refresh the 'reverse connection host' list
        if (gbl_revsql_debug == 1) {
            revconn_logmsg(LOGMSG_USER, "%s:%d Refreshing 'reverse-connection' hosts list\n", __func__, __LINE__);
        }

        if ((rc = refresh_reverse_conn_hosts()) != 0) {
            revconn_logmsg(LOGMSG_ERROR, "%s:%d Failed to refresh 'reverse-connection host' list (rc: %d)\n", __func__, __LINE__, rc);
            continue;
        }

        // Create new worker threads
        reverse_conn_host_tp *host;

        pthread_mutex_lock(&reverse_conn_hosts_mu);
        {
            LISTC_FOR_EACH(&reverse_conn_hosts, host, lnk) {
                rc = 0;

                pthread_mutex_lock(&host->mu);
                {
                    if (host->worker_state == REVERSE_CONN_WORKER_NEW) {
                        rc = pthread_create(&host->thd, NULL, reverse_connection_worker, host);
                    }
                }
                pthread_mutex_unlock(&host->mu);

                if (rc != 0) {
                    revconn_logmsg(LOGMSG_ERROR, "%s:%d Failed to create 'reverse-connection host' worker thread for %s@%s\n",
                                   __func__, __LINE__, host->dbname, host->host);
                }
            }
        }
        pthread_mutex_unlock(&reverse_conn_hosts_mu);
    }

    return 0;
}

int start_reverse_connections_manager() {
    if (reverse_conn_manager_running == 1) {
        revconn_logmsg(LOGMSG_ERROR, "Reverse connections manager thread is already running!\n");
        return 0;
    }

    // Start the 'reverse-connection' manager thread
    int rc = pthread_create(&reverse_conn_manager, NULL, reverse_connection_manager, NULL);
    if (rc != 0) {
        revconn_logmsg(LOGMSG_ERROR, "%s:%d pthread_create failed (rc: %d)\n", __func__, __LINE__, rc);
        return rc;
    }

    if (gbl_revsql_debug == 1) {
        revconn_logmsg(LOGMSG_USER, "%s:%d 'reverse-connection' manager thread started\n", __func__, __LINE__);
    }
    reverse_conn_manager_running = 1;

    return 0;
}

int stop_reverse_connections_manager() {
    int rc = 0;

    if (reverse_conn_manager_running == 0) {
        return 0;
    }

    stop_reverse_conn_manager = 1;

    if ((rc = pthread_join(reverse_conn_manager, NULL)) != 0) {
        revconn_logmsg(LOGMSG_ERROR, "Reverse connections manager thread failed to join (rc : %d)\n", rc);
        return 1;
    }
    revconn_logmsg(LOGMSG_USER, "Reverse connections manager thread has stopped\n");

    reverse_conn_manager_running = 0;
    stop_reverse_conn_manager = 0;

    return 0;
}

static char *state2str(int worker_state) {
    switch(worker_state) {
    case REVERSE_CONN_WORKER_NEW: return "new";
    case REVERSE_CONN_WORKER_RUNNING: return "running";
    case REVERSE_CONN_WORKER_EXITING: return "exiting";
    case REVERSE_CONN_WORKER_EXITED: return "exited";
    }
    return "unknown";
}

int dump_reverse_connection_host_list() {
    reverse_conn_host_tp *host;

    revconn_logmsg(LOGMSG_USER, "Reverse-connection host list:\n");
    pthread_mutex_lock(&reverse_conn_hosts_mu);
    {
        LISTC_FOR_EACH(&reverse_conn_hosts, host, lnk) {
            pthread_mutex_lock(&host->mu);
            {
                revconn_logmsg(LOGMSG_USER, "dbname: %s host: %s worker state: %s\n",
                       host->dbname, host->host, state2str(host->worker_state));
            }
            pthread_mutex_unlock(&host->mu);
        }
    }
    pthread_mutex_unlock(&reverse_conn_hosts_mu);
    return 0;
}
