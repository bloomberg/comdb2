/*
   Copyright 2021 Bloomberg Finance L.P.

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

#include <bb_getopt_long.h>
#include <cdb2api.h>
#include <string.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <sys/ioctl.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <list>
#include <vector>

typedef int (*cmd_handler)(int, char **, std::string &);

bool opt_verbose;
bool completed;

int opt_chunk_size = 50;

std::string opt_tier;
std::string dbname;
std::string master_host;
std::string opt_file;
std::string opt_command;

pthread_mutex_t mu = PTHREAD_MUTEX_INITIALIZER;

cdb2_hndl_tp *gbl_cdb2_handle = NULL;

enum {
    EXEC_ON_MASTER = 1 << 0,
    TRACK_PROGRESS = 1 << 1,
};

struct Command {
    cmd_handler handler;
    int flags;
};

typedef struct {
    int thread_id;
    int thread_sub_id;
    char *stage;
    char *status;
    unsigned long long processed_records;
    unsigned long long total_records;
    int rate;
    int remaining_time_sec;
} Progress_bar;

std::list<Progress_bar> progress_bar;

/* clang-format off */
 const char *usage_text =
    "Usage: cdb2ctl [options] dbname[@tier] <command>\n"
    "    Where <command> can be one of:\n"
    "        add tablename schema.csc2   : Create a table\n"
    "        alter tablename schema.csc2 : Modify a table schema\n"
    "        analyze tablename           : Analyze a table\n"
    "        drop tablename              : Drop a table\n"
    "        exec-chunk command          : Execute a set of commands in chunks\n"
    "        exec-nodes command          : Execute a command individually on\n"
    "                                      all the nodes of the cluster\n"
    "        fastinit tablename          : Truncate a table\n"
    "        rebuild tablename           : Rebuild a table\n"
    "        truncate tablename          : Truncate a table\n"
    "        verify tablename            : Verify a table\n"
    "\n"
    "Valid options include:\n"
    "    --file, -f file                 : File containing the commands\n"
    "    --size, -s n                    : Chunk size (default: 50)\n"
    "    --verbose, -v                   : Verbose output\n"
    "";
/* clang-format on */

static void usage(void)
{
    std::cout << usage_text << std::endl;
    exit(1);
}

static void verbose(std::string msg)
{
    if (!opt_verbose)
        return;
    std::cout << msg << std::endl;
}

static void hexdump(std::string &result, void *datap, int len)
{
    uint8_t *data = (uint8_t *)datap;
    int i;
    char s[100];
    for (i = 0; i < len; i++) {
        sprintf(s, "%02x", (unsigned int)data[i]);
        result += s;
    }
}

void dumpstring(std::string &result, char *s, int quotes, int quote_quotes)
{
    if (quotes)
        result += "'";
    while (*s) {
        if (*s == '\'' && quote_quotes)
            result += "''";
        else
            result += (char)*s;
        s++;
    }
    if (quotes)
        result += "'";
}

static void append_column_value(cdb2_hndl_tp *hndl, int col,
                                std::string &result)
{
    void *val;
    char s[1024];

    val = cdb2_column_value(hndl, col);

    if (val == NULL) {
        sprintf(s, "%s=NULL", cdb2_column_name(hndl, col));
        result += s;
        return;
    }

    switch (cdb2_column_type(hndl, col)) {
    case CDB2_INTEGER:
        sprintf(s, "%s=%lld", cdb2_column_name(hndl, col), *(long long *)val);
        result += s;
        break;
    case CDB2_REAL:
        sprintf(s, "%s=", cdb2_column_name(hndl, col));
        result += s;
        sprintf(s, "%f", *(double *)val);
        result += s;
        break;
    case CDB2_CSTRING:
        sprintf(s, "%s=", cdb2_column_name(hndl, col));
        result += s;
        dumpstring(result, (char *)val, 1, 0);
        break;
    case CDB2_BLOB:
        sprintf(s, "%s=", cdb2_column_name(hndl, col));
        result += s;
        sprintf(s, "x'");
        result += s;
        hexdump(result, val, cdb2_column_size(hndl, col));
        sprintf(s, "'");
        result += s;
        break;
    case CDB2_DATETIME: {
        cdb2_client_datetime_t *cdt = (cdb2_client_datetime_t *)val;
        sprintf(s, "%s=", cdb2_column_name(hndl, col));
        result += s;
        sprintf(s, "\"%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%3.3u %s\"",
                cdt->tm.tm_year + 1900, cdt->tm.tm_mon + 1, cdt->tm.tm_mday,
                cdt->tm.tm_hour, cdt->tm.tm_min, cdt->tm.tm_sec, cdt->msec,
                cdt->tzname);
        result += s;
        break;
    }
    case CDB2_DATETIMEUS: {
        cdb2_client_datetimeus_t *cdt = (cdb2_client_datetimeus_t *)val;
        sprintf(s, "%s=", cdb2_column_name(hndl, col));
        result += s;
        sprintf(s, "\"%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%6.6u %s\"",
                cdt->tm.tm_year + 1900, cdt->tm.tm_mon + 1, cdt->tm.tm_mday,
                cdt->tm.tm_hour, cdt->tm.tm_min, cdt->tm.tm_sec, cdt->usec,
                cdt->tzname);
        result += s;
        break;
    }
    case CDB2_INTERVALYM: {
        cdb2_client_intv_ym_t *ym = (cdb2_client_intv_ym_t *)val;
        sprintf(s, "%s=", cdb2_column_name(hndl, col));
        result += s;
        sprintf(s, "\"%s%u-%u\"", (ym->sign < 0) ? "- " : "", ym->years,
                ym->months);
        result += s;
        break;
    }
    case CDB2_INTERVALDS: {
        cdb2_client_intv_ds_t *ds = (cdb2_client_intv_ds_t *)val;
        sprintf(s, "%s=", cdb2_column_name(hndl, col));
        result += s;
        sprintf(s, "\"%s%u %2.2u:%2.2u:%2.2u.%3.3u\"",
                (ds->sign < 0) ? "- " : "", ds->days, ds->hours, ds->mins,
                ds->sec, ds->msec);
        result += s;
        break;
    }
    case CDB2_INTERVALDSUS: {
        cdb2_client_intv_dsus_t *ds = (cdb2_client_intv_dsus_t *)val;
        sprintf(s, "%s=", cdb2_column_name(hndl, col));
        result += s;
        sprintf(s, "\"%s%u %2.2u:%2.2u:%2.2u.%6.6u\"",
                (ds->sign < 0) ? "- " : "", ds->days, ds->hours, ds->mins,
                ds->sec, ds->usec);
        result += s;
        break;
    }
    }
}

static int connect()
{
    int rc = cdb2_open(&gbl_cdb2_handle, dbname.c_str(), opt_tier.c_str(), 0);
    if (rc != 0) {
        std::cerr << "failed to open connection handle (rc: " << rc << ")"
                  << std::endl;
        exit(1);
    }

    verbose("connection successful");
    return 0;
}

static int connect_to_master()
{
    int rc = cdb2_run_statement(
        gbl_cdb2_handle,
        "SELECT host FROM comdb2_cluster WHERE is_master = 'Y'");
    if (rc != 0) {
        std::cerr << "failed to execute command (rc: " << rc
                  << " error: " << cdb2_errstr(gbl_cdb2_handle) << ")"
                  << std::endl;
        exit(1);
    }
    while ((rc = cdb2_next_record(gbl_cdb2_handle)) == CDB2_OK) {
        char *host = (char *)cdb2_column_value(gbl_cdb2_handle, 0);
        dumpstring(master_host, host, 0, 0);
    }
    if (master_host.empty()) {
        std::cerr << "failed to get the master node host" << std::endl;
        exit(1);
    }

    // Close the old connection (TODO (NC): Check if we are already
    // connected to the master?)
    cdb2_close(gbl_cdb2_handle);

    // Try connecting to the master node.
    rc = cdb2_open(&gbl_cdb2_handle, dbname.c_str(), master_host.c_str(),
                   CDB2_DIRECT_CPU);
    if (rc != 0) {
        std::cerr << "failed to connect to the master node (rc: " << rc << ")"
                  << std::endl;
        exit(1);
    }
    verbose("connected to the master node");
    return 0;
}

static int execute_command(std::string &cmd, std::string &result,
                           cdb2_hndl_tp *cdb2_handle = gbl_cdb2_handle)
{
    int rc;

    rc = cdb2_run_statement(cdb2_handle, cmd.c_str());
    if (!rc) {
        int ncols = cdb2_numcolumns(cdb2_handle);

        while ((rc = cdb2_next_record(cdb2_handle)) == CDB2_OK) {
            for (int col = 0; col < ncols; col++) {
                append_column_value(cdb2_handle, col, result);
                if (col != ncols - 1) {
                    result += ", ";
                }
            }
            result += "\n";
        }
    }

    if (rc == CDB2_OK_DONE) {
        rc = CDB2_OK;
    }

    pthread_mutex_lock(&mu);
    completed = true;
    pthread_mutex_unlock(&mu);

    return rc;
}

static int execute_transaction(std::list<std::string> &cmd, std::string &result,
                               cdb2_hndl_tp *cdb2_handle = gbl_cdb2_handle)
{
    char set_cmd[32];
    int rc;
    std::list<std::string>::iterator it;

    snprintf(set_cmd, sizeof(set_cmd), "SET TRANSACTION CHUNK %d",
             opt_chunk_size);
    rc = cdb2_run_statement(cdb2_handle, set_cmd);
    if (rc)
        return rc;

    rc = cdb2_run_statement(cdb2_handle, "BEGIN");
    if (rc)
        return rc;

    for (it = cmd.begin(); !rc && it != cmd.end(); ++it) {
        rc = cdb2_run_statement(cdb2_handle, (*it).c_str());
        if (!rc) {
            int ncols = cdb2_numcolumns(cdb2_handle);

            while ((rc = cdb2_next_record(cdb2_handle)) == CDB2_OK) {
                for (int col = 0; col < ncols; col++) {
                    append_column_value(cdb2_handle, col, result);
                    if (col != ncols - 1) {
                        result += ", ";
                    }
                }
                result += "\n";
            }
        }

        if (rc == CDB2_OK_DONE) {
            rc = CDB2_OK;
        }
    }

    if (rc) {
        rc = cdb2_run_statement(cdb2_handle, "ROLLBACK");
    } else {
        rc = cdb2_run_statement(cdb2_handle, "COMMIT");
    }

    pthread_mutex_lock(&mu);
    completed = true;
    pthread_mutex_unlock(&mu);

    return rc;
}

#define STAGE_WIDTH 23
#define STATUS_WIDTH 10
#define PCT_WIDTH 6
#define REC_WIDTH 20
#define RATE_WIDTH 16
#define REMAINING_TIME_WIDTH 20

int stage_width;
int status_width;
int pct_width;
int rec_width;
int rate_width;
int remaining_time_width;
int lines_printed;

static void sigwinch_handler(int sig)
{
    struct winsize win_size;
    int total_width;

    ioctl(STDOUT_FILENO, TIOCGWINSZ, &win_size);
    const int max_width = win_size.ws_col - 10;

    total_width = 0;
    stage_width = 0;
    status_width = 0;
    pct_width = 0;
    rec_width = 0;
    rate_width = 0;
    remaining_time_width = 0;

    total_width += STAGE_WIDTH;
    if (max_width > total_width) {
        stage_width = STAGE_WIDTH;
    }
    total_width += STATUS_WIDTH;
    if (max_width > total_width) {
        status_width = STATUS_WIDTH;
    }
    total_width += PCT_WIDTH;
    if (max_width > total_width) {
        pct_width = PCT_WIDTH;
    }
    total_width += REC_WIDTH;
    if (max_width > total_width) {
        rec_width = REC_WIDTH;
    }
    total_width += RATE_WIDTH;
    if (max_width > total_width) {
        rate_width = RATE_WIDTH;
    }
    total_width += REMAINING_TIME_WIDTH;
    if (max_width > total_width) {
        remaining_time_width = REMAINING_TIME_WIDTH;
    }

    if (lines_printed > 0) {
        for (int i = 0; i < lines_printed; ++i) {
            printf("%*s", win_size.ws_col, " ");
            printf("\033[A");
        }
        printf("%*s", win_size.ws_col, " ");
    }
}

static void refresh_progress_bars()
{
    bool reposition_cursor = true;

    int lines = progress_bar.size();

    // Just have to reposition the cursor once.
    if (lines_printed > 0) {
        if (reposition_cursor || lines_printed != lines) {
            reposition_cursor = false;
            for (int i = 0; i < lines_printed; ++i) {
                printf("\033[A");
            }
        }
    }

    lines_printed = lines;

    std::list<Progress_bar>::iterator it;
    for (it = progress_bar.begin(); it != progress_bar.end(); ++it) {
        int pct = 0;
        int progress = 0;

        if (it->total_records > 0 && it->processed_records > 0) {
            pct = ((float(it->processed_records) / float(it->total_records)) *
                   100);

            char pct_buf[PCT_WIDTH];
            snprintf(pct_buf, sizeof(pct_buf), "%d%%", pct);

            char rec_buf[REC_WIDTH];
            snprintf(rec_buf, sizeof(rec_buf), "(%lld/%lld)",
                     it->processed_records, it->total_records);

            char rate_buf[RATE_WIDTH];
            snprintf(rate_buf, sizeof(rate_buf), "%d recs/sec", it->rate);

            char remaining_time_buf[REMAINING_TIME_WIDTH];
            snprintf(remaining_time_buf, sizeof(remaining_time_buf),
                     "%d secs remaining", it->remaining_time_sec);

            printf("%-*.*s -- %*.*s %*.*s %*.*s %*.*s %*.*s\n", stage_width,
                   stage_width, it->stage, status_width, status_width,
                   it->status, pct_width, pct_width, pct_buf, rec_width,
                   rec_width, rec_buf, rate_width, rate_width, rate_buf,
                   remaining_time_width, remaining_time_width,
                   remaining_time_buf);
        } else {
            printf("%-*.*s -- %*.*s\n", stage_width, stage_width, it->stage,
                   status_width, status_width, it->status);
        }
    }
}

static void *execute_progress_query_thd(void *table_name)
{
    cdb2_hndl_tp *progress_cdb2_handle = NULL;
    char query[200];
    int rc;
    int mon_start_time;
    int row_count;
    int col_count;
    Progress_bar bar;
    bool final_pass_done = false;

    mon_start_time = time(NULL);

    snprintf(
        query, sizeof(query),
        "select * from comdb2_progress where name='%s' and seed in "
        "(select seed from comdb2_progress where name='%s' and start_time > %d "
        "order by start_time desc limit 1) order by start_time",
        (char *)table_name, (char *)table_name, mon_start_time);

retry:
    if (progress_cdb2_handle == NULL) {
        rc = cdb2_open(&progress_cdb2_handle, dbname.c_str(),
                       master_host.c_str(), CDB2_DIRECT_CPU);
        if (rc != 0) {
            std::cerr << "failed to open connection handle (rc: " << rc << ")"
                      << std::endl;
            return NULL;
        }
    }

    rc = cdb2_run_statement(progress_cdb2_handle, query);
    if (rc != 0) {
        if (rc == -3) {
            // comdb2_progress does not exist
            return NULL;
        }

        std::cerr << "failed to execute command (rc: " << rc
                  << " error: " << cdb2_errstr(progress_cdb2_handle) << ")"
                  << std::endl;
        return NULL;
    }

    col_count = cdb2_numcolumns(progress_cdb2_handle);

    row_count = 0;
    while ((rc = cdb2_next_record(progress_cdb2_handle)) == CDB2_OK) {
        void *val;
        // thread_id
        val = cdb2_column_value(progress_cdb2_handle, 3);
        bar.thread_id = *(long long *)val;
        // thread_sub_id
        val = cdb2_column_value(progress_cdb2_handle, 4);
        bar.thread_sub_id = *(long long *)val;
        // stage
        val = cdb2_column_value(progress_cdb2_handle, 6);
        bar.stage = strdup((char *)val);
        // status
        val = cdb2_column_value(progress_cdb2_handle, 7);
        bar.status = strdup((char *)val);
        // processed_records
        val = cdb2_column_value(progress_cdb2_handle, 10);
        bar.processed_records = *(long long *)val;
        // total_records
        val = cdb2_column_value(progress_cdb2_handle, 11);
        bar.total_records = *(long long *)val;
        // rate
        val = cdb2_column_value(progress_cdb2_handle, 12);
        bar.rate = *(int *)val;
        // remaining_time
        val = cdb2_column_value(progress_cdb2_handle, 13);
        bar.remaining_time_sec = *(int *)val;

        bool found = false;
        std::list<Progress_bar>::iterator it;
        for (it = progress_bar.begin(); it != progress_bar.end(); ++it) {
            if ((it->thread_id == bar.thread_id) &&
                (it->thread_sub_id == bar.thread_sub_id) &&
                (strcmp(it->stage, bar.stage) == 0)) {
                found = true;
                it->stage = bar.stage;
                it->status = bar.status;
                it->processed_records = bar.processed_records;
                it->total_records = bar.total_records;
                it->rate = bar.rate;
                it->remaining_time_sec = bar.remaining_time_sec;
                break;
            }
        }

        if (!found) {
            progress_bar.push_back(bar);
        }

        ++row_count;
    }

    refresh_progress_bars();

    pthread_mutex_lock(&mu);
    if (!completed) {
        pthread_mutex_unlock(&mu);
        usleep(200000);
        goto retry;
    }

    if (completed && !final_pass_done) {
        pthread_mutex_unlock(&mu);
        usleep(200000);
        final_pass_done = true;
        goto retry;
    }
    pthread_mutex_unlock(&mu);

    if (rc == CDB2_OK_DONE) {
        rc = CDB2_OK;
    }

    return NULL;
}

static int track_progress(pthread_t *thr, const char *table_name)
{
    int rc;

    rc = pthread_create(thr, NULL, execute_progress_query_thd,
                        (void *)table_name);
    if (rc) {
        std::cerr << "failed to create thread" << strerror(errno) << std::endl;
        exit(1);
    }

    return 0;
}

static int handle_add_table_cmd(int argc, char **argv, std::string &result)
{
    verbose("executing 'add'");

    if (argc != 2) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    std::string table_name(argv[0]);
    std::string cmd;
    std::string line;

    cmd += "CREATE TABLE " + table_name + " { ";

    std::ifstream schema_file(argv[1]);
    if (!schema_file.is_open()) {
        std::cerr << "failed to open file" << std::endl;
        exit(1);
    }

    while (getline(schema_file, line)) {
        cmd += line + "\n";
    }

    cmd += "}";

    verbose("adding schema:\n" + cmd);

    int rc = execute_command(cmd, result);
    if (rc != 0) {
        result += "failed to create table\n";
    } else {
        result += "table created\n";
    }

    return rc;
}

static int handle_alter_table_cmd(int argc, char **argv, std::string &result)
{
    pthread_t thr;
    verbose("executing 'alter'");

    if (argc != 2) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    std::string table_name(argv[0]);
    std::string cmd;
    std::string line;

    cmd += "ALTER TABLE " + table_name + " { ";

    std::ifstream schema_file(argv[1]);
    if (!schema_file.is_open()) {
        std::cerr << "failed to open file" << std::endl;
        exit(1);
    }

    while (getline(schema_file, line)) {
        cmd += line + "\n";
    }

    cmd += "}";

    verbose("alter schema:\n" + cmd);

    track_progress(&thr, table_name.c_str());

    int rc = execute_command(cmd, result);
    if (rc != 0) {
        result += "failed to alter table\n";
    } else {
        result += "table altered\n";
    }

    pthread_join(thr, NULL);

    return rc;
}

static int handle_analyze_table_cmd(int argc, char **argv, std::string &result)
{
    pthread_t thr;

    verbose("executing 'analyze'");

    if (argc != 1) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    std::string table_name(argv[0]);

    std::string cmd = "ANALYZE " + table_name;

    verbose("analyze table:\n" + cmd);

    track_progress(&thr, table_name.c_str());

    int rc = execute_command(cmd, result);
    if (rc != 0) {
        result += "failed to analyze table\n";
    } else {
        result += "table analyzed\n";
    }

    pthread_join(thr, NULL);

    return 0;
}

static int handle_drop_table_cmd(int argc, char **argv, std::string &result)
{
    verbose("executing 'drop'");

    if (argc != 1) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    std::string table_name(argv[0]);
    std::string cmd;

    cmd += "DROP TABLE " + table_name;

    verbose("drop table:\n" + cmd);

    int rc = execute_command(cmd, result);
    if (rc != 0) {
        result += "failed to drop table\n";
    } else {
        result += "table dropped\n";
    }

    return rc;
}

static int handle_exec_chunk_cmd(int argc, char **argv, std::string &result)
{
    int c;
    int rc = 0;
    int opt_index = 0;
    std::list<std::string> cmd;

    verbose("executing 'exec-chunk'");

    /* The command could be supplied as command line argument. */
    /* There must be at least 1 more argument */
    if (argc > 0) {
        opt_command = argv[0];
    }

    if (opt_file.empty() && opt_command.empty()) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    /* Prefer reading the commands from file if specified. */
    if (!opt_file.empty()) {
        std::string line;
        std::ifstream tx_file(opt_file);
        if (!tx_file.is_open()) {
            std::cerr << "failed to open file" << std::endl;
            exit(1);
        }

        while (getline(tx_file, line)) {
            cmd.push_back(line);
        }
    } else {
        cmd.push_back(opt_command);
    }
    rc = execute_transaction(cmd, result);

    return rc;
}

typedef struct {
    std::string host;
    std::string command;
    std::string result;
    int rc;
} Work;

static void *execute_command_thd(void *info)
{
    cdb2_hndl_tp *cdb2_handle = NULL;
    Work *w = (Work *)info;

    int rc = cdb2_open(&cdb2_handle, dbname.c_str(), w->host.c_str(),
                       CDB2_DIRECT_CPU);
    if (rc != 0) {
        std::cerr << "failed to connect to '" << w->host << "' (rc: " << rc
                  << ")" << std::endl;
        exit(1);
    }
    w->rc = execute_command(w->command, w->result, cdb2_handle);
    cdb2_close(cdb2_handle);
    return NULL;
}
static int handle_exec_nodes_cmd(int argc, char **argv, std::string &result)
{
    int rc = 0;

    verbose("executing 'exec-nodes'");
    if (argc != 1) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    std::string cmd(argv[0]);

    rc = cdb2_run_statement(gbl_cdb2_handle, "SELECT host FROM comdb2_cluster");
    if (rc != 0) {
        std::cerr << "failed to execute command (rc: " << rc
                  << " error: " << cdb2_errstr(gbl_cdb2_handle) << ")"
                  << std::endl;
        exit(1);
    }

    std::vector<std::string> hosts;
    while ((rc = cdb2_next_record(gbl_cdb2_handle)) == CDB2_OK) {
        std::string host;
        char *val = (char *)cdb2_column_value(gbl_cdb2_handle, 0);
        dumpstring(host, (char *)val, 0, 0);
        hosts.push_back(host);
    }

    std::sort(hosts.begin(), hosts.end());

    int node_count = hosts.size();
    pthread_t thr[node_count];
    Work work[node_count];

    for (int i = 0; i < node_count; ++i) {
        work[i].host = hosts[i];
        work[i].command = cmd;
        rc = pthread_create(&thr[i], NULL, execute_command_thd,
                            (void *)&work[i]);
        if (rc) {
            std::cerr << "failed to create thread" << strerror(errno)
                      << std::endl;
            exit(1);
        }
    }
    /* Wait for all threads to exit. */
    for (int i = 0; i < node_count; ++i) {
        pthread_join(thr[i], NULL);
    }

    for (int i = 0; i < node_count; ++i) {
        if (i > 0)
            result += "\n";
        result += work[i].host + ":\n" + work[i].result;
    }

    return 0;
}

static int handle_rebuild_table_cmd(int argc, char **argv, std::string &result)
{
    pthread_t thr;

    verbose("executing 'rebuild'");

    if (argc != 1) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    std::string table_name(argv[0]);

    std::string cmd = "REBUILD " + table_name;

    verbose("rebuild table:\n" + cmd);

    track_progress(&thr, table_name.c_str());

    int rc = execute_command(cmd, result);
    if (rc != 0) {
        result += "failed to rebuild table\n";
    } else {
        result += "table rebuilt\n";
    }

    pthread_join(thr, NULL);

    return rc;
}

static int handle_truncate_table_cmd(int argc, char **argv, std::string &result)
{
    verbose("executing 'truncate'");

    if (argc != 1) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    std::string table_name(argv[0]);
    std::string cmd;

    cmd += "TRUNCATE TABLE " + table_name;

    verbose("truncate table:\n" + cmd);

    int rc = execute_command(cmd, result);
    if (rc != 0) {
        result += "failed to truncate table\n";
    } else {
        result += "table truncated\n";
    }

    return rc;
}

static int handle_verify_table_cmd(int argc, char **argv, std::string &result)
{
    pthread_t thr;

    verbose("executing 'verify'");

    if (argc != 1) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    std::string table_name(argv[0]);

    std::string cmd =
        "EXEC PROCEDURE sys.cmd.verify('" + table_name + "', 'parallel')";

    verbose("verify table:\n" + cmd);

    track_progress(&thr, table_name.c_str());

    int rc = execute_command(cmd, result);
    if (rc != 0) {
        result += "failed to verify table\n";
    } else {
        result += "table verified\n";
    }

    pthread_join(thr, NULL);

    return rc;
}

int main(int argc, char **argv)
{
    int c;
    int opt_index = 0;
    std::map<std::string, Command> cmd_hash;
    std::map<std::string, Command>::iterator it;
    std::string result;

    signal(SIGWINCH, sigwinch_handler);

    // Initialize globals
    sigwinch_handler(0);

    cmd_hash.insert(std::pair<std::string, Command>(
        "add", {handle_add_table_cmd, EXEC_ON_MASTER | TRACK_PROGRESS}));
    cmd_hash.insert(std::pair<std::string, Command>(
        "alter", {handle_alter_table_cmd, EXEC_ON_MASTER | TRACK_PROGRESS}));
    cmd_hash.insert(std::pair<std::string, Command>(
        "analyze",
        {handle_analyze_table_cmd, EXEC_ON_MASTER | TRACK_PROGRESS}));
    cmd_hash.insert(std::pair<std::string, Command>(
        "drop", {handle_drop_table_cmd, EXEC_ON_MASTER | TRACK_PROGRESS}));
    cmd_hash.insert(std::pair<std::string, Command>(
        "exec-chunk", {handle_exec_chunk_cmd, 0}));
    cmd_hash.insert(std::pair<std::string, Command>(
        "exec-nodes", {handle_exec_nodes_cmd, 0}));
    cmd_hash.insert(std::pair<std::string, Command>(
        "fastinit",
        {handle_truncate_table_cmd, EXEC_ON_MASTER | TRACK_PROGRESS}));
    cmd_hash.insert(std::pair<std::string, Command>(
        "rebuild",
        {handle_rebuild_table_cmd, EXEC_ON_MASTER | TRACK_PROGRESS}));
    cmd_hash.insert(std::pair<std::string, Command>(
        "truncate",
        {handle_truncate_table_cmd, EXEC_ON_MASTER | TRACK_PROGRESS}));
    cmd_hash.insert(std::pair<std::string, Command>(
        "verify", {handle_verify_table_cmd, EXEC_ON_MASTER | TRACK_PROGRESS}));

    struct option long_options[] = {
        {"file", required_argument, NULL, 'f'},
        {"help", no_argument, NULL, 'h'},
        {"size", required_argument, NULL, 's'},
        {"verbose", no_argument, NULL, 'v'},
        {0, 0, 0, 0},
    };

    while ((c = bb_getopt_long(argc, argv, (char *)"f:hs:v", long_options,
                               &opt_index)) != -1) {
        switch (c) {
        case 'f':
            opt_file = optarg;
            break;
        case 'h':
            usage();
            break;
        case 's':
            opt_chunk_size = atoi(optarg);
            break;
        case 'v':
            opt_verbose = true;
            break;
        }
    }

    /* There must be at least 1 more argument */
    if (optind + 2 > argc) {
        std::cerr << "invalid number of arguments; see usage" << std::endl;
        exit(1);
    }

    dbname = argv[optind++];
    std::size_t pos = dbname.find('@');
    if (pos != std::string::npos) {
        opt_tier = dbname.substr(pos + 1);
        dbname = dbname.substr(0, pos);
    }

    it = cmd_hash.find(argv[optind]);
    if (it != cmd_hash.end()) {
        /* Establish a connection. */
        connect();

        /* Connect to master if required. */
        if (it->second.flags & EXEC_ON_MASTER) {
            connect_to_master();
        }

        /* Pass rest of the arguments to the command handler. */
        int rc =
            it->second.handler(argc - optind - 1, argv + optind + 1, result);
        if (rc != 0) {
            std::cerr << "failed to execute command (rc: " << rc
                      << " error: " << cdb2_errstr(gbl_cdb2_handle) << ")"
                      << std::endl;
        }
        std::cout << result;
    } else {
        std::cerr << "invalid command " << argv[optind] << std::endl;
        exit(1);
    }

    return 0;
}
