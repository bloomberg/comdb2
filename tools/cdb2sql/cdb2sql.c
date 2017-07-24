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

/*
 * Comdb2 sql command line client.
 *
 * $Id: client.c 88379 2013-12-11 20:11:12Z mkhullar $
 */

#include <ctype.h>
#include <errno.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <signal.h>
#include <unistd.h>
#include <getopt.h>

#include <sbuf2.h>
#include <string.h>
#include <str0.h>

#include <readline/readline.h>
#include <readline/history.h>

#include "cdb2api.h"
#include <pthread.h>

#include <assert.h>

#include "mem.h"
#include "cdb2_constants.h"
#include "epochlib.h"

static char *dbname = NULL;
static char *dbtype = NULL;
static char *dbhostname = NULL;
static char main_prompt[MAX_DBNAME_LENGTH + 2];
static char gbl_in_stmt = 0;

/* display mode */
enum {
    DEFAULT = 0x0000, /* default output */
    TABS    = 0x0001, /* separate columns by tabs */
    BINARY  = 0x0002,  /* output binary */
    GENSQL  = 0x0004,  /* generate insert statements */
    /* flags */
    STDERR  = 0x1000
};

static int show_ports = 0;
static int debug_trace = 0;
static int pausemode = 0;
static int printmode = DEFAULT;
static int scriptmode = 0;
static int error = 0;
static int set_debug = 0;
static cdb2_hndl_tp *cdb2h = NULL;
static int time_mode = 0;
static int rowsleep = 0;
static int string_blobs = 0;
static int show_effects = 0;
static char doublefmt[32];
static int docost = 0;
static int maxretries = 0;
static FILE *redirect = NULL;
static int hold_stdout = -1;
static char *history_file = NULL;
static int istty = 0;
static char *gensql_tbl = NULL;
static char *prompt = main_prompt;

static void hexdump(FILE *f, void *datap, int len)
{
    u_char *data = (u_char *)datap;
    int i;
    for (i = 0; i < len; i++)
        fprintf(f, "%02x", (unsigned int)data[i]);
}

void dumpstring(FILE *f, char *s, int quotes, int quote_quotes)
{
    if (quotes)
        fprintf(f, "'");
    while (*s) {
        if (*s == '\'' && quote_quotes)
            fprintf(f, "''");
        else
            fprintf(f, "%c", *s);
        s++;
    }
    if (quotes)
        fprintf(f, "'");
}

static const char *usage_text =
    "Usage: cdb2sql [options] dbname [sql [type1 [type2 ...]]]\n"
    "\n"
    "Options:\n"
    " -h, --help             Help on usage \n"
    " -s, --script           Script mode (less verbose output)\n"
    "     --tabs             Seperate output columns with tabs rather\n"
    "                        than commas\n"
    " -c, --cdb2cfg          Change the config file to point to comdb2db\n"
    "                        configuration file\n"
    " -t, --type TYPE        Type of database or tier (ie 'dev' or 'prod',\n"
    "                                              default 'local')\n"
    " -n, --host HOSTNAME    Host to connect to and run query.\n"
    "     --debugtrace       Set debug trace flag on api handle\n"
    "     --showeffects      Show the effects of query at the end\n"
    "     --cost             Log the cost of query in db trace files\n"
    " -p, --precision #      Set precision for floation point outputs\n"
    "     --strblobs         Display blobs as strings\n"
    " -f, --file FL          reads queries from the specified file FL\n"
    "\n"
    " Examples: \n"
    " * Querying db with name mydb on local server \n"
    "     cdb2sql mydb 'select 1'\n"
    " * Query db via interactive session:\n"
    "     cdb2sql mydb - \n"
    " * Query db by connecting to a specific server:\n"
    "     cdb2sql mydb --host node1 'select 1'\n"
    " * Query db by connecting to a known set of servers/ports:\n"
    "     cdb2sql mydb @node1:port=19007,node2:port=19000 'select 1'\n";

void cdb2sql_usage(int exit_val)
{
    fprintf((exit_val == EXIT_SUCCESS) ? stdout : stderr, usage_text);
    exit(exit_val);
}

static char *read_line()
{
    static char *line = NULL;

    if (istty) {
        if (line) {
            free(line);
            line = NULL;
        }
        if ((line = readline(prompt)) != NULL && line[0] != 0)
            add_history(line);
        return line;
    }
    static size_t sz = 0;
    ssize_t n = getline(&line, &sz, stdin);
    if (n == -1) {
        if (line) {
            free(line);
            line = NULL;
        }
        return NULL;
    }
    if (line[n - 1] == '\n')
        line[n - 1] = 0;
    return line;
}

#define checkfortype(str, STRTYPE, ret)                                        \
    do {                                                                       \
        int l = sizeof(STRTYPE) - 1;                                           \
        if (strncasecmp((str), STRTYPE, l) == 0) {                             \
            (str) += l;                                                        \
            return ret;                                                        \
        }                                                                      \
    } while (0)

int get_type(const char **sqlstr)
{
    // char * strptr = *sqlstr;
    while (isspace(**sqlstr))
        (*sqlstr)++;
    if (strncasecmp(*sqlstr, "CDB2_", 5) != 0) {
        printf("Type expected after @bind\n");
        return -1;
    }
    *sqlstr += 5; // skip CDB2_
    checkfortype(*sqlstr, "INTEGER", CDB2_INTEGER);
    checkfortype(*sqlstr, "REAL", CDB2_REAL);
    checkfortype(*sqlstr, "CSTRING", CDB2_CSTRING);
    checkfortype(*sqlstr, "BLOB", CDB2_BLOB);
    checkfortype(*sqlstr, "DATETIME", CDB2_DATETIME);
    checkfortype(*sqlstr, "INTERVALYM", CDB2_INTERVALYM);
    checkfortype(*sqlstr, "INTERVALDS", CDB2_INTERVALDS);

    return -1;
}

char *get_parameter(const char **sqlstr)
{
    while (isspace(**sqlstr))
        (*sqlstr)++;
    const char *end = *sqlstr;
    while (!isspace(*end))
        end++;
    int len = end - (*sqlstr);
    if (len < 1) {
        printf("Parameter name expected after type\n");
        return NULL;
    }
    char *copy = strndup(*sqlstr, len);
    *sqlstr = end;
    return copy;
}

void *get_val(const char **sqlstr, int type, int *vallen)
{
    while (isspace(**sqlstr))
        (*sqlstr)++;
    const char *end = *sqlstr;
    while (*end)
        end++; // till \0
    int len = end - (*sqlstr);
    if (len < 1) {
        printf("Value expected after parameter\n");
        return NULL;
    }
    if (type == CDB2_INTEGER) {
        int64_t i = atol(*sqlstr);
        int64_t *val = malloc(sizeof(int64_t));
        *val = i;
        *vallen = sizeof(*val);
        return val;
    } else if (type == CDB2_REAL) {
        double d = atof(*sqlstr);
        double *val = malloc(sizeof(double));
        *val = d;
        *vallen = sizeof(*val);
        return val;
    } else if (type == CDB2_CSTRING) {
        char *val = strndup(*sqlstr, end - (*sqlstr));
        *vallen = len;
        return val;
    } else if (type == CDB2_DATETIME) {
        cdb2_client_datetime_t *dt = calloc(sizeof(cdb2_client_datetime_t), 1);
        int rc = sscanf(*sqlstr, "%04d-%02d-%02dT%02d:%02d:%02d",
                        &dt->tm.tm_year, &dt->tm.tm_mon, &dt->tm.tm_mday,
                        &dt->tm.tm_hour, &dt->tm.tm_min, &dt->tm.tm_sec);
        /* timezone not supported for now */
        if (rc != 6) {
            fprintf(stderr,
                    "invalid datetime (need format YYYY-MM-ddThh:mm:ss\n");
            return NULL;
        }
        dt->msec = 0;
        dt->tzname[0] = 0;
        dt->tm.tm_year -= 1900;
        dt->tm.tm_mon--;

        *vallen = sizeof(*dt);
        return dt;
    } else {
        /* ?? */
    }
    return NULL;
}

static int process_escape(const char *cmdstr)
{
    char copy[256];
    char *lasts;
    static const char *delims = " \r\n\t";
    char *tok;

    int len = strlen(cmdstr);

    strncpy(copy, cmdstr, sizeof(copy));
    copy[len] = '\0';

    /* get first token, skip @ */
    tok = strtok_r(copy + 1, delims, &lasts);

    if (!tok)
        return 0;

    if (strcmp(tok, "cdb2_close") == 0) {
        cdb2_close(cdb2h);
        cdb2h = NULL;
    } else if (strcmp(tok, "redirect") == 0) {
        tok = strtok_r(NULL, delims, &lasts);

        /* Close the redirect file. */
        if (!tok && redirect) {
            fclose(redirect);
            redirect = NULL;

            /* Move the saved stdout back to the real stdout. */
            dup2(hold_stdout, 1);
            close(hold_stdout);
            hold_stdout = -1;
        }
        /* User didn't supply a filename. */
        else if (!tok) {
            fprintf(stderr, "expected redirect filename.\n");
            return -1;
        } else {
            if (redirect)
                fclose(redirect);

            if (NULL == (redirect = fopen(tok, "w"))) {
                fprintf(stderr, "error opening redirect file, %s.\n",
                        strerror(errno));
                return -1;
            }

            if (-1 == hold_stdout) {
                hold_stdout = dup(1);
            }
            dup2(fileno(redirect), 1);
        }
    } else if (strcmp(tok, "row_sleep") == 0) {
        tok = strtok_r(NULL, delims, &lasts);
        if (!tok) {
            fprintf(stderr, "expected row sleep in seconds\n");
            return -1;
        }
        rowsleep = atoi(tok);
    } else if (strcmp(tok, "strblobs") == 0) {
        string_blobs = 1;
        printf("Blobs will be displayed as strings\n");
    } else if (strcmp(tok, "hexblobs") == 0) {
        string_blobs = 0;
        printf("Blobs will be displayed as hex\n");
    } else if (strcmp(tok, "time") == 0) {
        time_mode = time_mode ? 0 : 1;
        printf("Timing mode %s\n", time_mode ? "ON" : "OFF");
    } else {
        fprintf(stderr, "unknown command %s\n", tok);
        return -1;
    }
    return 0;
}

void printCol(FILE *f, cdb2_hndl_tp *cdb2h, void *val, int col, int printmode)
{
    switch (cdb2_column_type(cdb2h, col)) {
    case CDB2_INTEGER:
        if (printmode == DEFAULT)
            fprintf(f, "%s=%lld", cdb2_column_name(cdb2h, col),
                    *(long long *)val);
        else
            fprintf(f, "%lld", *(long long *)val);
        break;
    case CDB2_REAL:
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, doublefmt, *(double *)val);
        break;
    case CDB2_CSTRING:
        if (printmode == DEFAULT) {
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
            dumpstring(f, (char *)val, 1, 0);
        } else if (printmode & TABS)
            dumpstring(f, (char *)val, 0, 0);
        else
            dumpstring(f, (char *)val, 1, 1);
        break;
    case CDB2_BLOB:
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        if (string_blobs) {
            char *c = val;
            int len = cdb2_column_size(cdb2h, col);
            fputc('\'', stdout);
            while (len > 0) {
                if (isprint(*c) || *c == '\n' || *c == '\t') {
                    fputc(*c, stdout);
                } else {
                    fprintf(f, "\\x%02x", (int)*c);
                }
                len--;
                c++;
            }
            fputc('\'', stdout);
        } else {
            if (printmode == BINARY) {
                write(1, val, cdb2_column_size(cdb2h, col));
                exit(0);
            } else {
                fprintf(f, "x'");
                hexdump(f, val, cdb2_column_size(cdb2h, col));
                fprintf(f, "'");
            }
        }
        break;
    case CDB2_DATETIME: {
        cdb2_client_datetime_t *cdt = (cdb2_client_datetime_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%3.3u %s\"",
                cdt->tm.tm_year + 1900, cdt->tm.tm_mon + 1, cdt->tm.tm_mday,
                cdt->tm.tm_hour, cdt->tm.tm_min, cdt->tm.tm_sec, cdt->msec,
                cdt->tzname);
        break;
    }
    case CDB2_DATETIMEUS: {
        cdb2_client_datetimeus_t *cdt = (cdb2_client_datetimeus_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%4.4u-%2.2u-%2.2uT%2.2u%2.2u%2.2u.%6.6u %s\"",
                cdt->tm.tm_year + 1900, cdt->tm.tm_mon + 1, cdt->tm.tm_mday,
                cdt->tm.tm_hour, cdt->tm.tm_min, cdt->tm.tm_sec, cdt->usec,
                cdt->tzname);
        break;
    }
    case CDB2_INTERVALYM: {
        cdb2_client_intv_ym_t *ym = (cdb2_client_intv_ym_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%s%u-%u\"", (ym->sign < 0) ? "- " : "", ym->years,
                ym->months);
        break;
    }
    case CDB2_INTERVALDS: {
        cdb2_client_intv_ds_t *ds = (cdb2_client_intv_ds_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%s%u %2.2u:%2.2u:%2.2u.%3.3u\"",
                (ds->sign < 0) ? "- " : "", ds->days, ds->hours, ds->mins,
                ds->sec, ds->msec);
        break;
    }
    case CDB2_INTERVALDSUS: {
        cdb2_client_intv_dsus_t *ds = (cdb2_client_intv_dsus_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%s%u %2.2u:%2.2u:%2.2u.%6.6u\"",
                (ds->sign < 0) ? "- " : "", ds->days, ds->hours, ds->mins,
                ds->sec, ds->usec);
        break;
    }
    }
}

static int run_statement(const char *sql, int ntypes, int *types,
                         int *start_time, int *run_time)
{
    int rc;
    int ncols;
    int col;
    int cost;
    FILE *out = stdout;
    char cmd[60];

    int startms, rowms, endms;

    if (printmode & STDERR)
        out = stderr;

    startms = time_epochms();
    *start_time = 0;
    *run_time = 0;

    if (cdb2h == NULL) {
        if (dbhostname) {
            rc = cdb2_open(&cdb2h, dbname, dbhostname, CDB2_DIRECT_CPU);
        } else {
            rc = cdb2_open(&cdb2h, dbname, dbtype, 0);
        }

        cdb2_push_context(cdb2h, "cdb2sql");
        if (rc) {
            fprintf(stderr, "cdb2_open rc %d %s\n", rc, cdb2_errstr(cdb2h));
            cdb2_close(cdb2h);
            cdb2h = NULL;
            return 1;
        }
        if (debug_trace) {
            cdb2_set_debug_trace(cdb2h);
        }
        if (show_ports) {
            cdb2_dump_ports(cdb2h, stderr);
        }
        if (maxretries) {
            cdb2_set_max_retries(maxretries);
        }

        if (docost) {
            rc = cdb2_run_statement(cdb2h, "set getcost on");
            if (rc) {
                fprintf(stderr, "failed to run set getcost 1\n");
                return 1;
            }
        }

        /*
          Check and set user and password if they have been specified using
          the environment variables.

          Note: It is good to report the user about the use of environment
          variables to set user/password to avoid any surprises.
        */
        int length;

        if (getenv("COMDB2_USER")) {
            length = snprintf(cmd, sizeof(cmd), "set user %s",
                              getenv("COMDB2_USER"));

            if (length > sizeof(cmd)) {
                fprintf(stderr, "COMDB2_USER too long, ignored\n");
            } else {
                if ((length < 0) || ((cdb2_run_statement(cdb2h, cmd)) != 0)) {
                    fprintf(stderr, "Failed to set user using COMDB2_USER, "
                                    "exiting\n");
                    return 1;
                } else {
                    printf("Set user using COMDB2_USER\n");
                }
            }
        }

        if (getenv("COMDB2_PASSWORD")) {
            length = snprintf(cmd, sizeof(cmd), "set password %s",
                              getenv("COMDB2_PASSWORD"));

            if (length > sizeof(cmd)) {
                fprintf(stderr, "COMDB2_PASSWORD too long, ignored\n");
            } else {
                if ((length < 0) || ((cdb2_run_statement(cdb2h, cmd)) != 0)) {
                    fprintf(stderr, "Failed to set password using "
                                    "COMDB2_PASSWORD, exiting\n");
                    return 1;
                } else {
                    printf("Set password using COMDB2_PASSWORD\n");
                }
            }
        }
    }

    /* Bind parameter ability -- useful for debugging */
    if (sql[0] == '@') {
        if (strncasecmp(sql, "@bind", 5) == 0) {
            //@bind BINDTYPE parameter value
            sql += 5;

            int type = get_type(&sql);
            if (type < 0)
                return -1;

            char *parameter = get_parameter(&sql);
            if (parameter == NULL)
                return -1;

            int length;
            void *value = get_val(&sql, type, &length);

            rc = cdb2_bind_param(cdb2h, parameter, type, value, length);
            /* we have to leak parameter here -- freeing breaks the bind */
        } else
            rc = process_escape(sql);
        return rc;
    }

    {
        int retries = 0;
        while (retries < 10) {
            rc = cdb2_run_statement_typed(cdb2h, sql, ntypes, types);
            if (rc != CDB2ERR_IO_ERROR)
                break;
            retries++;
        }
    }
    rowms = time_epochms();
    *start_time = rowms - startms;

    cdb2_clearbindings(cdb2h);

    if (rc != CDB2_OK) {
        const char *err = cdb2_errstr(cdb2h);
        /* cdb2tcm mode needs to pass this info through stdout */
        FILE *out = stderr;

        fprintf(out, "[%s] failed with rc %d %s\n", sql, rc, err ? err : "");
        return rc;
    }

    ncols = cdb2_numcolumns(cdb2h);

    while ((rc = cdb2_next_record(cdb2h)) == CDB2_OK) {
        if (printmode == DEFAULT)
            fprintf(out, "(");
        else if (printmode == GENSQL) {
            printf("insert into %s (", gensql_tbl);
            for (col = 0; col < ncols; col++) {
                printf("%s", cdb2_column_name(cdb2h, col));
                if (col != ncols - 1)
                    printf(", ");
            }
            printf(") values (");
        }
        for (col = 0; col < ncols; col++) {
            void *val = cdb2_column_value(cdb2h, col);
            if (val == NULL) {
                if (printmode == DEFAULT)
                    fprintf(out, "%s=NULL", cdb2_column_name(cdb2h, col));
                else
                    fprintf(out, "NULL");
            } else {
                printCol(out, cdb2h, val, col, printmode);
            }
            if (col != ncols - 1) {
                if (printmode == DEFAULT)
                    fprintf(out, ", ");
                else if (printmode & TABS)
                    fprintf(out, "\t");
                else
                    fprintf(out, ", ");
            }
        }
        switch (printmode) {
        case DEFAULT:
            fprintf(out, ")\n");
            break;
        case TABS:
            fprintf(out, "\n");
            break;
        case GENSQL:
            fprintf(out, ");\n");
            break;
        }
        fflush(out);
        if (rowsleep)
            sleep(rowsleep);
        if (pausemode) {
            char input[128];
            int cmd;
            printf(">");
            fflush(stdout);
            if (fgets(input, sizeof(input), stdin) == input) {
                cmd = atoi(input);
                if (cmd == -1)
                    pausemode = 0;
            }
        }
    }

    endms = time_epochms();
    *run_time = endms - startms;
    if (rc != CDB2_OK_DONE) {
        const char *err = cdb2_errstr(cdb2h);
        fprintf(stderr, "[%s] failed with rc %d %s\n", sql, rc, err ? err : "");
        return rc;
    }

    if (show_effects) {
        cdb2_effects_tp effects;
        if (cdb2_get_effects(cdb2h, &effects) == 0) {
            printf("Number of rows affected %d\n", effects.num_affected);
            printf("Number of rows selected %d\n", effects.num_selected);
            printf("Number of rows deleted %d\n", effects.num_deleted);
            printf("Number of rows updated %d\n", effects.num_updated);
            printf("Number of rows inserted %d\n", effects.num_inserted);
        } else {
            printf("Effects not sent by comdb2 server. \n");
        }
    }
    return 0;
}

static void process_line(char *sql, int ntypes, int *types)
{
    char *sqlstr = sql;
    int rc;
    int len;

    /* Trim whitespace and then ignore comments */
    while (isspace(*sqlstr))
        sqlstr++;
    if (sqlstr[0] == '#' || sqlstr[0] == '\0')
        return;

    /* Lame hack - strip trailing ; so that we can understand the
     * sql generated by our own gensql mode.  Note that the sql
     * parser in comdb2 is ok with semicolons - it stops after the
     * first one it encounters. */
    len = strlen(sqlstr);
    while (len > 0 && isspace(sqlstr[len - 1]))
        len--;
    while (len > 0 && sqlstr[len - 1] == ';')
        len--;
    sqlstr[len] = '\0';

    int start_time_ms, run_time_ms;
    gbl_in_stmt = 1;
    rc = run_statement(sqlstr, ntypes, types, &start_time_ms, &run_time_ms);
    gbl_in_stmt = 0;

    if (rc != 0) {
        error++;
    } else if (!scriptmode) {
        printf("[%s] rc %d\n", sqlstr, rc);
        if (time_mode) {
            printf("  prep time  %d ms\n", start_time_ms);
            printf("  run time   %d ms\n", run_time_ms);
        }
    }

    if (docost) {
        int saved_printmode = printmode;
        printmode = TABS | STDERR;
        const char *costSql = "SELECT comdb2_prevquerycost() as Cost";
        run_statement(costSql, ntypes, types, &start_time_ms, &run_time_ms);
        printmode = saved_printmode;
    }
}

void load_readline_history()
{
    char *home;
    if ((home = getenv("HOME")) == NULL)
        return;
    char histfile[] = "/.cdb2sql_history";
    if ((history_file = malloc(strlen(home) + sizeof(histfile))) == NULL)
        return;
    sprintf(history_file, "%s%s", home, histfile);
    read_history(history_file);
}

#define HISTFILE_COMMANDS 200
void save_readline_history()
{
    stifle_history(HISTFILE_COMMANDS);
    write_history(history_file);
    free(history_file);
}

static int *process_typed_statement_args(int ntypes, char **args)
{
    int *types = NULL;

    if (ntypes > 0)
        types = malloc(ntypes * sizeof(int));

    for (int i = 0; i < ntypes; i++) {
        if (strcmp(args[i], "integer") == 0)
            types[i] = CDB2_INTEGER;
        else if (strcmp(args[i], "real") == 0)
            types[i] = CDB2_REAL;
        else if (strcmp(args[i], "cstring") == 0)
            types[i] = CDB2_CSTRING;
        else if (strcmp(args[i], "blob") == 0)
            types[i] = CDB2_BLOB;
        else {
            char *endp;
            types[i] = strtol(args[i], &endp, 10);
            if (*endp != 0) {
                fprintf(stderr, "Unknown type %s\n", args[i]);
                exit(EXIT_FAILURE);
            }
        }
    }
    return types;
}

static int is_multi_line(const char *sql)
{
    if (sql == NULL)
        return 0;
    while (isspace(*sql))
        ++sql;
    if (strncasecmp(sql, "create", 6) == 0) {
        sql += 6;
        while (isspace(*sql))
            ++sql;
        if (*sql == '\0' || strncasecmp(sql, "table", 5) == 0 ||
            strncasecmp(sql, "procedure", 9) == 0)
            return 1;
    } else if (strncasecmp(sql, "alter", 5) == 0) {
        sql += 5;
        while (isspace(*sql))
            ++sql;
        if (*sql == '\0' || strncasecmp(sql, "table", 5) == 0) {
            return 1;
        }
    }
    return 0;
}

static char *get_multi_line_statement(char *line)
{
    char *stmt = NULL;
    int slen = 0;
    char *nl = ""; // new-line
    int n = 0;     // len of nl

    char tmp_prompt[sizeof(main_prompt)];
    int spaces = strlen(dbname) - 3, dots = 3;
    if (spaces < 0) {
        dots += spaces;
        spaces = 0;
    }
    sprintf(tmp_prompt, "%.*s%.*s> ", spaces,
            "                                                                ",
            dots, "...");
    prompt = tmp_prompt;
    do {
        int len = strlen(line);
        int newlen = slen + len + n;
        stmt = realloc(stmt, newlen + 1);
        sprintf(&stmt[slen], "%s%s", nl, line);
        slen = newlen;
        if (stmt[slen - 2] == '$' && stmt[slen - 1] == '$') {
            stmt[slen - 2] = '\0';
            break;
        }
        nl = "\n";
        n = 1;
    } while ((line = read_line()) != NULL);
    prompt = main_prompt;
    return stmt;
}

static int dbtype_valid(char *type)
{
    if (type && (type[0] == '@' || strcasecmp(type, "dev") == 0 ||
        strcasecmp(type, "uat") == 0 ||
        strcasecmp(type, "default") == 0 ||
        strcasecmp(type, "alpha") == 0 ||
        strcasecmp(type, "beta") == 0 ||
        strcasecmp(type, "local") == 0 ||
        strcasecmp(type, "prod") == 0)) {
        return 1;
    }
    return 0;
}

static void replace_args(int argc, char *argv[])
{
    int ii;

    for (ii = 1; ii < argc; ii++) {
        if (argv[ii][0] != '-')
            continue;
        if (!strcmp(argv[ii], "-pause"))
            argv[ii] = "--pause";
        else if (!strcmp(argv[ii], "-binary"))
            argv[ii] = "--binary";
        else if (!strcmp(argv[ii], "-tabs"))
            argv[ii] = "--tabs";
        else if (!strcmp(argv[ii], "-strblobs"))
            argv[ii] = "--strblobs";
        else if (!strcmp(argv[ii], "-debug"))
            argv[ii] = "--debug";
        else if (!strcmp(argv[ii], "-debugtrace"))
            argv[ii] = "--debugtrace";
        else if (!strcmp(argv[ii], "-showports"))
            argv[ii] = "--showports";
        else if (!strcmp(argv[ii], "-showeffects"))
            argv[ii] = "--showeffects";
        else if (!strcmp(argv[ii], "-cost"))
            argv[ii] = "--cost";
        else if (!strcmp(argv[ii], "-exponent"))
            argv[ii] = "--exponent";
        else if (!strcmp(argv[ii], "-isatty"))
            argv[ii] = "--isatty";
        else if (!strcmp(argv[ii], "-isnotatty"))
            argv[ii] = "--isnotatty";
        else if (!strcmp(argv[ii], "-help"))
            argv[ii] = "--help";
        else if (!strcmp(argv[ii], "-script"))
            argv[ii] = "--script";
        else if (!strcmp(argv[ii], "-maxretries"))
            argv[ii] = "--maxretries";
        else if (!strcmp(argv[ii], "-precision"))
            argv[ii] = "--precision";
        else if (!strcmp(argv[ii], "-cdb2cfg"))
            argv[ii] = "--cdb2cfg";
        else if (!strcmp(argv[ii], "-file"))
            argv[ii] = "--file";
        else if (!strcmp(argv[ii], "-gensql"))
            argv[ii] = "--gensql";
        else if (!strcmp(argv[ii], "-type"))
            argv[ii] = "--type";
        else if (!strcmp(argv[ii], "-host"))
            argv[ii] = "--host";
    }
}

void send_cancel_cnonce(const char *cnonce)
{
    if (!gbl_in_stmt) return;
    cdb2_hndl_tp *cdb2h_2 = NULL; // use a new db handle
    int rc;
    if (dbhostname) {
        rc = cdb2_open(&cdb2h_2, dbname, dbhostname, CDB2_DIRECT_CPU);
    } else {
        rc = cdb2_open(&cdb2h_2, dbname, dbtype, 0);
    }
    if (rc) {
        if (debug_trace)
            fprintf(stderr, "cdb2_open rc %d %s\n", rc, cdb2_errstr(cdb2h));
        cdb2_close(cdb2h_2);
        return;
    }
    char expanded[256];
    for (int i = 0; i < 256 / 2 && cnonce[i] != '\0'; i++) {
        sprintf(&expanded[i * 2], "%2x", cnonce[i]);
    }
    char sql[256];
    snprintf(sql, 255, "exec procedure sys.cmd.send('sql cancelcnonce %s')",
             expanded);
    if (debug_trace) printf("Cancel sql string '%s'\n", sql);
    rc = cdb2_run_statement(cdb2h_2, sql);
    if (rc && debug_trace)
        fprintf(stderr, "failed to cancel rc %d with '%s'\n", rc, sql);
    cdb2_close(cdb2h_2);
    gbl_in_stmt = 0;
}

/* If ctrl_c was pressed to clear existing line and go to new line
 * If we see two ctrl_c in a row we exit.
 * However, after a ctrl_c if user typed something
 * (rl_line_buffer is not empty) and then issue a ctrl_c then dont exit.
 */
static void int_handler(int signum)
{
    printf("\n");
    rl_on_new_line();
    rl_replace_line("", 0);
    rl_redisplay();
    send_cancel_cnonce(cdb2_cnonce(cdb2h));
}

int main(int argc, char *argv[])
{
    static char *filename = NULL;
    static int precision = 0;
    static int exponent = 0;
    static int isttyarg = 0;
    static char *sql = NULL;
    int ntypes = 0;
    int *types = NULL;
    int opt_indx = 0;
    int c;

    comdb2ma_init(0, 0);
    sigignore(SIGPIPE);

    replace_args(argc, argv);

    static struct option long_options[] = {
        {"pause",      no_argument,       &pausemode,         1},
        {"binary",     no_argument,       &printmode,         BINARY},
        {"tabs",       no_argument,       &printmode,         TABS},
        {"strblobs",   no_argument,       &string_blobs,      1},
        {"debug",      no_argument,       &set_debug,         1},
        {"debugtrace", no_argument,       &debug_trace,       1},
        {"showports",  no_argument,       &show_ports,        1},
        {"showeffects",no_argument,       &show_effects,      1},
        {"cost",       no_argument,       &docost,            1},
        {"exponent",   no_argument,       &exponent,          1},
        {"isatty",     no_argument,       &isttyarg,          1},
        {"isnotatty",  no_argument,       &isttyarg,          2},
        {"help",       no_argument,       NULL,               'h'},
        {"script",     no_argument,       NULL,               's'},
        {"maxretries", required_argument, NULL,               'r'},
        {"precision",  required_argument, NULL,               'p'},
        {"cdb2cfg",    required_argument, NULL,               'c'},
        {"file",       required_argument, NULL,               'f'},
        {"gensql",     required_argument, NULL,               'g'},
        {"type",       required_argument, NULL,               't'},
        {"host",       required_argument, NULL,               'n'},
        {0, 0, 0, 0}
    };

    while ((c = getopt_long(argc, argv, "hsr:p:c:f:g:t:n:",
                            long_options, &opt_indx)) != -1) {
        switch (c) {
        case 0:
            break;
        case 'h':
            cdb2sql_usage(EXIT_SUCCESS);
            break;
        case 's':
            scriptmode = 1;
            break;
        case 'r':
            maxretries = atoi(optarg);
            break;
        case 'p':
            precision = atoi(optarg);
            break;
        case 'c':
            cdb2_set_comdb2db_config(optarg);
            break;
        case 'f':
            filename = optarg;
            break;
        case 'g':
            printmode = GENSQL;
            gensql_tbl = optarg;
            break;
        case 't':
            dbtype = optarg;
            break;
        case 'n':
            dbhostname = optarg;
            break;
        case '?':
            cdb2sql_usage(EXIT_FAILURE);
            break;
        }
    }

    if (getenv("COMDB2_IOLBF")) {
        setvbuf(stdout, 0, _IOLBF, 0);
        setvbuf(stderr, 0, _IOLBF, 0);
    }

    if (getenv("COMDB2_SQL_COST"))
        docost = 1;

    if (exponent) {
        if (precision > 0) {
            snprintf(doublefmt, sizeof(doublefmt), "%%.%dg", precision);
        } else {
            snprintf(doublefmt, sizeof(doublefmt), "%%g");
        }
    } else if (precision > 0) {
        snprintf(doublefmt, sizeof(doublefmt), "%%.%df", precision);
    } else {
        snprintf(doublefmt, sizeof(doublefmt), "%%f");
    }

    if (argc - optind < 1) {
        cdb2sql_usage(EXIT_FAILURE);
    }
    if (strlen(argv[optind]) >= MAX_DBNAME_LENGTH) {
        fprintf(stderr, "DB name \"%s\" too long\n", dbname);
        return 1;
    }

    dbname = argv[optind];
    optind++;
    if (dbtype == NULL && dbtype_valid(argv[optind])) {
        dbtype = argv[optind];
        optind++;
    } else {
        dbtype = "local"; /* might want "default" here */
    }

    sql = (optind < argc ? argv[optind] : "-");
    sprintf(main_prompt, "%s> ", dbname);
    optind++;

    ntypes = argc - optind;
    if (ntypes > 0)
        types = process_typed_statement_args(ntypes, &argv[optind]);
    if (sql && *sql != '-') {
        scriptmode = 1;
        process_line(sql, ntypes, types);
        if (cdb2h) {
            cdb2_close(cdb2h);
        }
        return (error == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    if (filename) {
        if (freopen(filename, "r", stdin) == NULL) {
            fprintf(stderr, "Error opening %s: %s\n", filename,
                    strerror(errno));
            return EXIT_FAILURE;
        }
    } else if (sql == NULL || *sql != '-') {
        cdb2sql_usage(EXIT_FAILURE);
    }

    istty = isatty(0);
    if (isttyarg == 1)
        istty = 1;
    if (isttyarg == 2)
        istty = 0;
    if (istty) {
        load_readline_history();
        struct sigaction sact;
        sact.sa_handler = int_handler;
        sigaction(SIGINT, &sact, NULL);
    }
    char *line;
    int multi;
    while ((line = read_line()) != NULL) {
        if (strncmp(line, "quit", 4) == 0)
            break;
        if ((multi = is_multi_line(line)) != 0)
            line = get_multi_line_statement(line);
        process_line(line, 0, NULL);
        if (multi)
            free(line);
    }
    if (istty)
        save_readline_history();

    if (cdb2h) {
        cdb2_close(cdb2h);
        cdb2h = NULL;
    }

    return (error == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
