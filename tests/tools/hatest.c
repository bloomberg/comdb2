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

#include <string.h>
#include <str0.h>

#include <readline/readline.h>
#include <readline/history.h>

#include <cdb2api.h>
#include <pthread.h>

#include <assert.h>

#include <mem.h>
#include <cdb2_constants.h>

/*#warning dbg started*/
#define dbg(...) /*fprintf(stderr, __VA_ARGS__)*/

char gbl_dbname[MAX_DBNAME_LENGTH];
char gbl_dbtype[100];

/* display mode */
enum {
    DEFAULT = 0x0000, /* default output */
    TABS = 0x0001,    /* separate columns by tabs */
    BINARY = 0x0002,  /* output binary */
    GENSQL = 0x0004,  /* generate insert statements */
    /* flags */
    STDERR = 0x1000
};

static int pausemode = 0;
static int printmode = DEFAULT;
static int scriptmode = 0;
static int error = 0;
static int set_debug = 0;
static cdb2_hndl_tp *cdb2h = NULL;
static int time_mode = 0;
static int gbl_rowsleep = 0;
static int gbl_string_blobs = 0;
static int gbl_show_effects = 0;
static char doublefmt[32];
static int docost = 0;
static FILE *redirect = NULL;
static int hold_stdout = -1;
static char *history_file = NULL;
static int istty = 0;
static char *prompt = "cdb2sql> ";
static char *gensql_tbl = NULL;

/* massive mode variables */
enum { MASSIVE_TRANS_SIZE = 15 };
enum { MASSIVE_THREADS_NUM = 10 }; /* < MASSIVE_THREADS_MAX*/

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

static const char *usage_text[] = {
    "Usage: cdb2sql [options] dbname dbtype sql [type1 [type2 ...]]", "",
    "Basic options:", "-h                 Help",
    "-s                 Script mode (less verbose output)",
    "-tabs              Seperate output columns with tabs rather than commas",
    "-cdb2cfg           Change the config file to point to comdb2db "
    "configuration file",
    "-showeffects       Show the effects of query at the end",
    "-cost              Log the cost of query in db trace files",
    "-precision #       Set precision for floation point outputs",
    "-strblobs          Display blobs as strings",
    "-f filename        reads queries from the specified file", NULL};

void usage(int exit_val)
{
    int ii;
    for (ii = 0; usage_text[ii]; ii++) {
        fprintf((exit_val == EXIT_SUCCESS) ? stdout : stderr, usage_text[ii],
                MASSIVE_TRANS_SIZE);
        fprintf((exit_val == EXIT_SUCCESS) ? stdout : stderr, "\n");
    }
    exit(exit_val);
}

static char *read_line()
{
    static char *line = NULL;
    if (istty) {
        if (line)
            free(line);
        if ((line = readline(prompt)) != NULL)
            add_history(line);
        return line;
    }
    static size_t sz = 0;
    ssize_t n = getline(&line, &sz, stdin);
    if (n == -1) {
        free(line);
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
        int i = atol(*sqlstr);
        int *val = malloc(sizeof(int));
        *val = i;
        *vallen = sizeof(int);
        return val;
    } else if (type == CDB2_REAL) {
        double d = atof(*sqlstr);
        double *val = malloc(sizeof(double));
        *val = d;
        *vallen = sizeof(int);
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
        gbl_rowsleep = atoi(tok);
    } else if (strcmp(tok, "strblobs") == 0) {
        gbl_string_blobs = 1;
        printf("Blobs will be displayed as strings\n");
    } else if (strcmp(tok, "hexblobs") == 0) {
        gbl_string_blobs = 0;
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
        if (gbl_string_blobs) {
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
        fprintf(f, "\"%s%u %2.2u:%2.2u:%2.2u.%3.3u\"", (ds->sign<0)?"- ":"",
                ds->days, ds->hours, ds->mins, ds->sec, ds->msec);
        break;
    }
    case CDB2_INTERVALDSUS: {
        cdb2_client_intv_dsus_t *ds = (cdb2_client_intv_dsus_t *)val;
        if (printmode == DEFAULT)
            fprintf(f, "%s=", cdb2_column_name(cdb2h, col));
        fprintf(f, "\"%s%u %2.2u:%2.2u:%2.2u.%6.6u\"", (ds->sign<0)?"- ":"",
                ds->days, ds->hours, ds->mins, ds->sec, ds->usec);
        break;
    }
    }
}

#include <sys/socket.h>
#define MAX_NODES 128

struct dummy_cdb2buf {
    int fd;
     /* Don't care about the rest. */
};

/* Should match with cdb2api.c. */
struct dummy_cdb2_hndl {
     char dbname[64];
     char cluster[64];
     char type[64];
     char hosts[MAX_NODES][64];
     int  ports[MAX_NODES];
     int  hosts_connected[MAX_NODES];
     struct dummy_cdb2buf   *sb;
     /* Don't care about the rest. */
};

void disconnect_cdb2h(cdb2_hndl_tp * cdb2h) {
    struct dummy_cdb2_hndl *dummy_hndl;
    dummy_hndl = (struct dummy_cdb2_hndl *)cdb2h;
    if (dummy_hndl->sb)
        shutdown(dummy_hndl->sb->fd, 2);
}

static int run_statement(const char *sql, int ntypes, int *types,
                         int *start_time, int *run_time)
{
    int rc;
    int ncols;
    int col;
    int cost;
    FILE *out = stdout;

    int startms, rowms, endms;

    if (printmode & STDERR)
        out = stderr;

    *start_time = 0;
    *run_time = 0;

    if (cdb2h == NULL) {
        if (gbl_dbtype[0] == '@' || strcasecmp(gbl_dbtype, "dev") == 0 ||
            strcasecmp(gbl_dbtype, "uat") == 0 ||
            strcasecmp(gbl_dbtype, "default") == 0 ||
            strcasecmp(gbl_dbtype, "alpha") == 0 ||
            strcasecmp(gbl_dbtype, "beta") == 0 ||
            strcasecmp(gbl_dbtype, "local") == 0 ||
            strcasecmp(gbl_dbtype, "prod") == 0) {
            rc = cdb2_open(&cdb2h, gbl_dbname, gbl_dbtype, 0);
        } else {
            rc = cdb2_open(&cdb2h, gbl_dbname, gbl_dbtype, CDB2_DIRECT_CPU);
        }
        if (rc) {
            fprintf(stderr, "cdb2_open rc %d %s\n", rc, cdb2_errstr(cdb2h));
            cdb2_close(cdb2h);
            cdb2h = NULL;
            return 1;
        }

        if (docost) {
            rc = cdb2_run_statement(cdb2h, "set getcost on");
            if (rc) {
                fprintf(stderr, "failed to run set getcost 1\n");
                return 1;
            }
        }
    }

    if (strcmp(sql, "disconnect") == 0) {
       disconnect_cdb2h(cdb2h);
       return 0;
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

            int rc = cdb2_bind_param(cdb2h, parameter, type, value, length);
            return rc;
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
        if (gbl_rowsleep)
            sleep(gbl_rowsleep);
        if (pausemode) {
            char input[128];
            int cmd;
            printf(">");
            fflush(stdout);
            fgets(input, sizeof(input), stdin);
            cmd = atoi(input);
            if (cmd == -1)
                pausemode = 0;
        }
    }
    *run_time = endms - startms;
    if (rc != CDB2_OK_DONE) {
        const char *err = cdb2_errstr(cdb2h);
        fprintf(stderr, "[%s] failed with rc %d %s\n", sql, rc, err ? err : "");
        return rc;
    }

    if (gbl_show_effects) {
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
    rc = run_statement(sqlstr, ntypes, types, &start_time_ms, &run_time_ms);

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
        rc =
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
    int *types = malloc(ntypes * sizeof(int));
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
    prompt = "    ...> ";
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
    prompt = "cdb2sql> ";
    return stmt;
}

int main(int argc, char *argv[])
{
    char *filename = NULL;
    int precision = 0;
    int exponent = 0;
    int isttyarg = 0;

    sigignore(SIGPIPE);

    argc--;
    argv++;

    while (argc && argv[0][0] == '-') {
        if (!strcmp(argv[0], "-h"))
            usage(EXIT_SUCCESS);
        else if (!strcmp(argv[0], "-s"))
            scriptmode = 1;
        else if (!strcmp(argv[0], "-pause"))
            pausemode = 1;
        else if (!strcmp(argv[0], "-binary"))
            printmode = BINARY;
        else if (!strcmp(argv[0], "-tabs"))
            printmode = TABS;
        else if (!strcmp(argv[0], "-strblobs"))
            gbl_string_blobs = 1;
        else if (!strcmp(argv[0], "-debug"))
            set_debug = 1;
        else if (!strcmp(argv[0], "-showeffects"))
            gbl_show_effects = 1;
        else if (!strcmp(argv[0], "-cost"))
            docost = 1;
        else if (!strcmp(argv[0], "-exponent"))
            exponent = 1;
        else if (!strcmp(argv[0], "-isatty"))
            isttyarg = 1;
        else if (!strcmp(argv[0], "-isnotatty"))
            isttyarg = 2;
        else if (!strcmp(argv[0], "-precision")) {
            argc--;
            argv++;
            if (argc < 1) {
                usage(EXIT_FAILURE);
            }
            precision = atoi(argv[0]);
        } else if (!strcmp(argv[0], "-cdb2cfg") || !strcmp(argv[0], "--cdb2cfg")) {
            argc--;
            argv++;
            if (argc < 1) {
                usage(EXIT_FAILURE);
            }
            cdb2_set_comdb2db_config(argv[0]);
        } else if (!strcmp(argv[0], "-f")) {
            argc--;
            argv++;
            if (argc < 1) {
                usage(EXIT_FAILURE);
            }
            filename = argv[0];
        } else if (!strcmp(argv[0], "-gensql")) {
            argc--;
            argv++;
            if (argc < 1) {
                usage(EXIT_FAILURE);
            }
            printmode = GENSQL;
            gensql_tbl = argv[0];
        } else {
            fprintf(stderr, "Unknown option \"%s\"\n", argv[0]);
            usage(EXIT_FAILURE);
        }

        argv++;
        argc--;
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

    if (argc < 2) {
        usage(EXIT_FAILURE);
    }

    char *dbname = argv[0];
    char *dbtype = argv[1];
    char *sql = (argc >= 3 ? argv[2] : NULL);
    if (strlen(dbname) > 80) {
        fprintf(stderr, "Invalid db name %s\n", dbname);
        return 1;
    }
    strcpy(gbl_dbname, dbname);
    strcpy(gbl_dbtype, dbtype);

    if (argc >= 3 && sql && *sql != '-') {
        int ntypes = 0;
        int *types = NULL;
        if (argc > 3) {
            ntypes = argc - 3;
            types = process_typed_statement_args(ntypes, &argv[3]);
        }
        scriptmode = 1;
        process_line(sql, ntypes, types);
        return (error == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
    }

    if (filename) {
        if (freopen(filename, "r", stdin) == NULL) {
            fprintf(stderr, "Error opening %s: %s\n", filename,
                    strerror(errno));
            return EXIT_FAILURE;
        }
    } else if (sql == NULL || *sql != '-') {
        usage(EXIT_FAILURE);
    }

    istty = isatty(0);
    if (isttyarg == 1)
        istty = 1;
    if (isttyarg == 2)
        istty = 0;
    if (istty)
        load_readline_history();
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
