/*
   Copyright 2017, 2021, Bloomberg Finance L.P.

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
#include <stdlib.h>
#include <string.h>
#include <dirent.h>
#include <sys/stat.h>
#include <unistd.h>
#include <netdb.h>
#include <sys/socket.h>

#include "fdb_whitelist.h"
#include "sqliteInt.h"
#include "comdb2.h"
#include "intern_strings.h"
#include "bb_oscompat.h"
#include "switches.h"
#include "util.h"
#include "sqllog.h"
#include "ssl_bend.h"
#include "translistener.h"
#include "rtcpu.h"
#include "config.h"
#include "phys_rep.h"
#include "phys_rep_lsn.h"
#include "macc_glue.h"
#include "disttxn.h"

extern int gbl_create_mode;
extern int gbl_fullrecovery;
extern int gbl_exit;
extern int gbl_recovery_timestamp;
extern int gbl_recovery_lsn_file;
extern int gbl_recovery_lsn_offset;
extern int gbl_rep_node_pri;
extern int gbl_bad_lrl_fatal;
extern int gbl_disable_new_snapshot;
extern int gbl_server_admin_mode;
extern int gbl_modsnap_asof;
extern int gbl_use_modsnap_for_snapshot;
extern const snap_impl_enum gbl_snap_fallback_impl;
extern const snap_impl_enum gbl_snap_backup_fallback_impl;
extern snap_impl_enum gbl_snap_impl;

int gbl_disable_access_controls;

extern char *gbl_recovery_options;
extern const char *gbl_repoplrl_fname;
extern char gbl_dbname[MAX_DBNAME_LENGTH];
extern char **sfuncs;
extern char **afuncs;
static int gbl_nogbllrl; /* don't load /bb/bin/comdb2*.lrl */

static int pre_read_option(char *, int);
static int read_lrl_option(struct dbenv *, char *, struct read_lrl_option_type *, int, int *);
static void fallback_from_snap_impl();

static struct option long_options[] = {
    {"lrl", required_argument, NULL, 0},
    {"repopnewlrl", required_argument, NULL, 0},
    {"recovertotime", required_argument, NULL, 0},
    {"recovertolsn", required_argument, NULL, 0},
    {"recovery_lsn", required_argument, NULL, 0},
    {"pidfile", required_argument, NULL, 0},
    {"help", no_argument, NULL, 'h'},
    {"create", no_argument, &gbl_create_mode, 1},
    {"fullrecovery", no_argument, &gbl_fullrecovery, 1},
    {"no-global-lrl", no_argument, &gbl_nogbllrl, 1},
    {"dir", required_argument, NULL, 0},
    {"tunable", required_argument, NULL, 0},
    {"version", no_argument, NULL, 'v'},
    {"insecure", no_argument, &gbl_disable_access_controls, 1},
    {"admin-mode", no_argument, &gbl_server_admin_mode, 1},
    {NULL, 0, NULL, 0}};

static const char *help_text =
    "Usage: comdb2 [OPTION]... NAME\n"
    "\n"
    "  --create                     creates a new database\n"
    "  --dir PATH                   specify path to database directory\n"
    "  --fullrecovery               runs full recovery after a hot copy\n"
    "  --help                       displays this help text and exit\n"
    "  --insecure                   disable access controls\n"
    "  --lrl PATH                   specify path to alternate lrl file\n"
    "  --recovertolsn FILE:OFFSET   recovers database to FILE:OFFSET\n"
    "  --recovertotime EPOCH        recovers database to EPOCH\n"
    "  --tunable STRING             override tunable\n"
    "  --version                    displays version information and exit\n"
    "\n"
    "Examples:\n"
    "  comdb2 name                  start database:name from default location\n"
    "  comdb2 --create name         create database:name at default location\n"
    "  comdb2 --dir /db name        start database:name at location:/db\n"
    ;

struct read_lrl_option_type {
    int lineno;
    const char *lrlname;
    const char *dbname;
};

void print_version_and_exit()
{
    logmsg(LOGMSG_USER, "comdb2 [%s] [%s] [%s] [%s] [%s]\n",
           gbl_db_version, gbl_db_codename, gbl_db_semver,
           gbl_db_git_version_sha, gbl_db_buildtype);
    exit(2);
}

void print_usage_and_exit(int rc)
{
    logmsg(LOGMSG_USER, "%s", help_text);
    exit(rc);
}

static int write_pidfile(const char *pidfile)
{
    FILE *f;
    f = fopen(pidfile, "w");
    if (f == NULL) {
        logmsg(LOGMSG_ERROR, "%s %s\n", pidfile, strerror(errno));
        return -1;
    }
    fprintf(f, "%d\n", (int)getpid());
    fclose(f);
    return 0;
}

static void set_dbdir(char *dir)
{
    if (dir == NULL)
        return;
    if (*dir == '/') {
        gbl_dbdir = strdup(dir);
        return;
    }
    char *wd = getcwd(NULL, 0);
    int n = snprintf(NULL, 0, "%s/%s", wd, dir);
    gbl_dbdir = malloc(++n);
    snprintf(gbl_dbdir, n, "%s/%s", wd, dir);
    os_free(wd);
}

#include <berkdb/dbinc/queue.h>
struct CmdLineTunable;
struct CmdLineTunable {
    char *arg;
    STAILQ_ENTRY(CmdLineTunable) entry;
};
STAILQ_HEAD(CmdLineTunables, CmdLineTunable) *cmd_line_tunables;

static void add_cmd_line_tunable(char *arg)
{
    if (cmd_line_tunables == NULL) {
        cmd_line_tunables = malloc(sizeof(*cmd_line_tunables));
        STAILQ_INIT(cmd_line_tunables);
    }
    struct CmdLineTunable *t = malloc(sizeof(*t));
    t->arg = arg;
    STAILQ_INSERT_TAIL(cmd_line_tunables, t, entry);
}

void add_cmd_line_tunables_to_file(FILE *f)
{
    if (cmd_line_tunables == NULL)
        return;
    struct CmdLineTunable *t, *tmp;
    STAILQ_FOREACH_SAFE(t, cmd_line_tunables, entry, tmp) {
        fprintf(f, "%s\n", t->arg);
        free(t);
    }
    free(cmd_line_tunables);
    cmd_line_tunables = NULL;
}

static void read_cmd_line_tunables(struct dbenv *dbenv)
{
    if (cmd_line_tunables == NULL)
        return;
    struct read_lrl_option_type options = {
        .lineno = 0, .lrlname = "cmd_line_args", .dbname = dbenv->envname};
    struct CmdLineTunable *t, *tmp;
    STAILQ_FOREACH_SAFE(t, cmd_line_tunables, entry, tmp) {
        read_lrl_option(dbenv, t->arg, &options, strlen(t->arg), NULL);
        free(t);
    }
    free(cmd_line_tunables);
    cmd_line_tunables = NULL;
}

int handle_cmdline_options(int argc, char **argv, char **lrlname)
{
    char *p;
    int c;
    int options_idx;

    while ((c = bb_getopt_long(argc, argv, "hv", long_options, &options_idx)) != -1) {
        if (c == 'h') print_usage_and_exit(0);
        if (c == 'v') print_version_and_exit();
        if (c == '?') return 1;

        switch (options_idx) {
        case 0: /* lrl */ *lrlname = optarg; break;
        case 1: /* repopnewlrl */
            logmsg(LOGMSG_INFO, "repopulate external .lrl mode.\n");
            gbl_repoplrl_fname = optarg;
            gbl_exit = 1;
            break;
        case 2: /* recovertotime */
            logmsg(LOGMSG_WARN, "force full recovery to timestamp %u\n",
                   gbl_recovery_timestamp);
            gbl_recovery_timestamp = strtoul(optarg, NULL, 10);
            gbl_fullrecovery = 1;
            break;
        case 3: /* recovertolsn */
            if ((p = strchr(optarg, ':')) == NULL) {
                logmsg(LOGMSG_FATAL, "recovertolsn: invalid lsn format.\n");
                exit(1);
            }

            p++;
            gbl_recovery_lsn_file = atoi(optarg);
            gbl_recovery_lsn_offset = atoi(p);
            logmsg(LOGMSG_WARN, "force full recovery to lsn %d:%d\n",
                   gbl_recovery_lsn_file, gbl_recovery_lsn_offset);
            gbl_fullrecovery = 1;
            break;
        case 4: /* recovery_lsn */ gbl_recovery_options = optarg; break;
        case 5: /* pidfile */ write_pidfile(optarg); break;
        case 10: /* dir */ set_dbdir(optarg); break;
        case 11: /* tunable */ add_cmd_line_tunable(optarg); break;
        }
    }
    return 0;
}

/* Validates the correctness of the supplied dbnum */
static int validate_dbnum(struct dbenv *dbenv, char *tablename, int dbnum)
{
    if (dbnum < 0) {
        logmsg(LOGMSG_ERROR,
               "Invalid dbnum entry in \"table/comdbg\" directive\n");
        return 1;
    }

    /* if db number matches parent database db number then
     * table name must match parent database name.  otherwise
     * we get mysterious failures to receive qtraps (setting
     * taskname to something not our task name causes initque
     * to fail, and the ldgblzr papers over this) */
    if (dbnum == dbenv->dbnum && strcasecmp(gbl_dbname, tablename) != 0) {
        logmsg(LOGMSG_ERROR,
               "Table %s has same db number as parent database but different "
               "name\n",
               tablename);
        return 1;
    }

    return 0;
}

struct deferred_option {
    char *option;
    int line;
    int len;
    LINKC_T(struct deferred_option) lnk;
};

LISTC_T(struct deferred_option) deferred_options;

static int defer_option(char *option, int len, int line)
{
    struct deferred_option *opt;
    if (len == -1) len = strlen(option);
    opt = malloc(sizeof(struct deferred_option));
    if (opt == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __FILE__, __LINE__);
        return 1;
    }
    opt->option = calloc(1, len + 1);
    if (opt->option == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __FILE__, __LINE__);
        free(opt);
        return 1;
    }
    memcpy(opt->option, option, len);
    opt->line = line;
    opt->len = strlen(opt->option);
    listc_abl(&deferred_options, opt);
    return 0;
}

int deferred_do_commands(struct dbenv *env, char *option,
                         struct read_lrl_option_type *p, int len)
{
    char *tok;
    int st = 0, tlen = 0;
    int rc = 0;

    tok = segtok(option, len, &st, &tlen);
    if (tokcmp(tok, tlen, "sqllogger") == 0) {
        sqllogger_process_message(option + st, len - st);
    } else if (tokcmp(tok, tlen, "do") == 0) {
        rc = process_command(env, option + st, len - st, 0);
    } else if (tokcmp(tok, tlen, "comdbg") == 0) {
        char *tablename;
        int dbnum;

        // table name
        tok = segtok(option, len, &st, &tlen);
        if (tok == 0) {
            logmsg(LOGMSG_ERROR, "comdbg: no table name specified\n");
            return -1;
        }
        tablename = tokdup(tok, tlen);

        // dbnum
        tok = segtok(option, len, &st, &tlen);
        if (tlen == 0) {
            logmsg(LOGMSG_ERROR, "comdbg: no dbnum specified\n");
            free(tablename);
            return -1;
        } else {
            dbnum = toknum(tok, tlen);
            if (validate_dbnum(env, tablename, dbnum)) {
                free(tablename);
                return -1;
            }
        }

        for (int i = 0; i < thedb->num_dbs; ++i) {
            if (strcasecmp(tablename, env->dbs[i]->tablename) == 0) {
                logmsg(LOGMSG_USER, "comdbg: overriding dbnum for %s (old: %d "
                       "new: %d)\n", env->dbs[i]->tablename, env->dbs[i]->dbnum,
                       dbnum);
                env->dbs[i]->dbnum = dbnum;
                break;
            }
        }
        free(tablename);
    }

    return rc;
}

void process_deferred_options(struct dbenv *dbenv, lrl_reader *callback)
{
    struct deferred_option *opt;
    LISTC_FOR_EACH(&deferred_options, opt, lnk) {
        callback(dbenv, opt->option, NULL, opt->len);
    }
}

void clear_deferred_options(void)
{
    struct deferred_option *opt;
    opt = listc_rtl(&deferred_options);
    while (opt) {
        free(opt->option);
        free(opt);
        opt = listc_rtl(&deferred_options);
    }
}

static char *legacy_options[] = {
    "allow_negative_column_size",
    "berkattr elect_highest_committed_gen 0",
    "clean_exit_on_sigterm off",
    "create_default_user",
    "ddl_cascade_drop 0",
    "decoupled_logputs off",
    "disable_inplace_blob_optimization",
    "disable_inplace_blobs",
    "disable_osql_blob_optimization",
    "disable_tagged_api_writes 0",
    "disable_tpsc_tblvers",
    "disallow write from beta if prod",
    "dont_forbid_ulonglong",
    "dont_init_with_inplace_updates",
    "dont_init_with_instant_schema_change",
    "dont_init_with_ondisk_header",
    "dont_prefix_foreign_keys",
    "dont_sort_nulls_with_header",
    "dont_superset_foreign_keys",
    "enable_sql_stmt_caching none",
    "enable_tagged_api",
    "init_with_time_based_genids",
    "legacy_schema on",
    "logmsg level info",
    "logmsg notimestamp",
    "logmsg skiplevel",
    "logput window 1",
    "mask_internal_tunables 0",
    "noblobstripe",
    "nochecksums",
    "nocrc32c",
    "nokeycompr",
    "no_null_blob_fix",
    "norcache",
    "no_static_tag_blob_fix",
    "nullfkey off",
    "nullsort high",
    "off fix_cstr",
    "off osql_odh_blob",
    "off return_long_column_names",
    "on accept_on_child_nets",
    "on disable_etc_services_lookup",
    "online_recovery off",
    "osql_send_startgen off",
    "setattr DIRECTIO 0",
    "setattr ENABLE_SEQNUM_GENERATIONS 0",
    "setattr MASTER_LEASE 0",
    "setattr NET_SEND_GBLCONTEXT 1",
    "sqlsortermaxmmapsize 268435456",
    "unnatural_types 1",
    "wal_osync 1",
    "usenames",
    "setattr max_sql_idle_time 864000",
};
int gbl_legacy_defaults = 0;
int pre_read_legacy_defaults(void *_, void *__)
{
    if (gbl_legacy_defaults != 0) return 0;
    gbl_legacy_defaults = 1;
    for (int i = 0; i < sizeof(legacy_options) / sizeof(legacy_options[0]); i++) {
        pre_read_option(legacy_options[i], strlen(legacy_options[i]));
    }
    return 0;
}

static void read_legacy_defaults(struct dbenv *dbenv,
                                 struct read_lrl_option_type *options)
{
    if (gbl_legacy_defaults != 1) return;
    gbl_legacy_defaults = 2;
    for (int i = 0; i < sizeof(legacy_options) / sizeof(legacy_options[0]); i++) {
        read_lrl_option(dbenv, legacy_options[i], options, strlen(legacy_options[i]), NULL);
    }
}

/* handles "if"'s, returns 1 if this isn't an "if" statement or if the statement
 * is true, 0 if it is false (ie if this line should be skipped)
 * this replaces a couple duplicate sections of code */
static int lrl_if(char **tok_inout, char *line, int line_len, int *st,
                  int *ltok)
{
    char *tok = *tok_inout;
    if (tokcmp(tok, *ltok, "if") == 0) {
        enum mach_class my_class = get_my_mach_class();
        tok = segtok(line, line_len, st, ltok);
        char *label = strndup(tok, *ltok);

        if ((strcmp(label, gbl_myhostname) != 0) &&
            (my_class == CLASS_UNKNOWN || my_class != mach_class_name2class(label))) {
            free(label);
            return 0;
        }
        free(label);

        tok = segtok(line, line_len, st, ltok);
        *tok_inout = tok;
    }

    return 1; /* there was no "if" statement or it was true */
}

void getmyaddr()
{
    if (comdb2_gethostbyname(&gbl_myhostname, &gbl_myaddr) != 0) {
        gbl_myaddr.s_addr = INADDR_LOOPBACK; /* default to localhost */
        return;
    }
}

static int pre_read_option(char *line, int llen)
{
    char *tok;
    int st = 0;
    int ltok;
    comdb2_tunable_err rc;

    tok = segtok(line, llen, &st, &ltok);
    if (ltok == 0 || tok[0] == '#') return 0;

    /* if this is an "if" statement that evaluates to false, skip */
    if (!lrl_if(&tok, line, llen, &st, &ltok)) {
        return 0;
    }

    /* Handle global tunables which are supposed to be read early. */
    rc = handle_lrl_tunable(tok, ltok, line + st, llen - st, READEARLY);

    /* Follow through, if the tunable is not found. */
    if (rc != TUNABLE_ERR_INVALID_TUNABLE) {
        return rc;
    }
    return 0;
}

static void pre_read_lrl_file(struct dbenv *dbenv, const char *lrlname)
{
    FILE *ff;
    char line[512];

    ff = fopen(lrlname, "r");

    if (ff == 0) {
        return;
    }

    while (fgets(line, sizeof(line), ff)) {
        pre_read_option(line, strlen(line));
    }

    fclose(ff); /* lets get one fd back */
}

struct lrl_tables_holder {
    char *line;
    int lineno;
};

static struct dbenv *read_lrl_file_int(struct dbenv *dbenv, const char *lrlname,
                                       int required)
{
    FILE *ff;
    char line[512] = {0}; // valgrind doesn't like sse42 instructions
    struct lrlfile *lrlfile;
    struct read_lrl_option_type options = {
        .lineno = 0, .lrlname = lrlname, .dbname = dbenv->envname,
    };

    dbenv->nsiblings = 1;

    lrlfile = malloc(sizeof(struct lrlfile));
    lrlfile->file = strdup(lrlname);
    listc_atl(&dbenv->lrl_files, lrlfile);

    ff = fopen(lrlname, "r");

    if (ff == 0) {
        if (required) {
            logmsg(LOGMSG_FATAL, "%s : %s\n", lrlname, strerror(errno));
            return 0;
        } else if (errno != ENOENT) {
            logmsgperror(lrlname);
        }
        return dbenv;
    }

    logmsg(LOGMSG_INFO, "processing %s...\n", lrlname);

    struct lrl_tables_holder *lrl_tables = NULL;
    struct lrl_tables_holder *cur_table;
    int num_lrl_tables = 0;
    int is_table = 0;
    while (fgets(line, sizeof(line), ff)) {
        char *s = strchr(line, '\n');
        if (s) *s = 0;
        options.lineno++;
        read_lrl_option(dbenv, line, &options, strlen(line), &is_table);
        if (is_table) { // read lrl tables last
            num_lrl_tables++;
            lrl_tables = realloc(lrl_tables, sizeof(*lrl_tables) * num_lrl_tables);
            cur_table = &lrl_tables[num_lrl_tables - 1];
            cur_table->line = strdup(line);
            cur_table->lineno = options.lineno;
        }
    }

    // read lrl options for creating table last (since nullsort may mess up in_defaults if run after creating tables)
    for (int i = 0; i < num_lrl_tables; i++) {
        cur_table = &lrl_tables[i];
        options.lineno = cur_table->lineno;
        read_lrl_option(dbenv, cur_table->line, &options, strlen(cur_table->line), NULL);
        free(cur_table->line);
    }
    free(lrl_tables);

    /* process legacy options (we deferred them) */

    if (gbl_disable_new_snapshot && (gbl_snap_impl == SNAP_IMPL_NEW)) {
        fallback_from_snap_impl();
    }

    if (gbl_rowlocks) {
        /* We can't ever choose a writer as a deadlock victim in rowlocks mode
           (at least without some kludgery, or snapshots) */
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN, 0);
    }

    fclose(ff);

    return dbenv;
}

static struct dbenv *read_lrl_file(struct dbenv *dbenv, const char *lrlname,
                                   int required)
{
    struct lrlfile *lrlfile;
    struct dbenv *out;
    out = read_lrl_file_int(dbenv, (char *)lrlname, required);

    lrlfile = listc_rtl(&dbenv->lrl_files);
    free(lrlfile->file);
    free(lrlfile);

    return out;
}

static char *get_qdb_name(const char *file)
{
    FILE *f = fopen(file, "r");
    if (f == NULL) {
        logmsg(LOGMSG_ERROR, "%s:fopen(\"%s\"):%s\n", __func__, file,
               strerror(errno));
        return NULL;
    }
    size_t n;
    ssize_t s;
    // Name of queue
    char *name = NULL;
    s = getline(&name, &n, f);
    fclose(f);
    if (s == -1) return NULL;

    name[s - 1] = 0;
    return name;
}

/* defines lrl directives that get ignored by comdb2 */
static int lrltokignore(char *tok, int ltok)
{
    /* used by comdb2backup script */
    if (tokcmp(tok, ltok, "backup") == 0) return 0;
    /* reserved for use by cmdb2filechk script */
    if (tokcmp(tok, ltok, "filechkopts") == 0) return 0;
    /*not a reserved token */
    return 1;
}

static int new_table_from_schema(struct dbenv *dbenv, char *tblname,
                                 char *fname, int dbnum, char *tok)
{
    struct dbtable *db;
    char *csc2;

    csc2 = load_text_file(fname);
    if (!csc2) {
        logmsg(LOGMSG_ERROR, "Error loading text from file %s\n", fname);
        return -1;
    }

    struct errstat err = {0};
    db = create_new_dbtable(dbenv, tblname, csc2, dbnum, 0, 0, 0, &err);
    if (!db) {
        logmsg(LOGMSG_ERROR, "%s\ncsc2:\"%s\"\n", err.errstr, csc2);
        free(csc2);
        return -1;
    }

    free(csc2);
    db->dbs_idx = dbenv->num_dbs;
    dbenv->dbs[dbenv->num_dbs++] = db;

    /* Add table to the hash. */
    hash_add(dbenv->db_hash, db);

    return 0;
}

/*
 * Toggles the 'modsnap' snapshot implementation on and off.
 *
 * switch_val: 1 if toggling on and 0 if toggling off.
 */
static void toggle_modsnap(int switch_val) {
    gbl_use_modsnap_for_snapshot = switch_val;
    gbl_modsnap_asof = switch_val;
}

/*
 * Toggles the 'new' snapshot implementation on and off.
 *
 * switch_val: 1 if toggling on and 0 if toggling off.
 */
static void toggle_new_snapisol(int switch_val) {
    gbl_new_snapisol = switch_val;
    gbl_new_snapisol_asof = switch_val;
    gbl_new_snapisol_logging = switch_val;
}

/*
 * Prints a string representation of a `snap_impl_enum` type,
 * or "UNKNOWN" if the value passed in is not a valid `snap_impl_enum`.
 *
 * impl: The value to output as a string.
 */
const char *snap_impl_str(snap_impl_enum impl) {
    switch (impl) {
    case SNAP_IMPL_ORIG:
        return "ORIGINAL";
        break;
    case SNAP_IMPL_NEW:
        return "NEW";
        break;
    case SNAP_IMPL_MODSNAP:
        return "MODSNAP";
        break;
    default:
        return "UNKNOWN";
        break;
    }
}

static const char * value_on_off(int switch_var) {
    const char * value = switch_var ? "ON" : "OFF";
    return value;
}

void print_snap_config(loglvl lvl) {
    logmsg(lvl, "Snapshot is %s; "
                "Implementation set to '%s'; "
                "gbl_use_modsnap_for_snapshot=%s; "
                "gbl_modsnap_asof=%s; "
                "gbl_new_snapisol=%s; "
                "gbl_new_snapisol_asof=%s; "
                "gbl_new_snapisol_logging=%s\n",
                value_on_off(gbl_snapisol),
                snap_impl_str(gbl_snap_impl),
                value_on_off(gbl_use_modsnap_for_snapshot),
                value_on_off(gbl_modsnap_asof),
                value_on_off(gbl_new_snapisol),
                value_on_off(gbl_new_snapisol_asof),
                value_on_off(gbl_new_snapisol_logging));
}

/*
 * Sets the snapshot implementation.
 *
 * impl: The implementation to be set.
 */
void set_snapshot_impl(snap_impl_enum impl) {
    gbl_snap_impl = impl;
    gbl_snap_impl == SNAP_IMPL_MODSNAP ? toggle_modsnap(1) : toggle_modsnap(0);
    gbl_snap_impl == SNAP_IMPL_NEW ? toggle_new_snapisol(1) : toggle_new_snapisol(0);
}

/*
 * Determines what the fallback implementation should be for the current
 * snapshot implementation and switches to it.
 */
static void fallback_from_snap_impl() {
    const snap_impl_enum fallback_impl = 
        gbl_snap_fallback_impl != gbl_snap_impl ? gbl_snap_fallback_impl : gbl_snap_backup_fallback_impl;

    assert(fallback_impl != gbl_snap_impl);
    set_snapshot_impl(fallback_impl);
}

/*
 * Enables snapshot isolation.
 *
 * dbenv: Parent environment.
 */
static void enable_snapshot(struct dbenv *dbenv) {
    if (gbl_snapisol) {
        return;
    }

    set_snapshot_impl(gbl_snap_impl); 
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_SNAPISOL, 1);
    gbl_snapisol = 1;
}

#define parse_lua_funcs(pfx)                                                                                           \
    do {                                                                                                               \
        tok = segtok(line, strlen(line), &st, &ltok);                                                                  \
        int num = toknum(tok, ltok);                                                                                   \
        pfx##funcs = malloc(sizeof(char *) * (num + 1));                                                               \
        int i;                                                                                                         \
        for (i = 0; i < num; ++i) {                                                                                    \
            tok = segtok(line, strlen(line), &st, &ltok);                                                              \
            pfx##funcs[i] = tokdup(tok, ltok);                                                                         \
        }                                                                                                              \
        pfx##funcs[i] = NULL;                                                                                          \
    } while (0)

static int read_lrl_option(struct dbenv *dbenv, char *line,
                           struct read_lrl_option_type *options, int len,
                           int *is_table)
{
    char *tok;
    int st = 0;
    int ltok;
    int ii, kk;
    int rc;

    if (is_table)
        *is_table = 0;

    tok = segtok(line, len, &st, &ltok);

    /* Skip processing this line, if it is:
      - empty, or
      - commented out (#), or
      - an "if <tier> <lrl>" line and we're in a different tier
    */

    if (ltok == 0 || tok[0] == '#' || (!lrl_if(&tok, line, len, &st, &ltok))) {
        return 0;
    }

    if (tokcmp(tok, ltok, "on") == 0) {
        change_switch(1, line, len, st);
    } else if (tokcmp(tok, ltok, "off") == 0) {
        change_switch(0, line, len, st);
    } else if (tokcmp(tok, ltok, "setattr") == 0) {
        char name[48] = {0}; // oh valgrind
        int value;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "%s:%d: expected attribute name\n",
                   options->lrlname, options->lineno);
            return -1;
        }
        tokcpy0(tok, ltok, name, sizeof(name));
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "%s:%d: expected attribute value\n",
                   options->lrlname, options->lineno);
            return -1;
        }
        value = toknum(tok, ltok);
        if (bdb_attr_set_by_name(NULL, dbenv->bdb_attr, name, value) != 0) {
            logmsg(LOGMSG_ERROR, "%s:%d: bad attribute name %s\n",
                   options->lrlname, options->lineno, name);
        }
    } else if (tokcmp(tok, ltok, "sqlsortermaxmmapsize") == 0) {
        tok = segtok(line, len, &st, &ltok);
        long long maxmmapsz = toknumll(tok, ltok);
        logmsg(LOGMSG_INFO, "setting sqlsortermaxmmapsize to %lld bytes\n",
               maxmmapsz);
        sqlite3_config(SQLITE_CONFIG_MMAP_SIZE, SQLITE_DEFAULT_MMAP_SIZE,
                       maxmmapsz);
    } else if (tokcmp(tok, ltok, "cache") == 0) { /* cache <nn> <kb|mb|gb> */
        tok = segtok(line, len, &st, &ltok);
        int nn = toknum(tok, ltok);
        tok = segtok(line, len, &st, &ltok);
        if (tokcmp(tok, ltok, "kb") == 0)
            dbenv->cacheszkb = nn;
        else if (tokcmp(tok, ltok, "mb") == 0)
            dbenv->cacheszkb = nn * 1024;
        else if (tokcmp(tok, ltok, "gb") == 0)
            dbenv->cacheszkb = nn * 1024 * 1024;
        else
            logmsg(LOGMSG_ERROR, "bad unit for cache sz - needs kb|mb|gb\n");
        logmsg(LOGMSG_INFO, "cache size is %dKB\n", dbenv->cacheszkb);
    } else if (tokcmp(tok, ltok, "dedicated_network_suffixes") == 0) {
        while (1) {
            char suffix[50];

            tok = segtok(line, len, &st, &ltok);
            if (ltok == 0) break;
            if (ltok >= 50) {
                logmsg(LOGMSG_ERROR, "suffix name too long. max : %d\n",
                                    49);
                return -1;
            }
            tokcpy(tok, ltok, suffix);

            if (net_add_to_subnets(suffix, options->lrlname)) {
                return -1;
            }
        }
    } else if (tokcmp(tok, ltok, "nullsort") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected argument for nullsort\n");
            return -1;
        } else if (tokcmp(tok, ltok, "high") == 0) {
            /* default */
            comdb2_types_set_null_bit(null_bit_high);
            logmsg(LOGMSG_INFO, "nulls will sort high\n");
        } else if (tokcmp(tok, ltok, "low") == 0) {
            comdb2_types_set_null_bit(null_bit_low);
            logmsg(LOGMSG_INFO, "nulls will sort low\n");
        } else {
            logmsg(LOGMSG_ERROR, "Invalid argument for nullsort\n");
        }
    } else if (tokcmp(tok, ltok, "port") == 0) {
        char hostname[255];
        int port1 = 0;
        int port2 = 0;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR,
                   "Expected hostname port for \"port\" directive\n");
            return -1;
        }
        if (ltok >= sizeof(hostname)) {
            logmsg(LOGMSG_ERROR,
                   "Unexpectedly long hostname %.*s len %d max %zu\n", ltok,
                   hostname, ltok, sizeof(hostname));
            return -1;
        }
        tokcpy(tok, ltok, hostname);
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR,
                   "Expected hostname port for \"port\" directive\n");
            return -1;
        }
        port1 = toknum(tok, ltok);
        if (port1 <= 0 || port1 >= 65536) {
            logmsg(LOGMSG_ERROR, "Port out of range for \"port\" directive\n");
            return -1;
        }
        /* optional port2, defaults to 0 */
        tok = segtok(line, len, &st, &ltok);
        if (ltok != 0) {
            port2 = toknum(tok, ltok);
            if (port2 <= 0 || port2 >= 65536) {
                logmsg(LOGMSG_ERROR, "Port out of range for \"port\" directive\n");
                return -1;
            }
        }
        if (dbenv->nsiblings > 1) {
            for (ii = 0; ii < dbenv->nsiblings; ii++) {
                if (strcmp(dbenv->sibling_hostname[ii], hostname) == 0) {
                    dbenv->sibling_port[ii][NET_REPLICATION] = port1;
                    dbenv->sibling_port[ii][NET_SQL] = port2;
                    break;
                }
            }
            if (ii >= dbenv->nsiblings) {
                logmsg(LOGMSG_ERROR,
                       "Don't recognize %s as part of my cluster.  Please make "
                       "sure \"port\" directives follow \"cluster nodes\"\n",
                       hostname);
                return -1;
            }
        } else if (strcmp(hostname, "localhost") == 0) {
            /* nsiblings == 1 means there's no other nodes in the cluster */
            dbenv->sibling_port[0][NET_REPLICATION] = port1;
            dbenv->sibling_port[0][NET_SQL] = port2;
        }
    } else if (tokcmp(tok, ltok, "remsql_whitelist") == 0) {
        /* expected parse line: remsql_whitelist db1 db2 ...  */
        tok = segtok(line, len, &st, &ltok);
        while (ltok) {
            int lrc = fdb_add_dbname_to_whitelist(tok);
            if (lrc)
                return -1;
            tok = segtok(line, len, &st, &ltok);
        }
    } else if (tokcmp(tok, ltok, "cluster") == 0) {
        /*parse line...*/
        tok = segtok(line, len, &st, &ltok);
        if (tokcmp(tok, ltok, "nodes") == 0) {
            /*create replication group. only me by default*/
            while (1) {
                char nodename[512];
                tok = segtok(line, len, &st, &ltok);
                if (ltok == 0) break;
                if (ltok > sizeof(nodename)) {
                    logmsg(LOGMSG_ERROR,
                           "host %.*s name too long (expected < %zu)\n", ltok,
                           tok, sizeof(nodename));
                    return -1;
                }
                tokcpy(tok, ltok, nodename);
                errno = 0;

                /* Check to see if this name is another name for me. */
                struct in_addr addr;
                char *name = nodename;
                int rc = comdb2_gethostbyname(&name, &addr);
                if (rc!=0) {
                    logmsg(LOGMSG_FATAL, "Could not resolve host %s. Exiting Database...\n", name);
                    exit(1);
                }
                if (rc==0 && addr.s_addr == gbl_myaddr.s_addr) {
                    /* Assume I am better known by this name. */
                    gbl_myhostname_interned = intern_ptr(name);
                    gbl_myhostname = gbl_myhostname_interned->str;
                    gbl_mynodeid = machine_num(gbl_myhostname);
                }
                if (strcmp(gbl_myhostname, name) == 0 &&
                    gbl_rep_node_pri == 0) {
                    /* assign the priority of current node according to its
                     * sequence in nodes list. */
                    gbl_rep_node_pri = REPMAX - dbenv->nsiblings;
                    continue;
                }
                /* lets ignore duplicate for now and make a list out of what is
                 * given in lrl */
                for (kk = 1; kk < dbenv->nsiblings &&
                             strcmp(dbenv->sibling_hostname[kk], name);
                     kk++)
                    ; /*look for dupes*/
                if (kk == dbenv->nsiblings) {
                    /*not a dupe.*/
                    if (dbenv->nsiblings >= REPMAX) {
                        logmsg(LOGMSG_ERROR,
                               "too many sibling nodes (max=%d) in lrl %s\n",
                               REPMAX, options->lrlname);
                        return -1;
                    }
                    dbenv->sibling_hostname[dbenv->nsiblings] =
                        intern(name);
                    for (int netnum = 0; netnum < NET_MAX; netnum++)
                        dbenv->sibling_port[dbenv->nsiblings][netnum] = 0;
                    dbenv->nsiblings++;
                }
            }
            dbenv->sibling_hostname[0] = gbl_myhostname;
        }
    } else if (tokcmp(tok, ltok, "machine_classes") == 0) {
        int classval = 1;
        tok = segtok(line, len, &st, &ltok);
        while (ltok) {
            int lrc = mach_class_addclass(tok, classval);
            if (lrc)
                return -1;
            tok = segtok(line, len, &st, &ltok);
            classval++;
        }
    } else if (tokcmp(tok, ltok, "pagesize") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify options for pagesize.\n");
            return -1;
        }

        if (tokcmp(tok, ltok, "all") == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify size for all\n");
            return -1;
#if 0 
            NOT SURE WHY THE SHORTEN PATH, but commenting out to
                remove compiler warning
                ii=toknum(tok,ltok);

            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEDTA, ii);
            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEIX, ii);
#endif
        } else if (tokcmp(tok, ltok, "dta") == 0) {
            tok = segtok(line, len, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "Need to specify size for dta\n");
                return -1;
            }
            ii = toknum(tok, ltok);

            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEDTA, ii);
        } else if (tokcmp(tok, ltok, "ix") == 0) {
            tok = segtok(line, len, &st, &ltok);
            if (ltok == 0) {
                logmsg(LOGMSG_ERROR, "Need to specify size for ix\n");
                return -1;
            }
            ii = toknum(tok, ltok);
            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGESIZEIX, ii);
        } else {
            logmsg(LOGMSG_ERROR, "Need to specify options for pagesize.\n");
            return -1;
        }
    }
    /* we cant actually BECOME a different version of comdb2 at runtime,
       just because the lrl file told us to.  what we can do is check for
       some confused situations where the lrl file is for a different
       version of comdb2 than we are */
    else if (tokcmp(tok, ltok, "version") == 0) {
        tok = segtok(line, len, &st, &ltok);
        for (ii = 0; ii < 10; ii++)
            if (!isprint(tok[ii])) tok[ii] = '\0';

        logmsg(LOGMSG_ERROR, "lrl file for comdb2 version %s found\n", tok);

        if (strcmp(tok, COMDB2_VERSION)) {
            logmsg(LOGMSG_ERROR, "but we are version %s\n", COMDB2_VERSION);
            exit(1);
        }
    }

    else if (tokcmp(tok, ltok, "checkctags") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Need to specify options for checkctags.\n");
            return -1;
        }

        if (tokcmp(tok, ltok, "off") == 0) {
            logmsg(LOGMSG_INFO, "check tag logic is now off\n");
            gbl_check_client_tags = 0;
        } else if (tokcmp(tok, ltok, "soft") == 0) {
            logmsg(LOGMSG_INFO, "check tag logic will now produce warning\n");
            gbl_check_client_tags = 2;
        } else if (tokcmp(tok, ltok, "full") == 0) {
            logmsg(LOGMSG_INFO,
                   "check tag logic will now error out to client\n");
            gbl_check_client_tags = 1;
        } else {
            logmsg(LOGMSG_INFO, "Need to specify options for checktags.\n");
            return -1;
        }
    } else if (tokcmp(tok, ltok, "sync") == 0) {
        rc = process_sync_command(dbenv, line, len, st);
        if (rc != 0) {
            return -1;
        }
    } else if (tokcmp(tok, ltok, "sfuncs") == 0) {
        parse_lua_funcs(s);
    } else if (tokcmp(tok, ltok, "afuncs") == 0) {
        parse_lua_funcs(a);
    } else if (tokcmp(tok, ltok, "queuedb") == 0) {
        int nqdbs = thedb->num_qdbs;
        thedb->qdbs = realloc(thedb->qdbs, (nqdbs + 1) * sizeof(dbtable *));
        if (thedb->qdbs == NULL) {
            logmsgperror("realloc");
            return -1;
        }
        tok = segtok(line, len, &st, &ltok);
        char *qfname = tokdup(tok, ltok);
        char *name = get_qdb_name(qfname);
        if (name == NULL) {
            logmsg(LOGMSG_ERROR, "Failed to obtain queuedb name from:%s\n",
                   qfname);
            return -1;
        }
        dbtable *qdb = newqdb(dbenv, name, 65536, 65536, 1);
        if (qdb == NULL) {
            logmsg(LOGMSG_ERROR, "newqdb failed for:%s\n", name);
            return -1;
        }
        free(name);
        free(qfname);
        thedb->qdbs[nqdbs] = qdb;
        ++thedb->num_qdbs;
    } else if (tokcmp(tok, ltok, "table") == 0) {
        if (is_table) {
            *is_table = 1;
            return -1;
        }

        /*
         * variants:
         * table foo foo.lrl        # load a table given secondary lrl files
         * table foo foo.csc        # load a table from a csc file
         * table foo foo.csc dbnum  # load a table from a csc file and have
         * # it also accept requests as db# dbnum
         *
         * relative paths are looked for relative to the parent lrl
         *
         * table     sqlite_stat1 bin/comdb2_stats.csc2
         */
        char *fname;
        char *tblname;
        char tmpname[MAXTABLELEN];

        dbenv->dbs = realloc(dbenv->dbs,
                             (dbenv->num_dbs + 1) * sizeof(struct dbtable *));

        tok = segtok(line, len, &st, &ltok); /* tbl name */
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Malformed \"table\" directive\n");
            return -1;
        }

        tblname = tokdup(tok, ltok);

        tok = segtok(line, len, &st, &ltok);
        fname = getdbrelpath(tokdup(tok, ltok));

        /* if it's a schema file, allocate a struct dbtable, populate with crap
         * data, then load schema.  if it's an lrl file, we don't support it
         * anymore */

        if (strstr(fname, ".lrl") != 0) {
            logmsg(LOGMSG_ERROR, "this version of comdb2 does not support "
                                 "loading another lrl from this lrl\n");
            return -1;
        } else if (strstr(fname, ".csc2") != 0) {
            int dbnum;

            bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_GENIDS, 1);

            /* optional dbnum */
            tok = segtok(line, len, &st, &ltok);
            if (ltok >= MAXTABLELEN) {
                logmsg(LOGMSG_ERROR, "dbnum too long. Max %d\n", MAXTABLELEN-1);
                return -1;
            }
            tokcpy(tok, ltok, tmpname);
            if (ltok == 0)
                dbnum = 0;
            else {
                dbnum = toknum(tok, ltok);

                if (validate_dbnum(dbenv, tblname, dbnum)) {
                    return -1;
                }
            }

            rc = new_table_from_schema(dbenv, tblname, fname, dbnum, tok);
            if (rc)
                return rc;

        } else {
            logmsg(LOGMSG_ERROR, "Invalid table option\n");
            return -1;
        }
    } else if (tokcmp(tok, ltok, "allow") == 0 ||
               tokcmp(tok, ltok, "disallow") == 0 ||
               tokcmp(tok, ltok, "clrpol") == 0 ||
               tokcmp(tok, ltok, "setclass") == 0) {
        if (dbenv->num_allow_lines >= dbenv->max_allow_lines) {
            dbenv->max_allow_lines += 1;
            dbenv->allow_lines = realloc(
                dbenv->allow_lines, sizeof(char *) * dbenv->max_allow_lines);
            if (!dbenv->allow_lines) {
                logmsg(LOGMSG_ERROR, "out of memory\n");
                return -1;
            }
        }
        dbenv->allow_lines[dbenv->num_allow_lines] = strdup(line);
        if (!dbenv->allow_lines[dbenv->num_allow_lines]) {
            logmsg(LOGMSG_ERROR, "out of memory\n");
            return -1;
        }
        dbenv->num_allow_lines++;
    } else if (tokcmp(tok, ltok, "comdbg") == 0) {
        defer_option(line, len, options->lineno);
    } else if (tokcmp(tok, ltok, "debug") == 0) {
        tok = segtok(line, len, &st, &ltok);
        while (ltok > 0) {
            if (tokcmp(tok, ltok, "rtcpu") == 0) {
                logmsg(LOGMSG_INFO, "enable rtcpu debugging\n");
                gbl_rtcpu_debug = 1;
            }
            tok = segtok(line, len, &st, &ltok);
        }
    } else if (tokcmp(tok, ltok, "resource") == 0) {
        /* I used to allow a one argument version of resource -
         *   resource <filepath>
         * This wasn't implemented too well and caused problems.  Explicit
         * two argument version is better:
         *   resource <name> <filepath>
         */
        char *name = NULL;
        char *file = NULL;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected resource name\n");
            return -1;
        }
        name = tokdup(tok, ltok);
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected resource path\n");
            free(name);
            return -1;
        }
        file = tokdup(tok, ltok);
        addresource(name, file);
        if (name) free(name);
        if (file) free(file);
    } else if (tokcmp(tok, ltok, "procedure") == 0) {
        char *name;
        char *jartok;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected stored procedure name\n");
            return -1;
        }
        name = tokdup(tok, ltok);
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR,
                   "Expected stored procedure jar resource name\n");
            free(name);
            return -1;
        }
        jartok = tokdup(tok, ltok);
        if (javasp_add_procedure(name, jartok, line + st) != 0) return -1;
        free(name);
        free(jartok);
    } else if (tokcmp(tok, ltok, "use_parallel_schema_change") == 0) {
        gbl_default_sc_scanmode = SCAN_PARALLEL;
        logmsg(LOGMSG_INFO,
               "using parallel scan mode for schema changes by default\n");
    } else if (tokcmp(tok, ltok, "use_llmeta") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_LLMETA, 1);
        logmsg(LOGMSG_INFO, "using low level meta table\n");
    } else if (tokcmp(tok, ltok, "enable_logical_logging") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_SNAPISOL, 1);
        logmsg(LOGMSG_INFO, "Enabled logical logging\n");
    } else if (tokcmp(tok, ltok, "enable_snapshot_isolation") == 0) {
        enable_snapshot(dbenv);
    } else if (tokcmp(tok, ltok, "enable_new_snapshot") == 0 ||
               tokcmp(tok, ltok, "enable_new_snapshot_asof") == 0) {
        set_snapshot_impl(SNAP_IMPL_NEW);
        enable_snapshot(dbenv);
    } else if (tokcmp(tok, ltok, "enable_new_snapshot_logging") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_SNAPISOL, 1);
        gbl_new_snapisol_logging = 1;
        logmsg(LOGMSG_INFO, "Enabled new snapshot logging\n");
    } else if (tokcmp(tok, ltok, "disable_new_snapshot") == 0) {
        gbl_disable_new_snapshot = 1;
        logmsg(LOGMSG_INFO, "Disabled new snapshot\n");
    } else if (tokcmp(tok, ltok, "enable_serial_isolation") == 0) {
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_SNAPISOL, 1);
        gbl_snapisol = 1;
        gbl_selectv_rangechk = 1;
    } else if (tokcmp(tok, ltok, "mallocregions") == 0) {
        if ((strcmp(COMDB2_VERSION, "2") == 0) ||
            (strcmp(COMDB2_VERSION, "old") == 0)) {
            logmsg(LOGMSG_INFO, "Using os-supplied malloc for regions\n");
            berkdb_use_malloc_for_regions();
            gbl_malloc_regions = 1;
        }
    } else if (lrltokignore(tok, ltok) == 0) {
        /* ignore this line */
    } else if (tokcmp(tok, ltok, "proxy") == 0) {
        char *proxy_line;
        tok = segline(line, len, &st, &ltok);
        if (ltok > 0) {
            proxy_line = tokdup(tok, ltok);
            handle_proxy_lrl_line(proxy_line);
            free(proxy_line);
        }
    } else if (tokcmp(tok, ltok, "setsqlattr") == 0) {
        char *attrname = NULL;
        char *attrval = NULL;
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected sql attribute name\n");
            return -1;
        }
        attrname = tokdup(tok, ltok);
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected sql attribute name\n");
            free(attrname);
            return -1;
        }
        attrval = tokdup(tok, ltok);
        sqlite3_set_tunable_by_name(attrname, attrval);
        free(attrname);
        free(attrval);
    } else if (tokcmp(tok, ltok, "querylimit") == 0) {
        rc = query_limit_cmd(line, len, st);
        if (rc) return -1;
    } else if (tokcmp(tok, ltok, "iopool") == 0) {
        berkdb_iopool_process_message(line, len, st);
    } else if (tokcmp(tok, ltok, "decimal_rounding") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok > 0 && tok[0]) {
            gbl_decimal_rounding = dec_parse_rounding(tok, ltok);
            logmsg(LOGMSG_INFO, "Default decimal rounding is %s\n",
                   dec_print_mode(gbl_decimal_rounding));
        } else {
            logmsg(LOGMSG_ERROR,
                   "Missing option for decimal rounding, current is %s\n",
                   dec_print_mode(gbl_decimal_rounding));
        }
    } else if (tokcmp(tok, ltok, "berkattr") == 0) {
        char *attr = NULL, *value = NULL;
        tok = segtok(line, len, &st, &ltok);
        if (tok) {
            attr = tokdup(tok, ltok);
            tok = segtok(line, len, &st, &ltok);
            if (tok) {
                value = tokdup(tok, ltok);
            }
        }
        if (attr && value)
            bdb_berkdb_set_attr_after_open(dbenv->bdb_attr, attr, value,
                                           atoi(value));
        free(attr);
        free(value);
    } else if (tokcmp(line, ltok, "sqllogger") == 0) {
        /* This is one of several things we can't do until we have more of an
         * environment set up. What would be nice is if processing options was
         * decoupled from reading files, so we could build a list of deferred
         * options and call process_lrl_line on them one by one. One day. No,
         * pre_read_lrl_file isn't what I want. */
        defer_option(line, len, options->lineno);
    } else if (tokcmp(line, ltok, "location") == 0) {
        /* ignore - these are processed by init_file_locations */
    } else if (tokcmp(tok, ltok, "include") == 0) {
        char *file;
        struct lrlfile *lrlfile;

        tok = segtok(line, len, &st, &ltok);
        if (tok == NULL) {
            logmsg(LOGMSG_ERROR, "expected file after include\n");
            return -1;
        }
        file = tokdup(tok, ltok);

        LISTC_FOR_EACH(&dbenv->lrl_files, lrlfile, lnk)
        {
            if (strcmp(lrlfile->file, file) == 0) {
                logmsg(LOGMSG_ERROR, "Attempted to nest includes for %s\n",
                       file);
                LISTC_FOR_EACH(&dbenv->lrl_files, lrlfile, lnk)
                {
                    logmsg(LOGMSG_ERROR, ">> %s\n", lrlfile->file);
                }
                free(file);
                return -1;
            }
        }

        read_lrl_file(dbenv, file, 0);
    } else if (tokcmp(line, ltok, "do") == 0) {
        defer_option(line, len, options->lineno);
    } else if (tokcmp(line, ltok, "default_datetime_precision") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok <= 0) {
            logmsg(LOGMSG_ERROR, "Expected # for temptable_limit.\n");
            return 0;
        }
        DTTZ_TEXT_TO_PREC(tok, gbl_datetime_precision, 0, return 0);
    } else if (tokcmp(line, strlen("ssl"), "ssl") == 0) {
        /* Let's have a separate function for ssl directives. */
        rc = ssl_process_lrl(line, len);
        if (rc != 0)
            return -1;
    } else if (tokcmp(tok, ltok, "legacy_defaults") == 0) {
        /* Process here because can't pass to handle_lrl_tunable (where it is
         * marked as READEARLY) */
        read_legacy_defaults(dbenv, options);

    } else if (tokcmp(tok, ltok, "replicate_from") == 0) {
        /* 'replicate_from <dbname> <prod|beta|alpha|dev|host|@hst1,hst2,hst3..>' */
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_FATAL, "Must specify a database to replicate from\n");
            exit(1);
        }
        gbl_physrep_source_dbname = tokdup(tok, ltok);

        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_FATAL, "Must specify a type\n");
            exit(1);
        }
        gbl_physrep_source_host = tokdup(tok, ltok);

        gbl_is_physical_replicant = 1;

    } else if (tokcmp(tok, ltok, "physrep_metadb") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_FATAL, "Must specify a database to replicate from\n");
            exit(1);
        }
        gbl_physrep_metadb_name = tokdup(tok, ltok);

        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_FATAL, "Must specify a type\n");
            exit(1);
        }
        gbl_physrep_metadb_host = tokdup(tok, ltok);
    } else if (tokcmp(tok, ltok, "physrep_ignore") == 0) {
        /* Tables that should ignore replication */
        while ((tok = segtok(line, len, &st, &ltok)) != NULL && ltok > 0) {
            char *table = tokdup(tok, ltok);
            logmsg(LOGMSG_INFO, "Physrep ignoring table %s\n", table);
            physrep_add_ignore_table(table);
        }
    } else if (tokcmp(tok, ltok, "physrep_fanout_override") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_FATAL, "physrep_fanout_override requires dbname & fanout value\n");
            return -1;
        }
        char *dbname = alloca(ltok + 1);
        tokcpy(tok, ltok, dbname);

        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_FATAL, "physrep_fanout_override requires dbname & fanout value\n");
            return -1;
        }
        int fanout = toknum(tok, ltok);
        physrep_fanout_override(dbname, fanout);
    } else if (tokcmp(tok, ltok, "replicate_wait") == 0) {
        tok = segtok(line, len, &st, &ltok);

        /* need to replicate a database */
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR,
                   "Must specify # of seconds to wait for timestamp\n");
            return -1;
        }
        gbl_deferred_phys_flag = 1;

        char *wait = tokdup(tok, ltok);
        gbl_deferred_phys_update = atol(wait);
        logmsg(LOGMSG_USER, "Waiting for %u seconds for replication\n",
               gbl_deferred_phys_update);
        free(wait);
    } else if (tokcmp(tok, ltok, "remap_machine_class_to_fdb_tier") == 0) {
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected argument for remap_machine_class_to_fdb_tier\n");
            return -1;
        }
        const char *cls = tokdup(tok, ltok);
        tok = segtok(line, len, &st, &ltok);
        if (ltok == 0) {
            logmsg(LOGMSG_ERROR, "Expected argument for remap_machine_class_to_fdb_tier\n");
            return -1;
        }
        const char *tier = tokdup(tok, ltok);
        if (mach_class_remap_fdb_tier(cls, tier) == 0)
            logmsg(LOGMSG_USER, "Successfully remapped machine class '%s' to fdb tier '%s'\n", cls, tier);
        else
            logmsg(LOGMSG_USER, "Failed remapping machine class '%s' to fdb tier '%s'\n", cls, tier);
    } else if (tokcmp(tok, ltok, "allow-coordinator") == 0) {
        process_allow_coordinator(&line[st], len - st);
    } else {
        // see if any plugins know how to handle this
        struct lrl_handler *h;
        rc = 1;
        LISTC_FOR_EACH(&dbenv->lrl_handlers, h, lnk) {
            rc = h->handle(dbenv, tok);
            if (rc == 0)
                break;
        }

        if (rc) {
            /* Handle tunables registered under tunables sub-system. */
            rc = handle_lrl_tunable(tok, ltok, line + st, len - st, 0);

            if (rc) {
                if (gbl_bad_lrl_fatal) {
                    logmsg(LOGMSG_ERROR, "unknown opcode '%.*s' in lrl %s\n", ltok,
                            tok, options->lrlname);
                    return -1;
                } else {
                    logmsg(LOGMSG_WARN, "unknown opcode '%.*s' in lrl %s\n", ltok,
                            tok, options->lrlname);
                }
            }
        }
    }

    if (gbl_disable_new_snapshot && (gbl_snap_impl == SNAP_IMPL_NEW)) {
        fallback_from_snap_impl();
    }

    if (gbl_rowlocks) {
        /* We can't ever choose a writer as a deadlock victim in rowlocks mode
           (at least without some kludgery, or snapshots) */
        bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_PAGE_ORDER_TABLESCAN, 0);
    }

    return 0;
}

static int read_config_dir(struct dbenv *dbenv, char *dir)
{
    DIR *d = NULL;
    int rc = 0;
    struct dirent *ent, *out;

    ent = malloc(bb_dirent_size(dir));

    d = opendir(dir);
    if (d == NULL) {
        rc = -1;
        goto done;
    }
    while (bb_readdir(d, ent, &out) == 0 && out != NULL) {
        int len;
        len = strlen(ent->d_name);
        if (strcmp(ent->d_name + len - 4, ".lrl") == 0) {
            char *file = comdb2_asprintf("%s/%s", dir, ent->d_name);
            pre_read_lrl_file(dbenv, file);
            rc = (read_lrl_file(dbenv, file, 0) == NULL);
            if (rc) logmsg(LOGMSG_ERROR, "Error processing %s\n", file);
            free(file);
            if (rc) goto done;
        }
    }

done:
    if (d) closedir(d);
    free(ent);
    return rc;
}

static int global_is_local(const char *lrlfile, const char *pwd,
                           const char *cfgfile)
{
    int pwdlen;

    if (!pwd || !lrlfile)
        return 0;

    pwdlen = strlen(pwd) + 1 /* / */;

    if (strlen(lrlfile) <= pwdlen)
        return 0;

    if (strncasecmp(lrlfile, pwd, strlen(pwd)))
        return 0;

    if (strncasecmp(&lrlfile[pwdlen], cfgfile, strlen(cfgfile) + 1))
        return 0;

    /* identical */
    return 1;
}

int read_lrl_files(struct dbenv *dbenv, const char *lrlname)
{
    int loaded_comdb2 = 0;
    int loaded_comdb2_local = 0;
    const char *crtdir = getenv("PWD");

    listc_init(&deferred_options, offsetof(struct deferred_option, lnk));
    listc_init(&dbenv->lrl_files, offsetof(struct lrlfile, lnk));

#   ifdef LEGACY_DEFAULTS
    struct read_lrl_option_type options = {0};
    options.lrlname = "legacy_defaults";
    options.dbname = dbenv->envname;
    pre_read_legacy_defaults(NULL, NULL);
    read_legacy_defaults(dbenv, &options);
#   endif

    if (lrlname) pre_read_lrl_file(dbenv, lrlname);

    /* if we havn't been told not to load the /bb/bin/ config files */
    if (!gbl_nogbllrl) {
        char *lrlfile;

        /* firm wide defaults */
        lrlfile = comdb2_location("config", "comdb2.lrl");
        loaded_comdb2 = global_is_local(lrlfile, crtdir, "comdb2.lrl");
        if (!read_lrl_file(dbenv, lrlfile, 0 /*not required*/)) {
            free(lrlfile);
            return 0;
        }
        free(lrlfile);

        /* local defaults */
        lrlfile = comdb2_location("config", "comdb2_local.lrl");
        loaded_comdb2_local = global_is_local(lrlfile, crtdir, "comdb2_local.lrl");
        if (!read_lrl_file(dbenv, lrlfile, 0)) {
            free(lrlfile);
            return 0;
        }
        free(lrlfile);

        char *confdir = comdb2_location("config", "comdb2.d");
        struct stat st;
        int rc = stat(confdir, &st);
        if (rc == 0 && S_ISDIR(st.st_mode)) {
            if (read_config_dir(dbenv, confdir)) {
                free(confdir);
                return 0;
            }
        }
        free(confdir);
    } else {
        /* disable loading comdb2.lrl and comdb2_local.lrl with an absolute
         * path in /bb/bin. comdb2.lrl and comdb2_local.lrl in the pwd are
         * still loaded */
        logmsg(LOGMSG_INFO, "Not loading %s/bin/comdb2.lrl and "
                            "%s/bin/comdb2_local.lrl.\n",
               gbl_config_root, gbl_config_root);
    }

    /* look for overriding lrl's in the local directory */
    if (!loaded_comdb2 &&
        !read_lrl_file(dbenv, "comdb2.lrl", 0 /*not required*/)) {
        return 0;
    }

    /* local defaults */
    if (!loaded_comdb2_local &&
        !read_lrl_file(dbenv, "comdb2_local.lrl", 0 /*not required*/)) {
        return 0;
    }

    /* if env variable is set, process another lrl.. */
    const char *envlrlname = getenv("COMDB2_CONFIG");
    if (envlrlname && !read_lrl_file(dbenv, envlrlname, 1 /*required*/)) {
        return 1;
    }

    /* this database */
    if (lrlname && !read_lrl_file(dbenv, lrlname, 1 /*required*/)) {
        return 1;
    }

    /* switch to keyless mode as long as no mode has been selected yet */
    bdb_attr_set(dbenv->bdb_attr, BDB_ATTR_GENIDS, 1);

    if (dbenv->basedir == NULL && gbl_dbdir) {
        dbenv->basedir = strdup(gbl_dbdir);
    }
    if (dbenv->basedir == NULL) {
        if ((dbenv->basedir = getenv("COMDB2_DB_DIR")) != NULL) {
            dbenv->basedir = strdup(dbenv->basedir);
        }
    }
    if (dbenv->basedir == NULL) {
        dbenv->basedir = comdb2_location("database", "%s", dbenv->envname);
    }
    if (dbenv->basedir == NULL) {
        logmsg(LOGMSG_ERROR, "must specify database directory\n");
        return 0;
    }

    if (lrlname == NULL) {
        char *lrl =
            comdb2_asprintf("%s/%s.lrl", dbenv->basedir, dbenv->envname);
        if (access(lrl, F_OK) == 0) {
            if (read_lrl_file(dbenv, lrl, 0) == NULL) {
                return 0;
            }
            lrlname = lrl;
        } else
            /* (NC) TODO: Should this be freed here? */
            free((char *)lrlname);
    }

    if (!gbl_create_mode) {
        read_cmd_line_tunables(dbenv);

        /* usenames is not supported with physical replication */
        if (gbl_is_physical_replicant && !gbl_nonames) {
            logmsg(LOGMSG_ERROR,
                   "Cannot start a physical replicant under usenames\n");
            return 1;
        }
    }

    return 0;
}
