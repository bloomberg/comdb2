/* all macc's globals */
#include "macc.h"
#include "maccparse.h"
#include <string.h>

struct constraint constraints[MAXCNSTRTS];
struct check_constraint check_constraints[MAXCNSTRTS];
struct symbol symb[MAX];
struct table tables[MAXTBLS];
struct constant constants[MAX];
unsigned int un_start[MAX];
unsigned int un_end[MAX];
unsigned int un_reset[MAX];
unsigned int un_case[MAX];
short cur_dpth[MAX_DEPTH];
int dpth_idx = 0;

struct key *keys[MAXKEYS], *workkey, *rngs[MAXRNGS];
int keyixnum[MAXKEYS];
int keyexprnum[MAXKEYS];
int workkeyflag;
int workkeypieceflag;
struct expression expr[EXPRMAX];
struct expr_table exprtab[EXPRTABMAX];
int ex_p;
int et_p;

int func_jstfnd;
int sorted[MAX];
int nsym;
int ntables;
int ncnst;
int prcnst;
int nrngs;
int nkeys;
int bufszb, bufszhw, bufszw;
int maxrngsz;
int rngrrnoff[MAXRNGS];
int any_errors;
int current_line = 0;
int current_case = -1;
int current_union = 0;
char *union_names[MAX];
int union_index = -1;
int union_level = -1;
int un_init = 0;
char *customcode = NULL;
char includename[256];
char includefiles[MAX_INCLUDES][MAX_INC_NAME];
int includetypes[MAX_INCLUDES];
int nincludes = 0;
char *blankchar = "";
int ixblocksize[MAXINDEX], lv0size[MAXINDEX];
int spltpercnt[MAXINDEX];

int case_table[MAX];   /* names of the cases */
int cn_p;              /* pointer into casenames[] */
int ixsize[MAXINDEX];  /* index size */
int ixflags[MAXINDEX]; /* index size */

int flag_anyname = 0; /* allow any db name - normally restricted to xxDB*/
char sync_names[4][8] = {"none", "full", "normal", "source"};
int cluster_nodes[MAX_CLUSTER];
int ncluster = 0;
int createlv0 = 0;
char *opt_incldir = 0;
char *opt_dtadir = 0;
char *opt_lrldir = 0;
char *opt_prefix = 0;
int opt_cachesz = 300;
int opt_dbnum = 0;
int opt_remote = 0;
char *opt_dbname = 0;    /* database+table name */
char opt_maindbname[MAX_DBNAME_LENGTH]; /* database name */
char opt_tblname[64];    /* table name */
int opt_verbose;         /* 0=don't use verbose comments in .h */
int opt_copycsc;         /* 0=don't copy .csc to .inc */
char *opt_accname;       /* different name for acc routine/.f */
int opt_reclen;          /* optional record length */
int opt_sntoremb = 0;    /* USE SNTOREMB OPTION*/
int opt_nicedbvalue = 0; /* USE START NICE PROCMGR VALUE FOR DATABASE */
int opt_useglobals = 0;  /* GENERATE STATIC RECORD/CONTROL STRUCTS IN ACC
                            ROUTINE, ALONG WITH SHORTCUTS */
int opt_staticacc = 0;   /* Create Access Routine to be static */
int opt_noprefix = 2;    /* Don't generate prefixes for variables */
int opt_threadsafe = 1;  /* By default, access routine is thread-safe */
int opt_macc2pack = 0;
int opt_usenames = 0;
char *opt_filesfx = ""; /* Include file suffix..By default disabled */
int opt_sync = 0;
int opt_setsql = 0;
int opt_schematype = 2;
int nconstraints = -1;
int n_check_constraints = -1;

void init_globals()
{
    int i;
    nsym = 0;   /* INITIALIZE GLOBALS */
    ncnst = 0;  /* INITIALIZE GLOBALS */
    prcnst = 0; /* private constants */
    cn_p = 0;
    nrngs = 0;
    nkeys = 0;
    any_errors = 0;
    ex_p = 0;
    et_p = 0;
    ncluster = 0;
    ntables = 0;
    memset(symb, 0, sizeof(symb));
    memset(tables, 0, sizeof(tables));
    memset(constants, 0, sizeof(constants));
    memset(keys, 0, sizeof(keys));
    workkey = 0;
    workkeyflag = 0;
    workkeypieceflag = 0;
    memset(rngs, 0, sizeof(rngs));
    memset(un_start, 0, sizeof(un_start));
    memset(un_end, 0, sizeof(un_end));
    memset(un_reset, 0, sizeof(un_reset));
    memset(un_case, 0, sizeof(un_case));
    memset(ixsize, 0, sizeof(ixsize));
    memset(ixflags, 0, sizeof(ixflags));
    memset(keyixnum, 0, sizeof(keyixnum));
    memset(keyexprnum, 0, sizeof(keyexprnum));
    memset(expr, 0, sizeof(expr));
    memset(exprtab, 0, sizeof(exprtab));

    for (i = 0; i < MAXINDEX; i++) {
        ixblocksize[i] = 4096;
        lv0size[i] = 4096;
        spltpercnt[i] = 50;
    }
    opt_dbname = blankchar;
    opt_incldir = blankchar;
    opt_dtadir = blankchar;
    opt_lrldir = blankchar;
    opt_prefix = blankchar;
    opt_verbose = 0;  /* default = not verbose */
    opt_copycsc = 0;  /* don't copy csc */
    opt_accname = 0;  /* acc name = db name by default */
    opt_reclen = 0;   /* optional record length in bytes */
    flag_anyname = 0; /* DON'T ALLOW ANY NAME BY DEFAULT */
    memset(cur_dpth, 0, sizeof(short) * MAX_DEPTH);
    dpth_idx = 0;
    bufszb = 0;
    bufszhw = 0;
    bufszw = 0;
    maxrngsz = 0;
    any_errors = 0;
    current_line = 0;
    current_case = -1;
    current_union = 0;
    memset(union_names, 0, sizeof(union_names));
    union_index = -1;
    union_level = -1;
    un_init = 0;
    customcode = NULL;
    memset(includename, 0, sizeof(includename));
    memset(includefiles, 0, sizeof(includefiles));
    memset(includetypes, 0, sizeof(includetypes));
    nincludes = 0;
    memset(case_table, 0, sizeof(case_table));
    flag_anyname = 0;
    memset(cluster_nodes, 0, sizeof(cluster_nodes));
    ncluster = 0;
    createlv0 = 0;
    opt_cachesz = 300;
    opt_dbnum = 0;
    opt_remote = 0;
    opt_sntoremb = 0;    /* USE SNTOREMB OPTION*/
    opt_nicedbvalue = 0; /* USE START NICE PROCMGR VALUE FOR DATABASE */
    opt_useglobals = 0;  /* GENERATE STATIC RECORD/CONTROL STRUCTS IN ACC
                            ROUTINE, ALONG WITH SHORTCUTS */
    opt_staticacc = 0;   /* Create Access Routine to be static */
    opt_noprefix = 2;    /* Don't generate prefixes for variables */
    opt_threadsafe = 1;  /* By default, access routine is thread-safe */
    opt_macc2pack = 0;
    opt_usenames = 0;
    opt_filesfx = ""; /* Include file suffix..By default disabled */
    opt_sync = 0;
    opt_setsql = 0;
    opt_schematype = 2;
    memset(opt_maindbname, 0, sizeof(opt_maindbname));
    memset(opt_tblname, 0, sizeof(opt_tblname));
    memset(constraints, 0, sizeof(struct constraint) * MAXCNSTRTS);
    memset(check_constraints, 0, sizeof(struct check_constraint) * MAXCNSTRTS);
    nconstraints = -1;
    n_check_constraints = -1;
}
