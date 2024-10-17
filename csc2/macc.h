#ifndef __macc_h
#define __macc_h

#include "dynschemaload.h"
#include <cdb2_constants.h>

#include "mem.h"

#define ONDISKTAG ".ONDISK"
#define DEFAULTTAG ".DEFAULT"

enum synctype {
    SYNC_NONE = 0,
    SYNC_FULL = 1,
    SYNC_NORMAL = 2,
    SYNC_SOURCE = 3
};

#define MAX 1024              /* size of symbol table   */
#define COMDB2_MAX 1024       /* max # comdb2 symbols */
#define MAXFUNCS 24           /* maximum # of functions */
#define MAXRNGS 64            /* maximum # of ranges    */
#define MAXINITSIZE 256000000 /* safety limit on init files */
#define COMMENT_COLUMN 40     /* column for comments    */
#define MAX_CLUSTER 16
/* currently, we have 28 indeces in comdb2 due to buffer size restrictions */
#define MAX_KEY_INDEX 28

/* Limitation imposed by comdb2. If increased, many changes must be
made throughout comdb2. */
#define MAX_FIELDS_PER_KEY 128

/*
   The largest possible buffer through comdb2_api right now is update by rrn in
   WORDS as follows:

   4 words  +  2*((3 + MAXKEYLEN/4) * MAX_KEY_INDEX) + 6 words +
   2*(COMDB2_MAX_RECORD_SIZE/4) == 4018 words
   (blkstart)     (blkdelnod+blkadnod)                      blkupvrrn

   With current limit of 16K buffer, these are the values we have..this may
   increase in the future.
 */

#define MAX_INCLUDES 256
#define MAX_INC_NAME 256 /* maximum length of inlude filename */

#define MAXCNSTRTS 32
#define MAX_DEPTH 32
#define FORTRAN 1
#define CLANG 3
/* whether we generate test program that uses macros 0=no, 1=yes*/
#define MACRO_USE 1
#define NUMRESERVE 4
/* byte size specifying how the record struct should be aligned */
#define ALIGNMENT 8

/* MAXINDEX HAS BECOME OBSOLETE - FOR NOW, JUST KLUDGE TO FIX */
#define MAXKEYS 256  /* maximum # of keys with cases */
//#define MAXINDEX 256 /* max # of indices */

#define EXPRMAX 1024    /* maximum pieces of an expression */
#define EXPRTABMAX 1024 /* maximum number of expressions */

struct constant {
    int value;
    short type;
    char *nm;
};

struct fieldopt {
    int opttype;
    int valtype;
    union {
        double r8val;
        float r4val;
        unsigned long long u8val;
        long long i8val;
        int i4val;
        short i2val;
        char *strval;
        char *byteval; /* first int is size in bytes */
    } value;
};

enum pd_flags { PERIOD_SYSTEM = 0, PERIOD_BUSINESS = 1, PERIOD_MAX = 2 };

struct period {
    int enable;
    int start;
    int end;
};

enum ct_flags {
    CT_UPD_CASCADE = 0x00000001,
    CT_DEL_CASCADE = 0x00000002,
    CT_DEL_SETNULL = 0x00000008,
    CT_NO_OVERLAP  = 0x00000010,
};

enum ct_type { CT_FKEY, CT_CHECK };

struct constraint {
    char *consname;
    char *lclkey;
    int ncnstrts;
    int flags;
    char *table[MAXCNSTRTS];
    char *keynm[MAXCNSTRTS];
};

struct check_constraint {
    char *consname;
    char *expr;
};

struct symbol {
    char *nm;   /* symbol name                     */
    int dim[6]; /* # of elements in each dimension */
    char *dim_cnst[6]; /* if we have any constants in the symbol specs, store it */
    int arr;         /* if this is an array, is it C or Fortran */
    int size;        /* base size of variable           */
    int szof;        /* size of whole varaible/array    */
    char *szof_cnst;
    int type;        /* type of variable                */
    int align;       /* alignment necessary for this var*/
    int off;         /* offset in the common            */
    char *com;       /* A description                   */
    int un_member;   /* Member of which union, if any   */
    int un_max_size; /* If in union, give size of that union  */
    int un_idx;      /* this will tell if we're in the nested union */
    int padb;        /* #of bytes needed to pad the symbol in the struct*/
    int padex; /* #of pad bytes needed to pad this symbol on 8-byte boundary*/
    int padaf;
    int padcs;  /* #of pad bytes necessary for the case (if symbol is in it) */
    int caseno; /* Case statement to which this symbol belongs -1=none*/
    int dumped; /* Tells whether the symbol was placed in record struct */
    int padded; /* tells whether the symbol was padded for alignment  */
    short dpth_tree[MAX_DEPTH];
    int dpth;
    int numfo; /* number of field options for this symbol */
    struct fieldopt fopts[FLDOPT_MAX]; /* field option structures */
};

struct table {
    char table_tag[MAX_TAG_LEN + 1];
    int table_size;
    int table_type;
    int table_padbytes;
    int nsym;
    struct symbol sym[MAX];
};

struct expression {
    char *sym;
    char *symarr; /* if symbol is an array, stores dimensions...otherwise "" */
    int symnum;
    int opr;
    int num;
}; /* stores each piece of an expression */

struct expr_table { /* stores name associated with an expression */
    char *name;
    struct expression *expr;
    int elen; /* length of expression */
};

struct key {
    int sym;         /* # of symbol in table            */
    int stbl;        /* record table reference */
    int el[6];       /* element # for each dimension    */
    int rg[2];       /* substring info, [0]:[1]         */
    int rboff;       /* offset in RBUF (bytes) for rng  */
    int rcoff;       /* offset in record (bytes) for rng*/
    int keyflags;    /* any flags for this guy */
    struct key *cmp; /* compund key                     */
    char keytag[64];
    char *expr;
    int exprtype;
    int exprarraysz;
    char *where;
    struct partial_datacopy *pd;    /* partial datacopy fields (if any) */
};

enum KEYFLAGS {
    DESCEND = 1 /* this key piece is descending */
};


enum INDEXFLAGS {
    DUPKEY = 0x00000001,  /* duplicate key flag */
    RECNUMS = 0x00000002, /* index has key sequence numbers (COMDB2) */
    PRIMARY = 0x00000004,
    DATAKEY = 0x00000008, /* key flag to indicate index has data */
    UNIQNULLS = 0x00000010, /* all NULL values are treated as UNIQUE */
    PARTIALDATAKEY = 0x00000020 /* key flag to indicate index has some data */
};

typedef struct macc_globals_t {
    struct period periods[PERIOD_MAX];
    int nperiods;
    struct constraint constraints[MAXCNSTRTS];
    struct check_constraint check_constraints[MAXCNSTRTS];
    struct symbol symb[MAX];
    struct table tables[MAXTBLS];
    unsigned int un_start[MAX]; /* # of levels of union to start at this sym */
    unsigned int un_end[MAX];   /* # of levels of union to end at this sym */
    unsigned int un_reset[MAX]; /* reset offset flag for a union */
    unsigned int un_case[MAX];  /* reset offset flag for a case: */
    short cur_dpth[MAX_DEPTH];
    int dpth_idx;

    struct key *keys[MAXKEYS];
    struct key *workkey;
    struct key *rngs[MAXRNGS];
    struct partial_datacopy *head_pd;
    struct partial_datacopy *tail_pd;
    int keyixnum[MAXKEYS];   /* index # associated with a key */
    int keyexprnum[MAXKEYS]; /* case number associated with a key */
    int workkeyflag;         /* work key's flag */
    int workkeypieceflag;    /* work key piece's flag */
    struct expression expr[EXPRMAX];
    struct expr_table exprtab[EXPRTABMAX];
    int ex_p;
    int et_p; /* pointer into expr[] */ /* pointer into exprtab[] */

    int func_jstfnd; /*function # of jstfnd - needed for delete*/
    int sorted[MAX]; /* array to keep symbol table sorted */
    int nsym;        /* # of symbols */
    int ntables;
    int ncnst;                   /* # of constants */
    int prcnst;                  /* # of private constants */
    int nrngs;                   /* # of ranges */
    int nkeys;                   /* # of keys */
    int bufszb, bufszhw, bufszw; /* buffer size bytes/words */
    int maxrngsz;                /* the biggest range's size */
    int rngrrnoff[MAXRNGS];      /* rrn offset in rbuf for each range */
    int current_case;  /* for determining which case (in rectype) the variable
                          belongs to */
    int current_union; /* for determining which union (if any) the variable is
                          in */
    char *union_names[MAX]; /* array to store union names */
    int union_index;
    int union_level;
    int un_init; /* determines whether unions were computed  */
    char *customcode;
    char includename[256]; /* tolower'ed include name           */
    char includefiles[MAX_INCLUDES][MAX_INC_NAME];
    int includetypes[MAX_INCLUDES];
    int nincludes;
    int ixblocksize[MAXINDEX], lv0size[MAXINDEX];
    int spltpercnt[MAXINDEX];

    int case_table[MAX]; /* names of the cases */
    /* cases must have an expression associated with it */
    int cn_p; /* pointer into casenames[] */ /* pointer into case_table[] */
    int ixsize[MAXINDEX];                    /* index size in comdbg */
    int ixflags[MAXINDEX];                   /* flags for index */

    int flag_anyname; /* allow any db name - normally restricted to xxDB*/
    int cluster_nodes[MAX_CLUSTER];
    int ncluster;
    int createlv0;
    char *opt_incldir;
    char *opt_dtadir;
    char *opt_lrldir;
    char *opt_prefix;
    /* int opt_cachesz; */
    int opt_dbnum;
    int opt_remote;
    char *opt_dbname;                       /* database+table name */
    char opt_maindbname[MAX_DBNAME_LENGTH]; /* database name */
    char opt_tblname[64];                   /* table name */
    int opt_verbose;     /* 0=don't use verbose comments in .h */
    int opt_copycsc;     /* 0=don't copy .csc to .inc */
    char *opt_accname;   /* different name for acc routine/.f */
    int opt_reclen;      /* optional record length */
    int opt_sntoremb;    /* USE SNTOREMB OPTION*/
    int opt_nicedbvalue; /* USE START NICE PROCMGR VALUE FOR DATABASE */
    int opt_useglobals;  /* GENERATE STATIC RECORD/CONTROL STRUCTS IN ACC
                            ROUTINE, ALONG WITH SHORTCUTS */
    int opt_staticacc;   /* Create Access Routine to be static */
    int opt_noprefix;    /* Don't generate prefixes for variables */
    /* int opt_threadsafe;   By default, access routine is thread-safe */
    int opt_macc2pack;
    int opt_usenames;
    char *opt_filesfx; /* Include file suffix..By default disabled */
    int opt_sync;
    int opt_setsql;
    int opt_schematype;
    int nconstraints;
    int n_check_constraints;
} macc_globals_t;

extern int any_errors;              /* flag                              */
extern int current_line;            /* for printing errors               */
extern char *blankchar;
extern struct constant constants[MAX];
extern macc_globals_t *macc_globals;

char *eos(char *);

extern void OUTINIT(FILE *fil, char lang);
extern void OUT(char *fmt, ...);

int numkeys();
int numix();
void resolve_case_names();
void set_constraint_mod(int start, int op, int type);
void set_constraint_name(char *name, enum ct_type type);
void start_constraint_list(char *keyname, int no_overlap);
void start_periods_list(void);
void add_period(char *name, char *start, char *end);
void add_constraint(char *tbl, char *key);
void add_check_constraint(char *expr);
void add_constant(char *name, int value, short type);
void add_fldopt(int opttype, int valtype, void *value);
void reset_array();
void reset_range();
void start_table(char *tag, int preset);
void end_table();
void rec_c_add(int typ, int size, char *name, char *cmnt);
void reset_fldopt(void);
void add_array(int dim, char *label);
int constant(char *var);
void add_range(int rg);
void key_add_tag(char *tag, char *exprname, char *where);
void key_piece_clear();
void key_setdup();
void key_setrecnums(void);
void key_setprimary(void);
void key_setdatakey(void);
void key_setpartialdatakey(void);
void key_setuniqnulls(void);
void reset_key_exprtype(void);
void key_exprtype_add(int type, int arraysz);
void key_piece_add(char *buf, int is_expr);
void datakey_piece_add(char *buf);
void key_piece_setdescend();
int keysize(struct key *ck);
int character(int tidx, int idx);
int offpad(int offset, int align);
int wholekeysize(struct key *wk);
char *typetxt(int t, int size);
int numdim(int dm[6]);
int calc_rng(int rng, int *ask);
char *printf_type_txt(int t, int size);
int process_array_(int dim[6], int rg[2], char *buf, int *arr, char *dim_cn[6]);
int compute_all_data(int tidx);
int gettable(char *tabletag);
int compute_key_data(void);
int yyparse();

/* prototypes for routines called by maccparse.y */
void end_union();
void start_union(char *name);
void end_rectypedef();
void start_rectypedef(char *rtname);
void start_case(char *txt);
int add_cluster_node(int node);
void rng_add(int i);
void expr_assoc_name(char *name);
void expr_clear();
void expr_add_pc(char *sym, int op, int num);

#endif
