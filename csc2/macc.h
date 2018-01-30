#include <stdio.h>
#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include "dynschemaload.h"
#include <cdb2_constants.h>

#include "mem_csc2.h"
#include "mem_override.h"

extern char *revision;
extern char VER[16]; /* version info */

#define ONDISKTAG ".ONDISK"
#define DEFAULTTAG ".DEFAULT"

extern char sync_names[4][8];
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
#define MAX_KEY_SIZE 512
#define MAX_KEY_INDEX                                                          \
    28 /* currently, we have 28 indeces in comdb2 due to buffer size           \
          restrictions */

#define MAX_FIELDS_PER_KEY                                                     \
    128 /* Limitation imposed by comdb2.                                       \
           If increased, many changes must be made                             \
           throughout comdb2. */

/*
   The largest possible buffer through comdb2_api right now is update by rrn in
   WORDS as follows:

   4 words  +  2*((3 + MAX_KEY_SIZE/4) * MAX_KEY_INDEX) + 6 words +
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
#define MACRO_USE                                                              \
    1 /* whether we generate test program that uses macros 0=no, 1=yes*/
#define NUMRESERVE 4
#define ALIGNMENT                                                              \
    8 /* byte size specifying how the record struct should be aligned */
extern int
    current_case; /* for determining which case (in rectype) the variable
                     belongs to */
extern int
    current_union; /* for determining which union (if any) the variable is in */
extern char *union_names[MAX]; /* array to store union names */
extern int un_init;            /* determines whether unions were computed  */
extern char *maccfuncpath;

/* MAXINDEX HAS BECOME OBSOLETE - FOR NOW, JUST KLUDGE TO FIX */
#define MAXKEYS 256  /* maximum # of keys with cases */
//#define MAXINDEX 256 /* max # of indices */

#define EXPRMAX 1024    /* maximum pieces of an expression */
#define EXPRTABMAX 1024 /* maximum number of expressions */

extern struct constant {
    int value;
    short type;
    char *nm;
} constants[MAX];

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

enum ct_flags { CT_UPD_CASCADE = 0x00000001, CT_DEL_CASCADE = 0x00000002 };

extern struct constraint {
    char *consname;
    char *lclkey;
    int ncnstrts;
    int flags;
    char *table[MAXCNSTRTS];
    char *keynm[MAXCNSTRTS];
} constraints[MAXCNSTRTS];

extern int nconstraints;

extern struct symbol {
    char *nm;   /* symbol name                     */
    int dim[6]; /* # of elements in each dimension */
    char *
        dim_cnst[6]; /* if we have any constants in the symbol specs, store it*/
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
} sym[MAX];

extern struct table {
    char table_tag[MAX_TAG_LEN + 1];
    int table_size;
    int table_type;
    int table_padbytes;
    int nsym;
    struct symbol sym[MAX];
} tables[MAXTBLS];
extern int ntables;

extern short cur_dpth[MAX_DEPTH];
extern int dpth_idx;
extern unsigned int
    un_start[MAX];               /* # of levels of union to start at this sym */
extern unsigned int un_end[MAX]; /* # of levels of union to end at this sym */
extern unsigned int un_reset[MAX]; /* reset offset flag for a union */
extern unsigned int un_case[MAX];  /* reset offset flag for a case: */
extern int union_index;
extern int union_level;
extern int ncluster;
extern int cluster_nodes[MAX_CLUSTER];

extern struct expression {
    char *sym;
    char *symarr; /* if symbol is an array, stores dimensions...otherwise "" */
    int symnum;
    int opr;
    int num;
} expr[EXPRMAX]; /* stores each piece of an expression */

extern struct expr_table {/* stores name associated with an expression */
    char *name;
    struct expression *expr;
    int elen; /* length of expression */
} exprtab[EXPRTABMAX];

extern int et_p; /* pointer into exprtab[] */
extern int ex_p; /* pointer into expr[] */

extern int
    case_table[MAX]; /* cases must have an expression associated with it */
extern int cn_p;     /* pointer into case_table[] */

extern struct key {
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
} * keys[MAXKEYS], *workkey, *rngs[MAXRNGS];

enum KEYFLAGS {
    DESCEND = 1 /* this key piece is descending */
};

extern int workkeyflag;         /* work key's flag */
extern int workkeypieceflag;    /* work key piece's flag */
extern int keyixnum[MAXKEYS];   /* index # associated with a key */
extern int keyexprnum[MAXKEYS]; /* case number associated with a key */

extern int ixsize[MAXINDEX];  /* index size in comdbg */
extern int ixflags[MAXINDEX]; /* flags for index */

enum INDEXFLAGS {
    DUPKEY = 0x00000001,  /* duplicate key flag */
    RECNUMS = 0x00000002, /* index has key sequence numbers (COMDB2) */
    PRIMARY = 0x00000004,
    DATAKEY = 0x00000008 /* key flag to indicate index has data */
};

extern int fncs[MAXFUNCS];          /* functions                         */
extern int func_jstfnd;             /*function # of jstfnd - needed for delete*/
extern int sorted[MAX];             /* array to keep symbol table sorted */
extern int nsym;                    /* # of symbols                      */
extern int ncnst;                   /* # of constants                    */
extern int prcnst;                  /* # of private constants            */
extern int nrngs;                   /* # of ranges                       */
extern int nkeys;                   /* # of keys                         */
extern int bufszb, bufszhw, bufszw; /* buffer size bytes/words           */
extern int maxrngsz;                /* the biggest range's size          */
extern int rngrrnoff[MAXRNGS];      /* rrn offset in rbuf for each range */
extern int any_errors;              /* flag                              */
extern int current_line;            /* for printing errors               */
extern char includename[256];       /* tolower'ed include name           */
extern char includefiles[MAX_INCLUDES][MAX_INC_NAME];
extern int includetypes[MAX_INCLUDES];
extern int nincludes;
/* options */
extern int createlv0;
extern int ixblocksize[], lv0size[];
extern int spltpercnt[];

extern char *customcode;
extern char *blankchar;
extern char *opt_dbname;
extern char opt_maindbname[64];
extern char opt_tblname[64];
extern char *opt_incldir;
extern char *opt_dtadir;
extern char *opt_lrldir;
extern int opt_dbnum;
extern int opt_remote;
extern int opt_verbose;   /* 0=don't use verbose comments in .h */
extern int opt_copycsc;   /* 0=don't copy .csc to .inc */
extern char *opt_accname; /* different name for acc routine/.f */
extern int opt_reclen;    /* optional record length in bytes */
extern int flag_anyname;  /* command line arg flag */
extern int opt_sntoremb;
extern int opt_nicedbvalue;
extern int opt_useglobals;
extern int opt_staticacc;
extern int opt_threadsafe;
extern int opt_cachesz;
extern int opt_codestyle;
extern int opt_macc2pack;
extern int opt_usenames;
extern char *opt_filesfx;
extern int opt_sync;
extern int opt_setsql;
extern int opt_schematype;
extern char *opt_dbtag;

char *eos(char *);

#define TABLENAME opt_tblname
#define MAINDBNAME opt_maindbname
#define DBNAME opt_dbname
#define INCLDIR opt_incldir
#define DTADIR opt_dtadir
#define ACCNAME opt_accname
#define FUNC_JSTFND func_jstfnd

extern void OUTINIT(FILE *fil, char lang);
extern void OUT(char *fmt, ...);

int numkeys();
int numix();
void resolve_case_names();
void end_constraint_list(void);
void set_constraint_mod(int start, int op, int type);
void set_constraint_name(char *name);
void start_constraint_list(char *tblname);
void add_constraint(char *tbl, char *key);
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
void reset_key_exprtype(void);
void key_exprtype_add(int type, int arraysz);
void key_piece_add(char *buf, int is_expr);
void key_piece_setdescend();
char *strcpylower(char *c);
int keysize(struct key *ck);
int character(int tidx, int idx);
int offpad(int offset, int align);
int wholekeysize(struct key *wk);
char *strcpyupper(char *c);
char *typetxt(int t, int size);
int numdim(int dm[6]);
char *sqltypetxt(int t, int size);
int calc_rng(int rng, int *ask);
char *printf_type_txt(int t, int size);
void strupper(char *c);
void strlower(char *s, int max);
int process_array_(int dim[6], int rg[2], char *buf, int *arr, char *dim_cn[6]);
int compute_all_data(int tidx);
int gettable(char *tabletag);
void init_globals();
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
