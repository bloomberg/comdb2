/*
   MACC - access routine generator
   */
#include <pthread.h>
#include <stdio.h>
#include <sys/types.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <ctype.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include "csctypes.h"
#include "maccparse.h"
#include "macc.h"
#include <dynschematypes.h>
#include "flibc.h"
#include <netinet/in.h>
#include <list.h>
#include <stddef.h>
#include <cdb2_constants.h>

#include "dynschemaload.h"

#include <strbuf.h>
#include <logmsg.h>

extern int yyparse(void);
extern int compute_key_data(void);
extern void init_globals(void);
extern int compute_all_data(int tidx);

char *revision = "$Revision: 1.24 $";
int unionflag = 0;
int dimidx = 0, dims[7], rngidx = 0, ranges[2], range_or_array = 0, range = 0,
    declaration = 0, cparse = -1;
char *dims_cnst[7];

int lastidx = 0;
#define MAX_NESTED_RECTYPE 16
static int ncases = -1;
static int nested_rectype[MAX_NESTED_RECTYPE];

struct fieldopt fieldopts[FLDOPT_MAX];
int nfieldopt = 0;

static int allow_bools = 0;
static int used_bools = 0;

#define MIN(a, b) ((a < b) ? a : b)

static strbuf *errors = NULL;
static strbuf *syntax_errors = NULL;

int get_union_size(int un);
int get_case_size(int csn);
void reset_array();
void reset_range();
void reset_fldopt(void);
void add_fldopt(int opttype, int valtype, void *value);

static void
key_add_comn(int ix, char *tag, char *exprname,
             char *where); /* used by parser, adds a completed key */
static int dyns_get_field_info_comn(char *tag, int fidx, char *name,
                                    int namelen, int *type, int *offset,
                                    int *elsize, int *fullsize, int *arr,
                                    int use_server_types);
static int dyns_is_field_array_comn(char *tag, int fidx);
static int dyns_get_field_arr_dims_comn(char *tag, int fidx, int *dims,
                                        int ndims, int *nodims);
static int dyns_field_depth_comn(char *tag, int fidx, dpth_t *dpthinfo,
                                 int ndpthsinfo, int *ndpthout);
static int dyns_get_field_option_comn(char *tag, int fidx, int option,
                                      int *value_type, int *value_sz,
                                      void *valuebuf, int vbsz);

void csc2_error(const char *fmt, ...);
void csc2_syntax_error(const char *fmt, ...);

void dyns_allow_bools(void) { allow_bools = 1; }

void dyns_disallow_bools(void) { allow_bools = 0; }

int dyns_used_bools(void) { return used_bools; }

void start_constraint_list(char *keyname)
{
    if (nconstraints >= MAXCNSTRTS) {
        csc2_error("ERROR: TOO MANY CONSTRAINTS SPECIFIED. MAX %d\n",
                   MAXCNSTRTS);
        any_errors++;
        return;
    }
    constraints[nconstraints].flags = 0;
    constraints[nconstraints].ncnstrts = 0;
    constraints[nconstraints].lclkey = keyname;
    /*  fprintf(stderr, "constraints for key %s\n", keyname);*/
}

void set_constraint_mod(int start, int op, int type)
{
    /*fprintf(stderr, "%d %d %d\n", start, op, type);*/
    if (type == 0)
        return;
    if (op == 0)
        constraints[nconstraints].flags |= CT_UPD_CASCADE;
    else if (op == 1)
        constraints[nconstraints].flags |= CT_DEL_CASCADE;
}

void set_constraint_name(char *name)
{
    constraints[nconstraints].consname = name;
}

void end_constraint_list(void)
{
    /*  fprintf(stderr, "constraint: end list\n");*/
    nconstraints++;
}

void add_constraint(char *tbl, char *key)
{
    int cidx = constraints[nconstraints].ncnstrts;
    if (cidx >= MAXCNSTRTS) {
        csc2_error("ERROR: TOO MANY RULES SPECIFIED IN CONSTRAINT FOR KEY: %s. "
                   "(MAX: %d)\n",
                   constraints[nconstraints].lclkey, MAXCNSTRTS);
        any_errors++;
        return;
    }
    constraints[nconstraints].ncnstrts++;
    constraints[nconstraints].table[cidx] = tbl;
    constraints[nconstraints].keynm[cidx] = key;
    /*  fprintf(stderr, "constraint: tbl %s key %s %d\n",
     * tbl,key,nconstraints);*/
}

int constant(char *var)
{
    int i;
    for (i = 0; i < ncnst; i++) {
        if (strcmp(var, constants[i].nm) == 0)
            return i;
    }
    return -1;
}

int numix() /* count # of indices */
{
    int i, nix;
    for (i = 0, nix = 0; i < nkeys; i++) {
        if (keyixnum[i] > nix)
            nix = keyixnum[i];
    }
    /* special case for no keys */
    if (nkeys == 0)
        return 0;
    return nix + 1;
}

int numkeys() /* count # of conditional keys */ { return nkeys; }

int numdim(int dm[6]) /* COUNTS # OF DIMS IN A DM ARRAY */
{
    int i;
    for (i = 0; i < 6 && (dm[i] != -1); i++)
        ;
    return i;
}

char *eos(char *line) /* RETURNS END OF STRING */
{
    return line + strlen(line);
}

#if 0
void strupper(char *c)				/* STRING TO UPPER CASE */
{
    while (*c) {
        *c=toupper(*c);
        c++;
    }
}

void strlower(char *c)				/* STRING TO LOWER CASE */
{
    while (*c) {
        *c=tolower(*c);
        c++;
    }
}
#endif

char *strcpylower(char *c) /* STRING TO LOWER CASE */
{
    char *tmp, *c1 = (char *)csc2_malloc(strlen(c) * sizeof(char) + 1);
    strcpy(c1, c);
    tmp = c1;
    while (*tmp) {
        *tmp = tolower(*tmp);
        tmp++;
    }
    return c1;
}

char *strcpyupper(char *c) /* STRING TO UPPER CASE */
{
    char *tmp, *c1;
    if (strlen(c) < 1)
        return c;
    c1 = (char *)csc2_malloc(strlen(c) * sizeof(char) + 1);
    strcpy(c1, c);
    tmp = c1;
    while (*tmp) {
        *tmp = toupper(*tmp);
        tmp++;
    }
    return c1;
}

int field_type(int macctype, int use_server_types)
{
    switch (macctype) {
    /* all unsigned ints */
    case T_ULONG:
    case T_ULONGLONG:
    case T_UINTEGER2:
    case T_UINTEGER4:
        return (use_server_types) ? SERVER_UINT : CLIENT_UINT;
    /* all ints */
    case T_INTEGER2:
    case T_INTEGER4:
    case T_LOGICAL:
        return (use_server_types) ? SERVER_BINT : CLIENT_INT;
    case T_LONGLONG:
        return (use_server_types) ? SERVER_BINT : CLIENT_INT;

    case T_UCHAR:
        return (use_server_types) ? SERVER_BYTEARRAY : CLIENT_BYTEARRAY;
    case T_PSTR:
        return (use_server_types) ? SERVER_BCSTR : CLIENT_PSTR;
    case T_CSTR:
        return (use_server_types) ? SERVER_BCSTR : CLIENT_CSTR;
    case T_REAL4:
    case T_REAL8:
        return (use_server_types) ? SERVER_BREAL : CLIENT_REAL;
    case T_BLOB:
        return (use_server_types) ? SERVER_BLOB : CLIENT_BLOB;
    case T_BLOB2:
        return (use_server_types) ? SERVER_BLOB2 : CLIENT_BLOB;
    case T_VUTF8:
        return (use_server_types) ? SERVER_VUTF8 : CLIENT_VUTF8;
    case T_DATETIME:
        return (use_server_types) ? SERVER_DATETIME : CLIENT_DATETIME;
    case T_DATETIMEUS:
        return (use_server_types) ? SERVER_DATETIMEUS : CLIENT_DATETIMEUS;
    case T_INTERVALYM:
        return (use_server_types) ? SERVER_INTVYM : CLIENT_INTVYM;
    case T_INTERVALDS:
        return (use_server_types) ? SERVER_INTVDS : CLIENT_INTVDS;
    case T_INTERVALDSUS:
        return (use_server_types) ? SERVER_INTVDSUS : CLIENT_INTVDSUS;
    case T_DECIMAL32:
    case T_DECIMAL64:
    case T_DECIMAL128:
        return (use_server_types) ? SERVER_DECIMAL : CLIENT_CSTR;

    default:
        return 0;
    }
}

#if 0
char * sqltypetxt(int t, int size)
{
    switch (t) {
        case T_UINTEGER2: return "ushort";
        case T_UINTEGER4: return "uinteger";
        case T_INTEGER2: return "short";
        case T_INTEGER4: return "int";
        case T_LOGICAL: return "int";
        case T_LONG: return "int";
        case T_ULONG: return "uinteger";
        case T_UCHAR:
        case T_BLOB:
                      {
                          return "blob";
                      }
        case T_DATETIME: return "datetime";
        case T_INTERVALYM: return "intervalym";
        case T_INTERVALDS: return "intervalds";

        case T_DECIMAL32: return "decimal32";
        case T_DECIMAL64: return "decimal64";
        case T_DECIMAL128: return "decimal128";

        case T_PSTR:  
        case T_CSTR:
        case T_FCHAR: 
        case T_VUTF8:
                           {
                               return "char";
                           }
        case T_REAL4: return "smallfloat";
        case T_REAL8: return "double";

        default: return "UNKNOWN_TYPE";
    };
}
#endif

char *opertxt(int t)
{
    switch (t) {
    case T_GT:
        return " > ";
    case T_LT:
        return " < ";
    case '(':
        return "(";
    case ')':
        return ")";
    default:
        return ".?UNKNOWN OPERATOR?.";
    }
}

int check_options() /* CHECK VALIDITY OF OPTIONS      */
{
    int ii, jj = 0;
    int ondisktag = 0, defaulttag = 0, numnormtags = 0;

    /* current restriction on SQL is that it does not support arrays, nor any
     * unions, cases, etc */
    for (jj = 0; jj < ntables; jj++) {
        int lcldsktag = 0;
        if (!strcmp(tables[jj].table_tag, ONDISKTAG)) {
            ondisktag = 1;
            lcldsktag = 1;
        }

        if (!strcmp(tables[jj].table_tag, DEFAULTTAG)) {
            defaulttag = 1;
        }

        if (strcmp(tables[jj].table_tag, DEFAULTTAG) &&
            strcmp(tables[jj].table_tag, ONDISKTAG)) {
            numnormtags++;
        }

        if (tables[jj].nsym == 0) {
            csc2_error("ERROR: TAG '%s' IS EMPTY\n", tables[jj].table_tag);
            any_errors++;
        }

        for (ii = 0; ii < tables[jj].nsym; ii++) {
            if (tables[jj].sym[ii].dpth != 0) {
                csc2_error("Record \"%s\" has UNIONS or CASE statements. "
                                "SQL does not support this currently.\n",
                        tables[jj].table_tag);
                any_errors++;
                break;
            }
            if (numdim(tables[jj].sym[ii].dim) > 0) {
                fprintf(stderr, "Record \"%s\" has ARRAY fields. SQL does not "
                                "support this currently.\n",
                        tables[jj].table_tag);
                any_errors++;
                break;
            }
            /*	  if (tables[jj].sym[ii].type==T_PSTR)
                  {
                  fprintf(stderr, "Record \"%s\" has PSTRING fields. SQL does
               not support this currently.\n",tables[jj].table_tag);
                  any_errors++;
                  break;
                  }*/
            /* make sure all fields exist in '.ONDISK' tag */
            if (!lcldsktag) {
                int ondskidx = 0, k = 0;

                for (ondskidx = 0; ondskidx < ntables; ondskidx++) {
                    if (!strcmp(tables[ondskidx].table_tag, ONDISKTAG)) {
                        break;
                    }
                }
                if (ondskidx == ntables) {
                    csc2_error("ERROR \"%s\" TAG DOES NOT EXIST IN SCHEMA!\n",
                               ONDISKTAG);
                    any_errors++;
                    break;
                }
                for (k = 0; k < tables[ondskidx].nsym; k++) {
                    if (!strcmp(tables[jj].sym[ii].nm,
                                tables[ondskidx].sym[k].nm)) {
                        break;
                    }
                }
                if (k == tables[ondskidx].nsym) {
                    csc2_error("ERROR FIELD \"%s\" IN TAG \"%s\" DOES NOT "
                               "EXIST IN \"%s\" TAG!\n",
                               tables[jj].sym[ii].nm, tables[jj].table_tag,
                               ONDISKTAG);
                    any_errors++;
                    break;
                }
                /* make sure that blob fields do not have different types in
                 * different tags */
                if ((tables[ondskidx].sym[k].type == T_BLOB ||
                     tables[jj].sym[ii].type == T_BLOB ||
                     tables[ondskidx].sym[k].type == T_BLOB2 ||
                     tables[jj].sym[ii].type == T_BLOB2 ||
                     tables[ondskidx].sym[k].type == T_VUTF8 ||
                     tables[jj].sym[ii].type == T_VUTF8) &&
                    (tables[ondskidx].sym[k].type != tables[jj].sym[ii].type)) {

                    if (tables[ondskidx].sym[k].type == T_BLOB2 &&
                        tables[jj].sym[ii].type == T_BLOB) {
                        /*Do nothing.*/
                    } else {
                        csc2_error("ERROR FIELD \"%s\" IN TAG \"%s\" HAS "
                                   "DIFFERENT TYPE IN \"%s\" TAG - THIS IS NOT "
                                   "ALLOWED FOR BLOBS OR VUTF8S\n",
                                   tables[jj].sym[ii].nm, tables[jj].table_tag,
                                   ONDISKTAG);
                        any_errors++;
                        break;
                    }
                }
                /* make sure that there are no blobs in the default tag */
                if (strcmp(tables[jj].table_tag, DEFAULTTAG) == 0 &&
                    (tables[ondskidx].sym[k].type == T_BLOB ||
                     tables[ondskidx].sym[k].type == T_BLOB2 ||
                     tables[ondskidx].sym[k].type == T_VUTF8)) {
                    csc2_error("ERROR FIELD \"%s\" IS A BLOB OR VUTF8 "
                               "FIELD, IT MAY NOT APPEAR IN DEFAULT TAG.\n",
                               tables[jj].sym[ii].nm);
                    any_errors++;
                    break;
                }
            }
        }
    }

    for (ii = 0; ii < numkeys(); ii++) {
        struct key *ck = keys[ii];
        int jj = 0, cnt = 0, goterr = 0;
        while (ck) {
            /* skip indexes on expressions */
            if (ck->expr)
                goto next;
            jj = ck->sym;
            if (ck->rg[0] != 0 || ck->rg[1] != 0) {
                csc2_error("Key %d has substring specified for element "
                                "'%s'. SQL does not support this currently.\n",
                        ii, tables[ck->stbl].sym[jj].nm);
                goterr = 1;
                any_errors++;
                break;
            }
            /* don't allow blobs to be indexed */
            if (tables[ck->stbl].sym[jj].type == T_BLOB ||
                tables[ck->stbl].sym[jj].type == T_BLOB2 ||
                tables[ck->stbl].sym[jj].type == T_VUTF8) {
                csc2_error("Key %d contains a blob or vutf8 field '%s'.  "
                                "This is not supported.\n",
                        ii, tables[ck->stbl].sym[jj].nm);
                goterr = 1;
                any_errors++;
                break;
            }

        next:
            if (goterr)
                break;
            ck = ck->cmp;
            cnt++;
        }
    }
    if (!ondisktag) {
        csc2_error("ONDISK tag not defined.\n");
        any_errors++;
    }
    return any_errors;
}

int gettable(char *tabletag)
{
    int i = 0;
    for (i = 0; i < ntables; i++) {
        if (!strcmp(tables[i].table_tag, tabletag))
            return i;
    }
    return -1;
}

int getsymbol(char *tabletag, char *nm, int *tblidx) /* GETS A SYMBOL BY NAME */
{
    int i = 0, tbl = 0;
    tbl = gettable(tabletag);
    if (tbl < 0) {
        /*fprintf(stderr, "ERROR: TABLE NOT FOUND %s\n", tabletag);*/
        return -1;
    }
    *tblidx = tbl;
    for (i = 0; i < tables[tbl].nsym; i++) {
        if (strcmp(nm, tables[tbl].sym[i].nm) == 0)
            break;
    }
    if (i < tables[tbl].nsym)
        return i;
    *tblidx = -1;
    return -1;
}

int getexpr(char *nm) /* GET EXPRESSION BY NAME  */
{
    int i;
    for (i = 0; i < et_p; i++) {
        if (strcmp(nm, exprtab[i].name) == 0)
            break;
    }
    if (i < et_p)
        return i;
    return -1;
}

int arroff(int s, int el[6], int rg[2])
/* CALCS BYTE OFFSET OF ARRAY ELEMENT */
{ /* GIVEN ELEMENT# and CHAR RG  */
    int i, j, mul;

    for (i = (6 - 1), j = 0, mul = tables[ntables].sym[s].size; i >= 0; i--) {
        if (tables[ntables].sym[s].dim[i] == -1)
            continue;

        if (el[i] == -1) {
            csc2_error("ERROR CALCULATING ARRAY OFFSET FOR SYMBOL %s!\n",
                       tables[ntables].sym[s].nm);
            any_errors++;
            return -1;
        }
        if (!declaration)
            j += mul * (el[i]);
        else
            j += mul * (el[i] - 1);
        mul *= tables[ntables].sym[s].dim[i];
    }
    if (rg[0] || rg[1]) {
        if (rg[0] == -1)
            rg[0] = 1;
        if (rg[1] == -1)
            rg[1] = tables[ntables].sym[s].size;
        j += rg[0] - 1;
    }
    return j;
}

int addtokey(int sym, int tbl, int dim[6],
             int rg[2]) /* ADDS A VARIABLE TO THE WORK KEY */
{
    struct key *nk = (struct key *)csc2_malloc(sizeof(struct key));
    struct key *kp;
    int keyfields = 0;
    if (!nk) {
        csc2_error("ERROR: OUT OF MEM: %s - ABORTING\n", strerror(errno));
        any_errors++;
        return -1;
    }
    nk->cmp = 0;
    memcpy(nk->el, dim, sizeof(int) * 6);
    memcpy(nk->rg, rg, sizeof(int) * 2);
    nk->sym = sym;
    nk->stbl = tbl;
    nk->keyflags = workkeypieceflag;
    nk->expr = NULL;
    if (!workkey) {
        workkey = nk; /* empty list */
    } else {
        for (kp = workkey; kp->cmp; kp = kp->cmp) { /* add to end of list */
            keyfields++;
        }
        if (keyfields >= MAX_FIELDS_PER_KEY) {
            csc2_error("ERROR: TOO MANY FIELDS IN KEY - MAX IS %d\n",
                       MAX_FIELDS_PER_KEY);
            any_errors++;
            return -1;
        }
        kp->cmp = nk;
    }
    return 0;
}

int keysize(struct key *ck) /* CALCULATES SIZE OF A STRUCT KEY */
{
    int arr, chr, rng;

    int ondtidx = ck->stbl;

    if (ck->expr) {
        return 1; /* size of index on expressions will be calculated later */
    }
    if (ondtidx < 0) {
        csc2_error("ERROR: INVALID KEY TABLE INDEX %d.\n", ck->stbl);
        any_errors++;
        return 0;
    }
    arr = (tables[ondtidx].sym[ck->sym].dim[0] != -1); /* is this an array? */
    rng = (ck->rg[0] > 0 && ck->rg[1] > 0); /* is an element specified?  */
    chr = ((tables[ondtidx].sym[ck->sym].type == T_PSTR) ||
           (tables[ondtidx].sym[ck->sym].type == T_UCHAR) ||
           (tables[ondtidx].sym[ck->sym].type ==
            T_CSTR)); /* is this a character type? */

    if (rng < 0) { /* report this odd error     */
        csc2_error("ERROR: BAD RANGE FOR %s(%d:%d), SYMBOL #%d\n",
                   tables[ondtidx].sym[ck->sym].nm, ck->rg[0], ck->rg[1],
                   ck->sym);
        any_errors++;
        return 0;
    }
    if (chr && rng) /* a character range */
        return ck->rg[1] - ck->rg[0] + 1;
    else if (arr &&
             (ck->el[0] != -1)) /* sym is an array & key specifies 1 element */
    {
        return tables[ondtidx].sym[ck->sym].size; /* 1 element in an array */
    } else {
        return tables[ondtidx].sym[ck->sym].szof; /* a whole array or a
                                                     non-array is specified */
    }
}

static int
keyondisksize(struct key *wk) /* CALCULATE THE SIZE IN BYTES OF A WHOLE KEY */
{
    struct key *ck;
    int sz;
    sz = 0;
    ck = wk;
    while (ck) {
        sz += keysize(ck);
        ck = ck->cmp;
        if (ck) {
            /* one byte extra header will be needed for every column in key. */
            sz++;
        }
    }
    return sz;
}

int wholekeysize(
    struct key *wk) /* CALCULATE THE SIZE IN BYTES OF A WHOLE KEY */
{
    struct key *ck;
    int sz;
    sz = 0;
    ck = wk;
    while (ck) {
        sz += keysize(ck);
        ck = ck->cmp;
    }
    return sz;
}

int add_cluster_node(int node)
{
    int i = 0;
    if (node <= 0 || node > 10000) {
        csc2_error("ERROR at line %3d: ILLEGAL NODE#, VALID=1-10000\n",
                   current_line);
        any_errors++;
        return -1;
    }
    if (ncluster >= MAX_CLUSTER) {
        csc2_error("ERROR at line %3d: CLUSTER LIMIT REACHED, MAX=%d\n",
                   current_line, MAX_CLUSTER);
        any_errors++;
        return -1;
    }
    for (i = 0; i < ncluster; i++) {
        if (cluster_nodes[i] == node) {
            csc2_error(
                "ERROR at line %3d: NODE %d ALREADY INCLUDED IN CLUSTER\n",
                current_line, node);
            any_errors++;
            return -1;
        }
    }
    cluster_nodes[ncluster++] = node;
    return 0;
}

int get_union_size(int un)
{
    int i, largest = -1;
    if (un == -1)
        return 0;
    for (i = 0; i < nsym; i++) {
        if (sym[i].un_idx == un) {
            if (sym[i].caseno == -1) {
                if (sym[i].szof > largest)
                    largest = sym[i].szof;
            } else {
                int cs = sym[i].caseno, csize = 0, j = 0, first = -1;
                for (j = 0; j < nsym; j++) {
                    if (sym[j].caseno == cs && sym[j].un_member == un) {
                        csize += sym[j].szof;
                        if (first != -1)
                            csize += sym[j].padb;
                        first = j;
                    }
                }
                if (largest < csize)
                    largest = csize;
            }
        }
    }
    return largest;
}

int get_prev_sym(int idx)
{
    int i;
    if (idx > 0) {
        if (sym[idx - 1].un_member == -1)
            return (idx - 1);
        else if (sym[idx].un_member != -1 && sym[idx].caseno == -1) {
            int decr = idx - 1;
            while (sym[decr].un_member == sym[idx].un_member && decr >= 0)
                decr--;
            if (decr < 0)
                return -1;
            return decr;
        } else if (sym[idx].caseno != -1) {
            for (i = idx; i >= 0; i--) {
                if (sym[i].un_member == sym[idx].un_member &&
                    sym[i].caseno == sym[idx].caseno && i != idx) {
                    return i;
                }
            }
        }
    }
    return -1;
}

int get_case_size(int csn)
{
    int cs = sym[csn].caseno, csize = 0, j, first = -1;
    for (j = 0; j < nsym; j++) {
        if ((sym[csn].un_member == sym[j].un_member) && (sym[j].caseno == cs) &&
            (sym[j].caseno != -1)) {
            /*printf(" %s %d %d\n", sym[j].nm, sym[j].szof, sym[j].padb);*/
            csize += sym[j].szof;
            if (first != -1 && sym[j].padb != -1)
                csize += sym[j].padb;
            first = j;
        }
    }
    return csize;
}

void set_split(int ix, int percnt)
{
    int lo, hi;
    if (ix < -1 || ix > 16) {
        csc2_error("ERROR at line %3d: ILLEGAL INDEX #, VALID=0-15\n",
                   current_line);
        any_errors++;
        return;
    }
    if (percnt < 1 || percnt > 99) {
        csc2_error("ERROR at line %3d: BAD SPLIT PERCENT %d, VALID = 1-99\n",
                   current_line, percnt);
        any_errors++;
        return;
    }
    if (ix == -1) {
        lo = 0;
        hi = 15;
    } else {
        lo = hi = percnt;
    }
    for (ix = lo; ix <= hi; ix++) {
        spltpercnt[ix] = percnt;
    }
}

void key_setdup() /* used by parser, sets duplicate flag */
{
    workkeyflag |= DUPKEY;
}

void key_setrecnums(void) { workkeyflag |= RECNUMS; }

void key_setprimary(void) { workkeyflag |= PRIMARY; }

void key_setdatakey(void) { workkeyflag |= DATAKEY; }

void key_setuniqnulls(void) { workkeyflag |= UNIQNULLS; }

void key_piece_clear() /* used by parser, clears work key */
{
    workkey = 0;          /* clear work key */
    workkeyflag = 0;      /* clear flag for work key */
    workkeypieceflag = 0; /* clear key piece's flags */
}

void key_piece_setdescend()
{
    workkeypieceflag |= DESCEND; /* set DESCEND flag for key piece */
}

void key_add_tag(char *tag, char *exprname, char *where)
{
    key_add_comn(-1, tag, exprname, where);
}

void key_add(int ix, char *exprname) /* used by parser, adds a completed key */
{
    if (ix < 0 || ix >= MAX_KEY_INDEX) {
        csc2_error("Error at line %3d: ILLEGAL INDEX #%d, VALID=0-%d\n",
                current_line, ix, MAX_KEY_INDEX - 1);
        csc2_syntax_error("Error at line %3d: ILLEGAL INDEX #%d, VALID=0-%d",
                          current_line, ix, MAX_KEY_INDEX - 1);
        any_errors++;
        return;
    }
    key_add_comn(ix, NULL, exprname, NULL);
}

static void key_add_comn(int ix, char *tag, char *exprname,
                         char *where) /* used by parser, adds a completed key */
{
    int exprnum, ii, maxidx = 0, loweridx = 0;

    if (!workkey) {
        csc2_error("ERROR: KEY FAILED\n");
        any_errors++;
        return;
    }

    maxidx = MAX_KEY_INDEX;
    for (ii = 0; ii < ntables; ii++) {
        if (strcasecmp(tag, tables[ii].table_tag) == 0) {
            csc2_error("ERROR: NAME CLASH BETWEEN TAG AND KEY NAME '%s'.\n",
                       tag);
            any_errors++;
            return;
        }
    }
#if 1
    if ((workkeyflag & DUPKEY) && (workkeyflag & PRIMARY)) {
        csc2_error("ERROR: DUPLICATES NOT ALLOWED ON PRIMARY KEY\n");
        any_errors++;
        return;
    }
    if ((workkeyflag & DUPKEY) && (workkeyflag & UNIQNULLS)) {
        csc2_error("ERROR: DUPLICATES NOT ALLOWED ON UNIQUE NULLS\n");
        any_errors++;
        return;
    }
#endif
    if (ix == -1) {
        int lastix = -1;

        for (ii = 0; ii < nkeys; ii++) {
            if (keyixnum[ii] > lastix)
                lastix = keyixnum[ii];
            ix = keyixnum[ii];
#if 1
            if (ixflags[ix] & PRIMARY && workkeyflag & PRIMARY) {
                csc2_error("ERROR: PRIMARY KEY ALREADY SPECIFIED.  CANNOT HAVE "
                           ">1 PRIMARY KEYS (%s)\n",
                           tag);
                any_errors++;
                return;
            }
#endif
        }
        if (ii == nkeys) {
            ix = lastix + 1;
#if 0
            if (workkeyflag&PRIMARY)
            {
                fprintf(stderr,"ERROR: PRIMARY KEY ALREADY SPECIFIED.  CANNOT HAVE >1 PRIMARY KEYS (%s)\n",tag);
                any_errors++;
                return;
            }
#endif
        }
    }

    if (exprname == 0) { /* get expression */
        exprnum = -1;
    } else {
        exprnum = getexpr(exprname);
        if (exprnum == -1) {
            csc2_error("Error at line %3d: CAN'T FIND TYPE '%s'\n",
                    current_line, exprname);
            csc2_syntax_error("Error at line %3d: CAN'T FIND TYPE '%s'",
                              current_line, exprname);
            any_errors++;
            return;
        }
    }
    int sz = keyondisksize(workkey);
    if (sz > MAX_KEY_SIZE) { /* COMDB2 CURRENTLY SUPPORTS 512 byte KEYS*/
        csc2_error(
            "Error at line %3d: KEY %s TOO BIG(%d)! VALID SIZE=1-%d BYTES\n",
            current_line, tag, sz, MAX_KEY_SIZE);
        csc2_syntax_error(
            "Error at line %3d: KEY %s TOO BIG(%d)! VALID SIZE=1-%d BYTES",
            current_line, tag, sz, MAX_KEY_SIZE);
        any_errors++;
        return;
    }
    if (ix != 0) {
        int idxfnd = 0;
        loweridx = ix - 1;
        for (ii = 0; ii < nkeys; ii++) {
            if (keyixnum[ii] == loweridx) {
                idxfnd = 1;
                break;
            }
        }
        if (!idxfnd) {
            csc2_error(
                    "Error at line %3d: INDEX %d IS MISSING OR OUT OF ORDER!\n",
                    current_line, loweridx);
            csc2_syntax_error(
                "Error at line %3d: INDEX %d IS MISSING OR OUT OF ORDER!",
                current_line, loweridx);
            any_errors++;
            return;
        }
    }

    for (ii = 0; ii < nkeys; ii++) {
        if (keyixnum[ii] > ix)
            break;
        if (keyixnum[ii] < ix)
            continue;
        if (exprnum == -1) { /* no expression */
            if (keyexprnum[ii] == -1) {
                csc2_error("Error at line %3d: TWO KEYS FOR INDEX %d!\n",
                        current_line, keyixnum[ii]);
                csc2_syntax_error("Error at line %3d: TWO KEYS FOR INDEX %d!",
                                  current_line, keyixnum[ii]);
                any_errors++;
                return;
            }
        } else {
            if (keyexprnum[ii] == -1)
                break;
        }
    }

    if (ii < nkeys) { /* insert into proper slot */
        memmove(keys + ii + 1, keys + ii, (nkeys - ii) * sizeof(keys[0]));
        memmove(keyixnum + ii + 1, keyixnum + ii,
                (nkeys - ii) * sizeof(keyixnum[0]));
        memmove(keyexprnum + ii + 1, keyexprnum + ii,
                (nkeys - ii) * sizeof(keyexprnum[0]));
    }

    keys[ii] = workkey;        /* remember key */
    keyixnum[ii] = ix;         /* remember ix number associated with key */
    keyexprnum[ii] = exprnum;  /* remember expr assoc with key */
    ixflags[ix] = workkeyflag; /* remember flags */
    if (tag != NULL) {
        int idxfnd = 0, jj = 0;
        strupper(tag);
        for (jj = 0; jj < nkeys; jj++) {
            if (keyixnum[jj] != ix && !strcasecmp(tag, keys[jj]->keytag)) {
                csc2_error("Error at line %3d: CANT HAVE SAME TAG '%s' "
                                "FOR INDICES %d AND %d!\n",
                        current_line, tag, ix, keyixnum[jj]);
                csc2_syntax_error("Error at line %3d: CANT HAVE SAME TAG '%s' "
                                  "FOR INDICES %d AND %d!",
                                  current_line, tag, ix, keyixnum[jj]);
                any_errors++;
                return;
            }
        }
        strncpy(keys[ii]->keytag, tag, sizeof(keys[ii]->keytag));
    } else {
        sprintf(keys[ii]->keytag, "DEFAULT_ix_%d", ix);
    }
    if (strlen(tag) >= MAXIDXNAMELEN) {
        csc2_error(
            "WARNING: line %3d: index name '%s' longer than %d characters!\n",
            current_line, tag, MAXIDXNAMELEN - 1);
    }
    if (where && strlen(where) != 0) {
        keys[ii]->where = csc2_strdup(where);
    } else {
        keys[ii]->where = NULL;
    }
    nkeys++; /* next key */
}

void rng_add(int i) /* used by parser, adds a completed rng */
{
    if (i < 0 || i >= MAXRNGS) {
        csc2_error("Error at line %3d: ILLEGAL RANGE #%d, VALID=0-%d\n",
                current_line, i, MAXRNGS);
        csc2_syntax_error("Error at line %3d: ILLEGAL RANGE #%d, VALID=0-%d",
                          current_line, i, MAXRNGS);
        any_errors++;
        return;
    }
    if (!rngs[i])
        nrngs++;
    rngs[i] = workkey;
}

int expridx_type = 0;
int expridx_arraysz = 0;

void reset_key_exprtype(void)
{
    expridx_type = 0;
    expridx_arraysz = 0;
}

void key_exprtype_add(int type, int arraysz)
{
    expridx_type = type;
    expridx_arraysz = arraysz;
}

void key_piece_add(char *buf,
                   int is_expr) /* used by parser, adds a piece of a key */
{
    int el[6], rg[2], i, ret, t, found = 0, pointer = -1, tidx = 0;
    char *cp, *tag;

    if (is_expr) {
        struct key *nk = (struct key *)csc2_malloc(sizeof(struct key));
        struct key *kp;
        int keyfields = 0;
        if (!nk) {
            csc2_error("ERROR: OUT OF MEM: %s - ABORTING\n", strerror(errno));
            any_errors++;
            return;
        }
        nk->cmp = 0;
        nk->stbl = gettable(ONDISKTAG);
        if (nk->stbl < 0) {
            any_errors++;
            return;
        }
        nk->keyflags = workkeypieceflag;
        nk->expr = csc2_strdup(buf);
        nk->exprtype = expridx_type;
        nk->exprarraysz = expridx_arraysz;
        if (!workkey) {
            workkey = nk; /* empty list */
        } else {
            for (kp = workkey; kp->cmp; kp = kp->cmp) { /* add to end of list */
                keyfields++;
            }
            if (keyfields >= MAX_FIELDS_PER_KEY) {
                csc2_error("ERROR: TOO MANY FIELDS IN KEY - MAX IS %d\n",
                           MAX_FIELDS_PER_KEY);
                any_errors++;
                return;
            }
            kp->cmp = nk;
        }
        return;
    }

    strlower(buf, strlen(buf));
    process_array_(el, rg, buf, &t, NULL);
    cp = strchr(buf, '(');
    if (cp)
        *cp = 0;

    tag = ONDISKTAG;
    i = getsymbol(tag, buf, &tidx);

    if (i == -1) {
        tag = (ntables > 1) ? ONDISKTAG : DEFAULTTAG;
        i = getsymbol(tag, buf, &tidx);
    }
    if (i == -1) {
        csc2_error("Error at line %3d: SYMBOL NOT FOUND: %s.\n",
                current_line, buf);
        csc2_syntax_error("Error at line %3d: SYMBOL NOT FOUND: %s.",
                          current_line, buf);
        csc2_error("IF IN MULTI-TABLE MODE MAKE SURE %s TAG IS DEFINED\n",
                ONDISKTAG);
        any_errors++;
        return;
    } else {
        if (rg[0] || rg[1]) {
            if (tables[tidx].sym[i].type != T_PSTR &&
                tables[tidx].sym[i].type != T_UCHAR &&
                tables[tidx].sym[i].type != T_CSTR) {
                csc2_error("Error at line %3d: BAD KEY: %s\n",
                        current_line, buf);
                csc2_syntax_error("Error at line %3d: BAD KEY: %s",
                                  current_line, buf);
                any_errors++;
                return;
            } else if (rg[0] == -1) { /* -1 = no # in range, ie: c(1:) */
                rg[0] = 1;
            } else if (rg[1] == -1) {
                rg[1] = tables[tidx].sym[i].size;
            }
            if ((rg[0] - 1) < 0 || (rg[0] - 1) >= tables[tidx].sym[i].size) {
                csc2_error(
                    "Error at line %3d: RANGE INDEX 0 IS OUT OF BOUNDS: %s\n",
                    current_line, buf);
                csc2_syntax_error(
                    "Error at line %3d: RANGE INDEX 0 IS OUT OF BOUNDS: %s",
                    current_line, buf);
                any_errors++;
                return;
            }
            if ((rg[1] - 1) < (rg[0] - 1) ||
                (rg[1] - 1) >= tables[tidx].sym[i].size) {
                csc2_error(
                    "Error at line %3d: RANGE INDEX 1 IS OUT OF BOUNDS: %s\n",
                    current_line, buf);
                csc2_syntax_error(
                    "Error at line %3d: RANGE INDEX 1 IS OUT OF BOUNDS: %s",
                    current_line, buf);
                any_errors++;
                return;
            }
        }
        if (numdim(el) != numdim(tables[tidx].sym[i].dim)) {
            csc2_error("Error at line %3d: WRONG # OF SUBSCRIPTS: %s\n",
                    current_line, buf);
            csc2_syntax_error("Error at line %3d: WRONG # OF SUBSCRIPTS: %s",
                              current_line, buf);
            any_errors++;
            return;
        }
        for (t = 0; t < 6 && (tables[tidx].sym[i].dim[t] != -1); t++) {
            if (el[t] >= tables[tidx].sym[i].dim[t] || (el[t] < 0)) {
                csc2_error(
                        "Error at line %3d: INVALID ARRAY SUBSCRIPT: %s\n",
                        current_line, buf);
                csc2_syntax_error(
                    "Error at line %3d: INVALID ARRAY SUBSCRIPT: %s",
                    current_line, buf);
                any_errors++;
                return;
            }
        }
        addtokey(i, tidx, el, rg); /* add this key to compound key */
    }
    workkeypieceflag = 0;
}

void rec_c_add(int typ, int size, char *name, char *cmnt)
{
    /* ADDS A SYMBOL TO SYM TABLE */
    int i = 0, aln = 0, siz = 0;
    int rg[2] = {0, 0};
    char *cp, *sizcn = NULL;
    /*    printf("NAME %s %d\n", name, lastidx);*/

    /* don't overrun memory */
    if (any_errors)
        return;

    if (typ == T_LOGICAL) {
        used_bools = 1;
        csc2_error("Error at line %3d: 'bool' DATATYPE IS DEPRECATED - "
                        "USE INT INSTEAD\n",
                current_line);
        csc2_syntax_error("Error at line %3d: 'bool' DATATYPE IS DEPRECATED - "
                          "USE INT INSTEAD",
                          current_line);
        if (!allow_bools)
            any_errors++;
    }

    if (typ == T_BLOB && dims[0] != -1) {
        typ = T_BLOB2;
    }

    /* we have just parsed the character array, and last element gives the
     * size */
    if (lastidx - 1 != -1 &&
        (typ == T_PSTR || typ == T_CSTR || typ == T_UCHAR || typ == T_VUTF8 ||
         typ == T_BLOB2)) {
        siz = dims[0];
        sizcn = dims_cnst[0];
        /* get rid of the first element of the array (remember parsing done in
         * last->first way ) push later elements forward in the array */
        for (i = 0; i < 5 && (dims[i] != -1); i++) {
            dims[i] = dims[i + 1];
            dims_cnst[i] = dims_cnst[i + 1];
        }
        /* reset the lastidx, since another char array maybe in the record */
        lastidx = 0;
    } else
        siz = size;

    strlower(name, strlen(name));

    switch (typ) {
    case T_UINTEGER2:
    case T_INTEGER2:
        aln = 2;
        break;
    case T_ULONG:
        aln = 4;
        break;
    case T_UINTEGER4:
    case T_INTEGER4:
        aln = 4;
        break;
    case T_REAL4:
        aln = 4;
        break;
    case T_ULONGLONG:
    case T_LONGLONG:
    case T_REAL8:
        aln = 8;
        break;
    case T_PSTR:
        aln = 1;
        break;
    case T_UCHAR:
        aln = 1;
        break;
    case T_CSTR:
        aln = 1;
        break;
    case T_LOGICAL:
        aln = 4;
        break;
    case T_BLOB:
    case T_BLOB2:
    case T_VUTF8: /* TODO is this right? */
                  /* ANSWER: No, not really.  The blob type is always 16 bytes,
                   * but is layed out differently on 32 bit or 64 bit platforms.
                   * It has size_t and void* members, which are 4 bytes when 32 bit
                   * and 8 bytes when 64 bit.  So the correct alignment depends on
                   * your platform.. and we can't have that, because this code has
                   * to be consistent regardless of the memory model.  aln=8 would
                   * be the most appropriate choice here, but it looks like on day
                   * one we went for aln=16 for blobs, so it wil have to stay that
                   * way.  aln cannot be 4 because then on 64 bit platforms the]
                   * compiler may start adding its own padding to ensure correct
                   * alignment of the struct members. */
        aln = 16;
        break;
    case T_DATETIME:
    case T_DATETIMEUS:
        siz = 76; /*this is HARDCODED to the size of cdb2_client_datetime_t*/
        aln = 4;  /*largest element of the struct is an int */
        break;
    case T_INTERVALYM:
        siz =
            3 *
            sizeof(
                int); /*this is HARDCODED to the size of cdb2_client_intv_ym_t*/
        aln = 4;      /*largest element of the struct is an int */
        break;
    case T_INTERVALDS:
    case T_INTERVALDSUS:
        siz =
            6 *
            sizeof(
                int); /*this is HARDCODED to the size of cdb2_client_intv_ds_t*/
        aln = 4;      /*largest element of the struct is an int */
        break;
    case T_DECIMAL32:
        aln = 1;
        siz = 1 /*sign*/ + 1 /*dot*/ + 7 /*coefdigits*/ + 1 /*E*/ +
              1 /*expsign*/ + 2 /*exp*/ + 1 /*zero*/; /* 14bytes */
        break;
    case T_DECIMAL64:
        aln = 1;
        siz = 1 /*sign*/ + 1 /*dot*/ + 16 /*coefdigits*/ + 1 /*E*/ +
              1 /*expsign*/ + 3 /*exp*/ + 1 /*zero*/; /* 24 bytes */
        break;
    case T_DECIMAL128:
        aln = 1;
        siz = 1 /*sign*/ + 1 /*dot*/ + 34 /*coefdigits*/ + 1 /*E*/ +
              1 /*expsign*/ + 4 /*exp*/ + 1 /*zero*/; /* 43 bytes */
        break;
    default:
        /*huh?*/
        csc2_error( "%d Error at line %3d: UNKNOWN TYPE: %s\n", __LINE__,
                current_line, name);
        csc2_syntax_error("%d Error at line %3d: UNKNOWN TYPE: %s", __LINE__,
                          current_line, name);
        any_errors++;
        return;
    }

    if (tables[ntables].nsym >= MAX) {
        csc2_error( "Error at line %3d: SYMBOL TABLE FULL: %s\n",
                current_line, name);
        csc2_syntax_error("Error at line %3d: SYMBOL TABLE FULL: %s",
                          current_line, name);
        any_errors++;
        return;
    }

    if (tables[ntables].nsym >= COMDB2_MAX) {
        csc2_error(
                "Error at line %3d: TOO MANY SYMBOLS. MAX %d. AT SYM %s\n",
                current_line, COMDB2_MAX, name);
        csc2_syntax_error(
            "Error at line %3d: TOO MANY SYMBOLS. MAX %d. AT SYM %s",
            current_line, COMDB2_MAX, name);
        any_errors++;
        return;
    }

    if (process_array_(&(tables[ntables].sym[tables[ntables].nsym].dim[0]), rg,
                       name, &(tables[ntables].sym[tables[ntables].nsym].arr),
                       &tables[ntables]
                            .sym[tables[ntables].nsym]
                            .dim_cnst[0])) { /* fill dimension array */
        csc2_error( "Error at line %3d: BAD ARRAY "
                        "SPECIFIER: %s\n",
                current_line, name);
        csc2_syntax_error("Error at line %3d: BAD ARRAY "
                          "SPECIFIER: %s",
                          current_line, name);
        any_errors++;
        return;
    }

    tables[ntables].sym[tables[ntables].nsym].numfo = nfieldopt;
    memcpy(tables[ntables].sym[tables[ntables].nsym].fopts, fieldopts,
           nfieldopt * sizeof(struct fieldopt));
    for (i = 0; i < nfieldopt; i++) {

        int j = 0;
        /*printf("%d(%d) option %d==%d type %d tag: %s\n
         * ",nfieldopt,opt_schematype,
         * tables[ntables].sym[tables[ntables].nsym].fopts[i].opttype,FLDOPT_NULL,tables[ntables].sym[tables[ntables].nsym].fopts[i].valtype,tables[ntables].table_tag);*/
        if (((tables[ntables].sym[tables[ntables].nsym].fopts[i].opttype) ==
             FLDOPT_NULL) &&
            strcmp(tables[ntables].table_tag, ONDISKTAG)) {
            csc2_error( "Error at line %3d: SYMBOL '%s' MAY NOT HAVE NULL "
                            "OPTION SET.\n",
                    current_line, name);
            csc2_syntax_error(
                "Error at line %3d: SYMBOL '%s' MAY NOT HAVE NULL "
                "OPTION SET.",
                current_line, name);
            csc2_error( "Error at line %3d: MUST BE IN %s SCHEMA.\n",
                    current_line, ONDISKTAG);
            csc2_syntax_error("Error at line %3d: MUST BE IN %s SCHEMA.",
                              current_line, ONDISKTAG);
            any_errors++;
            return;
        }

        /* only byte arrays may have a padding value, and it must be specified
         * as an int */
        if (FLDOPT_PADDING ==
            tables[ntables].sym[tables[ntables].nsym].fopts[i].opttype) {
            if (typ != T_UCHAR) {
                csc2_error( "Error at line %3d: DBPAD MAY ONLY BE APPLIED "
                                "TO BYTE ARRAYS\n",
                        current_line);
                csc2_syntax_error(
                    "Error at line %3d: DBPAD MAY ONLY BE APPLIED "
                    "TO BYTE ARRAYS",
                    current_line);
                any_errors++;
                return;
            }
        } else
            switch (typ) {
            case T_LOGICAL:
            case T_UINTEGER2:
            case T_INTEGER2:
            case T_LONGLONG:
            case T_ULONGLONG:
            case T_ULONG:
            case T_UINTEGER4:
            case T_INTEGER4:
                if (tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype != CLIENT_INT &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype != CLIENT_UINT &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].opttype != FLDOPT_NULL) {
                    csc2_error( "Error at line %3d: FIELD OPTION TYPE IN "
                                    "SCHEMA MUST MATCH FIELD TYPE: %s\n",
                            current_line, name);
                    csc2_syntax_error("Error at line %3d: FIELD OPTION TYPE IN "
                                      "SCHEMA MUST MATCH FIELD TYPE: %s",
                                      current_line, name);
                    any_errors++;
                    return;
                }
                break;

            case T_REAL4:
            case T_REAL8:
                if (tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype != CLIENT_REAL &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].opttype != FLDOPT_NULL) {
                    csc2_error( "Error at line %3d: FIELD OPTION TYPE IN "
                                    "SCHEMA MUST MATCH FIELD TYPE: %s\n",
                            current_line, name);
                    csc2_syntax_error("Error at line %3d: FIELD OPTION TYPE IN "
                                      "SCHEMA MUST MATCH FIELD TYPE: %s",
                                      current_line, name);
                    any_errors++;
                    return;
                }
                break;
            case T_UCHAR:
                /* for byte arrays, allow only integers which will then be
                 * interpreted as the padding value (99% of people want 0).
                 * Sadly we have to allow cstring as well because a bunch of
                 * people have it in their production databases.  cstring is a
                 * noop and gets ignored. */
                if (tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype != CLIENT_INT &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype != CLIENT_BYTEARRAY &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype != CLIENT_CSTR &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].opttype != FLDOPT_NULL) {
                    csc2_error(
                            "Error at line %3d: FIELD OPTION TYPE IN "
                            "SCHEMA MUST BE AN INTEGER OR HEX FOR THIS FIELD "
                            "TYPE: %s\n",
                            current_line, name);
                    csc2_syntax_error(
                        "Error at line %3d: FIELD OPTION TYPE IN "
                        "SCHEMA MUST BE AN INTEGER OR HEX FOR THIS FIELD "
                        "TYPE: %s",
                        current_line, name);
                    any_errors++;
                    return;
                }
                if (tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype == CLIENT_CSTR &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].opttype != FLDOPT_NULL) {
                    csc2_error(
                            "Warning at line %3d: STRING DEFAULT OPTION "
                            "WILL BE IGNORED FOR FIELD: %s\n",
                            current_line, name);
                }
                if (tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype == CLIENT_INT &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].opttype != FLDOPT_NULL &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].value.i8val != 0) {
                    csc2_error( "Error at line %3d: FIELD OPTION TYPE IN "
                                    "SCHEMA MUST BE ZERO FOR THIS FIELD TYPE: "
                                    "%s\n",
                            current_line, name);
                    csc2_syntax_error(
                        "Error at line %3d: FIELD OPTION TYPE IN "
                        "SCHEMA MUST BE ZERO FOR THIS FIELD TYPE: "
                        "%s",
                        current_line, name);
                    any_errors++;
                    return;
                }
                break;
            case T_PSTR:
            case T_CSTR:
            case T_VUTF8:
            case T_DATETIME:
            case T_DATETIMEUS:
                if (tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype != CLIENT_CSTR &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].valtype != CLIENT_PSTR &&
                    tables[ntables].sym[tables[ntables].nsym].fopts
                            [i].opttype != FLDOPT_NULL) {
                    csc2_error( "Error at line %3d: FIELD OPTION TYPE IN "
                                    "SCHEMA MUST MATCH FIELD TYPE: %s\n",
                            current_line, name);
                    csc2_syntax_error("Error at line %3d: FIELD OPTION TYPE IN "
                                      "SCHEMA MUST MATCH FIELD TYPE: %s",
                                      current_line, name);
                    any_errors++;
                    return;
                }
                break;
            case T_INTERVALYM:
            case T_INTERVALDS:
            case T_INTERVALDSUS:
            case T_DECIMAL32:
            case T_DECIMAL64:
            case T_DECIMAL128:
            case T_BLOB:
            case T_BLOB2:
                if (tables[ntables].sym[tables[ntables].nsym].fopts
                        [i].opttype != FLDOPT_NULL) {
                    csc2_error( "Error at line %3d: CANNOT SPECIFY "
                                    "LOAD/STORE OPTIONS FOR BLOB/DATE FIELDS\n",
                            current_line);
                    csc2_syntax_error("Error at line %3d: CANNOT SPECIFY "
                                      "LOAD/STORE OPTIONS FOR BLOB/DATE FIELDS",
                                      current_line);
                    any_errors++;
                    return;
                }
                break;
            default:
                /*huh?*/
                csc2_error( "%d Error at line %3d: UNKNOWN TYPE: %s\n",
                        __LINE__, current_line, name);
                csc2_syntax_error("%d Error at line %3d: UNKNOWN TYPE: %s",
                                  __LINE__, current_line, name);
                any_errors++;
                return;
            }

        for (j = 0; j < nfieldopt; j++) {
            if (j == i)
                continue;
            if (tables[ntables].sym[tables[ntables].nsym].fopts[i].opttype ==
                tables[ntables].sym[tables[ntables].nsym].fopts[j].opttype) {
                csc2_error( "Error at line %3d: FIELD IN SCHEMA CANNOT "
                                "HAVE OPTIONS REPEATED: %s\n",
                        current_line, name);
                csc2_syntax_error("Error at line %3d: FIELD IN SCHEMA CANNOT "
                                  "HAVE OPTIONS REPEATED: %s",
                                  current_line, name);
                any_errors++;
            }
        }
    }

    if (rg[0] || rg[1]) {
        csc2_error( "Error at line %3d: CHARACTER RANGE "
                        "NOT ALLOWED IN RECORD{}: %s\n",
                current_line, name);
        csc2_syntax_error("Error at line %3d: CHARACTER RANGE "
                          "NOT ALLOWED IN RECORD{}: %s",
                          current_line, name);
        any_errors++;
        return;
    }
    cp = strchr(name, '(');
    if (cp)
        *cp = 0;
    if (unionflag) {
        un_reset[tables[ntables].nsym]++;
        tables[ntables].sym[tables[ntables].nsym].un_member = union_level;
        tables[ntables].sym[tables[ntables].nsym].un_idx = union_index;
    } else {
        tables[ntables].sym[tables[ntables].nsym].un_member = -1;
        tables[ntables].sym[tables[ntables].nsym].un_idx = -1;
    }
    if (tables[ntables].sym[tables[ntables].nsym].arr != 1 &&
        tables[ntables].sym[tables[ntables].nsym].arr != 0)
        tables[ntables].sym[tables[ntables].nsym].arr = -1;
    tables[ntables].sym[tables[ntables].nsym].nm = name;

    if (siz != -1) {
        tables[ntables].sym[tables[ntables].nsym].size = siz;
        tables[ntables].sym[tables[ntables].nsym].szof_cnst = sizcn;
    } else
        tables[ntables].sym[tables[ntables].nsym].size = aln;

    lastidx = 0;

    tables[ntables].sym[tables[ntables].nsym].type = typ;
    tables[ntables].sym[tables[ntables].nsym].caseno = current_case;
    if (tables[ntables].sym[tables[ntables].nsym].caseno != -1) {
        tables[ntables].sym[tables[ntables].nsym].un_member = union_level;
        tables[ntables].sym[tables[ntables].nsym].un_idx = union_index;
    }

    memcpy(tables[ntables].sym[tables[ntables].nsym].dpth_tree, cur_dpth,
           sizeof(short) * dpth_idx);
    tables[ntables].sym[tables[ntables].nsym].dpth = dpth_idx;

    tables[ntables].sym[tables[ntables].nsym].dumped = 0;
    tables[ntables].sym[tables[ntables].nsym].padded = 0;
    tables[ntables].sym[tables[ntables].nsym].align = aln;
    tables[ntables].sym[tables[ntables].nsym].padb = -1;
    tables[ntables].sym[tables[ntables].nsym].padex = 0;
    tables[ntables].sym[tables[ntables].nsym].padaf = 0;
    tables[ntables].sym[tables[ntables].nsym].padcs = -1;

    if (siz != -1)
        tables[ntables].sym[tables[ntables].nsym].szof =
            arroff(tables[ntables].nsym,
                   tables[ntables].sym[tables[ntables].nsym].dim, rg) +
            siz;
    /* vutf8 strings that don't have a specified length need to be treated
     * specially later on, everything else defaults to it's alignment */
    else if (typ != T_VUTF8)
        tables[ntables].sym[tables[ntables].nsym].szof =
            arroff(tables[ntables].nsym,
                   tables[ntables].sym[tables[ntables].nsym].dim, rg) +
            aln;
/* This never belonged here anyway. */
#if 0
    /* ondisk schema has different size rules.  I feel dirty hacking this kind
     * of database internals information in here. -- Sam J */
    if (strcmp(tables[ntables].table_tag, ONDISKTAG) == 0) {
        switch(tables[ntables].sym[tables[ntables].nsym].type)
        {
            case T_CSTR:
                /* C string needs no adjustment - ondisk length is numchars - 1 + 1. */
                break;
            case T_BLOB:
                /* server side blob is 5 bytes */
                tables[ntables].sym[tables[ntables].nsym].szof=5;
                tables[ntables].sym[tables[ntables].nsym].size=5;
            default:
                /* All other types have an extra byte ondisk - the null/not null byte */
                tables[ntables].sym[tables[ntables].nsym].szof++;
                tables[ntables].sym[tables[ntables].nsym].size++;
                break;
        }
    }
#endif
    if (strlen(cmnt) == 0) {
        tables[ntables].sym[tables[ntables].nsym].com = blankchar;
    } else {
        tables[ntables].sym[tables[ntables].nsym].com =
            (char *)csc2_malloc(strlen(cmnt) + 1);
        if (tables[ntables].sym[tables[ntables].nsym].com == NULL) {
            csc2_error("ERROR: OUT OF MEMORY, SYMBOL %s!!!\n",
                       tables[ntables].sym[tables[ntables].nsym].nm);
            csc2_error("ABORTING\n");
            return;
        }
        strcpy(tables[ntables].sym[tables[ntables].nsym].com, cmnt);
    }
    for (i = 0; i < tables[ntables].nsym; i++) {
        if (!strcmp(tables[ntables].sym[i].nm,
                    tables[ntables].sym[tables[ntables].nsym].nm)) {
            csc2_error( "Error at line %3d: DUPLICATE VARIABLE NAMES ARE "
                            "NOT ALLOWED (variable '%s')\n",
                    current_line, name);
            csc2_syntax_error("Error at line %3d: DUPLICATE VARIABLE NAMES ARE "
                              "NOT ALLOWED (variable '%s')",
                              current_line, name);
            any_errors++;
        }
    }

    /* check for pointlessly short strings */
    if (T_CSTR == typ && tables[ntables].sym[tables[ntables].nsym].size < 2) {
        csc2_error( "Error at line %3d: CSTRINGS ARE \\0 TERMINATED SO "
                        "MUST BE AT LEAST 2 BYTES IN SIZE\n",
                current_line);
        csc2_syntax_error("Error at line %3d: CSTRINGS ARE \\0 TERMINATED SO "
                          "MUST BE AT LEAST 2 BYTES IN SIZE",
                          current_line);
        any_errors++;
    } else if (T_PSTR == typ &&
               tables[ntables].sym[tables[ntables].nsym].size < 1) {
        csc2_error(
                "Error at line %3d: ZERO LENGTH PSTRINGS ARE NOT ALLOWED\n",
                current_line);
        csc2_syntax_error(
            "Error at line %3d: ZERO LENGTH PSTRINGS ARE NOT ALLOWED",
            current_line);
        any_errors++;
    } else if (T_UCHAR == typ &&
               tables[ntables].sym[tables[ntables].nsym].size < 1) {
        csc2_error(
                "Error at line %3d: ZERO LENGTH BYTE ARRAYS ARE NOT ALLOWED\n",
                current_line);
        csc2_syntax_error(
            "Error at line %3d: ZERO LENGTH BYTE ARRAYS ARE NOT ALLOWED",
            current_line);
        any_errors++;
    }
    tables[ntables].nsym++;
}

void add_constant(char *name, int value, short type)
{
    if (nsym >= MAX) {
        csc2_error( "Error at line %3d: CONSTANTS TABLE FULL: %s\n",
                current_line, name);
        csc2_syntax_error("Error at line %3d: CONSTANTS TABLE FULL: %s",
                          current_line, name);
        any_errors++;
        return;
    }
    if (constant(name) != -1) {
        csc2_error( "Error at line %3d: REDEFINING CONSTANT: %s\n",
                current_line, name);
        csc2_syntax_error("Error at line %3d: REDEFINING CONSTANT: %s",
                          current_line, name);
        any_errors++;
        return;
    }
    constants[ncnst].nm = name;
    constants[ncnst].value = value;
    if (type == 1)
        prcnst++;
    constants[ncnst].type = type;
    ncnst++;
}

void start_table(char *tag, int preset)
{
    int i = 0;

    /* This has been allowed forever so just print a warning */
    if (strlen(tag) > MAX_TAG_LEN) {
        csc2_error("ERROR: TAG '%s' EXCEEDS MAXIMUM LENGTH\n", tag);
        csc2_error("THIS WILL BE TRUNCATED TO %d CHARACTERS\n", MAX_TAG_LEN);
    }

    for (i = 0; i < strlen(tag); i++) {
        if (!isalpha((int)tag[i]) && !isdigit((int)tag[i])) {
            if (i > 0 && tag[i] == '_')
                continue;
            if (preset && tag[i] == '.') {
                continue;
            }
            csc2_error(
                "ERROR: INVALID CHARACTER IN TAG %s DECLARATION: '%c'.\n", tag,
                tag[i]);
            if (tag[i] == '_')
                csc2_error("'_' CANNOT BE FIRST CHARACTER.\n");
            else
                csc2_error("ONLY LETTERS, DIGITS, AND '_' ARE ALLOWED.\n");
            any_errors++;
            return;
        }
    }

    if (!strcmp(tag, ONDISKTAG) && numkeys() != 0) {
        csc2_error("ERROR: KEYS DEFINED BEFORE %s SCHEMA.\n", tag);
        any_errors++;
        return;
    }
    for (i = 0; i < ntables; i++) {
        if (!strcmp(tables[i].table_tag, tag)) {
            csc2_error("TABLE ERROR: TABLE WITH TAG '%s' ALREADY DEFINED.\n",
                       tag);
            any_errors++;
            return;
        }
    }
    if (ntables >= MAXTBLS) {
        csc2_error("TABLE ERROR: ONLY UP TO %d TABLES ALLOWED\n", MAXTBLS);
        any_errors++;
        return;
    }
    strncpy(tables[ntables].table_tag, tag,
            sizeof(tables[ntables].table_tag) - 1);
    tables[ntables].table_tag[sizeof(tables[ntables].table_tag) - 1] = '\0';
}

void end_table()
{
    if (any_errors == 0)
        if (compute_all_data(ntables) != 0)
            ++any_errors;
    ntables++;
    unionflag = 0;
    current_union = 0;
    union_index = -1;
    union_level = -1;
    un_init = 0;
    memset(union_names, 0, sizeof(union_names));
    memset(case_table, 0, sizeof(case_table));
    dpth_idx = 0;
    current_case = -1;
    cn_p = 0;
}

void add_array(int dim, char *label)
{
    if (dimidx >= 6) {
        csc2_error("ARRAY ERROR: ONLY UP TO 6 DIMENSIONS ALLOWED\n");
        any_errors++;
        reset_array();
        return;
    }
    if (dimidx == 0)
        reset_array();
    dims[dimidx] = dim;
    dims_cnst[dimidx] = label;
    dimidx++;
}

void add_fldopt(int opttype, int valtype, void *value)
{
    if (nfieldopt >= 8) {
        csc2_error("FIELD OPTION ERROR: ONLY UP TO 8 OPTIONS ALLOWED\n");
        any_errors++;
        reset_fldopt();
        return;
    }
    if (valtype != CLIENT_INT && valtype != CLIENT_REAL &&
        valtype != CLIENT_CSTR && valtype != CLIENT_BYTEARRAY) {
        csc2_error("FIELD OPTION ERROR: INVALID VALUE TYPE %d\n", valtype);
        any_errors++;
        reset_fldopt();
        return;
    }
    if (opttype != FLDOPT_DBSTORE && opttype != FLDOPT_DBLOAD &&
        opttype != FLDOPT_NULL && opttype != FLDOPT_PADDING) {
        csc2_error("FIELD OPTION ERROR: INVALID OPTION TYPE %d\n", opttype);
        any_errors++;
        reset_fldopt();
        return;
    }
    if (opttype == FLDOPT_PADDING) {
        int ivalue;
        if (valtype != CLIENT_INT) {
            csc2_error("FIELD OPTION ERROR: INVALID VALUE TYPE FOR PADDING\n");
            any_errors++;
            reset_fldopt();
            return;
        }
        ivalue = *((int *)value);
        if (ivalue < 0 || ivalue > 0xff) {
            csc2_error("FIELD OPTION ERROR: PADDING VALUE MUST BE IN THE RANGE "
                       "0..255\n");
            any_errors++;
            reset_fldopt();
            return;
        }
    }
    if (nfieldopt == 0)
        reset_fldopt();
    /*fprintf(stderr, "add opt %d %d\n", opttype, valtype);*/

    fieldopts[nfieldopt].opttype = opttype;
    fieldopts[nfieldopt].valtype = valtype;
    if (valtype == CLIENT_INT) /* integer numeric */
    {
        fieldopts[nfieldopt].value.u8val = 0;
        if (opttype == FLDOPT_DBSTORE || opttype == FLDOPT_DBLOAD) {
            errno = 0;
            fieldopts[nfieldopt].value.i8val = strtoll(value, NULL, 10);
            if (errno) {
                errno = 0;
                fieldopts[nfieldopt].value.u8val = strtoull(value, NULL, 10);
                if (errno) {
                    logmsgperror("strtoull");
                    csc2_error("FIELD OPTION ERROR: CONVERSION FAILED FROM %s "
                               "TO NUMBER\n",
                               (char *)value);
                    any_errors++;
                    reset_fldopt();
                    return;
                }
            }
        } else {
            fieldopts[nfieldopt].value.i4val = *(int *)value;
        }
    }
    if (valtype == CLIENT_REAL) /* floating point numeric */
        fieldopts[nfieldopt].value.r8val = *(double *)value;
    if (valtype == CLIENT_CSTR) /* string */
        fieldopts[nfieldopt].value.strval = (char *)value;
    if (valtype == CLIENT_BYTEARRAY) /* string */
        fieldopts[nfieldopt].value.byteval = (char *)value;
    nfieldopt++;
}

void reset_fldopt(void) { nfieldopt = 0; }

void add_range(int rg)
{
    if (rngidx >= 2) {
        csc2_error("RANGE ERROR: ONLY UP TO 2 RANGE INDICES ALLOWED\n");
        any_errors++;
        reset_range();
        return;
    }
    if (rngidx == 0)
        reset_range();
    ranges[rngidx] = rg;
    rngidx++;
}

void reset_array()
{
    int i;
    for (i = 0; i < 6; i++) {
        dims[i] = -1;
        dims_cnst[i] = NULL;
    }
    dimidx = 0;
    range_or_array = 0;
    declaration = 0;
}

void reset_range()
{
    int i;
    for (i = 0; i < 2; i++) {
        ranges[i] = 0;
    }
    rngidx = 0;
    range = 0;
}

int offpad(int offset, int align) /* RETS PADDING GIVEN OFFSET AND ALIGN */
{
    int k;
    k = align - (offset % align); /* calc bytes needed for padding */
    if (k == align)
        k = 0; /* adjust for no padding necces. */
    return k;
}

int numsegs(struct key *cr) /* COUNT # of PIECES IN THIS KEY */
{
    int i = 0;
    while (cr) {
        i++;
        cr = cr->cmp;
    }
    return i;
}

int compar(int *a, int *b)
{
    static int *arr;

    if (!a) {
        arr = b;
        return 0;
    }

    if (arr[*a] < arr[*b])
        return -1;
    if (arr[*a] > arr[*b])
        return 1;
    return 0;
}

int calc_rng(int rng, int *ask)
{ /* sets up rng offsets & sizes of rngs  */
    int i, j, ii, jj, off, roff, sadj, eadj, n;
    struct key *cr, *cl, *askrngpcs[512];
    int rcs[512], rce[512], srt[512];
    int ss, ee, s, e;
    memset(askrngpcs, 0, sizeof(askrngpcs));
    n = 0;
    cr = rngs[rng];
    while (cr) {
        j = cr->sym;
        /* get offset of this var in rec*/
        roff = sym[j].off + arroff(j, cr->el, cr->rg);
        /* set offset (bytes) in record */
        cr->rcoff = roff;
        sadj = offpad(roff, 2);          /* start's adjustment for hw align*/
        rcs[n] = roff;                   /* store starting offset */
        rce[n] = roff + keysize(cr) - 1; /* ending offset */
        askrngpcs[n] = cr;               /* assoc. rng pc with ask array   */
        n++;                             /* next rng piece                 */
        cr = cr->cmp;
    }
    for (i = 0; i < n; i++)
        srt[i] = i;
    compar(0, rcs); /* set up compare rtn for rcs compare */
    qsort(srt, n, 4,
          (int (*)(const void *,
                   const void *))compar); /* sort the array by start */
    off = 0;
    for (ii = 0; ii < n; ii++) {
        i = srt[ii];
        cl = askrngpcs[i]; /* left rng piece */
        if (!cl)
            continue;
        sadj = offpad(rcs[i], 2); /* get hw adjustment */
        cl->rboff = off + sadj;   /* set rbuf offset   */
        s = rcs[i] / 2;           /* get hw start      */
        e = rce[i] / 2;           /* get hw end        */
        for (jj = ii + 1; jj < n; jj++) {
            j = srt[jj];
            cr = askrngpcs[j]; /* right rng piece */
            if (!cr)
                continue;
            ss = rcs[j] / 2; /* get hw start of right rng pc */
            ee = rce[j] / 2; /* get hw end of right rng pc */
            if ((ss >= s && ss <= e + 1) ||
                (ee >= s - 1 && ee <= e)) { /* adjacent ranges */
                if (ee > e) {               /* adjust end position */
                    rce[i] = rce[j];
                    e = ee;
                }
                /* set this guy's rbuf offset */
                cr->rboff = cl->rboff + cr->rcoff - cl->rcoff;
                askrngpcs[j] = 0; /* delete him from the compress list */
            }
        }
        eadj = offpad(rce[i] + 1, 2); /* hw adjustment for end */
        off += ((rce[i] + eadj) - (rcs[i] - sadj)) + 1; /* new rbuf offset */
    }
    rngrrnoff[rng] = off; /* store where the rrn will be */
    jj = 2;
    for (ii = 0, ask[2] = 0; ii < n; ii++) { /* now set up ask array */
        i = srt[ii];
        if (!askrngpcs[i])
            continue;
        if (ask[2] == 39)
            csc2_error( "WARNING: ASK ARRAY EXCEEDS MAXIMUM SIZE\n");
        if (ask[2] >= 38)
            break;
        ask[2]++;
        ask[ask[2] * 2 + 1] = rcs[i] / 2 + 1;
        j = rce[i] / 2 - rcs[i] / 2 + 1;
        ask[ask[2] * 2 + 2] = j;
        jj += j;
    }
    if (jj > maxrngsz)
        maxrngsz = jj;
    return jj;
}

/*IN A UNION{} DECLARATION-ALL SYMBOLS ADDED AT SAME LOCATION*/
void start_union(char *name)
{
    char *dpth_info = NULL;
    unionflag = 1;
    if (name != NULL) {
        union_names[current_union] = name;
    } else {
        union_names[current_union] = NULL;
    }
    current_union++;
    if (union_index < (current_union - 1))
        union_index = current_union - 1;
    un_start[union_index] = tables[ntables].nsym;

    if (dpth_idx + 1 >= MAX_DEPTH) {
        csc2_error( "Error at line %3d: PARSE TREE DEPTH TOO BIG %s\n",
                current_line, name);
        csc2_syntax_error("Error at line %3d: PARSE TREE DEPTH TOO BIG %s",
                          current_line, name);
        any_errors++;
        return;
    }
    dpth_info = (char *)&cur_dpth[dpth_idx++];
    dpth_info[0] = 'u';
    dpth_info[1] = (u_char)union_index;
    union_level++;
}

void end_union()
{
    unionflag = 0;
    un_end[union_index] = tables[ntables].nsym - 1;
    union_index--;
    union_level--;
    dpth_idx--;
}

/*IN A RECTYPE{ CASE: } DECLARATION*/
void start_rectypedef(char *rtname)
{
    char *dpth_info = NULL;
    if (rtname != NULL) {
        union_names[current_union] = rtname;
    } else {
        union_names[current_union] = NULL;
    }
    current_union++;

    if (union_index < (current_union - 1))
        union_index = current_union - 1;
    un_start[union_index] = tables[ntables].nsym;

    if (dpth_idx + 1 >= MAX_DEPTH) {
        csc2_error( "Error at line %3d: PARSE TREE DEPTH TOO BIG %s\n",
                current_line, rtname);
        csc2_syntax_error("Error at line %3d: PARSE TREE DEPTH TOO BIG %s",
                          current_line, rtname);
        any_errors++;
        return;
    }
    dpth_info = (char *)&cur_dpth[dpth_idx++];
    dpth_info[0] = 'u';
    dpth_info[1] = (u_char)union_index;

    union_level++;
    if (ncases + 1 >= MAX_NESTED_RECTYPE || ncases < -1) {
        csc2_error( "MAXIMUM NESTED RECTYPE'S REACHED (16)");
        any_errors++;
        return;
    }
    nested_rectype[++ncases] = 0;
}

void end_rectypedef()
{
    un_end[union_index] = tables[ntables].nsym - 1;
    union_index--;
    union_level--;
    dpth_idx -= 2;
    current_case -= nested_rectype[ncases];
    if (ncases - 1 >= 0)
        nested_rectype[ncases - 1] += nested_rectype[ncases];
    nested_rectype[ncases--] = 0;
}

/* ADD CASE: TO RECTYPE */
void start_case(char *txt)
{
    int i;
    char *dpth_info = NULL;
    i = getexpr(txt);
    if (i == -1) {
        csc2_error( "Error at line %3d: CAN'T FIND TYPE"
                        " IN CASE STATEMENT: %s\n",
                current_line, txt);
        csc2_syntax_error("Error at line %3d: CAN'T FIND TYPE"
                          " IN CASE STATEMENT: %s",
                          current_line, txt);
        any_errors++;
        return;
    }
    /*	printf("CASE %s %d\n", txt, i);*/

    case_table[cn_p] = i; /* add to case name table */
    current_case = cn_p;
    nested_rectype[ncases]++;
    cn_p++;
    un_reset[tables[ntables].nsym]++; /* add to union reset table */

    if (dpth_idx + 1 >= MAX_DEPTH) {
        csc2_error( "Error at line %3d: PARSE TREE DEPTH TOO BIG %s\n",
                current_line, txt);
        csc2_syntax_error("Error at line %3d: PARSE TREE DEPTH TOO BIG %s",
                          current_line, txt);
        any_errors++;
        return;
    }
    if (dpth_idx - 1 >= 0) {
        dpth_info = (char *)&cur_dpth[dpth_idx - 1];
        if (dpth_info[0] == 'c') {
            dpth_info[0] = 'c';
            dpth_info[1] = (u_char)current_case;
        } else {
            dpth_info = (char *)&cur_dpth[dpth_idx++];
            dpth_info[0] = 'c';
            dpth_info[1] = (u_char)current_case;
        }
    } else {
        dpth_info = (char *)&cur_dpth[dpth_idx++];
        dpth_info[0] = 'c';
        dpth_info[1] = (u_char)current_case;
    }
}

void expr_clear() { ex_p = 0; }

void expr_add_pc(char *sym, int op, int num)
{
    int el[6], rg[2], arr, i;
    char arrstr[1024];
    if (process_array_(el, rg, 0, &arr, NULL)) {
        csc2_error( "Error at line %3d: BAD CONDITION ARRAY "
                        "SPECIFIER\n",
                current_line);
        csc2_syntax_error("Error at line %3d: BAD CONDITION ARRAY "
                          "SPECIFIER",
                          current_line);
        any_errors++;
        return;
    }

    if (ex_p >= EXPRMAX) {
        csc2_error( "Error at line %3d: OUT OF EXPRESSION SPACE",
                current_line);
        csc2_syntax_error("Error at line %3d: OUT OF EXPRESSION SPACE",
                          current_line);
        any_errors++;
        return;
    }
    if (sym)
        strlower(sym, strlen(sym));

    sprintf(arrstr, "");
    for (i = 0; i < 6 && el[i] != -1; i++)
        sprintf(eos(arrstr), "[%d]", el[i]);

    expr[ex_p].sym = sym;
    expr[ex_p].symarr = (char *)csc2_malloc(strlen(arrstr) + 1);
    strcpy(expr[ex_p].symarr, arrstr);
    expr[ex_p].opr = op;
    expr[ex_p].num = num;
    ex_p++;
}

void expr_assoc_name(char *name)
{
    if (et_p >= EXPRTABMAX) {
        csc2_error( "Error at line %3d: OUT OF EXPRESSION TABLE "
                        "SPACE",
                current_line);
        csc2_syntax_error("Error at line %3d: OUT OF EXPRESSION TABLE "
                          "SPACE",
                          current_line);
        any_errors++;
        return;
    }
    exprtab[et_p].name = name;
    exprtab[et_p].elen = ex_p;
    exprtab[et_p].expr =
        (struct expression *)csc2_malloc(sizeof(struct expression) * ex_p);
    if (exprtab[et_p].expr == 0) {
        logmsgperror("expr_assoc_name(): saving expression");
        any_errors++;
        return;
    }
    memcpy(exprtab[et_p].expr, expr, sizeof(struct expression) * ex_p);
    et_p++;
}

void resolve_case_names()
{
    int i, j, k, tidx = 0;
    for (i = 0; i < et_p; i++) {
        for (j = 0; j < exprtab[i].elen; j++) {
            if (exprtab[i].expr[j].sym) {
                exprtab[i].expr[j].symnum =
                    getsymbol(ONDISKTAG, exprtab[i].expr[j].sym, &tidx);
                if (exprtab[i].expr[j].symnum == -1) {
                    exprtab[i].expr[j].symnum =
                        getsymbol((ntables > 1) ? ONDISKTAG : DEFAULTTAG,
                                  exprtab[i].expr[j].sym, &tidx);
                }
                if (exprtab[i].expr[j].symnum == -1) {
                    csc2_error("ERROR: Unresolved variable: '%s"
                               "' in type '%s'\n",
                               exprtab[i].expr[j].sym, exprtab[i].name);
                    any_errors++;
                }
                for (k = exprtab[i].expr[j].symnum + 1; k < nsym; k++) {
                    if (strcmp(exprtab[i].expr[j].sym,
                               tables[tidx].sym[k].nm) == 0) {
                        csc2_error("ERROR: Condition %s Symbol Name is Found "
                                   "More Than Once! '%s'\n",
                                   exprtab[i].name, exprtab[i].expr[j].sym);
                        any_errors++;
                    }
                }
            } else {
                exprtab[i].expr[j].symnum = -1;
            }
        }
    }
}

static void onescompl(unsigned char *cc, int len)
{
    int ii;
    for (ii = 0; ii < len; ii++, cc++)
        *cc = (0xff - *cc);
}

/**********************************************************************/

/*    THIS IS THE API USED IN COMDB2 TO EXTRACT INFO ABOUT THIS DATABASE */
char *dyns_field_option_text(int option)
{
    switch (option) {
    case FLDOPT_DBSTORE:
        return "dbstore";
    case FLDOPT_DBLOAD:
        return "dbload";
    case FLDOPT_NULL:
        return "null";
    default:
        return "unknown";
    }
}

static char fullname[256];

static char *ischematext;
static int ipos;
static int iusestr = 0;

int macc_isatty(int fd)
{
    return 1;
}


int macc_getc(FILE *fh)
{
    extern FILE *yyin; /* lexer's input file */
    int ch;
    int numread;
    unsigned char byte;

    if (fh != yyin) {
        csc2_error( "Someone called macc_getc but not with yyin!\n");
        return EOF;
    }


    if (iusestr) {
        ch = ischematext[ipos];
        if (ch == 0) {
            ch = EOF;
        } else {
            ipos++;
        }
    } else {
        int fd = fileno(fh);

        numread = read(fd, &byte, 1);
        ch = byte;
        if (numread != 1)
            ch = EOF;

        /*ch = getc(fh); */
    }

    return ch;
}

int macc_fread(void *ptr, size_t size, size_t nmemb, FILE *stream) {
    int bytes = 0;
    char *s = (char*) ptr;

    for (int i = 0; i < size * nmemb; i++, s++, bytes++) {
        int ch = macc_getc(stream);
        if (ch == EOF)
            break;
        *s = ch;
    }
    return bytes;
}

int macc_ferror(FILE *fh)
{
    extern FILE *yyin; /* lexer's input file */
    int ch;
    int numread;
    unsigned char byte;

    if (fh != yyin) {
        csc2_error( "Someone called macc_ferror but not with yyin!\n");
        return 1;
    }

    return 0;
}

static int dyns_load_schema_int(char *filename, char *schematxt, char *dbname,
                                char *tablename)
{
    char buf[256], *ifn = NULL;
    int i = 0, lastix = 0;
    int flag_winout = 0; /* true? then parse for winmacc */
    int fhopen = 0;
    FILE *fil;         /* my output file               */
    extern FILE *yyin; /* lexer's input file           */
    /*extern*/ int yy_flex_debug;
    int only_source = 0;
    void winmacc_output();

    yy_flex_debug = 0;
    strcpy(VER, revision + 10); /* get my version               */
    ifn = strchr(VER, '$');     /* clean up version text        */
    if (ifn)
        *ifn = 0;

    init_globals();

    flag_anyname = 1;
    if (strlen(dbname) >= MAX_DBNAME_LENGTH || strlen(dbname) < 3) {
        csc2_error("ERROR: BAD DATABASE NAME '%s'. VALID=3-%d CHARACTERS\n",
                   dbname, MAX_DBNAME_LENGTH - 1);
        return -1;
    }
    sprintf(fullname, "%s_%s", dbname, tablename);
    opt_dbname = fullname;
    opt_copycsc = 0;
    strncpy(opt_maindbname, dbname, sizeof(opt_maindbname));
    strncpy(opt_tblname, tablename, sizeof(opt_tblname));

    /* check args for an input filename or any options */
    if (schematxt) {
        ifn = dbname;
        ischematext = schematxt;
        ipos = 0;
        iusestr = 1;
    } else if (filename) {
        ifn = filename;
        if (ifn) {
            yyin = fopen(ifn, "r");
            if (yyin == NULL) {
                csc2_error( "Can't open file '%s': %s\n", ifn,
                        strerror(errno));
                return -1;
            }
            fhopen = 1;
        }
        iusestr = 0;
    } else {
        csc2_error( "BAD CALL\n");
        return -1;
    }

    if (yyparse() || any_errors || check_options()) {
        csc2_error("FOUND ERRORS IN SCHEMA. ABORTING!\n");
        if (fhopen)
            fclose((FILE *)yyin);
        return -1;
    }
    if (fhopen)
        fclose((FILE *)yyin); /* re-open the .csc file to copy into include */

    if (compute_key_data() < 0) {
        csc2_error("FOUND ERRORS IN PROCESSING DATA. ABORTING!\n");
        return -1;
    }

    return 0;
}

int dyns_load_schema_string(char *schematxt, char *dbname, char *tablename)
{
    return dyns_load_schema_int(NULL, schematxt, dbname, tablename);
}

int dyns_load_schema(char *filename, char *dbname, char *tablename)
{
    return dyns_load_schema_int(filename, NULL, dbname, tablename);
}

/* form key based on record buffer */
/* WARNING: THIS ROUTINE DOES NOT SUPPORT CONDITIONAL KEYS/ARRAYS, CURRENTLY */
int dyns_form_key(int index, char *record, int recsz, char *key, int keysize)
{
    int npieces = 0, rc = 0, ksz = 0, i = 0, keyofft = 0;
    char pname[128];
    int type = 0, recofft = 0, plen = 0, descend = 0;
    char *expr;
    if (index < 0 || index >= numix()) {
        return -1;
    }
    if (numix() != numkeys()) {
        /* conditional keys...punt */
        return -2;
    }
    ksz = dyns_get_idx_size(index);
    if (ksz <= 0)
        return -3;
    if (ksz > keysize)
        return -4;
    npieces = dyns_get_idx_piece_count(index);
    if (npieces <= 0)
        return -5;
    for (i = 0; i < npieces; i++) {
        expr = NULL;
        rc = dyns_get_idx_piece(index, i, pname, sizeof(pname), &type, &recofft,
                                &plen, &descend, &expr);
        if (rc <= 0)
            return -6;
        if (recofft < 0 || recofft >= recsz) {
            return -7;
        }
        if (plen <= 0 || plen + recofft > recsz) {
            return -8;
        }
        if ((keyofft + plen) > ksz)
            return -9;
        memcpy(&key[keyofft], &record[recofft], plen);
        if (descend) {
            onescompl((unsigned char *)&key[keyofft], plen);
        }
        keyofft += plen;
    }
    if (keyofft != ksz)
        return -10;
    return 0;
}

/* does key have the specified flags? */
static int dyns_is_idx_flagged(int index, int flags)
{
    int lastix = 0, i = 0;
    if (index < 0 || index >= numix()) {
        return -1;
    }
    for (lastix = -1, i = 0; i < numkeys(); i++) {
        if (lastix == keyixnum[i])
            continue;
        lastix = keyixnum[i];
        if (keyixnum[i] != index)
            continue;
        if (ixflags[keyixnum[i]] & flags)
            return 1;
        break;
    }
    return 0;
}

/* is key duplicate? */
int dyns_is_idx_dup(int index)
{
    return dyns_is_idx_flagged(index, DUPKEY);
}

/* is key copy-data key? */
int dyns_is_idx_datacopy(int index)
{
    return dyns_is_idx_flagged(index, DATAKEY);
}

/* is key duplicate? */
int dyns_is_idx_primary(int index)
{
    return dyns_is_idx_flagged(index, PRIMARY);
}

/* does this index have recnums? */
int dyns_is_idx_recnum(int index)
{
    return dyns_is_idx_flagged(index, RECNUMS);
}

/* does this index treat all NULL values are UNIQUE? */
int dyns_is_idx_uniqnulls(int index)
{
    return dyns_is_idx_flagged(index, UNIQNULLS);
}

int dyns_get_idx_tag(int index, char *tag, int tlen, char **where)
{
    int lastix = 0, i = 0;
    if (index < 0 || index >= numix()) {
        return -1;
    }
    for (lastix = -1, i = 0; i < numkeys(); i++) {
        if (lastix == keyixnum[i])
            continue;
        lastix = keyixnum[i];
        if (keyixnum[i] != index)
            continue;
        strncpy(tag, keys[i]->keytag, MIN(tlen, sizeof(keys[i]->keytag)));
        *where = keys[i]->where;
        return 0;
    }
    return -1;
}
/* number of keys in this database (schema) */
int dyns_get_idx_count(void) { return numix(); }

/* key size for a specific index */
int dyns_get_idx_size(int index)
{
    int lastix = 0, i = 0;
    if (index < 0 || index >= numix()) {
        return -1;
    }
    for (lastix = -1, i = 0; i < numkeys(); i++) {
        if (lastix == keyixnum[i])
            continue;
        lastix = keyixnum[i];
        if (keyixnum[i] != index)
            continue;
        return ixsize[keyixnum[i]];
    }
    return -1;
}

/* get a specific key piece along with its information */
/* WARNING: THIS ROUTINE DOES NOT SUPPORT CONDITIONAL KEYS/ARRAYS, CURRENTLY */
int dyns_get_idx_piece(int index, int piece, char *sname, int slen, int *type,
                       int *offset, int *plen, int *descend, char **pexpr)
{
    int lastix = 0, keynum = 0, rc = 0;
    struct key *ck = NULL;
    int pcnt = 0;

    if (index < 0 || index >= numix()) {
        return -1;
    }
    if (numix() != numkeys()) {
        /* conditional keys...punt */
        return -1;
    }

    for (lastix = -1, keynum = 0; keynum < numkeys(); keynum++) {
        if (lastix == keyixnum[keynum])
            continue;
        lastix = keyixnum[keynum];
        if (keyixnum[keynum] != index)
            continue;

        ck = keys[keynum];
        while (ck) {
            if (pcnt == piece) {
                int rofft = 0, esz = 0, fsz = 0, arr = 0;
                if (ck->expr) {
                    *pexpr = ck->expr;
                    switch (ck->exprtype) {
                    case 0:
                        *type = 0;
                        *plen = 0;
                        break;
                    case T_UINTEGER2:
                        *type = SERVER_UINT;
                        *plen = 2;
                        break;
                    case T_INTEGER2:
                        *type = SERVER_BINT;
                        *plen = 2;
                        break;
                    case T_ULONG:
                    case T_UINTEGER4:
                        *type = SERVER_UINT;
                        *plen = 4;
                        break;
                    case T_INTEGER4:
                    case T_LOGICAL:
                        *type = SERVER_BINT;
                        *plen = 4;
                        break;
                    case T_REAL4:
                        *type = SERVER_BREAL;
                        *plen = 4;
                        break;
                    case T_ULONGLONG:
                        *type = SERVER_UINT;
                        *plen = 8;
                        break;
                    case T_LONGLONG:
                        *type = SERVER_BINT;
                        *plen = 8;
                        break;
                    case T_REAL8:
                        *type = SERVER_BREAL;
                        *plen = 8;
                        break;
                    case T_PSTR:
                    case T_CSTR:
                        *type = SERVER_BCSTR;
                        *plen = 1 * ck->exprarraysz;
                        break;
                    case T_UCHAR:
                        *type = SERVER_BYTEARRAY;
                        *plen = 1 * ck->exprarraysz;
                        break;
                    case T_DATETIME:
                        *type = SERVER_DATETIME;
                        *plen = 76;
                        break;
                    case T_DATETIMEUS:
                        *type = SERVER_DATETIMEUS;
                        *plen = 76;
                        break;
                    case T_INTERVALYM:
                        *type = SERVER_INTVYM;
                        *plen =
                            3 * sizeof(int); /*this is HARDCODED to the size of
                                                cdb2_client_intv_ym_t*/
                        break;
                    case T_INTERVALDS:
                        *type = SERVER_INTVDS;
                        *plen =
                            6 * sizeof(int); /*this is HARDCODED to the size of
                                                cdb2_client_intv_ds_t*/
                        break;
                    case T_INTERVALDSUS:
                        *type = SERVER_INTVDSUS;
                        *plen =
                            6 * sizeof(int); /*this is HARDCODED to the size of
                                                cdb2_client_intv_ds_t*/
                        break;
                    case T_DECIMAL32:
                        *type = SERVER_DECIMAL;
                        *plen = 1 /*sign*/ + 1 /*dot*/ + 7 /*coefdigits*/ +
                                1 /*E*/ + 1 /*expsign*/ + 2 /*exp*/ +
                                1 /*zero*/; /* 14bytes */
                        break;
                    case T_DECIMAL64:
                        *type = SERVER_DECIMAL;
                        *plen = 1 /*sign*/ + 1 /*dot*/ + 16 /*coefdigits*/ +
                                1 /*E*/ + 1 /*expsign*/ + 3 /*exp*/ +
                                1 /*zero*/; /* 24 bytes */
                        break;
                    case T_DECIMAL128:
                        *type = SERVER_DECIMAL;
                        *plen = 1 /*sign*/ + 1 /*dot*/ + 34 /*coefdigits*/ +
                                1 /*E*/ + 1 /*expsign*/ + 4 /*exp*/ +
                                1 /*zero*/; /* 43 bytes */
                        break;
                    default:
                        /*huh?*/
                        *type = 0;
                        *plen = 0;
                        csc2_error(
                            "%s: ignored unknown index expression type %d\n",
                            __func__, ck->exprtype);
                        break;
                    }
                    *descend = 0;
                    if (ck->keyflags & DESCEND) {
                        *descend = 1;
                    }
                    return 0;
                }
                rc = dyns_get_table_field_info(dyns_get_table_tag(ck->stbl),
                                               ck->sym, sname, slen, type,
                                               &rofft, &esz, &fsz, &arr, 0);
                if (rc != 0)
                    return -1;
                if (*type == CLIENT_CSTR || *type == CLIENT_PSTR) {
                    if (ck->rg[0] != 0)
                        *offset = rofft + (ck->rg[0] - 1);
                    else
                        *offset = rofft;
                    if (ck->rg[1] != 0) {
                        *plen = (ck->rg[1] - ck->rg[0] + 1);
                    } else
                        *plen = fsz;
                } else {
                    *offset = rofft;
                    *plen = fsz;
                }
                *descend = 0;
                if (ck->keyflags & DESCEND) {
                    *descend = 1;
                }
                return 0;
            }
            pcnt++;
            ck = ck->cmp;
        }
        return pcnt;
    }

    return -1;
}

/* number of pieces the specific index is made of */
/* WARNING: THIS ROUTINE DOES NOT SUPPORT CONDITIONAL KEYS, CURRENTLY */
int dyns_get_idx_piece_count(int index)
{
    int lastix = 0, keynum = 0;
    struct key *ck = NULL;
    int pcnt = 0;

    if (index < 0 || index >= numix()) {
        return -1;
    }
    if (numix() != numkeys()) {
        /* conditional keys...punt */
        return -1;
    }
    for (lastix = -1, keynum = 0; keynum < numkeys(); keynum++) {
        if (lastix == keyixnum[keynum])
            continue;
        lastix = keyixnum[keynum];
        if (keyixnum[keynum] != index)
            continue;

        ck = keys[keynum];
        while (ck) {
            pcnt++;
            ck = ck->cmp;
        }
        return pcnt;
    }

    return 0;
}

/* database number of this schema */
int dyns_get_db_num(void) { return opt_dbnum; }

/* data directory of this schema */
int dyns_get_dtadir(char *dir, int len)
{
    if (len <= strlen(opt_dtadir)) {
        return -1;
    }
    bzero(dir, len);
    strncpy(dir, opt_dtadir, strlen(opt_dtadir));
    return 0;
}

/* database name */
int dyns_get_db_name(char *name, int len)
{
    if (len <= strlen(DBNAME)) {
        return -1;
    }
    bzero(name, len);
    strncpy(name, DBNAME, strlen(DBNAME));
    return 0;
}

/* record size */
int dyns_get_db_table_size(void)
{
    int tidx = gettable(DEFAULTTAG);
    if (tidx < 0)
        return -1;
    return tables[tidx].table_size;
}

/* number of fields in the record */
int dyns_get_field_count(void)
{
    int tidx = gettable(DEFAULTTAG);
    if (tidx < 0)
        return -1;
    return tables[tidx].nsym;
}

int dyns_is_field_array(int fidx)
{
    return dyns_is_field_array_comn(NULL, fidx);
}

int dyns_is_table_field_array(char *tabletag, int fidx)
{
    return dyns_is_field_array_comn(tabletag, fidx);
}

static int dyns_is_field_array_comn(char *tag, int fidx)
{
    int tidx = gettable(tag == NULL ? DEFAULTTAG : tag);
    if (tidx < 0)
        return -1;
    if (fidx < 0 || fidx >= tables[tidx].nsym)
        return -1;
    return (numdim(tables[tidx].sym[fidx].dim) > 0);
}

int dyns_get_field_arr_dims(int fidx, int *dims, int ndims, int *nodims)
{
    return dyns_get_field_arr_dims_comn(NULL, fidx, dims, ndims, nodims);
}

int dyns_get_table_field_arr_dims(char *tabletag, int fidx, int *dims,
                                  int ndims, int *nodims)
{
    return dyns_get_field_arr_dims_comn(tabletag, fidx, dims, ndims, nodims);
}

int dyns_get_field_arr_dims_comn(char *tag, int fidx, int *dims, int ndims,
                                 int *nodims)
{
    int i = 0;
    int tidx = gettable(tag == NULL ? DEFAULTTAG : tag);
    if (tidx < 0)
        return -1;

    if (dyns_is_field_array(fidx) < 0)
        return -1;
    *nodims = 0;
    for (i = 0;
         tables[tidx].sym[fidx].dim[i] != -1 && i < ((6 > ndims) ? ndims : 6);
         i++) {
        dims[i] = tables[tidx].sym[fidx].dim[i];
        *nodims = *nodims + 1;
    }
    return 0;
}

char *dyns_get_table_tag(int tidx)
{
    if (tidx < 0 || tidx >= ntables)
        return "";
    return tables[tidx].table_tag;
}
/* get specific record field with its info. */
int dyns_get_field_info(int fidx, char *name, int namelen, int *type,
                        int *offset, int *elsize, int *fullsize)
{
    return dyns_get_field_info_comn(NULL, fidx, name, namelen, type, offset,
                                    elsize, fullsize, NULL, 0);
}

int dyns_get_table_field_info(char *tabletag, int fidx, char *name, int namelen,
                              int *type, int *offset, int *elsize,
                              int *fullsize, int *arr, int use_server_types)
{
    return dyns_get_field_info_comn(tabletag, fidx, name, namelen, type, offset,
                                    elsize, fullsize, arr, use_server_types);
}

static int dyns_get_field_info_comn(char *tag, int fidx, char *name,
                                    int namelen, int *type, int *offset,
                                    int *elsize, int *fullsize, int *arr,
                                    int use_server_types)
{
    int tidx = gettable(tag == NULL ? DEFAULTTAG : tag);
    if (tidx < 0)
        return -1;
    if (fidx < 0 || fidx >= tables[tidx].nsym)
        return -1;
    if (type != NULL) {
        *type = field_type(tables[tidx].sym[fidx].type, use_server_types);
    }
    if (name != NULL) {
        bzero(name, namelen);
        strncpy(name, tables[tidx].sym[fidx].nm, namelen);
    }
    if (offset != NULL) {
        *offset = tables[tidx].sym[fidx].off;
    }
    if (elsize != NULL) {
        *elsize = tables[tidx].sym[fidx].size;
    }
    if (fullsize != NULL) {
        *fullsize = tables[tidx].sym[fidx].szof;
    }
    if (arr != NULL) {
        *arr = (numdim(tables[tidx].sym[fidx].dim) > 0);
    }
    return 0;
}

/* load field options */

int dyns_get_field_option(int fidx, int option, int *value_type, int *value_sz,
                          void *valuebuf, int vbsz)
{
    return dyns_get_field_option_comn(NULL, fidx, option, value_type, value_sz,
                                      valuebuf, vbsz);
}

int dyns_get_table_field_option(char *tag, int fidx, int option,
                                int *value_type, int *value_sz, void *valuebuf,
                                int vbsz)
{
    return dyns_get_field_option_comn(tag, fidx, option, value_type, value_sz,
                                      valuebuf, vbsz);
}

static int dyns_get_field_option_comn(char *tag, int fidx, int option,
                                      int *value_type, int *value_sz,
                                      void *valuebuf, int vbsz)
{
    int i = 0, length = 0;
    /* int tidx=gettable(tag==NULL?DEFAULTTAG:tag);*/
    int tidx = gettable(ONDISKTAG);
    if (strcmp(tag, ONDISKTAG))
        return -1;
    if (tidx < 0)
        return -1;
    if (fidx < 0 || fidx >= tables[tidx].nsym)
        return -1;
#if 1
    *value_type = field_type(tables[tidx].sym[fidx].type, 0);
    for (i = 0; i < tables[tidx].sym[fidx].numfo; i++) {
        if (tables[tidx].sym[fidx].fopts[i].opttype == option) {
            if ((option == FLDOPT_NULL || option == FLDOPT_PADDING) &&
                vbsz >= sizeof(int)) {
                int tmpval = tables[tidx].sym[fidx].fopts[i].value.i4val;
                *value_type = CLIENT_INT;
                *value_sz = sizeof(int);
                memcpy(valuebuf, &tmpval, sizeof(int));
                return 0;
            }

            switch (tables[tidx].sym[fidx].fopts[i].valtype) {
            case CLIENT_INT: {
                if ((*value_type == CLIENT_INT || *value_type == CLIENT_UINT) &&
                    vbsz >= sizeof(uint64_t)) {
                    extern int gbl_broken_num_parser;
                    if (gbl_broken_num_parser) {
                        int tmpval =
                            htonl(tables[tidx].sym[fidx].fopts[i].value.i4val);
                        memcpy(valuebuf, &tmpval, sizeof(tmpval));
                        *value_sz = sizeof(tmpval);
                    } else {
                        uint64_t tmpval = flibc_htonll(
                            tables[tidx].sym[fidx].fopts[i].value.u8val);
                        memcpy(valuebuf, &tmpval, sizeof(tmpval));
                        *value_sz = sizeof(tmpval);
                    }
                    return 0;
                } else if (*value_type == CLIENT_REAL &&
                           vbsz >= sizeof(double)) {
                    double tmpval = flibc_htond((
                        (double)(tables[tidx].sym[fidx].fopts[i].value.i4val)));
                    memcpy(valuebuf, &tmpval, sizeof(double));
                    *value_sz = sizeof(double);
                    return 0;
                } else if (*value_type == CLIENT_BYTEARRAY &&
                           vbsz >= tables[tidx].sym[fidx].szof) {
                    /* construct a byte array memset with this value */
                    memset(valuebuf,
                           tables[tidx].sym[fidx].fopts[i].value.i4val,
                           tables[tidx].sym[fidx].szof);
                    *value_sz = tables[tidx].sym[fidx].szof;
                    return 0;
                }
                return -1;
            }
            case CLIENT_REAL: {
                if (*value_type == CLIENT_REAL && vbsz >= sizeof(double)) {
                    double tmpval = flibc_htond(
                        (double)(tables[tidx].sym[fidx].fopts[i].value.r8val));
                    memcpy(valuebuf, &tmpval, sizeof(double));
                    *value_sz = sizeof(double);
                    return 0;
                }
                return -1;
            }
            case CLIENT_BYTEARRAY: {
                int *bytes;
                int length;
                bytes = (int *)tables[tidx].sym[fidx].fopts[i].value.byteval;
                if (!bytes) {
                    csc2_error(
                            "dyns_get_field_option_comn: null byteval\n");
                    return -1;
                }
                length = *bytes;
                bytes++;
                if (*value_type == CLIENT_BYTEARRAY && vbsz >= length) {
                    memcpy(valuebuf, bytes, length);
                    *value_sz = length;
                    return 0;
                }
                return -1;
            }
            case CLIENT_CSTR: {
                if ((*value_type == CLIENT_CSTR || *value_type == CLIENT_PSTR ||
                     *value_type == CLIENT_VUTF8) &&
                    vbsz >
                        strlen(tables[tidx].sym[fidx].fopts[i].value.strval)) {
                    bzero(valuebuf,
                          strlen(tables[tidx].sym[fidx].fopts[i].value.strval) +
                              1);
                    memcpy(
                        valuebuf, tables[tidx].sym[fidx].fopts[i].value.strval,
                        strlen(tables[tidx].sym[fidx].fopts[i].value.strval));
                    *value_sz =
                        strlen(tables[tidx].sym[fidx].fopts[i].value.strval);
                    return 0;
                } else if (*value_type == CLIENT_DATETIME ||
                           *value_type == CLIENT_DATETIMEUS) {
                    memcpy(
                        valuebuf, tables[tidx].sym[fidx].fopts[i].value.strval,
                        strlen(tables[tidx].sym[fidx].fopts[i].value.strval));
                    *value_sz =
                        strlen(tables[tidx].sym[fidx].fopts[i].value.strval);
                    return 0;
                } else if (*value_type == CLIENT_BYTEARRAY &&
                           vbsz >= tables[tidx].sym[fidx].szof) {
                    /* There are several production databases that try to
                     * specify a default load/store for a byte array using a
                     * string.  Previously we didn't catch this, so people put
                     * in all kinds of wacky interpretations of how this
                     * could work.  We want to disallow all of them, but can't
                     * without breaking them.  So we silently ignore strings. */
                    *value_type = CLIENT_MINTYPE;
                    *value_sz = 0;
                    memset(valuebuf, 0, vbsz);
                    return -1;
                }
                return -1;
            }
            default:
                return -1;
            }
        }
    }

#endif
    *value_type = CLIENT_MINTYPE;
    *value_sz = 0;
    memset(valuebuf, 0, vbsz);
    return -1;
}

/* field depth */

int dyns_field_depth(int fidx, dpth_t *dpthinfo, int ndpthsinfo, int *ndpthout)
{
    return dyns_field_depth_comn(NULL, fidx, dpthinfo, ndpthsinfo, ndpthout);
}

int dyns_table_field_depth(char *tabletag, int fidx, dpth_t *dpthinfo,
                           int ndpthsinfo, int *ndpthout)
{
    return dyns_field_depth_comn(tabletag, fidx, dpthinfo, ndpthsinfo,
                                 ndpthout);
}

static int dyns_field_depth_comn(char *tag, int fidx, dpth_t *dpthinfo,
                                 int ndpthsinfo, int *ndpthout)
{
    int i = 0;
    int tidx = gettable(tag == NULL ? DEFAULTTAG : tag);
    if (tidx < 0)
        return -1;
    *ndpthout = 0;
    if (fidx < 0 || fidx >= tables[tidx].nsym)
        return -1;
    for (i = 0; i < tables[tidx].sym[fidx].dpth; i++) {
        char *curdpth = NULL;
        if (i >= ndpthsinfo) {
            return 1;
        }
        curdpth = (char *)(&(tables[tidx].sym[fidx].dpth_tree[i]));
        memset(&dpthinfo[*ndpthout], 0, sizeof(dpth_t));
        dpthinfo[*ndpthout].struct_type =
            ((curdpth[0] == 'u') ? FLDDPTH_UNION : FLDDPTH_STRUCT);
        dpthinfo[*ndpthout].struct_number = (int)(curdpth[1]);
        *ndpthout = *ndpthout + 1;
    }
    return 0;
}

/* simple field type */
int dyns_field_type(int fidx)
{
    int tidx = gettable(DEFAULTTAG);
    if (tidx < 0)
        return -1;
    if (fidx < 0 || fidx >= tables[tidx].nsym)
        return -1;
    switch (tables[tidx].sym[fidx].type) {
    case T_UINTEGER2:
        return COMDB2_USHORT;
    case T_UINTEGER4:
        return COMDB2_UINT;
    case T_INTEGER2:
        return COMDB2_SHORT;
    case T_INTEGER4:
        return COMDB2_INT;
    case T_LOGICAL:
        return COMDB2_INT;
    /*    case T_LONG: return COMDB2_LONG;
          case T_ULONG: return COMDB2_ULONG;*/
    case T_ULONGLONG:
        return COMDB2_ULONGLONG;
    case T_LONGLONG:
        return COMDB2_LONGLONG;
    case T_UCHAR:
        return COMDB2_BYTE;
    case T_PSTR:
        return COMDB2_PSTR;
    case T_CSTR:
        return COMDB2_CSTR;
    case T_REAL4:
        return COMDB2_FLOAT;
    case T_REAL8:
        return COMDB2_DOUBLE;
    case T_BLOB:
        return COMDB2_BLOB;
    case T_BLOB2:
        return COMDB2_VUTF8;
    case T_VUTF8:
        return COMDB2_VUTF8;
    case T_DATETIME:
        return COMDB2_DATETIME;
    case T_DATETIMEUS:
        return COMDB2_DATETIMEUS;
    case T_INTERVALYM:
        return COMDB2_INTERVALYM;
    case T_INTERVALDS:
        return COMDB2_INTERVALDS;
    case T_INTERVALDSUS:
        return COMDB2_INTERVALDSUS;
    default:
        return -1;
    }
}

int dyns_get_table_count(void) { return ntables; }

int dyns_get_table_tag_size(char *tabletag)
{
    int i = 0;
    for (i = 0; i < ntables; i++) {
        if (!strncmp(tabletag, tables[i].table_tag,
                     sizeof(tables[i].table_tag))) {
            return tables[i].table_size;
        }
    }
    return -1;
}

int dyns_get_table_field_count(char *tabletag)
{
    int i = 0;

    for (i = 0; i < ntables; i++) {
        if (!strncmp(tabletag, tables[i].table_tag,
                     sizeof(tables[i].table_tag))) {
            return tables[i].nsym;
        }
    }
    return -1;
}

int dyns_get_constraint_count(void)
{
    if (nconstraints < 0)
        return 0;
    return nconstraints;
}

int dyns_get_constraint_at(int idx, char **consname, char **keyname,
                           int *rulecnt, int *flags)
{
    if (idx < 0 || idx >= nconstraints)
        return -1;
    *consname = constraints[idx].consname;
    *keyname = constraints[idx].lclkey;
    *rulecnt = constraints[idx].ncnstrts;
    *flags = constraints[idx].flags;
    return 0;
}

int dyns_get_constraint_rule(int cidx, int ridx, char **tblname, char **keynm)
{
    int rcnt = 0;
    if (cidx < 0 || cidx >= nconstraints)
        return -1;
    rcnt = constraints[cidx].ncnstrts;
    if (ridx < 0 || ridx >= rcnt)
        return -1;
    *tblname = constraints[cidx].table[ridx];
    *keynm = constraints[cidx].keynm[ridx];
    return 0;
}

static pthread_once_t once = PTHREAD_ONCE_INIT;
struct csc2_mem_block {
    int sz;
    LINKC_T(struct csc2_mem_block) lnk;
    double mem[1];
};

static LISTC_T(struct csc2_mem_block) csc2_allocated_blocks;

static void init_csc2_malloc(void)
{
    listc_init(&csc2_allocated_blocks, offsetof(struct csc2_mem_block, lnk));
}

void *csc2_malloc(size_t sz)
{
    struct csc2_mem_block *blk;

    pthread_once(&once, init_csc2_malloc);

    blk = malloc(offsetof(struct csc2_mem_block, mem) + sz);
    blk->sz = sz;
    listc_abl(&csc2_allocated_blocks, blk);
    return &blk->mem[0];
}

char *csc2_strdup(char *s)
{
    struct csc2_mem_block *blk;
    int len;

    pthread_once(&once, init_csc2_malloc);

    len = strlen(s);
    blk = malloc(offsetof(struct csc2_mem_block, mem) + len + 1);
    blk->sz = len;
    strcpy((char *)&blk->mem[0], s);
    listc_abl(&csc2_allocated_blocks, blk);
    return (char *)&blk->mem[0];
}

void csc2_free_all(void)
{
    struct csc2_mem_block *blk;
    blk = listc_rtl(&csc2_allocated_blocks);
    while (blk) {
        free(blk);
        blk = listc_rtl(&csc2_allocated_blocks);
    }
    if (errors) {
        strbuf_free(errors);
        errors = NULL;
    }
    if (syntax_errors) {
        strbuf_free(syntax_errors);
        syntax_errors = NULL;
    }
}

char *csc2_get_errors(void)
{
    if (errors == NULL)
        return NULL;
    return (char *)strbuf_buf(errors);
}

char *csc2_get_syntax_errors(void)
{
    if (syntax_errors == NULL)
        return NULL;
    return (char *)strbuf_buf(syntax_errors);
}

void csc2_error(const char *fmt, ...)
{
    char s[1];
    va_list args;
    int len;
    char *out;

    if (errors == NULL) {
        errors = strbuf_new();
        if (errors == NULL)
            return;
    }

    va_start(args, fmt);
    len = vsnprintf(s, 1, fmt, args);
    va_end(args);
    if (len <= 0) {
        return;
    }
    len++;
    out = malloc(len);
    va_start(args, fmt);
    vsnprintf(out, len, fmt, args);
    va_end(args);
    strbuf_append(errors, out);
    logmsg(LOGMSG_ERROR, "%s", out);
    free(out);
}

void csc2_syntax_error(const char *fmt, ...)
{
    char s[1];
    va_list args;
    int len;
    char *out;

    if (syntax_errors == NULL) {
        syntax_errors = strbuf_new();
        if (syntax_errors == NULL)
            return;
    }

    va_start(args, fmt);
    len = vsnprintf(s, 1, fmt, args);
    va_end(args);
    if (len <= 0) {
        return;
    }
    len++;
    out = malloc(len);
    va_start(args, fmt);
    vsnprintf(out, len, fmt, args);
    va_end(args);
    strbuf_append(syntax_errors, out);
    free(out);
}
