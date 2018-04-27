/*
   Copyright 2015, 2018 Bloomberg Finance L.P.

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

#include <strings.h>
#include "sql.h"
#include "sqlexplain.h"

/*
  enhancements to sqlites built in explain facility.
  this provides english like explain output as opposed to
  assembly like output.

  to check difference between opcodes that are in sqlite/vdbe.c and sqlexplain.c
  can use cmd below on each file and compare outputs:
    grep OP_ <file> | grep case | cut -d":" -f1 | awk '{print $2}' | sort -u

 */

#include <ctype.h>
#include <netinet/in.h>

#include <sbuf2.h>
#include "sqliteInt.h"
#include "os.h"
#include "vdbeInt.h"
#include "comdb2.h"
#include "sqlite3.h"
#include "views.h"
#include "logmsg.h"

char hex[] = {'0', '1', '2', '3', '4', '5', '6', '7',
              '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'};

int all_opcodes = 0;

static char *entity_type(struct cursor_info *cinfo)
{
    if (cinfo->ix == -1)
        return "table";
    else
        return "index";
}

static char opcode_to_sign(int opcode)
{
    switch (opcode) {
    case OP_Add:
        return '+';
    case OP_Subtract:
        return '-';
    case OP_Multiply:
        return '*';
    case OP_Divide:
        return '/';
    case OP_Remainder:
        return '%';
    default:
        return '?';
    }
}

static void print_field(Vdbe *v, struct cursor_info *cinfo, int num, char *buf)
{
    if (cinfo->remote) {
        sprintf(buf, "%s", fdb_sqlexplain_get_field_name(v, cinfo->rootpage,
                                                         cinfo->ix, num));
        return;
    }

    if (!cinfo->istemp && cinfo->rootpage == 0) {
        sprintf(buf, "field%d", num);
        return;
    }

    if (cinfo->rootpage == 1) {
        const char *indexname = NULL;
        switch (num) {
        case 0:
            indexname = "type";
            break;
        case 1:
            indexname = "name";
            break;
        case 2:
            indexname = "tbl_name";
            break;
        case 3:
            indexname = "rootpage";
            break;
        case 4:
            indexname = "sql";
            break;
        case 5:
            indexname = "csc2";
            break;
        default:
            indexname = "???";
            break;
        }
        sprintf(buf, "\"%s\"", indexname);
        return;
    } 
    
    struct dbtable *db = NULL;
    if (cinfo->tbl < thedb->num_dbs) {
        db = thedb->dbs[cinfo->tbl];
    }

    if (db == NULL) {
        sprintf(buf, "field%d", num);
        return;
    }

    struct schema *sc = NULL;
    char scname[100];

    if (cinfo->ix != -1) {
        if (cinfo->ix <= db->nix) {
            sc = db->ixschema[cinfo->ix];
        } else {
            snprintf(scname, sizeof(scname), ".ONDISK_ix_%d", cinfo->ix);
            sc = find_tag_schema(db->tablename, scname);
        }
    } else {
        sc = db->schema;
    }

    if (sc == NULL) {
        sprintf(buf, "field%d", num);
        return;
    }

    if (num >= sc->nmembers && sc->datacopy) /* datacopy */
    {
        num = sc->datacopy[num - sc->nmembers];
        sc = db->schema;
        sprintf(buf, "\"%s\" (datacopy)", sc->member[num].name);
    } else if (num < sc->nmembers) {
        sprintf(buf, "\"%s\"", sc->member[num].name);
    } else {
        sprintf(buf, "field%d", num);
    }
}

static int print_cursor_description(strbuf *out, struct cursor_info *cinfo)
{
    int m;
    struct schema *sc;
    char scname[100];
    int is_index = 0;

    if (cinfo->remote == 1) {
        char *desc = fdb_sqlexplain_get_name(cinfo->rootpage);
        strbuf_appendf(out, "remote %s", (desc) ? desc : "???");
        free(desc);
    } else if (cinfo->istemp && cinfo->rootpage == 1) {
        strbuf_appendf(out, "table \"sqlite_temp_master\"");
    } else if (cinfo->rootpage == 1) {
        strbuf_appendf(out, "table \"sqlite_master\"");
    } else if (!cinfo->istemp && cinfo->rootpage == 0) {
        strbuf_appendf(out, "temp table");
    } else if (cinfo->istemp) {
        strbuf_appendf(out, "temporary ");
        if (cinfo->ix == -1) {
            strbuf_appendf(out, "table ");
        } else {
            strbuf_appendf(out, "index ");
        }
        strbuf_appendf(out, "temp_%d", cinfo->tbl);
    } else {
        struct dbtable *db;
        db = thedb->dbs[cinfo->tbl];

        if (cinfo->ix != -1) {
            if (cinfo->ix <= db->nix) {
                sc = db->ixschema[cinfo->ix];
            } else {
                snprintf(scname, sizeof(scname), ".ONDISK_ix_%d", cinfo->ix);
                sc = find_tag_schema(db->tablename, scname);
            }
            strbuf_appendf(out, "index \"%s\" of ",
                           sc ? (sc->csctag ? sc->csctag : sc->tag) : "???");
            is_index = 1;

            /*
            if (sc == NULL)
            {
                strbuf_appendf(out,"???");
                return;
            }
            for (m = 0; m < sc->nmembers; m++)
            {
                strbuf_appendf(out,"%s", sc->member[m].name);
                if (m != sc->nmembers-1)
                    strbuf_appendf(out,", ");
            }
            */
        }
        strbuf_appendf(out, "table \"%s\"", db->tablename);
    }
    strbuf_appendf(out, " ");
    return is_index;
}

static void print_mem(strbuf *out, Mem *m)
{
    int i;
    switch (m->flags) {
    case MEM_Null:
        strbuf_appendf(out, "null");
        break;
    case MEM_Str:
        strbuf_appendf(out, "string \"%.*s\"", m->n, m->z);
        break;
    case MEM_Int:
        strbuf_appendf(out, "int %lld", (long long)m->u.i);
        break;
    case MEM_Real:
        strbuf_appendf(out, "real %f", m->u.r);
        break;
    case MEM_Blob:
        strbuf_appendf(out, "blob x'");
        for (i = 0; i < m->n; i++)
            strbuf_appendf(out, "%02x", ((unsigned char *)m->z)[i]);
        strbuf_appendf(out, "'");
        break;
    }
}

static char *op_to_sign(int op)
{
    switch (op) {
    case OP_Eq:
        return "==";
    case OP_Ne:
        return "!=";
    case OP_Lt:
    case OP_SeekLT:
    case OP_IdxLT:
        return "<";
    case OP_Le:
    case OP_SeekLE:
    case OP_IdxLE:
        return "<=";
    case OP_Gt:
    case OP_SeekGT:
    case OP_IdxGT:
        return ">";
    case OP_Ge:
    case OP_SeekGE:
    case OP_IdxGE:
        return ">=";
    default:
        return "????";
    }
}

static void affinity_to_text(char *aff, strbuf *out)
{
    char ch;
    char *zAff = aff;
    int len = strlen(aff);

    while (len--) {
        ch = SQLITE_AFF_MASK & *aff;
        ++aff;

        if (islower(ch)) {
            logmsg(LOGMSG_FATAL, "Lower case affinity: %s!!\n", zAff);
            logmsg(LOGMSG_FATAL, "Contact Comdb2 team with query to fix!\n");
            exit(1);
        }

        switch (ch) {

        case SQLITE_AFF_TEXT:
            strbuf_appendf(out, "text");
            break;

        case SQLITE_AFF_DATETIME:
            strbuf_appendf(out, "datetime");
            break;

        case SQLITE_AFF_INTV_YE:
            strbuf_appendf(out, "year");
            break;

        case SQLITE_AFF_INTV_MO:
            strbuf_appendf(out, "month");
            break;

        case SQLITE_AFF_INTV_DY:
            strbuf_appendf(out, "day");
            break;

        case SQLITE_AFF_INTV_HO:
            strbuf_appendf(out, "hour");
            break;

        case SQLITE_AFF_INTV_MI:
            strbuf_appendf(out, "minute");
            break;

        case SQLITE_AFF_INTV_SE:
            strbuf_appendf(out, "second");
            break;

        case SQLITE_AFF_BLOB:
            strbuf_appendf(out, "blob");
            break;

        case SQLITE_AFF_NUMERIC:
            strbuf_appendf(out, "numeric");
            break;

        case SQLITE_AFF_INTEGER:
            strbuf_appendf(out, "integer");
            break;

        case SQLITE_AFF_REAL:
            strbuf_appendf(out, "double");
            break;

        case SQLITE_AFF_SMALL:
            strbuf_appendf(out, "float");
            break;

        default:
            strbuf_appendf(out, "unknown");
            break;
        }

        if (len)
            strbuf_appendf(out, ", ");
    }
    strbuf_appendf(out, " (affinity: %s)", zAff);
}

#ifndef ABS
#define ABS(a) (((a) < 0) ? (-(a)) : (a))
#endif

extern int sqlite3WhereTrace;

static void describe_cursor(Vdbe *v, int pc, struct cursor_info *cur)
{
    Op *op = &v->aOp[pc];
    bzero(cur, sizeof cur);
    if (op->p3 <= 1) {
        struct sql_thread *sqlthd = pthread_getspecific(query_info_key);
        struct dbtable *db = get_sqlite_db(sqlthd, op->p2, &cur->ix);
        cur->tbl = db->dbs_idx;
        cur->rootpage = op->p2;
        if (op->p3 == 1)
            cur->istemp = 1;
        if (op->p4type == P4_KEYINFO)
            cur->key = op->p4.pKeyInfo;
        else
            cur->cols = op->p4.i;
    } else {
        /* remote */
        cur->rootpage = op->p2;
        cur->remote = 1;
    }
}

/*
** Parameter azArray points to a zero-terminated array of strings. zStr
** points to a single nul-terminated string. Return non-zero if zStr
** is equal, according to strcmp(), to any of the strings in the array.
** Otherwise, return zero.
*/
static int op_in_array(const int op, const int *opArray)
{
    int i;
    for (i = 0; opArray[i]; i++) {
        if (op == opArray[i])
            return 1;
    }
    return 0;
}

static int str_in_array(const char *zStr, const char **azArray)
{
    int i;
    for (i = 0; azArray[i]; i++) {
        if (0 == strcmp(zStr, azArray[i]))
            return 1;
    }
    return 0;
}

/* COPIED FROM sqlite/shell.c (TODO: expose the original def and use here?) */
/*
** If compiled statement pSql appears to be an EXPLAIN statement, allocate
** and populate the IndentInfo.aiIndent[] array with the number of
** spaces each opcode should be indented before it is output.
**
** The indenting rules are:
**
**     * For each "Next", "Prev", "VNext" or "VPrev" instruction, indent
**       all opcodes that occur between the p2 jump destination and the opcode
**       itself by 2 spaces.
**
**     * For each "Goto", if the jump destination is earlier in the program
**       and ends on one of:
**          Yield  SeekGt  SeekLt  RowSetRead  Rewind
**       or if the P1 parameter is one instead of zero,
**       then indent all opcodes between the earlier instruction
**       and "Goto" by 2 spaces.
*/
void explain_data_prepare(IndentInfo *p, Vdbe *v)
{
    // const char *zSql;               /* The text of the SQL statement */
    const char *z;    /* Used to check if this is an EXPLAIN */
    int *abYield = 0; /* True if op is an OP_Yield */
    int nAlloc = 0;   /* Allocated size of p->aiIndent[], abYield */
    int pc;           /* Index of operation in p->aiIndent[] */

    const int opNext[] = {OP_Next, OP_Prev, /* OP_VPrev, */ OP_VNext,
                          OP_SorterNext, OP_NextIfOpen, OP_PrevIfOpen, 0};
    const int opYield[] = {OP_Yield,      OP_SeekLT, OP_SeekGT,
                           OP_RowSetRead, OP_Rewind, 0};
    const int opGoto[] = {OP_Goto, 0};

    const char *azNext[] = {"Next",       "Prev",       "VPrev",      "VNext",
                            "SorterNext", "NextIfOpen", "PrevIfOpen", 0};
    const char *azYield[] = {"Yield",      "SeekLT", "SeekGT",
                             "RowSetRead", "Rewind", 0};
    const char *azGoto[] = {"Goto", 0};

    /* Try to figure out if this is really an EXPLAIN statement. If this
    ** cannot be verified, return early.  */
    // zSql = sqlite3_sql(pSql);
    // if( zSql==0 ) return;
    // for(z=zSql; *z==' ' || *z=='\t' || *z=='\n' || *z=='\f' || *z=='\r';
    // z++);
    // if( sqlite3_strnicmp(z, "explain", 7) ) return;

    for (pc = 0; pc < v->nOp; pc++) {
        int i;
        Op *op = &v->aOp[pc];

        int iAddr = pc; // sqlite3_column_int(pSql, 0);
        // const char *zOp = (const char*)sqlite3_column_text(pSql, 1);
        const int opcode = op->opcode;

        /* Set p2 to the P2 field of the current opcode. Then, assuming that
        ** p2 is an instruction address, set variable p2op to the index of that
        ** instruction in the aiIndent[] array. p2 and p2op may be different if
        ** the current instruction is part of a sub-program generated by an
        ** SQL trigger or foreign key.  */
        int p2 = op->p2; // sqlite3_column_int(pSql, 3);
        int p2op = (p2 + (pc - iAddr));

        /* Grow the p->aiIndent array as required */
        if (pc >= nAlloc) {
            nAlloc += 100;
            p->aiIndent =
                (int *)sqlite3_realloc(p->aiIndent, nAlloc * sizeof(int));
            abYield = (int *)sqlite3_realloc(abYield, nAlloc * sizeof(int));
        }
        abYield[pc] = op_in_array(opcode, opYield);
        p->aiIndent[pc] = 0;
        p->nIndent = pc + 1;

        if (op_in_array(opcode, opNext)) {
            for (i = p2op; i < pc; i++)
                p->aiIndent[i] += 1;
        }
        if (op_in_array(opcode, opGoto) && p2op < p->nIndent &&
            (abYield[p2op] || op->p1 /*sqlite3_column_int(pSql, 2) */)) {
            for (i = p2op + 1; i < pc; i++)
                p->aiIndent[i] += 1;
        }
    }

    sqlite3_free(abYield);
    // sqlite3_reset(pSql);
}

/* COPIED FROM sqlite/shell.c (TODO: expose the original def and use here?) */
/*
** Free the array allocated by explain_data_prepare().
*/
void explain_data_delete(IndentInfo *p)
{
    sqlite3_free(p->aiIndent);
    p->aiIndent = 0;
    p->nIndent = 0;
}

void get_one_explain_line(sqlite3 *hndl, strbuf *out, Vdbe *v, int indent,
                          int largestwidth, int pc, struct cursor_info *cur)
{
    char str[2];
    Op *op = &v->aOp[pc];
    if (op->opcode != OP_Column) {
        strbuf_appendf(out, "%3d [%*s]: ", pc, largestwidth,
                       sqlite3OpcodeName(op->opcode));
        strbuf_appendf(out, "%*s", indent * 4, "");
    }
    switch (op->opcode) {
    case OP_Goto:
        strbuf_appendf(out, "Go to %d", op->p2);
        break;
    case OP_Gosub:
        strbuf_appendf(out, "R%d = %d; Jump to %d", op->p1, pc, op->p2);
        break;
    case OP_Return:
        strbuf_appendf(out, "Jump to R%d + 1", op->p1);
        break;
    case OP_Yield:
        strbuf_appendf(out, "Swap PC with value in R%d", op->p1);
        break;
    case OP_HaltIfNull:
        strbuf_appendf(out, "Halt if R%d == NULL", op->p3);
        break;
    case OP_Halt:
        strbuf_append(out, "Stop");
        break;

    case OP_Cast:
        strbuf_appendf(out, "Convert R%d to a ", op->p1);
        str[1] = '\0';
        str[0] = op->p2;
        affinity_to_text(str, out);
        break;

    case OP_Integer:
        strbuf_appendf(out, "R%d = %d", op->p2, op->p1);
        break;
    case OP_Int64:
        strbuf_appendf(out, "R%d = %ld", op->p2, op->p4.pI64);
        break;
    case OP_Real:
        strbuf_appendf(out, "R%d = %f", op->p2, *op->p4.pReal);
        break;
    case OP_String8:
    case OP_String:
        strbuf_appendf(out, "R%d = \"%s\"", op->p2, op->p4.z);
        break;
    case OP_Null:
        strbuf_appendf(out, "R%d = NULL", op->p2);
        break;
    case OP_SoftNull:
        strbuf_appendf(out, "Set register R%d to have the value NULL as seen "
                            "by the OP_MakeRecord, do not free prev val",
                       op->p2);
        break;
    case OP_Blob: {
        strbuf_appendf(out, "R%d = x'", op->p2);
        for (int i = 0; i < op->p1; ++i) {
            strbuf_appendf(out, "%c%c", hex[(op->p4.z[i] & 0xf0) >> 4],
                           hex[op->p4.z[i] & 0x0f]);
        }
        strbuf_append(out, "'");
        break;
    }
    case OP_Variable:
        strbuf_appendf(out,
                       "Transfer the values of variable %d into register R%d ",
                       op->p1 - 1, op->p2);
        print_mem(out, &v->aVar[op->p1 - 1]);
        break;
    case OP_Move:
        if (op->p3 > 1)
            strbuf_appendf(out, "R%d..R%d <- R%d..R%d", op->p2,
                           op->p2 + op->p3 - 1, op->p1, op->p1 + op->p3 - 1);
        else
            strbuf_appendf(out, "R%d <- R%d", op->p2, op->p1);
        break;
    case OP_Copy:
        strbuf_appendf(out, "R%d = R%d (deep copy)", op->p2, op->p1);
        break;
    case OP_SCopy:
        strbuf_appendf(out, "R%d = R%d (shallow copy)", op->p2, op->p1);
        break;
    case OP_ResultRow:
        if (op->p2 > 1)
            strbuf_appendf(out, "Row available at R%d..R%d", op->p1,
                           op->p1 + op->p2 - 1);
        else
            strbuf_appendf(out, "Row available at R%d", op->p1);
        break;
    case OP_Concat:
        strbuf_appendf(out, "R%d = R%d+R%d", op->p3, op->p2, op->p1);
        break;
    case OP_Add:
    case OP_Subtract:
    case OP_Multiply:
    case OP_Divide:
    case OP_Remainder:
        strbuf_appendf(out, "R%d = R%d %c R%d", op->p3, op->p2,
                       opcode_to_sign(op->opcode), op->p1);
        break;
    case OP_CollSeq:
        strbuf_appendf(out, "Using collation sequence %s", op->p4.pColl->zName);
        break;
    case OP_Function0:
    case OP_Function:
        if (op->p5 > 1) {
            strbuf_appendf(out, "R%d = %s(R%d..R%d)", op->p3,
                           op->p4type == P4_FUNCDEF ? op->p4.pFunc->zName : "",
                           op->p2, op->p2 + op->p5 - 1);
        } else {
            strbuf_appendf(out, "R%d = %s(R%d)", op->p3,
                           op->p4type == P4_FUNCDEF ? op->p4.pFunc->zName : "",
                           op->p2);
        }
        break;
    case OP_BitAnd:
        strbuf_appendf(out, "R%d = R%d & R%d", op->p3, op->p2, op->p1);
        break;
    case OP_BitOr:
        strbuf_appendf(out, "R%d = R%d | R%d", op->p3, op->p2, op->p1);
        break;
    case OP_ShiftLeft:
        strbuf_appendf(out, "R%d = R%d << R%d", op->p3, op->p2, op->p1);
        break;
    case OP_ShiftRight:
        strbuf_appendf(out, "R%d = R%d >> R%d", op->p3, op->p2, op->p1);
        break;
    case OP_AddImm:
        strbuf_appendf(out, "R%d += %d", op->p1, op->p2);
        break;
    case OP_MustBeInt:
        strbuf_appendf(out, "Goto %d if conversion of R%d to integer fails",
                       op->p2, op->p1);
        break;
    case OP_RealAffinity:
        strbuf_appendf(out, "Convert R%d to float (if its an integer)", op->p1);
        break;

    case OP_Eq:
    case OP_Ne:
    case OP_Lt:
    case OP_Le:
    case OP_Gt:
    case OP_Ge:
        if (op->p5 & SQLITE_STOREP2)
            strbuf_appendf(out, "R%d = Result of R%d %s R%d", op->p2, op->p3,
                           op_to_sign(op->opcode), op->p1);
        else if (op->p5 & SQLITE_JUMPIFNULL)
            strbuf_appendf(out, "If R%d %s R%d or either is NULL goto %d",
                           op->p3, op_to_sign(op->opcode), op->p1, op->p2);
        else
            strbuf_appendf(out, "If R%d %s R%d goto %d", op->p3,
                           op_to_sign(op->opcode), op->p1, op->p2);
        /*
           if (op->p4type == P4_COLLSEQ)
           strbuf_appendf(out,"Use collating sequence %s for comparison.",
           op->p4.pColl->zName);
         */
        break;
    case OP_Permutation:
        strbuf_append(out,
                      "Set the permutation to be the array of integers in P4");
        break;
    case OP_Compare:
        if (op->p3 > 1)
            strbuf_appendf(out, "Compare R%d..R%d and R%d..R%d; store result "
                                "for subsequent Jump",
                           op->p1, op->p1 + op->p3 - 1, op->p2,
                           op->p2 + op->p3 - 1);
        else
            strbuf_appendf(
                out, "Compare R%d and R%d; store result for subsequent Jump",
                op->p1, op->p2);
        break;
    case OP_Jump:
        strbuf_appendf(out, "Jump to %d, %d or %d depending on most recent "
                            "Compare (less, equal to, or greater)",
                       op->p1, op->p2, op->p3);
        break;
    case OP_And:
        strbuf_append(out,
                      "Do logical AND between P1 and P2, store result in P3");
        break;
    case OP_Or:
        strbuf_append(out,
                      "Do logical OR between P1 and P2, store result in P3");
        break;
    case OP_Not:
        strbuf_append(out,
                      "Interpret P1 as boolean and store its complement in P2");
        break;
    case OP_BitNot:
        strbuf_append(
            out, "Interpret P1 as integer and store ones-complement in P2");
        break;
    case OP_If:
    case OP_IfNot:
        strbuf_appendf(out, "Jump to %d if R%d is %s. ", op->p2, op->p1,
                       (op->opcode == OP_If ? "TRUE" : "FALSE"));
        if (op->p3)
            strbuf_appendf(out, "Jump to %d if NULL.", op->p3);
        break;
    case OP_IsNull:
    case OP_NotNull:
        strbuf_appendf(out, "Jump to %d if R%d is %s.", op->p2, op->p1,
                       op->opcode == OP_IsNull ? "NULL" : "not NULL");
        break;
    case OP_Column: {
        int pc_ = pc;
        char buf[512];
        int col_count = 0;
        int cursor;
        while (v->aOp[pc_].opcode == OP_Column) {
            ++pc_;
            ++col_count;
        }

        if (all_opcodes || col_count < 2 ||
            (v->aOp[pc_].opcode != OP_MakeRecord &&
             v->aOp[pc_].opcode != OP_ResultRow) ||
            col_count != v->aOp[pc_].p2) {
            /* Don't collapse OP_Column instructions */
            strbuf_appendf(out, "%3d [%*s]: ", pc, largestwidth,
                           sqlite3OpcodeName(op->opcode));
            strbuf_appendf(out, "%*s", indent * 4, "");
            print_field(v, &cur[op->p1], op->p2, buf); /* field name into buf */
            strbuf_appendf(out, "R%d = %s from cursor [%d] on ", op->p3, buf,
                           op->p1);
            print_cursor_description(out, &cur[op->p1]);
        } else {
            op = &v->aOp[pc_];
            strbuf_appendf(out, "%3d [%*s]: ", pc_, largestwidth,
                           sqlite3OpcodeName(op->opcode));
            strbuf_appendf(out, "%*s", indent * 4, "");
            strbuf_appendf(out, "R%d = R%d..R%d [", op->p3, op->p1,
                           op->p1 + op->p2 - 1);

            cursor = v->aOp[pc].p1;
            while (v->aOp[pc].opcode == OP_Column) {
                op = &v->aOp[pc];
                if (cursor != op->p1) {
                    strbuf_appendf(out, " (from cursor %d)", cursor);
                    cursor = op->p1;
                }
                print_field(v, &cur[op->p1], op->p2, buf);
                strbuf_appendf(out, "%s", buf);
                if (pc < pc_ - 1) {
                    strbuf_append(out, ", ");
                }
                ++pc;
            }
            strbuf_appendf(out, " (from cursor %d)]", cursor);
        }
        break;
    }
    case OP_Affinity:
        if (op->p2 > 1)
            strbuf_appendf(out, "Convert R%d..R%d into ", op->p1,
                           op->p1 + op->p2 - 1);
        else
            strbuf_appendf(out, "Convert R%d into ", op->p1);
        affinity_to_text(op->p4.z, out);
        break;
    case OP_MakeRecord:
        if (op->p2 > 1)
            strbuf_appendf(out, "R%d = R%d..R%d", op->p3, op->p1,
                           op->p1 + op->p2 - 1);
        else
            strbuf_appendf(out, "R%d = R%d", op->p3, op->p1);
        break;
    case OP_Count:
        strbuf_appendf(out, "R%d = select count(*) from cursor [%d] on ",
                       op->p2, op->p1);
        print_cursor_description(out, &cur[op->p1]);
        break;
    case OP_Savepoint:
        strbuf_appendf(
            out, "%s the savepoint named by parameter P4(%s)",
            (op->p1 == 0 ? "Open" : (op->p1 == 1 ? "Commit" : "Rollback")),
            op->p4.z);
        break;
    case OP_AutoCommit:
        strbuf_appendf(out, "Set database auto-commit flag to P1(%d). ",
                       op->p1);
        if (op->p2)
            strbuf_append(out, "Rollback any active btree transactions");
        break;
    case OP_Transaction:
        strbuf_appendf(out, "Start a %s transaction",
                       op->p2 ? "write" : "readonly");
        break;
    case OP_ReadCookie:
    case OP_SetCookie:
    case OP_Noop:
    case OP_Explain:
    case OP_Vacuum:
        strbuf_appendf(out, "No-op (%d)",
                       op->opcode); /* don't care about these */
        break;
    case OP_ReopenIdx:
        describe_cursor(v, pc, &cur[op->p1]);
        strbuf_appendf(out, "Open read cursor [%d] if not already open on ",
                       op->p1);
        int is_index = print_cursor_description(out, &cur[op->p1]);
        if (is_index && op->opcode == OP_OpenRead) {
            if (op->p5 == 0xff) {
                strbuf_append(out, "(not a covering index)");
            } else {
                strbuf_append(out, "(covering index)");
            }
        }
        break;
    case OP_OpenRead_Record:
    case OP_OpenRead:
    case OP_OpenWrite:
        describe_cursor(v, pc, &cur[op->p1]);
        strbuf_appendf(out, "Open %s cursor [%d] on ",
                       (op->opcode != OP_OpenWrite ? "read" : "write"), op->p1);
        is_index = print_cursor_description(out, &cur[op->p1]);
        if (is_index && op->opcode == OP_OpenRead) {
            if (op->p5 == 0xff) {
                strbuf_append(out, "(not a covering index)");
            } else {
                strbuf_append(out, "(covering index)");
            }
        }
        break;
    case OP_OpenAutoindex:
    case OP_OpenEphemeral: {
        strbuf_appendf(out,
                       "Create a temp %s, and cursor [%d] to operate on it",
                       (op->p3 ? "index" : "table"), op->p1);
        struct KeyInfo *info = op->p4.pKeyInfo;
        if (info && info->aSortOrder) {
            int i;
            strbuf_append(out, " sort order (");
            for (i = 0; i < info->nField; i++) {
                if (info->aSortOrder[i])
                    strbuf_append(out, "desc");
                else
                    strbuf_append(out, "asc");
                if (i != info->nField - 1)
                    strbuf_append(out, ", ");
            }
            strbuf_append(out, ")");
        }
        if (op->p5 == BTREE_UNORDERED) {
            strbuf_append(out, " [Hash table]");
        }
        break;
    }
    case OP_OpenPseudo:
        strbuf_appendf(
            out, "Open cursor [%d] on pseudo table. Table will have single row",
            op->p1);
        break;
    case OP_Close:
        strbuf_appendf(out, "Close cursor [%d]", op->p1);
        break;
    case OP_SeekLT:
    case OP_SeekLE:
    case OP_SeekGE:
    case OP_SeekGT: {
        strbuf_appendf(out, "Move cursor [%d] to %s entry %s ", op->p1,
                       (op->opcode == OP_SeekLT || op->opcode == OP_SeekLE)
                           ? "largest"
                           : "smallest",
                       op_to_sign(op->opcode));
        if (op->p4.i == 0) {
            strbuf_append(out, "R3. ");
        } else if (op->p4.i > 1) {
            strbuf_appendf(out, "R%d..R%d. ", op->p3, op->p3 + op->p4.i - 1);
        } else {
            strbuf_appendf(out, "R%d. ", op->p3);
        }
        strbuf_appendf(out, "If no such records exist, go to %d", op->p2);
        break;
    }
    case OP_Seek:
        strbuf_appendf(out, "Move cursor [%d] to rowid of index cursor [%d]",
                       op->p1, op->p2);
        break;
    case OP_NoConflict:
    case OP_NotFound:
    case OP_Found:
        strbuf_appendf(out, "If cursor [%d] %scontain%s ", op->p1,
                       op->opcode == OP_NotFound ? "doesn't " : "",
                       op->opcode == OP_NotFound ? "" : "s");
        if (op->p4.i == 1) {
            strbuf_appendf(out, "R%d", op->p3);
        } else if (op->p4.i > 1) {
            strbuf_appendf(out, "R%d..R%d", op->p3, op->p3 + op->p4.i - 1);
        } else {
            strbuf_appendf(out, "blob in R%d", op->p3);
        }
        strbuf_appendf(out, ", then jump to %d", op->p2);

        break;
    case OP_NotExists:
        strbuf_appendf(out,
                       "If record id in R%d can't be found using cursor [%d]",
                       op->p3, op->p1);
        if (op->p2)
            strbuf_appendf(out, "go to %d", op->p2);
        else
            strbuf_appendf(out, "raise an SQLITE_CORRUPT error");
        break;
    case OP_Sequence:
        strbuf_appendf(out,
                       "R%d = next available sequence number for cursor [%d]",
                       op->p2, op->p1);
        break;
    case OP_NewRowid:
        strbuf_appendf(
            out, "Copy new (unused) record number for cursor [%d] into R%d",
            op->p1, op->p2);
        break;
    case OP_Insert:
    case OP_InsertInt:
        strbuf_appendf(out, "Write record in R%d into ", op->p2);
        print_cursor_description(out, &cur[op->p1]);
        strbuf_appendf(out, " using cursor [%d]", op->p1);
        break;
    case OP_Delete:
        strbuf_appendf(out, "Delete current record from cursor [%d] on",
                       op->p1);
        print_cursor_description(out, &cur[op->p1]);
        break;
    case OP_ResetCount:
        strbuf_append(
            out, "Move change counter to database handle's change counter.");
        break;
    case OP_RowKey:
    case OP_RowData:
        strbuf_appendf(out, "R%d = %s from cursor [%d] on ", op->p2,
                       op->opcode == OP_RowKey ? "key" : "data", op->p1);
        print_cursor_description(out, &cur[op->p1]);
        break;
    case OP_Rowid:
        strbuf_appendf(out, "R%d = genid of row pointed by cursor [%d]", op->p2,
                       op->p1);
        break;
    case OP_NullRow:
        strbuf_appendf(out, "Move cursor [%d] to null row.", op->p1);
        break;
    case OP_Last:
        strbuf_appendf(out, "Move cursor [%d] on ", op->p1);
        print_cursor_description(out, &cur[op->p1]);
        strbuf_append(out, "to last entry. ");
        if (op->p2)
            strbuf_appendf(out, "If no entries exist, go to %d", op->p2);
        break;
    case OP_Rewind:
    case OP_Sort:
    case OP_SorterSort:
        strbuf_appendf(out, "Move cursor [%d] to first entry. ", op->p1);
        if (op->p2)
            strbuf_appendf(out, "If no entries exist, go to %d", op->p2);
        break;
    case OP_ResetSorter:
        strbuf_appendf(out,
                       "Delete all contents from the ephemeral table or sorter"
                       "that is open on cursor [%d].",
                       op->p1);
        break;
    case OP_Next:
    case OP_NextIfOpen:
        strbuf_appendf(
            out, "Move cursor [%d] to next entry. If entry exists, go to %d",
            op->p1, op->p2);
        break;
    case OP_Prev:
    case OP_PrevIfOpen:
        strbuf_appendf(
            out,
            "Move cursor [%d] to previous entry. If entry exists, go to %d",
            op->p1, op->p2);
        break;
    case OP_IdxInsert:
        strbuf_appendf(out, "Write key in R%d into ", op->p2);
        print_cursor_description(out, &cur[op->p1]);
        strbuf_appendf(out, "using cursor [%d]", op->p1);
        break;
    case OP_IdxDelete:
        strbuf_appendf(out, "Delete key in ");
        if (op->p3 > 1) {
            strbuf_appendf(out, "R%d..R%d", op->p2, op->p2 + op->p3 - 1);
        } else {
            strbuf_appendf(out, "R%d", op->p2);
        }
        strbuf_appendf(out, " from ");
        print_cursor_description(out, &cur[op->p1]);
        strbuf_appendf(out, "using cursor [%d]", op->p1);
        break;
    case OP_IdxRowid:
        strbuf_appendf(out, "R%d = Rowid of current entry at cursor [%d]",
                       op->p2, op->p1);
        break;
    case OP_IdxGE:
    case OP_IdxGT:
    case OP_IdxLT:
    case OP_IdxLE:
        if (op->p4.i > 1)
            strbuf_appendf(out, "Jump to %d if cursor [%d] %s R%d..R%d", op->p2,
                           op->p1, op_to_sign(op->opcode), op->p3,
                           op->p3 + op->p4.i - 1);
        else
            strbuf_appendf(out, "Jump to %d if cursor [%d] %s R%d", op->p2,
                           op->p1, op_to_sign(op->opcode), op->p3);
        if (op->p5)
            strbuf_append(out, " (INCRKEY)");
        break;
    case OP_Destroy:
        strbuf_append(out, "Destroy ");
        print_cursor_description(out, &cur[op->p1]);
        break;
    case OP_Clear:
        strbuf_append(out, "Delete all rows from ");
        print_cursor_description(out, &cur[op->p1]);
        break;
    case OP_RowSetAdd:
        strbuf_appendf(out, "Insert R%d into boolean index in R%d", op->p2,
                       op->p1);
        break;
    case OP_RowSetRead:
        strbuf_appendf(
            out,
            "R%d = Smallest from boolean index in R%d. Jump to %d if empty",
            op->p3, op->p1, op->p2);
        break;
    case OP_RowSetTest:
        strbuf_appendf(
            out,
            "If R%d == R%d jump to R%d, otherwise add R%d to boolean index",
            op->p1, op->p3, op->p2, op->p3);
        break;
    case OP_MemMax:
        strbuf_append(out,
                      "Set value at P1 to max of it's current value and P2");
        break;
    case OP_IfPos:
        strbuf_appendf(out, "If R%d > 0 then R%d -= %d, go to %d", op->p1,
                       op->p1, op->p3, op->p2);
        break;
    case OP_OffsetLimit:
        strbuf_appendf(
            out, "If R%d > 0 then R%d = R%d + max(0, R%d) else R%d = (-1)",
            op->p1, op->p2, op->p1, op->p3, op->p2);
        break;
    case OP_DecrJumpZero:
        strbuf_appendf(out, "R%d --; If R%d == 0 jump to %d", op->p1, op->p1,
                       op->p2);
        break;
    case OP_IfNotZero:
        strbuf_appendf(out, "If R%d != 0 then R%d -= %d, jump to %d", op->p1,
                       op->p1, op->p3, op->p2);
        break;
    case OP_AggStep0:
    case OP_AggStep:
        strbuf_appendf(out, "R%d = %s(", op->p3,
                       ((struct FuncDef *)op->p4.pFunc)->zName);
        for (int i = 0; i < op->p5; ++i) {
            strbuf_appendf(out, "%sR%d", i ? ", " : "", op->p2 + i);
        }
        strbuf_append(out, ")");
        break;
    case OP_AggFinal:
        strbuf_appendf(out, "R%d = %s() finalizer", op->p1,
                       ((struct FuncDef *)op->p4.pFunc)->zName);
        break;
    case OP_Expire:
        if (op->p1)
            strbuf_append(out, "Invalidate current query plan");
        else
            strbuf_append(out, "Invalidate all query plans");
        break;
    case OP_CursorHint:
        strbuf_appendf(out, "Cursor [%d] table ", op->p1);
        print_cursor_description(out, &cur[op->p1]);
        char *descr = sqlite3ExprDescribe(hndl->pVdbe, op->p4.pExpr);
        strbuf_appendf(out, " hint \"%s\"",
                       (descr) ? descr : "(expression not parseable, see 592)");
        if (descr)
            sqlite3DbFree(hndl, descr);
        break;
    case OP_SorterOpen:
        strbuf_appendf(out, "Open sorter new table with %d field(s) and cursor "
                            "[%d] to operate on it",
                       op->p2, op->p1);
        struct KeyInfo *info = op->p4.pKeyInfo;
        if (info && info->aSortOrder) {
            int i;
            strbuf_append(out, " sort order (");
            for (i = 0; i < info->nField; i++) {
                if (info->aSortOrder[i])
                    strbuf_append(out, "desc");
                else
                    strbuf_append(out, "asc");
                if (i != info->nField - 1)
                    strbuf_append(out, ", ");
            }
            strbuf_append(out, ")");
        }
        break;
    case OP_SorterInsert:
        strbuf_appendf(out, "Write key in R%d into ", op->p2);
        strbuf_appendf(out, "sorter table using cursor [%d]", op->p1);
        break;
    case OP_SorterData:
        strbuf_appendf(out, "R%d = the current sorter data for sorter "
                            "cursor [%d]",
                       op->p2, op->p1);
        break;
    case OP_SorterCompare:
        strbuf_appendf(out, "if (key at cursor[%d]) != rtrim(R%d,P4) goto %d",
                       op->p1, op->p3, op->p2);
        break;
    case OP_SorterNext:
        strbuf_appendf(out, "Move cursor [%d] to next entry in sorter table. "
                            "If entry exists, go to %d",
                       op->p1, op->p2);
        break;
    case OP_Once:
        strbuf_appendf(out, "If flag F%d is set go to %d, otherwise set it "
                            "and fall through to next instruction",
                       op->p1, op->p2);
        break;

    case OP_ParseSchema:
        strbuf_append(out, "Read and parse all entries from the MASTER tables");
        break;

    case OP_CreateTable:
        strbuf_appendf(
            out, "Create new temp table. Root page number of the new table "
                 "in R%d ",
            op->p2);
        break;
    case OP_Init:
        strbuf_appendf(out, "Start at %d %s ", op->p2,
                       (op->p4.z ? op->p4.z : ""));
        break;
    case OP_LoadAnalysis:
        strbuf_append(out, "Read the sqlite_stat1 table for database");
        break;
    case OP_InitCoroutine:
        strbuf_appendf(out, "Set up register P1(%d) to OP_Yield to the "
                            "co-routine at address P3(%d)",
                       op->p1, op->p3);
        break;
    case OP_EndCoroutine:
        strbuf_appendf(out, "Jump to the P2(%d) parameter of the OP_Yield "
                            "instruction at the address in register P1(%d)",
                       op->p2, op->p1);
        break;

    case OP_ColumnsUsed: {
        unsigned long long mask = *(unsigned long long *)op->p4.pI64;
        char maskStr[65] = {0};
        int i;

        for (i = 0; i < 64; i++)
            maskStr[i] = ((mask >> i) & 1) ? '1' : '0';

        strbuf_appendf(out, "Cursor [%d] using column mask %s", op->p1,
                       maskStr);
    } break;
    case OP_OpFuncLoad:
        strbuf_appendf(out, "Load OpFunc P4(%s) into R%d",
                       op->p4.comdb2func, op->p2);
        break;
    case OP_OpFuncNext:
        strbuf_appendf(out, "if R%d has been fully read then continue, else jump to %d" ,
                       op->p1, op->p2);
        break;
    case OP_OpFuncExec:
        strbuf_appendf(out, "Exec OpFunc in R%d" , op->p1);
        break;
    case OP_OpFuncString:
        strbuf_appendf(out, "Next String of R%d into R%d", op->p1, op->p2);
        break;


    case OP_TableLock:
    case OP_VBegin:
    case OP_VCreate:
    case OP_VDestroy:
    case OP_VOpen:
    case OP_VColumn:
    case OP_VNext:
    case OP_VFilter:
    case OP_VRename:
    case OP_VUpdate:
    default:
        strbuf_append(out, "???");
        break;
    }
    if (op->zComment)
        strbuf_appendf(out, " (cmnt:%s)", op->zComment);
}

int newsql_dump_query_plan(struct sqlclntstate *clnt, sqlite3 *hndl)
{
    char *sql = clnt->sql;
    FILE *f = NULL;

    //if verbose explain get costs dumped to tmpfile by setting wheretrace
    if (clnt->is_explain == 2) {
        sqlite3WhereTrace = 0xfff;
        f = tmpfile();
        if (f == NULL) {
            logmsgperror("tmpfile");
        } else {
            io_override_set_std(f);
        }
    }

    sqlite3_stmt *stmt = NULL;
    char *eos;
    int rc = sqlite3_prepare_flags(hndl, sql, -1, &stmt, (const char **)&eos,
                                   SQLITE3_ENABLE_QUERY_PLAN);
    sqlite3WhereTrace = 0;
    if (f) 
        io_override_set_std(NULL);
    if (rc || !stmt) {
        char * errstr = (char *)sqlite3_errmsg(hndl);
        write_response(clnt, RESPONSE_ERROR_PREPARE, errstr, 0);
        return rc || 1;
    }

    char *cols[] = {"Plan"};
    write_response(clnt, RESPONSE_COLUMNS_STR, &cols, 1);

    strbuf *out = strbuf_new();
    if (!out) {
        logmsg(LOGMSG_FATAL, "strbuf_new returned NULL\n");
        exit(1);
    }
    IndentInfo indentation = {0};
    Vdbe *v = (Vdbe *)stmt;
    explain_data_prepare(&indentation, v);
    int maxwidth = 0; //print explain with precise width of line heading
    for (int pc = 0; pc < v->nOp; pc++) {
        Op *op = &v->aOp[pc];
        int oplen = strlen(sqlite3OpcodeName(op->opcode));
        if (maxwidth < oplen)
            maxwidth = oplen;
    }
    struct cursor_info cur[MAXCUR] = {0};

    for (int pc = 0; pc < v->nOp; pc++) {
        int indent = indentation.aiIndent[pc];
        if (indent < 0)
            indent = 0;
        get_one_explain_line(hndl, out, v, indent, maxwidth, pc, cur);
        char *row[] = {(char*)strbuf_buf(out)};
        write_response(clnt, RESPONSE_ROW_STR, row, 1);
        strbuf_clear(out);
    }

    //if this was verbose explain, get the cost from tmpfile f
    if (f) {
        rewind(f);
        char buf[32]; /* small stack size in appsock thd */
        while (fgets(buf, sizeof(buf), f))
            strbuf_appendf(out, "%s", buf);
        char *row[] = {(char*)strbuf_buf(out)};
        write_response(clnt, RESPONSE_ROW_STR, row, 1);
        fclose(f);
    }

    explain_data_delete(&indentation);
    strbuf_free(out);
    out = NULL;

    sqlite3_finalize(stmt);
    write_response(clnt, RESPONSE_ROW_LAST, NULL, 0);
    return 0;
}

