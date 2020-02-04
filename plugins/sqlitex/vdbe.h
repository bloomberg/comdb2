/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** Header file for the Virtual DataBase Engine (VDBE)
**
** This header defines the interface to the virtual database engine
** or VDBE.  The VDBE implements an abstract machine that runs a
** simple program to access and modify the underlying database.
*/
#ifndef _SQLITE_VDBE_H_
#define _SQLITE_VDBE_H_
#include <stdio.h>

/*
** A single VDBE is an opaque structure named "Vdbe".  Only routines
** in the source file sqliteVdbe.c are allowed to see the insides
** of this structure.
*/
typedef struct Vdbe Vdbe;

/*
** The names of the following types declared in vdbeInt.h are required
** for the VdbeOp definition.
*/
typedef struct Mem Mem;
typedef struct SubProgram SubProgram;

/****************** COMDB2 CUSTOM *************************************/

#define DEFAULT_OPFUNC_BUFLEN 512
  typedef struct OpFunc OpFunc;

  typedef  int (*vdbeFunc)(OpFunc* arg); // Used to refer to an external function;
  typedef  int (*vdbeFuncArgFree) (OpFunc* arg); 

  struct OpFunc {
    vdbeFunc func;
    vdbeFuncArgFree destructor;
    int     int_arg;
    void*   arg;
    char*   buf;
    char*   end;
    char*   readNext;
    char*   writeNext;
    int     len;
    int     rc;
    char*   errorMsg;
  };

 
/***************************************************************/

/*
** A single instruction of the virtual machine has an opcode
** and as many as three operands.  The instruction is recorded
** as an instance of the following structure:
*/
struct VdbeOp {
  u8 opcode;          /* What operation to perform */
  signed char p4type; /* One of the P4_xxx constants for p4 */
  u8 opflags;         /* Mask of the OPFLG_* flags in opcodes.h */
  u8 p5;              /* Fifth parameter is an unsigned character */
  int p1;             /* First operand */
  int p2;             /* Second parameter (often the jump destination) */
  int p3;             /* The third parameter */
  union {             /* fourth parameter */
    int i;                 /* Integer value if p4type==P4_INT32 */
    void *p;               /* Generic pointer */
    char *z;               /* Pointer to data for string (char array) types */
    i64 *pI64;             /* Used when p4type is P4_INT64 */
    double *pReal;         /* Used when p4type is P4_REAL */
    FuncDef *pFunc;        /* Used when p4type is P4_FUNCDEF */
    CollSeq *pColl;        /* Used when p4type is P4_COLLSEQ */
    Mem *pMem;             /* Used when p4type is P4_MEM */
    VTable *pVtab;         /* Used when p4type is P4_VTAB */
    KeyInfo *pKeyInfo;     /* Used when p4type is P4_KEYINFO */
    int *ai;               /* Used when p4type is P4_INTARRAY */
    SubProgram *pProgram;  /* Used when p4type is P4_SUBPROGRAM */
    OpFunc *comdb2func;    /* COMDB2 Operator to Function P4_OPFUNC */

#ifdef SQLITE_ENABLE_CURSOR_HINTS
    Expr *pExpr;           /* COMDB2: Used when p4type is P4_EXPR */
#endif
    int (*xAdvance)(BtCursor *, int *);
  } p4;
#ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
  char *zComment;          /* Comment to improve readability */
#endif
#ifdef VDBE_PROFILE
  u32 cnt;                 /* Number of times this instruction was executed */
  u64 cycles;              /* Total time spent executing this instruction */
#endif
#ifdef SQLITE_VDBE_COVERAGE
  int iSrcLine;            /* Source-code line that generated this opcode */
#endif
};
typedef struct VdbeOp VdbeOp;


/*
** A sub-routine used to implement a trigger program.
*/
struct SubProgram {
  VdbeOp *aOp;                  /* Array of opcodes for sub-program */
  int nOp;                      /* Elements in aOp[] */
  int nMem;                     /* Number of memory cells required */
  int nCsr;                     /* Number of cursors required */
  int nOnce;                    /* Number of OP_Once instructions */
  void *token;                  /* id that may be used to recursive triggers */
  SubProgram *pNext;            /* Next sub-program already visited */
};

/*
** A smaller version of VdbeOp used for the VdbeAddOpList() function because
** it takes up less space.
*/
struct VdbeOpList {
  u8 opcode;          /* What operation to perform */
  signed char p1;     /* First operand */
  signed char p2;     /* Second parameter (often the jump destination) */
  signed char p3;     /* Third parameter */
};
typedef struct VdbeOpList VdbeOpList;

/*
** Allowed values of VdbeOp.p4type
*/
#define P4_NOTUSED    0   /* The P4 parameter is not used */
#define P4_DYNAMIC  (-1)  /* Pointer to a string obtained from sqliteMalloc() */
#define P4_STATIC   (-2)  /* Pointer to a static string */
#define P4_COLLSEQ  (-4)  /* P4 is a pointer to a CollSeq structure */
#define P4_FUNCDEF  (-5)  /* P4 is a pointer to a FuncDef structure */
#define P4_KEYINFO  (-6)  /* P4 is a pointer to a KeyInfo structure */
#define P4_EXPR     (-7)  /* COMDB2: P4 is a pointer to an Expr tree */
#define P4_MEM      (-8)  /* P4 is a pointer to a Mem*    structure */
#define P4_TRANSIENT  0   /* P4 is a pointer to a transient string */
#define P4_VTAB     (-10) /* P4 is a pointer to an sqlitex_vtab structure */
#define P4_MPRINTF  (-11) /* P4 is a string obtained from sqlitex_mprintf() */
#define P4_REAL     (-12) /* P4 is a 64-bit floating point value */
#define P4_INT64    (-13) /* P4 is a 64-bit signed integer */
#define P4_INT32    (-14) /* P4 is a 32-bit signed integer */
#define P4_INTARRAY (-15) /* P4 is a vector of 32-bit integers */
#define P4_SUBPROGRAM  (-18) /* P4 is a pointer to a SubProgram structure */
#define P4_ADVANCE  (-19) /* P4 is a pointer to BtreeNext() or BtreePrev() */

#define P4_OPFUNC  (-20) /* P4 is a COMDB2 custom function */

/* Error message codes for OP_Halt */
#define P5_ConstraintNotNull 1
#define P5_ConstraintUnique  2
#define P5_ConstraintCheck   3
#define P5_ConstraintFK      4

/*
** The Vdbe.aColName array contains 5n Mem structures, where n is the 
** number of columns of data returned by the statement.
*/
#define COLNAME_NAME     0
#define COLNAME_DECLTYPE 1
#define COLNAME_DATABASE 2
#define COLNAME_TABLE    3
#define COLNAME_COLUMN   4
#ifdef SQLITE_ENABLE_COLUMN_METADATA
# define COLNAME_N        5      /* Number of COLNAME_xxx symbols */
#else
# ifdef SQLITE_OMIT_DECLTYPE
#   define COLNAME_N      1      /* Store only the name */
# else
#   define COLNAME_N      2      /* Store the name and decltype */
# endif
#endif

/*
** The following macro converts a relative address in the p2 field
** of a VdbeOp structure into a negative number so that 
** sqlitexVdbeAddOpList() knows that the address is relative.  Calling
** the macro again restores the address.
*/
#define ADDR(X)  (-1-(X))

/*
** The makefile scans the vdbe.c source file and creates the "opcodes.h"
** header file that defines a number for each opcode used by the VDBE.
*/
#include "opcodes.h"

/*
** Prototypes for the VDBE interface.  See comments on the implementation
** for a description of what each of these routines does.
*/
Vdbe *sqlitexVdbeCreate(Parse*);
int sqlitexVdbeAddOp0(Vdbe*,int);
int sqlitexVdbeAddOp1(Vdbe*,int,int);
int sqlitexVdbeAddOp2(Vdbe*,int,int,int);
int sqlitexVdbeAddOp3(Vdbe*,int,int,int,int);
int sqlitexVdbeAddOp4(Vdbe*,int,int,int,int,const char *zP4,int);
int sqlitexVdbeAddOp4Dup8(Vdbe*,int,int,int,int,const u8*,int);
int sqlitexVdbeAddOp4Int(Vdbe*,int,int,int,int,int);
int sqlitexVdbeAddOpList(Vdbe*, int nOp, VdbeOpList const *aOp, int iLineno);
void sqlitexVdbeAddParseSchemaOp(Vdbe*,int,char*);
void sqlitexVdbeAddTable(Vdbe*,Table*);
void sqlitexVdbeChangeP1(Vdbe*, u32 addr, int P1);
void sqlitexVdbeChangeP2(Vdbe*, u32 addr, int P2);
void sqlitexVdbeChangeP3(Vdbe*, u32 addr, int P3);
void sqlitexVdbeChangeP5(Vdbe*, u8 P5);
void sqlitexVdbeJumpHere(Vdbe*, int addr);
void sqlitexVdbeChangeToNoop(Vdbe*, int addr);
int sqlitexVdbeDeletePriorOpcode(Vdbe*, u8 op);
void sqlitexVdbeChangeP4(Vdbe*, int addr, const char *zP4, int N);
void sqlitexVdbeSetP4KeyInfo(Parse*, Index*);
void sqlitexVdbeUsesBtree(Vdbe*, int);
VdbeOp *sqlitexVdbeGetOp(Vdbe*, int);
int sqlitexVdbeMakeLabel(Vdbe*);
void sqlitexVdbeRunOnlyOnce(Vdbe*);
void sqlitexVdbeDelete(Vdbe*);
void sqlitexVdbeClearObject(sqlitex*,Vdbe*);
void sqlitexVdbeMakeReady(Vdbe*,Parse*);
int sqlitexVdbeFinalize(Vdbe*);
void sqlitexVdbeResolveLabel(Vdbe*, int);
int sqlitexVdbeCurrentAddr(Vdbe*);
#ifdef SQLITE_DEBUG
  int sqlitexVdbeAssertMayAbort(Vdbe *, int);
#endif
void sqlitexVdbeResetStepResult(Vdbe*);
void sqlitexVdbeRewind(Vdbe*);
int sqlitexVdbeReset(Vdbe*);
/* COMDB2 Modification. */
int sqlitexVdbeResetClock(Vdbe*);
void sqlitexVdbeSetNumCols(Vdbe*,int);
int sqlitexVdbeSetColName(Vdbe*, int, int, const char *, void(*)(void*));
void sqlitexVdbeCountChanges(Vdbe*);
sqlitex *sqlitexVdbeDb(Vdbe*);
void sqlitexVdbeSetSql(Vdbe*, const char *z, int n, int);
#ifdef SQLITE_ENABLE_NORMALIZE
void sqlitexVdbeAddDblquoteStr(sqlitex*,Vdbe*,const char*);
int sqlitexVdbeUsesDoubleQuotedString(Vdbe*,const char*);
#endif
void sqlitexVdbeSwap(Vdbe*,Vdbe*);
VdbeOp *sqlitexVdbeTakeOpArray(Vdbe*, int*, int*);
sqlitex_value *sqlitexVdbeGetBoundValue(Vdbe*, int, u8);
void sqlitexVdbeSetVarmask(Vdbe*, int);
#ifndef SQLITE_OMIT_TRACE
  char *sqlitexVdbeExpandSql(Vdbe*, const char*);
#endif
static inline int sqlitexMemCompare(const Mem*, const Mem*, const CollSeq*);

void sqlitexVdbeRecordUnpack(KeyInfo*,int,const void*,UnpackedRecord*);
static inline int sqlitexVdbeRecordCompare(int,const void*,UnpackedRecord*);
UnpackedRecord *sqlitexVdbeAllocUnpackedRecord(KeyInfo *, char *, int, char **);

typedef int (*RecordCompare)(int,const void*,UnpackedRecord*,int);
RecordCompare sqlitexVdbeFindCompare(UnpackedRecord*);

#ifndef SQLITE_OMIT_TRIGGER
void sqlitexVdbeLinkSubProgram(Vdbe *, SubProgram *);
#endif

/* Use SQLITE_ENABLE_COMMENTS to enable generation of extra comments on
** each VDBE opcode.
**
** Use the SQLITE_ENABLE_MODULE_COMMENTS macro to see some extra no-op
** comments in VDBE programs that show key decision points in the code
** generator.
*/
#ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
  void sqlitexVdbeComment(Vdbe*, const char*, ...);
# define VdbeComment(X)  sqlitexVdbeComment X
  void sqlitexVdbeNoopComment(Vdbe*, const char*, ...);
# define VdbeNoopComment(X)  sqlitexVdbeNoopComment X
# ifdef SQLITE_ENABLE_MODULE_COMMENTS
#   define VdbeModuleComment(X)  sqlitexVdbeNoopComment X
# else
#   define VdbeModuleComment(X)
# endif
#else
# define VdbeComment(X)
# define VdbeNoopComment(X)
# define VdbeModuleComment(X)
#endif

/*
** The VdbeCoverage macros are used to set a coverage testing point
** for VDBE branch instructions.  The coverage testing points are line
** numbers in the sqlitex.c source file.  VDBE branch coverage testing
** only works with an amalagmation build.  That's ok since a VDBE branch
** coverage build designed for testing the test suite only.  No application
** should ever ship with VDBE branch coverage measuring turned on.
**
**    VdbeCoverage(v)                  // Mark the previously coded instruction
**                                     // as a branch
**
**    VdbeCoverageIf(v, conditional)   // Mark previous if conditional true
**
**    VdbeCoverageAlwaysTaken(v)       // Previous branch is always taken
**
**    VdbeCoverageNeverTaken(v)        // Previous branch is never taken
**
** Every VDBE branch operation must be tagged with one of the macros above.
** If not, then when "make test" is run with -DSQLITE_VDBE_COVERAGE and
** -DSQLITE_DEBUG then an ALWAYS() will fail in the vdbeTakeBranch()
** routine in vdbe.c, alerting the developer to the missed tag.
*/
#ifdef SQLITE_VDBE_COVERAGE
  void sqlitexVdbeSetLineNumber(Vdbe*,int);
# define VdbeCoverage(v) sqlitexVdbeSetLineNumber(v,__LINE__)
# define VdbeCoverageIf(v,x) if(x)sqlitexVdbeSetLineNumber(v,__LINE__)
# define VdbeCoverageAlwaysTaken(v) sqlitexVdbeSetLineNumber(v,2);
# define VdbeCoverageNeverTaken(v) sqlitexVdbeSetLineNumber(v,1);
# define VDBE_OFFSET_LINENO(x) (__LINE__+x)
#else
# define VdbeCoverage(v)
# define VdbeCoverageIf(v,x)
# define VdbeCoverageAlwaysTaken(v)
# define VdbeCoverageNeverTaken(v)
# define VDBE_OFFSET_LINENO(x) 0
#endif

#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
void sqlitexVdbeScanStatus(Vdbe*, int, int, int, LogEst, const char*);
#else
# define sqlitexVdbeScanStatus(a,b,c,d,e)
#endif

#endif
