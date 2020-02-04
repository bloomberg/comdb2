/*
** 2003 September 6
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains code used for creating, destroying, and populating
** a VDBE (or an "sqlitex_stmt" as it is known to the outside world.) 
*/
#define START_INLINE_SERIALGET
#include <stdint.h>
#include <stddef.h>
#include "sqliteInt.h"
#include "vdbeInt.h"
#include "comdb2vdbe.h"
#define END_INLINE_SERIALGET
#include <sys/types.h>



/*
** Create a new virtual database engine.
*/
Vdbe *sqlitexVdbeCreate(Parse *pParse){
  sqlitex *db = pParse->db;
  Vdbe *p;
  p = sqlitexDbMallocZero(db, sizeof(Vdbe) );
  if( p==0 ) return 0;
  p->db = db;
  if( db->pVdbe ){
    db->pVdbe->pPrev = p;
  }
  p->pNext = db->pVdbe;
  p->pPrev = 0;

  /* COMDB2 MODIFICATION */
  p->updCols = 0;
  p->tbls = NULL;
  p->lockInfo = NULL;
  p->numTables = 0;
  /* due to stat4, we also need tzname during prepare
     set this here before starting to build anything */
  extern void  comdb2_set_sqlite_vdbe_tzname( Vdbe *p);
  comdb2_set_sqlite_vdbe_tzname(p);  

  db->pVdbe = p;
  p->magic = VDBE_MAGIC_INIT;
  p->pParse = pParse;
  assert( pParse->aLabel==0 );
  assert( pParse->nLabel==0 );
  assert( pParse->nOpAlloc==0 );
  return p;
}

/*
** Remember the SQL string for a prepared statement.
*/
void sqlitexVdbeSetSql(Vdbe *p, const char *z, int n, int isPrepareV2){
  assert( isPrepareV2==1 || isPrepareV2==0 );
  if( p==0 ) return;
#if defined(SQLITE_OMIT_TRACE) && !defined(SQLITE_ENABLE_SQLLOG)
  if( !isPrepareV2 ) return;
#endif
  assert( p->zSql==0 );
  p->zSql = sqlitexDbStrNDup(p->db, z, n);
  p->isPrepareV2 = (u8)isPrepareV2;
}

/*
** Return the SQL associated with a prepared statement
*/
const char *sqlitex_sql(sqlitex_stmt *pStmt){
  Vdbe *p = (Vdbe *)pStmt;
#if defined(SQLITE_OMIT_TRACE) && !defined(SQLITE_ENABLE_SQLLOG)
  return (p && p->isPrepareV2) ? p->zSql : 0;
#else
  return (p) ? p->zSql : 0;
#endif
}

#ifdef SQLITE_ENABLE_NORMALIZE
/*
** Return the normalized SQL associated with a prepared statement.
*/
const char *sqlitex_normalized_sql(sqlitex_stmt *pStmt){
  Vdbe *p = (Vdbe *)pStmt;
  if( p==0 ) return 0;
  if( p->zNormSql==0 && ALWAYS(p->zSql!=0) ){
    sqlitex_mutex_enter(p->db->mutex);
    p->zNormSql = sqlitexNormalize(p, p->zSql);
    sqlitex_mutex_leave(p->db->mutex);
  }
  return p->zNormSql;
}
#endif /* SQLITE_ENABLE_NORMALIZE */

#ifdef SQLITE_ENABLE_NORMALIZE
/*
** Add a new element to the Vdbe->pDblStr list.
*/
void sqlitexVdbeAddDblquoteStr(sqlitex *db, Vdbe *p, const char *z){
  if( p ){
    int n = sqlitexStrlen30(z);
    DblquoteStr *pStr = sqlitexDbMallocRaw(db,
                            sizeof(*pStr)+n+1-sizeof(pStr->z));
    if( pStr ){
      pStr->pNextStr = p->pDblStr;
      p->pDblStr = pStr;
      memcpy(pStr->z, z, n+1);
    }
  }
}
#endif

#ifdef SQLITE_ENABLE_NORMALIZE
/*
** zId of length nId is a double-quoted identifier.  Check to see if
** that identifier is really used as a string literal.
*/
int sqlitexVdbeUsesDoubleQuotedString(
  Vdbe *pVdbe,            /* The prepared statement */
  const char *zId         /* The double-quoted identifier */
){
  DblquoteStr *pStr;
  assert( zId!=0 );
  if( pVdbe->pDblStr==0 ) return 0;
  for(pStr=pVdbe->pDblStr; pStr; pStr=pStr->pNextStr){
    if( strcmp(zId, pStr->z)==0 ) return 1;
  }
  return 0;
}
#endif

/*
** Swap all content between two VDBE structures.
*/
void sqlitexVdbeSwap(Vdbe *pA, Vdbe *pB){
  Vdbe tmp, *pTmp;
  char *zTmp;
  int *iTmp;

  tmp = *pA;
  *pA = *pB;
  *pB = tmp;
  pTmp = pA->pNext;
  pA->pNext = pB->pNext;
  pB->pNext = pTmp;
  pTmp = pA->pPrev;
  pA->pPrev = pB->pPrev;
  pB->pPrev = pTmp;
  zTmp = pA->zSql;
  pA->zSql = pB->zSql;
  pB->zSql = zTmp;
#if 0
  zTmp = pA->zNormSql;
  pA->zNormSql = pB->zNormSql;
  pB->zNormSql = zTmp;
#endif

  /* COMDB2 Modification */
  iTmp = pA->updCols;
  pA->updCols = pB->updCols;
  pB->updCols = iTmp;

  pB->isPrepareV2 = pA->isPrepareV2;
}

/*
** Resize the Vdbe.aOp array so that it is at least nOp elements larger 
** than its current size. nOp is guaranteed to be less than or equal
** to 1024/sizeof(Op).
**
** If an out-of-memory error occurs while resizing the array, return
** SQLITE_NOMEM. In this case Vdbe.aOp and Parse.nOpAlloc remain 
** unchanged (this is so that any opcodes already allocated can be 
** correctly deallocated along with the rest of the Vdbe).
*/
static int growOpArray(Vdbe *v, int nOp){
  VdbeOp *pNew;
  Parse *p = v->pParse;

  /* The SQLITE_TEST_REALLOC_STRESS compile-time option is designed to force
  ** more frequent reallocs and hence provide more opportunities for 
  ** simulated OOM faults.  SQLITE_TEST_REALLOC_STRESS is generally used
  ** during testing only.  With SQLITE_TEST_REALLOC_STRESS grow the op array
  ** by the minimum* amount required until the size reaches 512.  Normal
  ** operation (without SQLITE_TEST_REALLOC_STRESS) is to double the current
  ** size of the op array or add 1KB of space, whichever is smaller. */
#ifdef SQLITE_TEST_REALLOC_STRESS
  int nNew = (p->nOpAlloc>=512 ? p->nOpAlloc*2 : p->nOpAlloc+nOp);
#else
  int nNew = (p->nOpAlloc ? p->nOpAlloc*2 : (int)(1024/sizeof(Op)));
  UNUSED_PARAMETER(nOp);
#endif

  assert( nOp<=(1024/sizeof(Op)) );
  assert( nNew>=(p->nOpAlloc+nOp) );
  pNew = sqlitexDbRealloc(p->db, v->aOp, nNew*sizeof(Op));
  if( pNew ){
    p->nOpAlloc = sqlitexDbMallocSize(p->db, pNew)/sizeof(Op);
    v->aOp = pNew;
  }
  return (pNew ? SQLITE_OK : SQLITE_NOMEM);
}

#ifdef SQLITE_DEBUG
/* This routine is just a convenient place to set a breakpoint that will
** fire after each opcode is inserted and displayed using
** "PRAGMA vdbe_addoptrace=on".
*/
static void test_addop_breakpoint(void){
  static int n = 0;
  n++;
}
#endif

/*
** Add a new instruction to the list of instructions current in the
** VDBE.  Return the address of the new instruction.
**
** Parameters:
**
**    p               Pointer to the VDBE
**
**    op              The opcode for this instruction
**
**    p1, p2, p3      Operands
**
** Use the sqlitexVdbeResolveLabel() function to fix an address and
** the sqlitexVdbeChangeP4() function to change the value of the P4
** operand.
*/
int sqlitexVdbeAddOp3(Vdbe *p, int op, int p1, int p2, int p3){
  int i;
  VdbeOp *pOp;

  i = p->nOp;
  assert( p->magic==VDBE_MAGIC_INIT );
  assert( op>0 && op<0xff );
  if( p->pParse->nOpAlloc<=i ){
    if( growOpArray(p, 1) ){
      return 1;
    }
  }
  p->nOp++;
  pOp = &p->aOp[i];
  pOp->opcode = (u8)op;
  pOp->p5 = 0;
  pOp->p1 = p1;
  pOp->p2 = p2;
  pOp->p3 = p3;
  pOp->p4.p = 0;
  pOp->p4type = P4_NOTUSED;
#ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
  pOp->zComment = 0;
#endif
#ifdef SQLITE_DEBUG
  if( p->db->flags & SQLITE_VdbeAddopTrace ){
    int jj, kk;
    Parse *pParse = p->pParse;
    for(jj=kk=0; jj<SQLITE_N_COLCACHE; jj++){
      struct yColCache *x = pParse->aColCache + jj;
      if( x->iLevel>pParse->iCacheLevel || x->iReg==0 ) continue;
      printf(" r[%d]={%d:%d}", x->iReg, x->iTable, x->iColumn);
      kk++;
    }
    if( kk ) printf("\n");
    sqlitexVdbePrintOp(0, i, &p->aOp[i]);
    test_addop_breakpoint();
  }
#endif
#ifdef VDBE_PROFILE
  pOp->cycles = 0;
  pOp->cnt = 0;
#endif
#ifdef SQLITE_VDBE_COVERAGE
  pOp->iSrcLine = 0;
#endif
  return i;
}
int sqlitexVdbeAddOp0(Vdbe *p, int op){
  return sqlitexVdbeAddOp3(p, op, 0, 0, 0);
}
int sqlitexVdbeAddOp1(Vdbe *p, int op, int p1){
  return sqlitexVdbeAddOp3(p, op, p1, 0, 0);
}
int sqlitexVdbeAddOp2(Vdbe *p, int op, int p1, int p2){
  return sqlitexVdbeAddOp3(p, op, p1, p2, 0);
}


/*
** Add an opcode that includes the p4 value as a pointer.
*/
int sqlitexVdbeAddOp4(
  Vdbe *p,            /* Add the opcode to this VM */
  int op,             /* The new opcode */
  int p1,             /* The P1 operand */
  int p2,             /* The P2 operand */
  int p3,             /* The P3 operand */
  const char *zP4,    /* The P4 operand */
  int p4type          /* P4 operand type */
){
  int addr = sqlitexVdbeAddOp3(p, op, p1, p2, p3);
  sqlitexVdbeChangeP4(p, addr, zP4, p4type);
  return addr;
}

/*
** Add an opcode that includes the p4 value with a P4_INT64 type.
*/
int sqlitexVdbeAddOp4Dup8(
  Vdbe *p,            /* Add the opcode to this VM */
  int op,             /* The new opcode */
  int p1,             /* The P1 operand */
  int p2,             /* The P2 operand */
  int p3,             /* The P3 operand */
  const u8 *zP4,      /* The P4 operand */
  int p4type          /* P4 operand type */
){
  char *p4copy = sqlitexDbMallocRaw(sqlitexVdbeDb(p), 8);
  if( p4copy ) memcpy(p4copy, zP4, 8);
  return sqlitexVdbeAddOp4(p, op, p1, p2, p3, p4copy, p4type);
}

/*
** Add an OP_ParseSchema opcode.  This routine is broken out from
** sqlitexVdbeAddOp4() since it needs to also needs to mark all btrees
** as having been used.
**
** The zWhere string must have been obtained from sqlitex_malloc().
** This routine will take ownership of the allocated memory.
*/
void sqlitexVdbeAddParseSchemaOp(Vdbe *p, int iDb, char *zWhere){
  int j;
  int addr = sqlitexVdbeAddOp3(p, OP_ParseSchema, iDb, 0, 0);
  sqlitexVdbeChangeP4(p, addr, zWhere, P4_DYNAMIC);
  for(j=0; j<p->db->nDb; j++) sqlitexVdbeUsesBtree(p, j);
}

/*
** Add an opcode that includes the p4 value as an integer.
*/
int sqlitexVdbeAddOp4Int(
  Vdbe *p,            /* Add the opcode to this VM */
  int op,             /* The new opcode */
  int p1,             /* The P1 operand */
  int p2,             /* The P2 operand */
  int p3,             /* The P3 operand */
  int p4              /* The P4 operand as an integer */
){
  int addr = sqlitexVdbeAddOp3(p, op, p1, p2, p3);
  sqlitexVdbeChangeP4(p, addr, SQLITE_INT_TO_PTR(p4), P4_INT32);
  return addr;
}

/*
** Create a new symbolic label for an instruction that has yet to be
** coded.  The symbolic label is really just a negative number.  The
** label can be used as the P2 value of an operation.  Later, when
** the label is resolved to a specific address, the VDBE will scan
** through its operation list and change all values of P2 which match
** the label into the resolved address.
**
** The VDBE knows that a P2 value is a label because labels are
** always negative and P2 values are suppose to be non-negative.
** Hence, a negative P2 value is a label that has yet to be resolved.
**
** Zero is returned if a malloc() fails.
*/
int sqlitexVdbeMakeLabel(Vdbe *v){
  Parse *p = v->pParse;
  int i = p->nLabel++;
  assert( v->magic==VDBE_MAGIC_INIT );
  if( (i & (i-1))==0 ){
    p->aLabel = sqlitexDbReallocOrFree(p->db, p->aLabel, 
                                       (i*2+1)*sizeof(p->aLabel[0]));
  }
  if( p->aLabel ){
    p->aLabel[i] = -1;
  }
  return -1-i;
}

/*
** Resolve label "x" to be the address of the next instruction to
** be inserted.  The parameter "x" must have been obtained from
** a prior call to sqlitexVdbeMakeLabel().
*/
void sqlitexVdbeResolveLabel(Vdbe *v, int x){
  Parse *p = v->pParse;
  int j = -1-x;
  assert( v->magic==VDBE_MAGIC_INIT );
  assert( j<p->nLabel );
  if( ALWAYS(j>=0) && p->aLabel ){
    p->aLabel[j] = v->nOp;
  }
  p->iFixedOp = v->nOp - 1;
}

/*
** Mark the VDBE as one that can only be run one time.
*/
void sqlitexVdbeRunOnlyOnce(Vdbe *p){
  p->runOnlyOnce = 1;
}

#ifdef SQLITE_DEBUG /* sqlitexAssertMayAbort() logic */

/*
** The following type and function are used to iterate through all opcodes
** in a Vdbe main program and each of the sub-programs (triggers) it may 
** invoke directly or indirectly. It should be used as follows:
**
**   Op *pOp;
**   VdbeOpIter sIter;
**
**   memset(&sIter, 0, sizeof(sIter));
**   sIter.v = v;                            // v is of type Vdbe* 
**   while( (pOp = opIterNext(&sIter)) ){
**     // Do something with pOp
**   }
**   sqlitexDbFree(v->db, sIter.apSub);
** 
*/
typedef struct VdbeOpIter VdbeOpIter;
struct VdbeOpIter {
  Vdbe *v;                   /* Vdbe to iterate through the opcodes of */
  SubProgram **apSub;        /* Array of subprograms */
  int nSub;                  /* Number of entries in apSub */
  int iAddr;                 /* Address of next instruction to return */
  int iSub;                  /* 0 = main program, 1 = first sub-program etc. */
};
static Op *opIterNext(VdbeOpIter *p){
  Vdbe *v = p->v;
  Op *pRet = 0;
  Op *aOp;
  int nOp;

  if( p->iSub<=p->nSub ){

    if( p->iSub==0 ){
      aOp = v->aOp;
      nOp = v->nOp;
    }else{
      aOp = p->apSub[p->iSub-1]->aOp;
      nOp = p->apSub[p->iSub-1]->nOp;
    }
    assert( p->iAddr<nOp );

    pRet = &aOp[p->iAddr];
    p->iAddr++;
    if( p->iAddr==nOp ){
      p->iSub++;
      p->iAddr = 0;
    }
  
    if( pRet->p4type==P4_SUBPROGRAM ){
      int nByte = (p->nSub+1)*sizeof(SubProgram*);
      int j;
      for(j=0; j<p->nSub; j++){
        if( p->apSub[j]==pRet->p4.pProgram ) break;
      }
      if( j==p->nSub ){
        p->apSub = sqlitexDbReallocOrFree(v->db, p->apSub, nByte);
        if( !p->apSub ){
          pRet = 0;
        }else{
          p->apSub[p->nSub++] = pRet->p4.pProgram;
        }
      }
    }
  }

  return pRet;
}

/*
** Check if the program stored in the VM associated with pParse may
** throw an ABORT exception (causing the statement, but not entire transaction
** to be rolled back). This condition is true if the main program or any
** sub-programs contains any of the following:
**
**   *  OP_Halt with P1=SQLITE_CONSTRAINT and P2=OE_Abort.
**   *  OP_HaltIfNull with P1=SQLITE_CONSTRAINT and P2=OE_Abort.
**   *  OP_Destroy
**   *  OP_VUpdate
**   *  OP_VRename
**   *  OP_FkCounter with P2==0 (immediate foreign key constraint)
**
** Then check that the value of Parse.mayAbort is true if an
** ABORT may be thrown, or false otherwise. Return true if it does
** match, or false otherwise. This function is intended to be used as
** part of an assert statement in the compiler. Similar to:
**
**   assert( sqlitexVdbeAssertMayAbort(pParse->pVdbe, pParse->mayAbort) );
*/
int sqlitexVdbeAssertMayAbort(Vdbe *v, int mayAbort){
  int hasAbort = 0;
  int hasFkCounter = 0;
  Op *pOp;
  VdbeOpIter sIter;
  memset(&sIter, 0, sizeof(sIter));
  sIter.v = v;

  while( (pOp = opIterNext(&sIter))!=0 ){
    int opcode = pOp->opcode;
    if( opcode==OP_Destroy || opcode==OP_VUpdate || opcode==OP_VRename 
     || ((opcode==OP_Halt || opcode==OP_HaltIfNull) 
      && ((pOp->p1&0xff)==SQLITE_CONSTRAINT && pOp->p2==OE_Abort))
    ){
      hasAbort = 1;
      break;
    }
#ifndef SQLITE_OMIT_FOREIGN_KEY
    if( opcode==OP_FkCounter && pOp->p1==0 && pOp->p2==1 ){
      hasFkCounter = 1;
    }
#endif
  }
  sqlitexDbFree(v->db, sIter.apSub);

  /* Return true if hasAbort==mayAbort. Or if a malloc failure occurred.
  ** If malloc failed, then the while() loop above may not have iterated
  ** through all opcodes and hasAbort may be set incorrectly. Return
  ** true for this case to prevent the assert() in the callers frame
  ** from failing.  */
  return ( v->db->mallocFailed || hasAbort==mayAbort || hasFkCounter );
}
#endif /* SQLITE_DEBUG - the sqlitexAssertMayAbort() function */

/*
** Loop through the program looking for P2 values that are negative
** on jump instructions.  Each such value is a label.  Resolve the
** label by setting the P2 value to its correct non-zero value.
**
** This routine is called once after all opcodes have been inserted.
**
** Variable *pMaxFuncArgs is set to the maximum value of any P2 argument 
** to an OP_Function, OP_AggStep or OP_VFilter opcode. This is used by 
** sqlitexVdbeMakeReady() to size the Vdbe.apArg[] array.
**
** The Op.opflags field is set on all opcodes.
*/
static void resolveP2Values(Vdbe *p, int *pMaxFuncArgs){
  int i;
  int nMaxArgs = *pMaxFuncArgs;
  Op *pOp;
  Parse *pParse = p->pParse;
  int *aLabel = pParse->aLabel;
  p->readOnly = 1;
  p->bIsReader = 0;
  for(pOp=p->aOp, i=p->nOp-1; i>=0; i--, pOp++){
    u8 opcode = pOp->opcode;

    /* NOTE: Be sure to update mkopcodeh.awk when adding or removing
    ** cases from this switch! */
    switch( opcode ){
      case OP_Function:
      case OP_AggStep: {
        if( pOp->p5>nMaxArgs ) nMaxArgs = pOp->p5;
        break;
      }
      case OP_Transaction: {
        if( pOp->p2!=0 ) p->readOnly = 0;
        /* fall thru */
      }
      case OP_AutoCommit:
      case OP_Savepoint: {
        p->bIsReader = 1;
        break;
      }
#ifndef SQLITE_OMIT_WAL
      case OP_Checkpoint:
#endif
      case OP_Vacuum:
      case OP_JournalMode: {
        p->readOnly = 0;
        p->bIsReader = 1;
        break;
      }
#ifndef SQLITE_OMIT_VIRTUALTABLE
      case OP_VUpdate: {
        if( pOp->p2>nMaxArgs ) nMaxArgs = pOp->p2;
        break;
      }
      case OP_VFilter: {
        int n;
        assert( p->nOp - i >= 3 );
        assert( pOp[-1].opcode==OP_Integer );
        n = pOp[-1].p1;
        if( n>nMaxArgs ) nMaxArgs = n;
        break;
      }
#endif
      case OP_Next:
      case OP_NextIfOpen:
      case OP_SorterNext: {
        pOp->p4.xAdvance = sqlite3BtreeNext;
        pOp->p4type = P4_ADVANCE;
        break;
      }
      case OP_Prev:
      case OP_PrevIfOpen: {
        pOp->p4.xAdvance = sqlite3BtreePrevious;
        pOp->p4type = P4_ADVANCE;
        break;
      }
    }

    pOp->opflags = sqlitexOpcodeProperty[opcode];
    if( (pOp->opflags & OPFLG_JUMP)!=0 && pOp->p2<0 ){
      assert( -1-pOp->p2<pParse->nLabel );
      pOp->p2 = aLabel[-1-pOp->p2];
    }
  }
  sqlitexDbFree(p->db, pParse->aLabel);
  pParse->aLabel = 0;
  pParse->nLabel = 0;
  *pMaxFuncArgs = nMaxArgs;
  assert( p->bIsReader!=0 || DbMaskAllZero(p->btreeMask) );
}

/*
** Return the address of the next instruction to be inserted.
*/
int sqlitexVdbeCurrentAddr(Vdbe *p){
  assert( p->magic==VDBE_MAGIC_INIT );
  return p->nOp;
}

/*
** This function returns a pointer to the array of opcodes associated with
** the Vdbe passed as the first argument. It is the callers responsibility
** to arrange for the returned array to be eventually freed using the 
** vdbeFreeOpArray() function.
**
** Before returning, *pnOp is set to the number of entries in the returned
** array. Also, *pnMaxArg is set to the larger of its current value and 
** the number of entries in the Vdbe.apArg[] array required to execute the 
** returned program.
*/
VdbeOp *sqlitexVdbeTakeOpArray(Vdbe *p, int *pnOp, int *pnMaxArg){
  VdbeOp *aOp = p->aOp;
  assert( aOp && !p->db->mallocFailed );

  /* Check that sqlitexVdbeUsesBtree() was not called on this VM */
  assert( DbMaskAllZero(p->btreeMask) );

  resolveP2Values(p, pnMaxArg);
  *pnOp = p->nOp;
  p->aOp = 0;
  return aOp;
}

/*
** Add a whole list of operations to the operation stack.  Return the
** address of the first operation added.
*/
int sqlitexVdbeAddOpList(Vdbe *p, int nOp, VdbeOpList const *aOp, int iLineno){
  int addr;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p->nOp + nOp > p->pParse->nOpAlloc && growOpArray(p, nOp) ){
    return 0;
  }
  addr = p->nOp;
  if( ALWAYS(nOp>0) ){
    int i;
    VdbeOpList const *pIn = aOp;
    for(i=0; i<nOp; i++, pIn++){
      int p2 = pIn->p2;
      VdbeOp *pOut = &p->aOp[i+addr];
      pOut->opcode = pIn->opcode;
      pOut->p1 = pIn->p1;
      if( p2<0 ){
        assert( sqlitexOpcodeProperty[pOut->opcode] & OPFLG_JUMP );
        pOut->p2 = addr + ADDR(p2);
      }else{
        pOut->p2 = p2;
      }
      pOut->p3 = pIn->p3;
      pOut->p4type = P4_NOTUSED;
      pOut->p4.p = 0;
      pOut->p5 = 0;
#ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
      pOut->zComment = 0;
#endif
#ifdef SQLITE_VDBE_COVERAGE
      pOut->iSrcLine = iLineno+i;
#else
      (void)iLineno;
#endif
#ifdef SQLITE_DEBUG
      if( p->db->flags & SQLITE_VdbeAddopTrace ){
        sqlitexVdbePrintOp(0, i+addr, &p->aOp[i+addr]);
      }
#endif
    }
    p->nOp += nOp;
  }
  return addr;
}

#if defined(SQLITE_ENABLE_STMT_SCANSTATUS)
/*
** Add an entry to the array of counters managed by sqlitex_stmt_scanstatus().
*/
void sqlitexVdbeScanStatus(
  Vdbe *p,                        /* VM to add scanstatus() to */
  int addrExplain,                /* Address of OP_Explain (or 0) */
  int addrLoop,                   /* Address of loop counter */ 
  int addrVisit,                  /* Address of rows visited counter */
  LogEst nEst,                    /* Estimated number of output rows */
  const char *zName               /* Name of table or index being scanned */
){
  int nByte = (p->nScan+1) * sizeof(ScanStatus);
  ScanStatus *aNew;
  aNew = (ScanStatus*)sqlitexDbRealloc(p->db, p->aScan, nByte);
  if( aNew ){
    ScanStatus *pNew = &aNew[p->nScan++];
    pNew->addrExplain = addrExplain;
    pNew->addrLoop = addrLoop;
    pNew->addrVisit = addrVisit;
    pNew->nEst = nEst;
    pNew->zName = sqlitexDbStrDup(p->db, zName);
    p->aScan = aNew;
  }
}
#endif


/*
** Change the value of the P1 operand for a specific instruction.
** This routine is useful when a large program is loaded from a
** static array using sqlitexVdbeAddOpList but we want to make a
** few minor changes to the program.
*/
void sqlitexVdbeChangeP1(Vdbe *p, u32 addr, int val){
  assert( p!=0 );
  if( ((u32)p->nOp)>addr ){
    p->aOp[addr].p1 = val;
  }
}

/*
** Change the value of the P2 operand for a specific instruction.
** This routine is useful for setting a jump destination.
*/
void sqlitexVdbeChangeP2(Vdbe *p, u32 addr, int val){
  assert( p!=0 );
  if( ((u32)p->nOp)>addr ){
    p->aOp[addr].p2 = val;
  }
}

/*
** Change the value of the P3 operand for a specific instruction.
*/
void sqlitexVdbeChangeP3(Vdbe *p, u32 addr, int val){
  assert( p!=0 );
  if( ((u32)p->nOp)>addr ){
    p->aOp[addr].p3 = val;
  }
}

/*
** Change the value of the P5 operand for the most recently
** added operation.
*/
void sqlitexVdbeChangeP5(Vdbe *p, u8 val){
  assert( p!=0 );
  if( p->aOp ){
    assert( p->nOp>0 );
    p->aOp[p->nOp-1].p5 = val;
  }
}

/*
** Change the P2 operand of instruction addr so that it points to
** the address of the next instruction to be coded.
*/
void sqlitexVdbeJumpHere(Vdbe *p, int addr){
  sqlitexVdbeChangeP2(p, addr, p->nOp);
  p->pParse->iFixedOp = p->nOp - 1;
}


/*
** If the input FuncDef structure is ephemeral, then free it.  If
** the FuncDef is not ephermal, then do nothing.
*/
static void freeEphemeralFunction(sqlitex *db, FuncDef *pDef){
  if( ALWAYS(pDef) && (pDef->funcFlags & SQLITE_FUNC_EPHEM)!=0 ){
    sqlitexDbFree(db, pDef);
  }
}

static void vdbeFreeOpArray(sqlitex *, Op *, int);

/*
** Delete a P4 value if necessary.
*/
static void freeP4(sqlitex *db, int p4type, void *p4){
  if( p4 ){
    assert( db );
    switch( p4type ){
      case P4_REAL:
      case P4_INT64:
      case P4_DYNAMIC:
      case P4_INTARRAY: {
        sqlitexDbFree(db, p4);
        break;
      }
      case P4_KEYINFO: {
        if( db->pnBytesFreed==0 ) sqlitexKeyInfoUnref((KeyInfo*)p4);
        break;
      }
#ifdef SQLITE_ENABLE_CURSOR_HINTS
      case P4_EXPR: {
        sqlitexExprDelete(db, (Expr*)p4);
        break;
      }
#endif
      case P4_MPRINTF: {
        if( db->pnBytesFreed==0 ) sqlitex_free(p4);
        break;
      }
      case P4_FUNCDEF: {
        freeEphemeralFunction(db, (FuncDef*)p4);
        break;
      }
      case P4_MEM: {
        if( db->pnBytesFreed==0 ){
          sqlitexValueFree((sqlitex_value*)p4);
        }else{
          Mem *p = (Mem*)p4;
          if( p->szMalloc ) sqlitexDbFree(db, p->zMalloc);
          sqlitexDbFree(db, p);
        }
        break;
      }
      case P4_VTAB : {
        if( db->pnBytesFreed==0 ) sqlitexVtabUnlock((VTable *)p4);
        break;
      }
      case P4_OPFUNC: { /* COMDB2 CUSTOMIZATION */
        OpFunc *func = (OpFunc*) p4;
        freeOpFuncX(func);
      }
    }
  }
}

/*
** Free the space allocated for aOp and any p4 values allocated for the
** opcodes contained within. If aOp is not NULL it is assumed to contain 
** nOp entries. 
*/
static void vdbeFreeOpArray(sqlitex *db, Op *aOp, int nOp){
  if( aOp ){
    Op *pOp;
    for(pOp=aOp; pOp<&aOp[nOp]; pOp++){
      freeP4(db, pOp->p4type, pOp->p4.p);
#ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
      sqlitexDbFree(db, pOp->zComment);
#endif     
    }
  }
  sqlitexDbFree(db, aOp);
}

/*
** Link the SubProgram object passed as the second argument into the linked
** list at Vdbe.pSubProgram. This list is used to delete all sub-program
** objects when the VM is no longer required.
*/
void sqlitexVdbeLinkSubProgram(Vdbe *pVdbe, SubProgram *p){
  p->pNext = pVdbe->pProgram;
  pVdbe->pProgram = p;
}

/*
** Change the opcode at addr into OP_Noop
*/
void sqlitexVdbeChangeToNoop(Vdbe *p, int addr){
  if( addr<p->nOp ){
    VdbeOp *pOp = &p->aOp[addr];
    sqlitex *db = p->db;
    freeP4(db, pOp->p4type, pOp->p4.p);
    memset(pOp, 0, sizeof(pOp[0]));
    pOp->opcode = OP_Noop;
    if( addr==p->nOp-1 ) p->nOp--;
  }
}

/*
** If the last opcode is "op" and it is not a jump destination,
** then remove it.  Return true if and only if an opcode was removed.
*/
int sqlitexVdbeDeletePriorOpcode(Vdbe *p, u8 op){
  if( (p->nOp-1)>(p->pParse->iFixedOp) && p->aOp[p->nOp-1].opcode==op ){
    sqlitexVdbeChangeToNoop(p, p->nOp-1);
    return 1;
  }else{
    return 0;
  }
}

/*
** Change the value of the P4 operand for a specific instruction.
** This routine is useful when a large program is loaded from a
** static array using sqlitexVdbeAddOpList but we want to make a
** few minor changes to the program.
**
** If n>=0 then the P4 operand is dynamic, meaning that a copy of
** the string is made into memory obtained from sqlitex_malloc().
** A value of n==0 means copy bytes of zP4 up to and including the
** first null byte.  If n>0 then copy n+1 bytes of zP4.
** 
** Other values of n (P4_STATIC, P4_COLLSEQ etc.) indicate that zP4 points
** to a string or structure that is guaranteed to exist for the lifetime of
** the Vdbe. In these cases we can just copy the pointer.
**
** If addr<0 then change P4 on the most recently inserted instruction.
*/
void sqlitexVdbeChangeP4(Vdbe *p, int addr, const char *zP4, int n){
  Op *pOp;
  sqlitex *db;
  assert( p!=0 );
  db = p->db;
  assert( p->magic==VDBE_MAGIC_INIT );
  if( p->aOp==0 || db->mallocFailed ){
    if( n!=P4_VTAB ){
      freeP4(db, n, (void*)*(char**)&zP4);
    }
    return;
  }
  assert( p->nOp>0 );
  assert( addr<p->nOp );
  if( addr<0 ){
    addr = p->nOp - 1;
  }
  pOp = &p->aOp[addr];
  assert( pOp->p4type==P4_NOTUSED
       || pOp->p4type==P4_INT32
       || pOp->p4type==P4_KEYINFO );
  freeP4(db, pOp->p4type, pOp->p4.p);
  pOp->p4.p = 0;
  if( n==P4_INT32 ){
    /* Note: this cast is safe, because the origin data point was an int
    ** that was cast to a (const char *). */
    pOp->p4.i = SQLITE_PTR_TO_INT(zP4);
    pOp->p4type = P4_INT32;
  }else if( zP4==0 ){
    pOp->p4.p = 0;
    pOp->p4type = P4_NOTUSED;
  }else if( n==P4_KEYINFO ){
    pOp->p4.p = (void*)zP4;
    pOp->p4type = P4_KEYINFO;
#ifdef SQLITE_ENABLE_CURSOR_HINTS
  }else if( n==P4_EXPR ){
    /* Responsibility for deleting the Expr tree is handed over to the
    ** VDBE by this operation.  The caller should have already invoked
    ** sqlitexExprDup() or whatever other routine is needed to make a 
    ** private copy of the tree. */
    pOp->p4.pExpr = (Expr*)zP4;
    pOp->p4type = P4_EXPR;
#endif
  }else if( n==P4_VTAB ){
    pOp->p4.p = (void*)zP4;
    pOp->p4type = P4_VTAB;
    sqlitexVtabLock((VTable *)zP4);
    assert( ((VTable *)zP4)->db==p->db );
  }else if (n == P4_OPFUNC) /* COMDB2 CUSTOMIZATION */
  {
    pOp->p4.comdb2func = (OpFunc*) zP4;
    pOp->p4type = n;
  }else if( n<0 ){
    pOp->p4.p = (void*)zP4;
    pOp->p4type = (signed char)n;
  }else{
    if( n==0 ) n = sqlitexStrlen30(zP4);
    pOp->p4.z = sqlitexDbStrNDup(p->db, zP4, n);
    pOp->p4type = P4_DYNAMIC;
  }
}

/*
** Set the P4 on the most recently added opcode to the KeyInfo for the
** index given.
*/
void sqlitexVdbeSetP4KeyInfo(Parse *pParse, Index *pIdx){
  Vdbe *v = pParse->pVdbe;
  assert( v!=0 );
  assert( pIdx!=0 );
  sqlitexVdbeChangeP4(v, -1, (char*)sqlitexKeyInfoOfIndex(pParse, pIdx),
                      P4_KEYINFO);
}

#ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
/*
** Change the comment on the most recently coded instruction.  Or
** insert a No-op and add the comment to that new instruction.  This
** makes the code easier to read during debugging.  None of this happens
** in a production build.
*/
static void vdbeVComment(Vdbe *p, const char *zFormat, va_list ap){
  assert( p->nOp>0 || p->aOp==0 );
  assert( p->aOp==0 || p->aOp[p->nOp-1].zComment==0 || p->db->mallocFailed );
  if( p->nOp ){
    assert( p->aOp );
    sqlitexDbFree(p->db, p->aOp[p->nOp-1].zComment);
    p->aOp[p->nOp-1].zComment = sqlitexVMPrintf(p->db, zFormat, ap);
  }
}
void sqlitexVdbeComment(Vdbe *p, const char *zFormat, ...){
  va_list ap;
  if( p ){
    va_start(ap, zFormat);
    vdbeVComment(p, zFormat, ap);
    va_end(ap);
  }
}
void sqlitexVdbeNoopComment(Vdbe *p, const char *zFormat, ...){
  va_list ap;
  if( p ){
    sqlitexVdbeAddOp0(p, OP_Noop);
    va_start(ap, zFormat);
    vdbeVComment(p, zFormat, ap);
    va_end(ap);
  }
}
#endif  /* NDEBUG */

#ifdef SQLITE_VDBE_COVERAGE
/*
** Set the value if the iSrcLine field for the previously coded instruction.
*/
void sqlitexVdbeSetLineNumber(Vdbe *v, int iLine){
  sqlitexVdbeGetOp(v,-1)->iSrcLine = iLine;
}
#endif /* SQLITE_VDBE_COVERAGE */

/*
** Return the opcode for a given address.  If the address is -1, then
** return the most recently inserted opcode.
**
** If a memory allocation error has occurred prior to the calling of this
** routine, then a pointer to a dummy VdbeOp will be returned.  That opcode
** is readable but not writable, though it is cast to a writable value.
** The return of a dummy opcode allows the call to continue functioning
** after an OOM fault without having to check to see if the return from 
** this routine is a valid pointer.  But because the dummy.opcode is 0,
** dummy will never be written to.  This is verified by code inspection and
** by running with Valgrind.
*/
VdbeOp *sqlitexVdbeGetOp(Vdbe *p, int addr){
  /* C89 specifies that the constant "dummy" will be initialized to all
  ** zeros, which is correct.  MSVC generates a warning, nevertheless. */
  static VdbeOp dummy;  /* Ignore the MSVC warning about no initializer */
  assert( p->magic==VDBE_MAGIC_INIT );
  if( addr<0 ){
    addr = p->nOp - 1;
  }
  assert( (addr>=0 && addr<p->nOp) || p->db->mallocFailed );
  if( p->db->mallocFailed ){
    return (VdbeOp*)&dummy;
  }else{
    return &p->aOp[addr];
  }
}

#if defined(SQLITE_ENABLE_EXPLAIN_COMMENTS)
/*
** Return an integer value for one of the parameters to the opcode pOp
** determined by character c.
*/
static int translateP(char c, const Op *pOp){
  if( c=='1' ) return pOp->p1;
  if( c=='2' ) return pOp->p2;
  if( c=='3' ) return pOp->p3;
  if( c=='4' ) return pOp->p4.i;
  return pOp->p5;
}

/*
** Compute a string for the "comment" field of a VDBE opcode listing.
**
** The Synopsis: field in comments in the vdbe.c source file gets converted
** to an extra string that is appended to the sqlitexOpcodeName().  In the
** absence of other comments, this synopsis becomes the comment on the opcode.
** Some translation occurs:
**
**       "PX"      ->  "r[X]"
**       "PX@PY"   ->  "r[X..X+Y-1]"  or "r[x]" if y is 0 or 1
**       "PX@PY+1" ->  "r[X..X+Y]"    or "r[x]" if y is 0
**       "PY..PY"  ->  "r[X..Y]"      or "r[x]" if y<=x
*/
static int displayComment(
  Vdbe *p,           /* COMDB2 MODIFICATION : we need vdbe for remote dbs */
  const Op *pOp,     /* The opcode to be commented */
  const char *zP4,   /* Previously obtained value for P4 */
  char *zTemp,       /* Write result here */
  int nTemp          /* Space available in zTemp[] */
){
  const char *zOpName;
  const char *zSynopsis;
  int nOpName;
  int ii, jj;
  zOpName = sqlitexOpcodeName(pOp->opcode);
  nOpName = sqlitexStrlen30(zOpName);
  if( zOpName[nOpName+1] ){
    int seenCom = 0;
    char c;
    zSynopsis = zOpName += nOpName + 1;
    for(ii=jj=0; jj<nTemp-1 && (c = zSynopsis[ii])!=0; ii++){
      if( c=='P' ){
        c = zSynopsis[++ii];
        if( c=='4' ){
          sqlitex_snprintf(nTemp-jj, zTemp+jj, "%s", zP4);
        }else if( c=='X' ){
          sqlitex_snprintf(nTemp-jj, zTemp+jj, "%s", pOp->zComment);
          seenCom = 1;
        }else{
          int v1 = translateP(c, pOp);
          int v2;
          sqlitex_snprintf(nTemp-jj, zTemp+jj, "%d", v1);
          if( strncmp(zSynopsis+ii+1, "@P", 2)==0 ){
            ii += 3;
            jj += sqlitexStrlen30(zTemp+jj);
            v2 = translateP(zSynopsis[ii], pOp);
            if( strncmp(zSynopsis+ii+1,"+1",2)==0 ){
              ii += 2;
              v2++;
            }
            if( v2>1 ){
              sqlitex_snprintf(nTemp-jj, zTemp+jj, "..%d", v1+v2-1);
            }
          }else if( strncmp(zSynopsis+ii+1, "..P3", 4)==0 && pOp->p3==0 ){
            ii += 4;
          }
        }
        jj += sqlitexStrlen30(zTemp+jj);
      }else{
        zTemp[jj++] = c;
      }
    }
    if( !seenCom && jj<nTemp-5 && pOp->zComment ){

      /* COMDB2 MODIFICATIONS */
      /* Note if this is an open table op, and this is a remote table,
         display the fully qualified name */
      if(pOp->opcode == OP_OpenRead || pOp->opcode == OP_OpenWrite || pOp->opcode == OP_OpenRead_Record)
      {
        /* is this remote db ?*/
        if(p && (pOp->p3>1) && (pOp->p3<p->db->nDb))
        {
          sqlitex_snprintf(nTemp-jj, zTemp+jj, "; %s.%s", p->db->aDb[pOp->p3].zName, pOp->zComment);
        }
        else
        {
          sqlitex_snprintf(nTemp-jj, zTemp+jj, "; %s", pOp->zComment);
        }
      }
      else
      {
        sqlitex_snprintf(nTemp-jj, zTemp+jj, "; %s", pOp->zComment);
      }
      jj += sqlitexStrlen30(zTemp+jj);
    }
    if( jj<nTemp ) zTemp[jj] = 0;
  }else if( pOp->zComment ){
    sqlitex_snprintf(nTemp, zTemp, "%s", pOp->zComment);
    jj = sqlitexStrlen30(zTemp);
  }else{
    zTemp[0] = 0;
    jj = 0;
  }
  return jj;
}
#endif /* SQLITE_DEBUG */

#if VDBE_DISPLAY_P4 && defined(SQLITE_ENABLE_CURSOR_HINTS)
/*
** Translate the P4.pExpr value for an OP_CursorHint opcode into text
** that can be displayed in the P4 column of EXPLAIN output.
*/
static int displayP4Expr(int nTemp, char *zTemp, Expr *pExpr){
  const char *zOp = 0;
  int n;
  switch( pExpr->op ){
    case TK_STRING:
      sqlitex_snprintf(nTemp, zTemp, "%Q", pExpr->u.zToken);
      break;



    case TK_INTEGER:
      sqlitex_snprintf(nTemp, zTemp, "%d", pExpr->u.iValue);
      break;

    case TK_NULL:
      sqlitex_snprintf(nTemp, zTemp, "NULL");
      break;

    case TK_REGISTER: {
      sqlitex_snprintf(nTemp, zTemp, "r[%d]", pExpr->iTable);
      break;
    }

    case TK_COLUMN: {
      if( pExpr->iColumn<0 ){
        sqlitex_snprintf(nTemp, zTemp, "rowid");
      }else{
        sqlitex_snprintf(nTemp, zTemp, "c%d", (int)pExpr->iColumn);
      }
      break;
    }

    case TK_LT:      zOp = "LT";      break;
    case TK_LE:      zOp = "LE";      break;
    case TK_GT:      zOp = "GT";      break;
    case TK_GE:      zOp = "GE";      break;
    case TK_NE:      zOp = "NE";      break;
    case TK_EQ:      zOp = "EQ";      break;
    case TK_IS:      zOp = "IS";      break;
    case TK_ISNOT:   zOp = "ISNOT";   break;
    case TK_AND:     zOp = "AND";     break;
    case TK_OR:      zOp = "OR";      break;
    case TK_PLUS:    zOp = "ADD";     break;
    case TK_STAR:    zOp = "MUL";     break;
    case TK_MINUS:   zOp = "SUB";     break;
    case TK_REM:     zOp = "REM";     break;
    case TK_BITAND:  zOp = "BITAND";  break;
    case TK_BITOR:   zOp = "BITOR";   break;
    case TK_SLASH:   zOp = "DIV";     break;
    case TK_LSHIFT:  zOp = "LSHIFT";  break;
    case TK_RSHIFT:  zOp = "RSHIFT";  break;
    case TK_CONCAT:  zOp = "CONCAT";  break;
    case TK_UMINUS:  zOp = "MINUS";   break;
    case TK_UPLUS:   zOp = "PLUS";    break;
    case TK_BITNOT:  zOp = "BITNOT";  break;
    case TK_NOT:     zOp = "NOT";     break;
    case TK_ISNULL:  zOp = "ISNULL";  break;
    case TK_NOTNULL: zOp = "NOTNULL"; break;

    default:
      sqlitex_snprintf(nTemp, zTemp, "%s", "expr");
      break;
  }

  if( zOp ){
    sqlitex_snprintf(nTemp, zTemp, "%s(", zOp);
    n = sqlitexStrlen30(zTemp);
    n += displayP4Expr(nTemp-n, zTemp+n, pExpr->pLeft);
    if( n<nTemp-1 && pExpr->pRight ){
      zTemp[n++] = ',';
      n += displayP4Expr(nTemp-n, zTemp+n, pExpr->pRight);
    }
    sqlitex_snprintf(nTemp-n, zTemp+n, ")");
  }
  return sqlitexStrlen30(zTemp);
}
#endif /* VDBE_DISPLAY_P4 && defined(SQLITE_ENABLE_CURSOR_HINTS) */


#if VDBE_DISPLAY_P4/*
** Compute a string that describes the P4 parameter for an opcode.
** Use zTemp for any required temporary buffer space.
*/
static char *displayP4(Op *pOp, char *zTemp, int nTemp){
  char *zP4 = zTemp;
  assert( nTemp>=20 );
  switch( pOp->p4type ){
    case P4_KEYINFO: {
      int i, j;
      KeyInfo *pKeyInfo = pOp->p4.pKeyInfo;
      assert( pKeyInfo->aSortOrder!=0 );
      sqlitex_snprintf(nTemp, zTemp, "k(%d", pKeyInfo->nField);
      i = sqlitexStrlen30(zTemp);
      for(j=0; j<pKeyInfo->nField; j++){
        CollSeq *pColl = pKeyInfo->aColl[j];
        const char *zColl = pColl ? pColl->zName : "nil";
        int n = sqlitexStrlen30(zColl);
        if( n==6 && memcmp(zColl,"BINARY",6)==0 ){
          zColl = "B";
          n = 1;
        }
        if( i+n>nTemp-6 ){
          memcpy(&zTemp[i],",...",4);
          break;
        }
        zTemp[i++] = ',';
        if( pKeyInfo->aSortOrder[j] ){
          zTemp[i++] = '-';
        }
        memcpy(&zTemp[i], zColl, n+1);
        i += n;
      }
      zTemp[i++] = ')';
      zTemp[i] = 0;
      assert( i<nTemp );
      break;
    }
#ifdef SQLITE_ENABLE_CURSOR_HINTS
    case P4_EXPR: {
      displayP4Expr(nTemp, zTemp, pOp->p4.pExpr);
      break;
    }
#endif
    case P4_COLLSEQ: {
      CollSeq *pColl = pOp->p4.pColl;
      sqlitex_snprintf(nTemp, zTemp, "(%.20s)", pColl->zName);
      break;
    }
    case P4_FUNCDEF: {
      FuncDef *pDef = pOp->p4.pFunc;
      sqlitex_snprintf(nTemp, zTemp, "%s(%d)", pDef->zName, pDef->nArg);
      break;
    }
    case P4_INT64: {
      sqlitex_snprintf(nTemp, zTemp, "%lld", *pOp->p4.pI64);
      break;
    }
    case P4_INT32: {
      sqlitex_snprintf(nTemp, zTemp, "%d", pOp->p4.i);
      break;
    }
    case P4_REAL: {
      sqlitex_snprintf(nTemp, zTemp, "%.16g", *pOp->p4.pReal);
      break;
    }
    case P4_MEM: {
      Mem *pMem = pOp->p4.pMem;
      if( pMem->flags & MEM_Str ){
        zP4 = pMem->z;
      }else if( pMem->flags & MEM_Int ){
        sqlitex_snprintf(nTemp, zTemp, "%lld", pMem->u.i);
      }else if( pMem->flags & MEM_Real ){
        sqlitex_snprintf(nTemp, zTemp, "%.16g", pMem->u.r);
      }else if( pMem->flags & MEM_Null ){
        sqlitex_snprintf(nTemp, zTemp, "NULL");
      }else{
        assert( pMem->flags & MEM_Blob );
        zP4 = "(blob)";
      }
      break;
    }
#ifndef SQLITE_OMIT_VIRTUALTABLE
    case P4_VTAB: {
      sqlitex_vtab *pVtab = pOp->p4.pVtab->pVtab;
      sqlitex_snprintf(nTemp, zTemp, "vtab:%p:%p", pVtab, pVtab->pModule);
      break;
    }
#endif
    case P4_INTARRAY: {
      int i, j;
      int *ai = pOp->p4.ai;
      int n = ai[0];   /* The first element of an INTARRAY is always the
                       ** count of the number of elements to follow */
      zTemp[0] = '[';
      for(i=j=1; i<n && j<nTemp-7; i++){
        if( j>1 ) zTemp[j++] = ',';
        sqlitex_snprintf(nTemp-j, zTemp+j, "%d", ai[i]);
        j += sqlitexStrlen30(zTemp+j);
      }
      if( i<n ){
        memcpy(zTemp+j, ",...]", 6);
      }else{
        memcpy(zTemp+j, "]", 2);
      }
      break;
    }
    case P4_SUBPROGRAM: {
      sqlitex_snprintf(nTemp, zTemp, "program");
      break;
    }
    case P4_ADVANCE: {
      zTemp[0] = 0;
      break;
    }
    default: {
      zP4 = pOp->p4.z;
      if( zP4==0 ){
        zP4 = zTemp;
        zTemp[0] = 0;
      }
    }
  }
  assert( zP4!=0 );
  return zP4;
}
#endif /* VDBE_DISPLAY_P4 */

/*
** Declare to the Vdbe that the BTree object at db->aDb[i] is used.
**
** The prepared statements need to know in advance the complete set of
** attached databases that will be use.  A mask of these databases
** is maintained in p->btreeMask.  The p->lockMask value is the subset of
** p->btreeMask of databases that will require a lock.
*/
void sqlitexVdbeUsesBtree(Vdbe *p, int i){
  assert( i>=0 && i<p->db->nDb && i<(int)sizeof(yDbMask)*8 );
  assert( i<(int)sizeof(p->btreeMask)*8 );
  DbMaskSet(p->btreeMask, i);
  if( i!=1 && sqlite3BtreeSharable(p->db->aDb[i].pBt) ){
    DbMaskSet(p->lockMask, i);
  }
}

#if !defined(SQLITE_OMIT_SHARED_CACHE) && SQLITE_THREADSAFE>0
/*
** If SQLite is compiled to support shared-cache mode and to be threadsafe,
** this routine obtains the mutex associated with each BtShared structure
** that may be accessed by the VM passed as an argument. In doing so it also
** sets the BtShared.db member of each of the BtShared structures, ensuring
** that the correct busy-handler callback is invoked if required.
**
** If SQLite is not threadsafe but does support shared-cache mode, then
** sqlite3BtreeEnter() is invoked to set the BtShared.db variables
** of all of BtShared structures accessible via the database handle 
** associated with the VM.
**
** If SQLite is not threadsafe and does not support shared-cache mode, this
** function is a no-op.
**
** The p->btreeMask field is a bitmask of all btrees that the prepared 
** statement p will ever use.  Let N be the number of bits in p->btreeMask
** corresponding to btrees that use shared cache.  Then the runtime of
** this routine is N*N.  But as N is rarely more than 1, this should not
** be a problem.
*/
void sqlitexVdbeEnter(Vdbe *p){
  int i;
  sqlitex *db;
  Db *aDb;
  int nDb;
  if( DbMaskAllZero(p->lockMask) ) return;  /* The common case */
  db = p->db;
  aDb = db->aDb;
  nDb = db->nDb;
  for(i=0; i<nDb; i++){
    if( i!=1 && DbMaskTest(p->lockMask,i) && ALWAYS(aDb[i].pBt!=0) ){
      sqlite3BtreeEnter(aDb[i].pBt);
    }
  }
}
#endif

#if !defined(SQLITE_OMIT_SHARED_CACHE) && SQLITE_THREADSAFE>0
/*
** Unlock all of the btrees previously locked by a call to sqlitexVdbeEnter().
*/
void sqlitexVdbeLeave(Vdbe *p){
  int i;
  sqlitex *db;
  Db *aDb;
  int nDb;
  if( DbMaskAllZero(p->lockMask) ) return;  /* The common case */
  db = p->db;
  aDb = db->aDb;
  nDb = db->nDb;
  for(i=0; i<nDb; i++){
    if( i!=1 && DbMaskTest(p->lockMask,i) && ALWAYS(aDb[i].pBt!=0) ){
      sqlite3BtreeLeave(aDb[i].pBt);
    }
  }
}
#endif

#if defined(VDBE_PROFILE) || defined(SQLITE_DEBUG)
/*
** Print a single opcode.  This routine is used for debugging only.
*/
void sqlitexVdbePrintOp(FILE *pOut, int pc, Op *pOp){
  char *zP4;
  char zPtr[50];
  char zCom[100];
  static const char *zFormat1 = "%4d %-13s %4d %4d %4d %-13s %.2X %s\n";
  if( pOut==0 ) pOut = stdout;
  zP4 = displayP4(pOp, zPtr, sizeof(zPtr));
#ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
  displayComment(NULL, pOp, zP4, zCom, sizeof(zCom));
#else
  zCom[0] = 0;
#endif
  /* NB:  The sqlitexOpcodeName() function is implemented by code created
  ** by the mkopcodeh.awk and mkopcodec.awk scripts which extract the
  ** information from the vdbe.c source text */
  fprintf(pOut, zFormat1, pc, 
      sqlitexOpcodeName(pOp->opcode), pOp->p1, pOp->p2, pOp->p3, zP4, pOp->p5,
      zCom
  );
  fflush(pOut);
}
#endif

/*
** Release an array of N Mem elements
*/
static void releaseMemArray(Mem *p, int N){
  if( p && N ){
    Mem *pEnd = &p[N];
    sqlitex *db = p->db;
    assert(db);
    u8 malloc_failed = db->mallocFailed;
    if( db->pnBytesFreed ){
      do{
        if( p->szMalloc ) sqlitexDbFree(db, p->zMalloc);
      }while( (++p)<pEnd );
      return;
    }
    do{
      assert( (&p[1])==pEnd || p[0].db==p[1].db );
      assert( sqlitexVdbeCheckMemInvariants(p) );

      /* This block is really an inlined version of sqlitexVdbeMemRelease()
      ** that takes advantage of the fact that the memory cell value is 
      ** being set to NULL after releasing any dynamic resources.
      **
      ** The justification for duplicating code is that according to 
      ** callgrind, this causes a certain test case to hit the CPU 4.7 
      ** percent less (x86 linux, gcc version 4.1.2, -O6) than if 
      ** sqlitexMemRelease() were called from here. With -O2, this jumps
      ** to 6.6 percent. The test case is inserting 1000 rows into a table 
      ** with no indexes using a single prepared INSERT statement, bind() 
      ** and reset(). Inserts are grouped into a transaction.
      */
      testcase( p->flags & MEM_Agg );
      testcase( p->flags & MEM_Dyn );
      testcase( p->flags & MEM_Frame );
      testcase( p->flags & MEM_RowSet );
      if( p->flags&(MEM_Agg|MEM_Dyn|MEM_Frame|MEM_RowSet) ){
        sqlitexVdbeMemRelease(p);
      }else if( p->szMalloc ){
        sqlitexDbFree(db, p->zMalloc);
        p->szMalloc = 0;
      }

      p->flags = MEM_Undefined;
    }while( (++p)<pEnd );
    db->mallocFailed = malloc_failed;
  }
}

/*
** Delete a VdbeFrame object and its contents. VdbeFrame objects are
** allocated by the OP_Program opcode in sqlitexVdbeExec().
*/
void sqlitexVdbeFrameDelete(VdbeFrame *p){
  int i;
  Mem *aMem = VdbeFrameMem(p);
  VdbeCursor **apCsr = (VdbeCursor **)&aMem[p->nChildMem];
  for(i=0; i<p->nChildCsr; i++){
    sqlitexVdbeFreeCursor(p->v, apCsr[i]);
  }
  releaseMemArray(aMem, p->nChildMem);
  sqlitexDbFree(p->v->db, p);
}

#ifndef SQLITE_OMIT_EXPLAIN
/*
** Give a listing of the program in the virtual machine.
**
** The interface is the same as sqlitexVdbeExec().  But instead of
** running the code, it invokes the callback once for each instruction.
** This feature is used to implement "EXPLAIN".
**
** When p->explain==1, each instruction is listed.  When
** p->explain==2, only OP_Explain instructions are listed and these
** are shown in a different format.  p->explain==2 is used to implement
** EXPLAIN QUERY PLAN.
**
** When p->explain==1, first the main program is listed, then each of
** the trigger subprograms are listed one by one.
*/
int sqlitexVdbeList(
  Vdbe *p                   /* The VDBE */
){
  int nRow;                            /* Stop when row count reaches this */
  int nSub = 0;                        /* Number of sub-vdbes seen so far */
  SubProgram **apSub = 0;              /* Array of sub-vdbes */
  Mem *pSub = 0;                       /* Memory cell hold array of subprogs */
  sqlitex *db = p->db;                 /* The database connection */
  int i;                               /* Loop counter */
  int rc = SQLITE_OK;                  /* Return code */
  Mem *pMem = &p->aMem[1];             /* First Mem of result set */

  assert( p->explain );
  assert( p->magic==VDBE_MAGIC_RUN );
  assert( p->rc==SQLITE_OK || p->rc==SQLITE_BUSY || p->rc==SQLITE_NOMEM );

  /* Even though this opcode does not use dynamic strings for
  ** the result, result columns may become dynamic if the user calls
  ** sqlitex_column_text16(), causing a translation to UTF-16 encoding.
  */
  releaseMemArray(pMem, 8);
  p->pResultSet = 0;

  if( p->rc==SQLITE_NOMEM ){
    /* This happens if a malloc() inside a call to sqlitex_column_text() or
    ** sqlitex_column_text16() failed.  */
    db->mallocFailed = 1;
    return SQLITE_ERROR;
  }

  /* When the number of output rows reaches nRow, that means the
  ** listing has finished and sqlitex_step() should return SQLITE_DONEX.
  ** nRow is the sum of the number of rows in the main program, plus
  ** the sum of the number of rows in all trigger subprograms encountered
  ** so far.  The nRow value will increase as new trigger subprograms are
  ** encountered, but p->pc will eventually catch up to nRow.
  */
  nRow = p->nOp;
  if( p->explain==1 ){
    /* The first 8 memory cells are used for the result set.  So we will
    ** commandeer the 9th cell to use as storage for an array of pointers
    ** to trigger subprograms.  The VDBE is guaranteed to have at least 9
    ** cells.  */
    assert( p->nMem>9 );
    pSub = &p->aMem[9];
    if( pSub->flags&MEM_Blob ){
      /* On the first call to sqlitex_step(), pSub will hold a NULL.  It is
      ** initialized to a BLOB by the P4_SUBPROGRAM processing logic below */
      nSub = pSub->n/sizeof(Vdbe*);
      apSub = (SubProgram **)pSub->z;
    }
    for(i=0; i<nSub; i++){
      nRow += apSub[i]->nOp;
    }
  }

  do{
    i = p->pc++;
  }while( i<nRow && p->explain==2 && p->aOp[i].opcode!=OP_Explain );
  if( i>=nRow ){
    p->rc = SQLITE_OK;
    rc = SQLITE_DONEX;
  }else if( db->u1.isInterrupted ){
    p->rc = SQLITE_INTERRUPT;
    rc = SQLITE_ERROR;
    sqlitexSetString(&p->zErrMsg, db, "%s", sqlitexErrStr(p->rc));
  }else{
    char *zP4;
    Op *pOp;
    if( i<p->nOp ){
      /* The output line number is small enough that we are still in the
      ** main program. */
      pOp = &p->aOp[i];
    }else{
      /* We are currently listing subprograms.  Figure out which one and
      ** pick up the appropriate opcode. */
      int j;
      i -= p->nOp;
      for(j=0; i>=apSub[j]->nOp; j++){
        i -= apSub[j]->nOp;
      }
      pOp = &apSub[j]->aOp[i];
    }
    if( p->explain==1 ){
      pMem->flags = MEM_Int;
      pMem->u.i = i;                                /* Program counter */
      pMem++;
  
      pMem->flags = MEM_Static|MEM_Str|MEM_Term;
      pMem->z = (char*)sqlitexOpcodeName(pOp->opcode); /* Opcode */
      assert( pMem->z!=0 );
      pMem->n = sqlitexStrlen30(pMem->z);
      pMem->enc = SQLITE_UTF8;
      pMem++;

      /* When an OP_Program opcode is encounter (the only opcode that has
      ** a P4_SUBPROGRAM argument), expand the size of the array of subprograms
      ** kept in p->aMem[9].z to hold the new program - assuming this subprogram
      ** has not already been seen.
      */
      if( pOp->p4type==P4_SUBPROGRAM ){
        int nByte = (nSub+1)*sizeof(SubProgram*);
        int j;
        for(j=0; j<nSub; j++){
          if( apSub[j]==pOp->p4.pProgram ) break;
        }
        if( j==nSub && SQLITE_OK==sqlitexVdbeMemGrow(pSub, nByte, nSub!=0) ){
          apSub = (SubProgram **)pSub->z;
          apSub[nSub++] = pOp->p4.pProgram;
          pSub->flags |= MEM_Blob;
          pSub->n = nSub*sizeof(SubProgram*);
        }
      }
    }

    pMem->flags = MEM_Int;
    pMem->u.i = pOp->p1;                          /* P1 */
    pMem++;

    pMem->flags = MEM_Int;
    pMem->u.i = pOp->p2;                          /* P2 */
    pMem++;

    pMem->flags = MEM_Int;
    pMem->u.i = pOp->p3;                          /* P3 */
    pMem++;

    if( sqlitexVdbeMemClearAndResize(pMem, 100) ){ /* P4 */
      assert( p->db->mallocFailed );
      return SQLITE_ERROR;
    }
    pMem->flags = MEM_Str|MEM_Term;
    zP4 = displayP4(pOp, pMem->z, pMem->szMalloc);
    if( zP4!=pMem->z ){
      sqlitexVdbeMemSetStr(pMem, zP4, -1, SQLITE_UTF8, 0);
    }else{
      assert( pMem->z!=0 );
      pMem->n = sqlitexStrlen30(pMem->z);
      pMem->enc = SQLITE_UTF8;
    }
    pMem++;

    if( p->explain==1 ){
      if( sqlitexVdbeMemClearAndResize(pMem, 4) ){
        assert( p->db->mallocFailed );
        return SQLITE_ERROR;
      }
      pMem->flags = MEM_Str|MEM_Term;
      pMem->n = 2;
      sqlitex_snprintf(3, pMem->z, "%.2x", pOp->p5);   /* P5 */
      pMem->enc = SQLITE_UTF8;
      pMem++;
  
#ifdef SQLITE_ENABLE_EXPLAIN_COMMENTS
      if( sqlitexVdbeMemClearAndResize(pMem, 500) ){
        assert( p->db->mallocFailed );
        return SQLITE_ERROR;
      }
      pMem->flags = MEM_Str|MEM_Term;
      pMem->n = displayComment(p, pOp, zP4, pMem->z, 500);
      pMem->enc = SQLITE_UTF8;
#else
      pMem->flags = MEM_Null;                       /* Comment */
#endif
    }

    p->nResColumn = 8 - 4*(p->explain-1);
    p->pResultSet = &p->aMem[1];
    p->rc = SQLITE_OK;
    rc = SQLITE_ROW;
  }
  return rc;
}
#endif /* SQLITE_OMIT_EXPLAIN */

#ifdef SQLITE_DEBUG
/*
** Print the SQL that was used to generate a VDBE program.
*/
void sqlitexVdbePrintSql(Vdbe *p){
  const char *z = 0;
  if( p->zSql ){
    z = p->zSql;
  }else if( p->nOp>=1 ){
    const VdbeOp *pOp = &p->aOp[0];
    if( pOp->opcode==OP_Init && pOp->p4.z!=0 ){
      z = pOp->p4.z;
      while( sqlitexIsspace(*z) ) z++;
    }
  }
  if( z ) printf("SQL: [%s]\n", z);
}
#endif

#if !defined(SQLITE_OMIT_TRACE) && defined(SQLITE_ENABLE_IOTRACE)
/*
** Print an IOTRACE message showing SQL content.
*/
void sqlitexVdbeIOTraceSql(Vdbe *p){
  int nOp = p->nOp;
  VdbeOp *pOp;
  if( sqlitexIoTrace==0 ) return;
  if( nOp<1 ) return;
  pOp = &p->aOp[0];
  if( pOp->opcode==OP_Init && pOp->p4.z!=0 ){
    int i, j;
    char z[1000];
    sqlitex_snprintf(sizeof(z), z, "%s", pOp->p4.z);
    for(i=0; sqlitexIsspace(z[i]); i++){}
    for(j=0; z[i]; i++){
      if( sqlitexIsspace(z[i]) ){
        if( z[i-1]!=' ' ){
          z[j++] = ' ';
        }
      }else{
        z[j++] = z[i];
      }
    }
    z[j] = 0;
    sqlitexIoTrace("SQL %s\n", z);
  }
}
#endif /* !SQLITE_OMIT_TRACE && SQLITE_ENABLE_IOTRACE */

/*
** Allocate space from a fixed size buffer and return a pointer to
** that space.  If insufficient space is available, return NULL.
**
** The pBuf parameter is the initial value of a pointer which will
** receive the new memory.  pBuf is normally NULL.  If pBuf is not
** NULL, it means that memory space has already been allocated and that
** this routine should not allocate any new memory.  When pBuf is not
** NULL simply return pBuf.  Only allocate new memory space when pBuf
** is NULL.
**
** nByte is the number of bytes of space needed.
**
** *ppFrom points to available space and pEnd points to the end of the
** available space.  When space is allocated, *ppFrom is advanced past
** the end of the allocated space.
**
** *pnByte is a counter of the number of bytes of space that have failed
** to allocate.  If there is insufficient space in *ppFrom to satisfy the
** request, then increment *pnByte by the amount of the request.
*/
static void *allocSpace(
  void *pBuf,          /* Where return pointer will be stored */
  int nByte,           /* Number of bytes to allocate */
  u8 **ppFrom,         /* IN/OUT: Allocate from *ppFrom */
  u8 *pEnd,            /* Pointer to 1 byte past the end of *ppFrom buffer */
  int *pnByte          /* If allocation cannot be made, increment *pnByte */
){
  assert( EIGHT_BYTE_ALIGNMENT(*ppFrom) );
  if( pBuf ) return pBuf;
  nByte = ROUND8(nByte);
  if( &(*ppFrom)[nByte] <= pEnd ){
    pBuf = (void*)*ppFrom;
    *ppFrom += nByte;
  }else{
    *pnByte += nByte;
  }
  return pBuf;
}

/*
** Rewind the VDBE back to the beginning in preparation for
** running it.
*/
void sqlitexVdbeRewind(Vdbe *p){
#if defined(SQLITE_DEBUG) || defined(VDBE_PROFILE)
  int i;
#endif
  assert( p!=0 );
  assert( p->magic==VDBE_MAGIC_INIT );

  /* There should be at least one opcode.
  */
  assert( p->nOp>0 );

  /* Set the magic to VDBE_MAGIC_RUN sooner rather than later. */
  p->magic = VDBE_MAGIC_RUN;

#ifdef SQLITE_DEBUG
  for(i=1; i<p->nMem; i++){
    assert( p->aMem[i].db==p->db );
  }
#endif
  p->pc = -1;
  p->rc = SQLITE_OK;
  p->errorAction = OE_Abort;
  p->magic = VDBE_MAGIC_RUN;
  p->nChange = 0;
  p->cacheCtr = 1;
  p->minWriteFileFormat = 255;
  p->iStatement = 0;
  p->nFkConstraint = 0;
#ifdef VDBE_PROFILE
  for(i=0; i<p->nOp; i++){
    p->aOp[i].cnt = 0;
    p->aOp[i].cycles = 0;
  }
#endif
}

/*
** Prepare a virtual machine for execution for the first time after
** creating the virtual machine.  This involves things such
** as allocating registers and initializing the program counter.
** After the VDBE has be prepped, it can be executed by one or more
** calls to sqlitexVdbeExec().  
**
** This function may be called exactly once on each virtual machine.
** After this routine is called the VM has been "packaged" and is ready
** to run.  After this routine is called, further calls to 
** sqlitexVdbeAddOp() functions are prohibited.  This routine disconnects
** the Vdbe from the Parse object that helped generate it so that the
** the Vdbe becomes an independent entity and the Parse object can be
** destroyed.
**
** Use the sqlitexVdbeRewind() procedure to restore a virtual machine back
** to its initial state after it has been run.
*/
void sqlitexVdbeMakeReady(
  Vdbe *p,                       /* The VDBE */
  Parse *pParse                  /* Parsing context */
){
  sqlitex *db;                   /* The database connection */
  int nVar;                      /* Number of parameters */
  int nMem;                      /* Number of VM memory registers */
  int nCursor;                   /* Number of cursors required */
  int nArg;                      /* Number of arguments in subprograms */
  int nOnce;                     /* Number of OP_Once instructions */
  int n;                         /* Loop counter */
  u8 *zCsr;                      /* Memory available for allocation */
  u8 *zEnd;                      /* First byte past allocated memory */
  int nByte;                     /* How much extra memory is needed */

  assert( p!=0 );
  assert( p->nOp>0 );
  assert( pParse!=0 );
  assert( p->magic==VDBE_MAGIC_INIT );
  assert( pParse==p->pParse );
  db = p->db;
  assert( db->mallocFailed==0 );
  nVar = pParse->nVar;
  nMem = pParse->nMem;
  nCursor = pParse->nTab;
  nArg = pParse->nMaxArg;
  nOnce = pParse->nOnce;
  if( nOnce==0 ) nOnce = 1; /* Ensure at least one byte in p->aOnceFlag[] */
  
  /* For each cursor required, also allocate a memory cell. Memory
  ** cells (nMem+1-nCursor)..nMem, inclusive, will never be used by
  ** the vdbe program. Instead they are used to allocate space for
  ** VdbeCursor/BtCursor structures. The blob of memory associated with 
  ** cursor 0 is stored in memory cell nMem. Memory cell (nMem-1)
  ** stores the blob of memory associated with cursor 1, etc.
  **
  ** See also: allocateCursor().
  */
  nMem += nCursor;

  /* Allocate space for memory registers, SQL variables, VDBE cursors and 
  ** an array to marshal SQL function arguments in.
  */
  zCsr = (u8*)&p->aOp[p->nOp];            /* Memory avaliable for allocation */
  zEnd = (u8*)&p->aOp[pParse->nOpAlloc];  /* First byte past end of zCsr[] */

  resolveP2Values(p, &nArg);
  p->usesStmtJournal = (u8)(pParse->isMultiWrite && pParse->mayAbort);
  if( pParse->explain && nMem<10 ){
    nMem = 10;
  }
  memset(zCsr, 0, zEnd-zCsr);
  zCsr += (zCsr - (u8*)0)&7;
  assert( EIGHT_BYTE_ALIGNMENT(zCsr) );
  p->expired = 0;

  /* Memory for registers, parameters, cursor, etc, is allocated in two
  ** passes.  On the first pass, we try to reuse unused space at the 
  ** end of the opcode array.  If we are unable to satisfy all memory
  ** requirements by reusing the opcode array tail, then the second
  ** pass will fill in the rest using a fresh allocation.  
  **
  ** This two-pass approach that reuses as much memory as possible from
  ** the leftover space at the end of the opcode array can significantly
  ** reduce the amount of memory held by a prepared statement.
  */
  do {
    nByte = 0;
    p->aMem = allocSpace(p->aMem, nMem*sizeof(Mem), &zCsr, zEnd, &nByte);
    p->aVar = allocSpace(p->aVar, nVar*sizeof(Mem), &zCsr, zEnd, &nByte);
    p->apArg = allocSpace(p->apArg, nArg*sizeof(Mem*), &zCsr, zEnd, &nByte);
    p->azVar = allocSpace(p->azVar, nVar*sizeof(char*), &zCsr, zEnd, &nByte);
    p->apCsr = allocSpace(p->apCsr, nCursor*sizeof(VdbeCursor*),
                          &zCsr, zEnd, &nByte);
    p->aOnceFlag = allocSpace(p->aOnceFlag, nOnce, &zCsr, zEnd, &nByte);
#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
    p->anExec = allocSpace(p->anExec, p->nOp*sizeof(i64), &zCsr, zEnd, &nByte);
#endif
    if( nByte ){
      p->pFree = sqlitexDbMallocZero(db, nByte);
    }
    zCsr = p->pFree;
    zEnd = &zCsr[nByte];
  }while( nByte && !db->mallocFailed );

  p->nCursor = nCursor;
  p->nOnceFlag = nOnce;
  if( p->aVar ){
    p->nVar = (ynVar)nVar;
    for(n=0; n<nVar; n++){
      p->aVar[n].flags = MEM_Null;
      p->aVar[n].db = db;
    }
  }
  if( p->azVar && pParse->nzVar>0 ){
    p->nzVar = pParse->nzVar;
    memcpy(p->azVar, pParse->azVar, p->nzVar*sizeof(p->azVar[0]));
    memset(pParse->azVar, 0, pParse->nzVar*sizeof(pParse->azVar[0]));
  }
  if( p->aMem ){
    p->aMem--;                      /* aMem[] goes from 1..nMem */
    p->nMem = nMem;                 /*       not from 0..nMem-1 */
    for(n=1; n<=nMem; n++){
      p->aMem[n].flags = MEM_Undefined;
      p->aMem[n].db = db;
    }
  }
  p->explain = pParse->explain;
  sqlitexVdbeRewind(p);
}

/*
** Close a VDBE cursor and release all the resources that cursor 
** happens to hold.
*/
void sqlitexVdbeFreeCursor(Vdbe *p, VdbeCursor *pCx){
  if( pCx==0 ){
    return;
  }
  sqlitexVdbeSorterClose(p->db, pCx);
  if( pCx->pBt ){
    sqlite3BtreeClose(pCx->pBt);
    /* The pCx->pCursor will be close automatically, if it exists, by
    ** the call above. */
  }else if( pCx->pCursor ){
    sqlite3BtreeCloseCursor(pCx->pCursor);
  }
#ifndef SQLITE_OMIT_VIRTUALTABLE
  else if( pCx->pVtabCursor ){
    sqlitex_vtab_cursor *pVtabCursor = pCx->pVtabCursor;
    const sqlitex_module *pModule = pVtabCursor->pVtab->pModule;
    p->inVtabMethod = 1;
    pModule->xClose(pVtabCursor);
    p->inVtabMethod = 0;
  }
#endif
}

/*
** Copy the values stored in the VdbeFrame structure to its Vdbe. This
** is used, for example, when a trigger sub-program is halted to restore
** control to the main program.
*/
int sqlitexVdbeFrameRestore(VdbeFrame *pFrame){
  Vdbe *v = pFrame->v;
#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
  v->anExec = pFrame->anExec;
#endif
  v->aOnceFlag = pFrame->aOnceFlag;
  v->nOnceFlag = pFrame->nOnceFlag;
  v->aOp = pFrame->aOp;
  v->nOp = pFrame->nOp;
  v->aMem = pFrame->aMem;
  v->nMem = pFrame->nMem;
  v->apCsr = pFrame->apCsr;
  v->nCursor = pFrame->nCursor;
  v->db->lastRowid = pFrame->lastRowid;
  v->nChange = pFrame->nChange;
  v->db->nChange = pFrame->nDbChange;
  return pFrame->pc;
}

/*
** Close all cursors.
**
** Also release any dynamic memory held by the VM in the Vdbe.aMem memory 
** cell array. This is necessary as the memory cell array may contain
** pointers to VdbeFrame objects, which may in turn contain pointers to
** open cursors.
*/
static void closeAllCursors(Vdbe *p){
  if( p->pFrame ){
    VdbeFrame *pFrame;
    for(pFrame=p->pFrame; pFrame->pParent; pFrame=pFrame->pParent);
    sqlitexVdbeFrameRestore(pFrame);
    p->pFrame = 0;
    p->nFrame = 0;
  }
  assert( p->nFrame==0 );

  if( p->apCsr ){
    int i;
    for(i=0; i<p->nCursor; i++){
      VdbeCursor *pC = p->apCsr[i];
      if( pC ){
        sqlitexVdbeFreeCursor(p, pC);
        p->apCsr[i] = 0;
      }
    }
  }
  if( p->aMem ){
    releaseMemArray(&p->aMem[1], p->nMem);
  }
  while( p->pDelFrame ){
    VdbeFrame *pDel = p->pDelFrame;
    p->pDelFrame = pDel->pParent;
    sqlitexVdbeFrameDelete(pDel);
  }

  /* Delete any auxdata allocations made by the VM */
  if( p->pAuxData ) sqlitexVdbeDeleteAuxData(p, -1, 0);
  assert( p->pAuxData==0 );
}

/*
** Clean up the VM after a single run.
*/
static void Cleanup(Vdbe *p){
  sqlitex *db = p->db;

#ifdef SQLITE_DEBUG
  /* Execute assert() statements to ensure that the Vdbe.apCsr[] and 
  ** Vdbe.aMem[] arrays have already been cleaned up.  */
  int i;
  if( p->apCsr ) for(i=0; i<p->nCursor; i++) assert( p->apCsr[i]==0 );
  if( p->aMem ){
    for(i=1; i<=p->nMem; i++) assert( p->aMem[i].flags==MEM_Undefined );
  }
#endif

  sqlitexDbFree(db, p->zErrMsg);

#if 0
  We need this for prepared statements;
  Clean this when finalize the request;
  /* COMDB2 MODIFICATION */
  if( p->updCols){
    sqlitex_free( p->updCols );
    p->updCols = 0;
  }
#endif

  if (p->lockInfo) {
      free(p->lockInfo);
      p->lockInfo = NULL;
  }
  p->zErrMsg = 0;
  p->pResultSet = 0;
}

/*
** Set the number of result columns that will be returned by this SQL
** statement. This is now set at compile time, rather than during
** execution of the vdbe program so that sqlitex_column_count() can
** be called on an SQL statement before sqlitex_step().
*/
void sqlitexVdbeSetNumCols(Vdbe *p, int nResColumn){
  Mem *pColName;
  int n;
  sqlitex *db = p->db;

  releaseMemArray(p->aColName, p->nResColumn*COLNAME_N);
  sqlitexDbFree(db, p->aColName);
  n = nResColumn*COLNAME_N;
  p->nResColumn = (u16)nResColumn;
  p->aColName = pColName = (Mem*)sqlitexDbMallocZero(db, sizeof(Mem)*n );
  if( p->aColName==0 ) return;
  while( n-- > 0 ){
    pColName->flags = MEM_Null;
    pColName->db = p->db;
    pColName++;
  }
}

void sqlitexVdbeAddTable(Vdbe *p, Table *table){
  p->numTables++;
  if (p->tbls == NULL) {
    p->tbls = sqlitexMalloc(sizeof(Table*)); 
  } else {
    p->tbls = sqlitexRealloc(p->tbls, sizeof(Table*) * p->numTables);
  }
  p->tbls[p->numTables-1] = table;
}


/*
** Set the name of the idx'th column to be returned by the SQL statement.
** zName must be a pointer to a nul terminated string.
**
** This call must be made after a call to sqlitexVdbeSetNumCols().
**
** The final parameter, xDel, must be one of SQLITE_DYNAMIC, SQLITEX_STATIC
** or SQLITEX_TRANSIENT. If it is SQLITE_DYNAMIC, then the buffer pointed
** to by zName will be freed by sqlitexDbFree() when the vdbe is destroyed.
*/
int sqlitexVdbeSetColName(
  Vdbe *p,                         /* Vdbe being configured */
  int idx,                         /* Index of column zName applies to */
  int var,                         /* One of the COLNAME_* constants */
  const char *zName,               /* Pointer to buffer containing name */
  void (*xDel)(void*)              /* Memory management strategy for zName */
){
  int rc;
  Mem *pColName;
  assert( idx<p->nResColumn );
  assert( var<COLNAME_N );
  if( p->db->mallocFailed ){
    assert( !zName || xDel!=SQLITE_DYNAMIC );
    return SQLITE_NOMEM;
  }
  assert( p->aColName!=0 );
  pColName = &(p->aColName[idx+var*p->nResColumn]);
  rc = sqlitexVdbeMemSetStr(pColName, zName, -1, SQLITE_UTF8, xDel);
  assert( rc!=0 || !zName || (pColName->flags&MEM_Term)!=0 );
  return rc;
}

/*
** A read or write transaction may or may not be active on database handle
** db. If a transaction is active, commit it. If there is a
** write-transaction spanning more than one database file, this routine
** takes care of the master journal trickery.
*/
static int vdbeCommit(sqlitex *db, Vdbe *p){
  int i;
  int nTrans = 0;  /* Number of databases with an active write-transaction */
  int rc = SQLITE_OK;
  int needXcommit = 0;

#ifdef SQLITE_OMIT_VIRTUALTABLE
  /* With this option, sqlitexVtabSync() is defined to be simply 
  ** SQLITE_OK so p is not used. 
  */
  UNUSED_PARAMETER(p);
#endif

  /* Before doing anything else, call the xSync() callback for any
  ** virtual module tables written in this transaction. This has to
  ** be done before determining whether a master journal file is 
  ** required, as an xSync() callback may add an attached database
  ** to the transaction.
  */
  rc = sqlitexVtabSync(db, p);

  /* This loop determines (a) if the commit hook should be invoked and
  ** (b) how many database files have open write transactions, not 
  ** including the temp database. (b) is important because if more than 
  ** one database file has an open write transaction, a master journal
  ** file is required for an atomic commit.
  */ 
  for(i=0; rc==SQLITE_OK && i<db->nDb; i++){ 
    Btree *pBt = db->aDb[i].pBt;
    if( sqlite3BtreeIsInTrans(pBt) ){
      needXcommit = 1;
      if( i!=1 ) nTrans++;
      sqlite3BtreeEnter(pBt);
      /* COMDB2 MODIFICATION */
      /* FIXME: TODO: XXX */
      /* rc = sqlitexPagerExclusiveLock(sqlite3BtreePager(pBt)); */
      sqlite3BtreeLeave(pBt);
    }
  }
  if( rc!=SQLITE_OK ){
    return rc;
  }

  /* If there are any write-transactions at all, invoke the commit hook */
  if( needXcommit && db->xCommitCallback ){
    rc = db->xCommitCallback(db->pCommitArg);
    if( rc ){
      return SQLITE_CONSTRAINT_COMMITHOOK;
    }
  }

  /* The simple case - no more than one database file (not counting the
  ** TEMP database) has a transaction active.   There is no need for the
  ** master-journal.
  **
  ** If the return value of sqlite3BtreeGetFilename() is a zero length
  ** string, it means the main database is :memory: or a temp file.  In 
  ** that case we do not support atomic multi-file commits, so use the 
  ** simple case then too.
  */
  if( 0==sqlitexStrlen30(sqlite3BtreeGetFilename(db->aDb[0].pBt))
   || nTrans<=1
  ){
    for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        rc = sqlite3BtreeCommitPhaseOne(pBt, 0);
      }
    }

    /* Do the commit only if all databases successfully complete phase 1. 
    ** If one of the BtreeCommitPhaseOne() calls fails, this indicates an
    ** IO error while deleting or truncating a journal file. It is unlikely,
    ** but could happen. In this case abandon processing and return the error.
    */
    for(i=0; rc==SQLITE_OK && i<db->nDb; i++){
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        rc = sqlite3BtreeCommitPhaseTwo(pBt, 0);
      }
    }
    if( rc==SQLITE_OK ){
      sqlitexVtabCommit(db);
    }
  }

  /* The complex case - There is a multi-file write-transaction active.
  ** This requires a master journal file to ensure the transaction is
  ** committed atomically.
  */
#ifndef SQLITE_OMIT_DISKIO
  else{
    sqlitex_vfs *pVfs = db->pVfs;
    int needSync = 0;
    char *zMaster = 0;   /* File-name for the master journal */
    char const *zMainFile = sqlite3BtreeGetFilename(db->aDb[0].pBt);
    sqlitex_file *pMaster = 0;
    i64 offset = 0;
    int res;
    int retryCount = 0;
    int nMainFile;

    /* Select a master journal file name */
    nMainFile = sqlitexStrlen30(zMainFile);
    zMaster = sqlitexMPrintf(db, "%s-mjXXXXXX9XXz", zMainFile);
    if( zMaster==0 ) return SQLITE_NOMEM;
    do {
      u32 iRandom;
      if( retryCount ){
        if( retryCount>100 ){
          sqlitex_log(SQLITE_FULL, "MJ delete: %s", zMaster);
          sqlitexOsDelete(pVfs, zMaster, 0);
          break;
        }else if( retryCount==1 ){
          sqlitex_log(SQLITE_FULL, "MJ collide: %s", zMaster);
        }
      }
      retryCount++;
      sqlitex_randomness(sizeof(iRandom), &iRandom);
      sqlitex_snprintf(13, &zMaster[nMainFile], "-mj%06X9%02X",
                               (iRandom>>8)&0xffffff, iRandom&0xff);
      /* The antipenultimate character of the master journal name must
      ** be "9" to avoid name collisions when using 8+3 filenames. */
      assert( zMaster[sqlitexStrlen30(zMaster)-3]=='9' );
      sqlitexFileSuffix3(zMainFile, zMaster);
      rc = sqlitexOsAccess(pVfs, zMaster, SQLITE_ACCESS_EXISTS, &res);
    }while( rc==SQLITE_OK && res );
    if( rc==SQLITE_OK ){
      /* Open the master journal. */
      rc = sqlitexOsOpenMalloc(pVfs, zMaster, &pMaster, 
          SQLITE_OPEN_READWRITE|SQLITE_OPEN_CREATE|
          SQLITE_OPEN_EXCLUSIVE|SQLITE_OPEN_MASTER_JOURNAL, 0
      );
    }
    if( rc!=SQLITE_OK ){
      sqlitexDbFree(db, zMaster);
      return rc;
    }
 
    /* Write the name of each database file in the transaction into the new
    ** master journal file. If an error occurs at this point close
    ** and delete the master journal file. All the individual journal files
    ** still have 'null' as the master journal pointer, so they will roll
    ** back independently if a failure occurs.
    */
    for(i=0; i<db->nDb; i++){
      Btree *pBt = db->aDb[i].pBt;
      if( sqlite3BtreeIsInTrans(pBt) ){
        char const *zFile = sqlite3BtreeGetJournalname(pBt);
        if( zFile==0 ){
          continue;  /* Ignore TEMP and :memory: databases */
        }
        assert( zFile[0]!=0 );
        if( !needSync && !sqlite3BtreeSyncDisabled(pBt) ){
          needSync = 1;
        }
        rc = sqlitexOsWrite(pMaster, zFile, sqlitexStrlen30(zFile)+1, offset);
        offset += sqlitexStrlen30(zFile)+1;
        if( rc!=SQLITE_OK ){
          sqlitexOsCloseFree(pMaster);
          sqlitexOsDelete(pVfs, zMaster, 0);
          sqlitexDbFree(db, zMaster);
          return rc;
        }
      }
    }

    /* Sync the master journal file. If the IOCAP_SEQUENTIAL device
    ** flag is set this is not required.
    */
    if( needSync 
     && 0==(sqlitexOsDeviceCharacteristics(pMaster)&SQLITE_IOCAP_SEQUENTIAL)
     && SQLITE_OK!=(rc = sqlitexOsSync(pMaster, SQLITE_SYNC_NORMAL))
    ){
      sqlitexOsCloseFree(pMaster);
      sqlitexOsDelete(pVfs, zMaster, 0);
      sqlitexDbFree(db, zMaster);
      return rc;
    }

    /* Sync all the db files involved in the transaction. The same call
    ** sets the master journal pointer in each individual journal. If
    ** an error occurs here, do not delete the master journal file.
    **
    ** If the error occurs during the first call to
    ** sqlite3BtreeCommitPhaseOne(), then there is a chance that the
    ** master journal file will be orphaned. But we cannot delete it,
    ** in case the master journal file name was written into the journal
    ** file before the failure occurred.
    */
    for(i=0; rc==SQLITE_OK && i<db->nDb; i++){ 
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        rc = sqlite3BtreeCommitPhaseOne(pBt, zMaster);
      }
    }
    sqlitexOsCloseFree(pMaster);
    assert( rc!=SQLITE_BUSY );
    if( rc!=SQLITE_OK ){
      sqlitexDbFree(db, zMaster);
      return rc;
    }

    /* Delete the master journal file. This commits the transaction. After
    ** doing this the directory is synced again before any individual
    ** transaction files are deleted.
    */
    rc = sqlitexOsDelete(pVfs, zMaster, needSync);
    sqlitexDbFree(db, zMaster);
    zMaster = 0;
    if( rc ){
      return rc;
    }

    /* All files and directories have already been synced, so the following
    ** calls to sqlite3BtreeCommitPhaseTwo() are only closing files and
    ** deleting or truncating journals. If something goes wrong while
    ** this is happening we don't really care. The integrity of the
    ** transaction is already guaranteed, but some stray 'cold' journals
    ** may be lying around. Returning an error code won't help matters.
    */
    disable_simulated_io_errors();
    sqlitexBeginBenignMalloc();
    for(i=0; i<db->nDb; i++){ 
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        sqlite3BtreeCommitPhaseTwo(pBt, 1);
      }
    }
    sqlitexEndBenignMalloc();
    enable_simulated_io_errors();

    sqlitexVtabCommit(db);
  }
#endif

  return rc;
}

/* 
** This routine checks that the sqlitex.nVdbeActive count variable
** matches the number of vdbe's in the list sqlitex.pVdbe that are
** currently active. An assertion fails if the two counts do not match.
** This is an internal self-check only - it is not an essential processing
** step.
**
** This is a no-op if NDEBUG is defined.
*/
#ifndef NDEBUG
static void checkActiveVdbeCnt(sqlitex *db){
  Vdbe *p;
  int cnt = 0;
  int nWrite = 0;
  int nRead = 0;
  p = db->pVdbe;
  while( p ){
    if( sqlitex_stmt_busy((sqlitex_stmt*)p) ){
      cnt++;
      if( p->readOnly==0 ) nWrite++;
      if( p->bIsReader ) nRead++;
    }
    p = p->pNext;
  }
  assert( cnt==db->nVdbeActive );
  assert( nWrite==db->nVdbeWrite );
  assert( nRead==db->nVdbeRead );
}
#else
#define checkActiveVdbeCnt(x)
#endif

/*
** If the Vdbe passed as the first argument opened a statement-transaction,
** close it now. Argument eOp must be either SAVEPOINT_ROLLBACK or
** SAVEPOINT_RELEASE. If it is SAVEPOINT_ROLLBACK, then the statement
** transaction is rolled back. If eOp is SAVEPOINT_RELEASE, then the 
** statement transaction is committed.
**
** If an IO error occurs, an SQLITE_IOERR_XXX error code is returned. 
** Otherwise SQLITE_OK.
*/
int sqlitexVdbeCloseStatement(Vdbe *p, int eOp){
  sqlitex *const db = p->db;
  int rc = SQLITE_OK;

  /* If p->iStatement is greater than zero, then this Vdbe opened a 
  ** statement transaction that should be closed here. The only exception
  ** is that an IO error may have occurred, causing an emergency rollback.
  ** In this case (db->nStatement==0), and there is nothing to do.
  */
  if( db->nStatement && p->iStatement ){
    int i;
    const int iSavepoint = p->iStatement-1;

    assert( eOp==SAVEPOINT_ROLLBACK || eOp==SAVEPOINT_RELEASE);
    assert( db->nStatement>0 );
    assert( p->iStatement==(db->nStatement+db->nSavepoint) );

    for(i=0; i<db->nDb; i++){ 
      int rc2 = SQLITE_OK;
      Btree *pBt = db->aDb[i].pBt;
      if( pBt ){
        if( eOp==SAVEPOINT_ROLLBACK ){
          rc2 = sqlite3BtreeSavepoint(pBt, SAVEPOINT_ROLLBACK, iSavepoint);
        }
        if( rc2==SQLITE_OK ){
          rc2 = sqlite3BtreeSavepoint(pBt, SAVEPOINT_RELEASE, iSavepoint);
        }
        if( rc==SQLITE_OK ){
          rc = rc2;
        }
      }
    }
    db->nStatement--;
    p->iStatement = 0;

    if( rc==SQLITE_OK ){
      if( eOp==SAVEPOINT_ROLLBACK ){
        rc = sqlitexVtabSavepoint(db, SAVEPOINT_ROLLBACK, iSavepoint);
      }
      if( rc==SQLITE_OK ){
        rc = sqlitexVtabSavepoint(db, SAVEPOINT_RELEASE, iSavepoint);
      }
    }

    /* If the statement transaction is being rolled back, also restore the 
    ** database handles deferred constraint counter to the value it had when 
    ** the statement transaction was opened.  */
    if( eOp==SAVEPOINT_ROLLBACK ){
      db->nDeferredCons = p->nStmtDefCons;
      db->nDeferredImmCons = p->nStmtDefImmCons;
    }
  }
  return rc;
}

/*
** This function is called when a transaction opened by the database 
** handle associated with the VM passed as an argument is about to be 
** committed. If there are outstanding deferred foreign key constraint
** violations, return SQLITE_ERROR. Otherwise, SQLITE_OK.
**
** If there are outstanding FK violations and this function returns 
** SQLITE_ERROR, set the result of the VM to SQLITE_CONSTRAINT_FOREIGNKEY
** and write an error message to it. Then return SQLITE_ERROR.
*/
#ifndef SQLITE_OMIT_FOREIGN_KEY
int sqlitexVdbeCheckFk(Vdbe *p, int deferred){
  sqlitex *db = p->db;
  if( (deferred && (db->nDeferredCons+db->nDeferredImmCons)>0) 
   || (!deferred && p->nFkConstraint>0) 
  ){
    p->rc = SQLITE_CONSTRAINT_FOREIGNKEY;
    p->errorAction = OE_Abort;
    sqlitexSetString(&p->zErrMsg, db, "FOREIGN KEY constraint failed");
    return SQLITE_ERROR;
  }
  return SQLITE_OK;
}
#endif

/*
** This routine is called the when a VDBE tries to halt.  If the VDBE
** has made changes and is in autocommit mode, then commit those
** changes.  If a rollback is needed, then do the rollback.
**
** This routine is the only way to move the state of a VM from
** SQLITE_MAGIC_RUN to SQLITE_MAGIC_HALT.  It is harmless to
** call this on a VM that is in the SQLITE_MAGIC_HALT state.
**
** Return an error code.  If the commit could not complete because of
** lock contention, return SQLITE_BUSY.  If SQLITE_BUSY is returned, it
** means the close did not happen and needs to be repeated.
*/
int sqlitexVdbeHalt(Vdbe *p){

  int rc;                         /* Used to store transient return codes */
  sqlitex *db = p->db;

  /* This function contains the logic that determines if a statement or
  ** transaction will be committed or rolled back as a result of the
  ** execution of this virtual machine. 
  **
  ** If any of the following errors occur:
  **
  **     SQLITE_NOMEM
  **     SQLITE_IOERR
  **     SQLITE_FULL
  **     SQLITE_INTERRUPT
  **
  ** Then the internal cache might have been left in an inconsistent
  ** state.  We need to rollback the statement transaction, if there is
  ** one, or the complete transaction if there is no statement transaction.
  */

  if( p->db->mallocFailed ){
    p->rc = SQLITE_NOMEM;
  }
  if( p->aOnceFlag ) memset(p->aOnceFlag, 0, p->nOnceFlag);
  closeAllCursors(p);
  if( p->magic!=VDBE_MAGIC_RUN ){
    return SQLITE_OK;
  }
  checkActiveVdbeCnt(db);

  /* No commit or rollback needed if the program never started or if the
  ** SQL statement does not read or write a database file.  */
  if( p->pc>=0 && p->bIsReader ){
    int mrc;   /* Primary error code from p->rc */
    int eStatementOp = 0;
    int isSpecialError;            /* Set to true if a 'special' error */

    /* Lock all btrees used by the statement */
    sqlitexVdbeEnter(p);

    /* Check for one of the special errors */
    mrc = p->rc & 0xff;
    isSpecialError = mrc==SQLITE_NOMEM || mrc==SQLITE_IOERR
                     || mrc==SQLITE_INTERRUPT || mrc==SQLITE_FULL;
    if( isSpecialError ){
      /* If the query was read-only and the error code is SQLITE_INTERRUPT, 
      ** no rollback is necessary. Otherwise, at least a savepoint 
      ** transaction must be rolled back to restore the database to a 
      ** consistent state.
      **
      ** Even if the statement is read-only, it is important to perform
      ** a statement or transaction rollback operation. If the error 
      ** occurred while writing to the journal, sub-journal or database
      ** file as part of an effort to free up cache space (see function
      ** pagerStress() in pager.c), the rollback is required to restore 
      ** the pager to a consistent state.
      */
      if( !p->readOnly || mrc!=SQLITE_INTERRUPT ){
        if( (mrc==SQLITE_NOMEM || mrc==SQLITE_FULL) && p->usesStmtJournal ){
          eStatementOp = SAVEPOINT_ROLLBACK;
        }else{
          /* We are forced to roll back the active transaction. Before doing
          ** so, abort any other statements this handle currently has active.
          */
          sqlitexRollbackAll(db, SQLITE_ABORT_ROLLBACK);
          sqlitexCloseSavepoints(db);
          db->autoCommit = 1;
          p->nChange = 0;
        }
      }
    }

    /* Check for immediate foreign key violations. */
    if( p->rc==SQLITE_OK ){
      sqlitexVdbeCheckFk(p, 0);
    }
  
    /* If the auto-commit flag is set and this is the only active writer 
    ** VM, then we do either a commit or rollback of the current transaction. 
    **
    ** Note: This block also runs if one of the special errors handled 
    ** above has occurred. 
    */
    if( !sqlitexVtabInSync(db) 
     && db->autoCommit 
     && db->nVdbeWrite==(p->readOnly==0) 
    ){
      if( p->rc==SQLITE_OK || (p->errorAction==OE_Fail && !isSpecialError) ){
        rc = sqlitexVdbeCheckFk(p, 1);
        if( rc!=SQLITE_OK ){
          if( NEVER(p->readOnly) ){
            sqlitexVdbeLeave(p);
            return SQLITE_ERROR;
          }
          rc = SQLITE_CONSTRAINT_FOREIGNKEY;
        }else{ 
          /* The auto-commit flag is true, the vdbe program was successful 
          ** or hit an 'OR FAIL' constraint and there are no deferred foreign
          ** key constraints to hold up the transaction. This means a commit 
          ** is required. */
          rc = vdbeCommit(db, p);
        }
        if( rc==SQLITE_BUSY && p->readOnly ){
          sqlitexVdbeLeave(p);

          /* COMDB2 MODIFICATION */
          printf("%s:%d SQLITE_BUSY\n", __FILE__, __LINE__);
          cheap_stack_trace();

          return SQLITE_BUSY;
        }else if( rc!=SQLITE_OK ){
          p->rc = rc;
          sqlitexRollbackAll(db, SQLITE_OK);
          p->nChange = 0;
        }else{
          db->nDeferredCons = 0;
          db->nDeferredImmCons = 0;
          db->flags &= ~SQLITE_DeferFKs;
          sqlitexCommitInternalChanges(db);
        }
      }else{
        sqlitexRollbackAll(db, SQLITE_OK);
        p->nChange = 0;
      }
      db->nStatement = 0;
    }else if( eStatementOp==0 ){
      if( p->rc==SQLITE_OK || p->errorAction==OE_Fail ){
        eStatementOp = SAVEPOINT_RELEASE;
      }else if( p->errorAction==OE_Abort ){
        eStatementOp = SAVEPOINT_ROLLBACK;
      }else{
        sqlitexRollbackAll(db, SQLITE_ABORT_ROLLBACK);
        sqlitexCloseSavepoints(db);
        db->autoCommit = 1;
        p->nChange = 0;
      }
    }
  
    /* If eStatementOp is non-zero, then a statement transaction needs to
    ** be committed or rolled back. Call sqlitexVdbeCloseStatement() to
    ** do so. If this operation returns an error, and the current statement
    ** error code is SQLITE_OK or SQLITE_CONSTRAINT, then promote the
    ** current statement error code.
    */
    if( eStatementOp ){
      rc = sqlitexVdbeCloseStatement(p, eStatementOp);
      if( rc ){
        if( p->rc==SQLITE_OK || (p->rc&0xff)==SQLITE_CONSTRAINT ){
          p->rc = rc;
          sqlitexDbFree(db, p->zErrMsg);
          p->zErrMsg = 0;
        }
        sqlitexRollbackAll(db, SQLITE_ABORT_ROLLBACK);
        sqlitexCloseSavepoints(db);
        db->autoCommit = 1;
        p->nChange = 0;
      }
    }
  
    /* If this was an INSERT, UPDATE or DELETE and no statement transaction
    ** has been rolled back, update the database connection change-counter. 
    */
    if( p->changeCntOn ){
      if( eStatementOp!=SAVEPOINT_ROLLBACK ){
        sqlitexVdbeSetChanges(db, p->nChange);
      }else{
        sqlitexVdbeSetChanges(db, 0);
      }
      p->nChange = 0;
    }

    /* Release the locks */
    sqlitexVdbeLeave(p);
  }

  /* We have successfully halted and closed the VM.  Record this fact. */
  if( p->pc>=0 ){
    db->nVdbeActive--;
    if( !p->readOnly ) db->nVdbeWrite--;
    if( p->bIsReader ) db->nVdbeRead--;
    assert( db->nVdbeActive>=db->nVdbeRead );
    assert( db->nVdbeRead>=db->nVdbeWrite );
    assert( db->nVdbeWrite>=0 );
  }
  p->magic = VDBE_MAGIC_HALT;
  checkActiveVdbeCnt(db);
  if( p->db->mallocFailed ){
    p->rc = SQLITE_NOMEM;
  }

  /* If the auto-commit flag is set to true, then any locks that were held
  ** by connection db have now been released. Call sqlitexConnectionUnlocked() 
  ** to invoke any required unlock-notify callbacks.
  */
  if( db->autoCommit ){
    sqlitexConnectionUnlocked(db);
  }

  assert( db->nVdbeActive>0 || db->autoCommit==0 || db->nStatement==0 );
  return (p->rc==SQLITE_BUSY ? SQLITE_BUSY : SQLITE_OK);
}


/*
** Each VDBE holds the result of the most recent sqlitex_step() call
** in p->rc.  This routine sets that result back to SQLITE_OK.
*/
void sqlitexVdbeResetStepResult(Vdbe *p){
  p->rc = SQLITE_OK;
}

/* COMDB2 Modification. 
** Reset the tspec before execution of new statement. 
 */
int sqlitexVdbeResetClock(Vdbe *p){
  if(p) {
    clock_gettime(CLOCK_REALTIME, &p->tspec);
  }
  return SQLITE_OK;
}
/*
** Copy the error code and error message belonging to the VDBE passed
** as the first argument to its database handle (so that they will be 
** returned by calls to sqlitex_errcode() and sqlitex_errmsg()).
**
** This function does not clear the VDBE error code or message, just
** copies them to the database handle.
*/
int sqlitexVdbeTransferError(Vdbe *p){
  sqlitex *db = p->db;
  int rc = p->rc;
  if( p->zErrMsg ){
    u8 mallocFailed = db->mallocFailed;
    sqlitexBeginBenignMalloc();
    if( db->pErr==0 ) db->pErr = sqlitexValueNew(db);
    sqlitexValueSetStr(db->pErr, -1, p->zErrMsg, SQLITE_UTF8, SQLITEX_TRANSIENT);
    sqlitexEndBenignMalloc();
    db->mallocFailed = mallocFailed;
    db->errCode = rc;
  }else{
    sqlitexError(db, rc);
  }
  return rc;
}

#ifdef SQLITE_ENABLE_SQLLOG
/*
** If an SQLITE_CONFIG_SQLLOG hook is registered and the VM has been run, 
** invoke it.
*/
static void vdbeInvokeSqllog(Vdbe *v){
  if( sqlitexGlobalConfig.xSqllog && v->rc==SQLITE_OK && v->zSql && v->pc>=0 ){
    char *zExpanded = sqlitexVdbeExpandSql(v, v->zSql);
    assert( v->db->init.busy==0 );
    if( zExpanded ){
      sqlitexGlobalConfig.xSqllog(
          sqlitexGlobalConfig.pSqllogArg, v->db, zExpanded, 1
      );
      sqlitexDbFree(v->db, zExpanded);
    }
  }
}
#else
# define vdbeInvokeSqllog(x)
#endif

/*
** Clean up a VDBE after execution but do not delete the VDBE just yet.
** Write any error messages into *pzErrMsg.  Return the result code.
**
** After this routine is run, the VDBE should be ready to be executed
** again.
**
** To look at it another way, this routine resets the state of the
** virtual machine from VDBE_MAGIC_RUN or VDBE_MAGIC_HALT back to
** VDBE_MAGIC_INIT.
*/
int sqlitexVdbeReset(Vdbe *p){
  sqlitex *db;
  db = p->db;

  /* If the VM did not run to completion or if it encountered an
  ** error, then it might not have been halted properly.  So halt
  ** it now.
  */
  sqlitexVdbeHalt(p);

  /* If the VDBE has be run even partially, then transfer the error code
  ** and error message from the VDBE into the main database structure.  But
  ** if the VDBE has just been set to run but has not actually executed any
  ** instructions yet, leave the main database error information unchanged.
  */
  if( p->pc>=0 ){
    vdbeInvokeSqllog(p);
    sqlitexVdbeTransferError(p);
    sqlitexDbFree(db, p->zErrMsg);
    p->zErrMsg = 0;
    if( p->runOnlyOnce ) p->expired = 1;
  }else if( p->rc && p->expired ){
    /* The expired flag was set on the VDBE before the first call
    ** to sqlitex_step(). For consistency (since sqlitex_step() was
    ** called), set the database error in this case as well.
    */
    sqlitexErrorWithMsg(db, p->rc, p->zErrMsg ? "%s" : 0, p->zErrMsg);
    sqlitexDbFree(db, p->zErrMsg);
    p->zErrMsg = 0;
  }

  /* Reclaim all memory used by the VDBE
  */
  Cleanup(p);

  /* Save profiling information from this VDBE run.
  */
#ifdef VDBE_PROFILE
  {
    FILE *out = fopen("vdbe_profile.out", "a");
    if( out ){
      int i;
      fprintf(out, "---- ");
      for(i=0; i<p->nOp; i++){
        fprintf(out, "%02x", p->aOp[i].opcode);
      }
      fprintf(out, "\n");
      if( p->zSql ){
        char c, pc = 0;
        fprintf(out, "-- ");
        for(i=0; (c = p->zSql[i])!=0; i++){
          if( pc=='\n' ) fprintf(out, "-- ");
          putc(c, out);
          pc = c;
        }
        if( pc!='\n' ) fprintf(out, "\n");
      }
      for(i=0; i<p->nOp; i++){
        char zHdr[100];
        sqlitex_snprintf(sizeof(zHdr), zHdr, "%6u %12llu %8llu ",
           p->aOp[i].cnt,
           p->aOp[i].cycles,
           p->aOp[i].cnt>0 ? p->aOp[i].cycles/p->aOp[i].cnt : 0
        );
        fprintf(out, "%s", zHdr);
        sqlitexVdbePrintOp(out, i, &p->aOp[i]);
      }
      fclose(out);
    }
  }
#endif
  p->iCurrentTime = 0;
  p->magic = VDBE_MAGIC_INIT;
  return p->rc & db->errMask;
}
 
/*
** Clean up and delete a VDBE after execution.  Return an integer which is
** the result code.  Write any error message text into *pzErrMsg.
*/
int sqlitexVdbeFinalize(Vdbe *p){
  int rc = SQLITE_OK;
  if( p->magic==VDBE_MAGIC_RUN || p->magic==VDBE_MAGIC_HALT ){

    /* COMDB2 MODIFICATION */
    /* this needs to called when we finalize, not on reset */
    if( p->updCols){
      sqlitex_free( p->updCols );
      p->updCols = 0;
    }

    rc = sqlitexVdbeReset(p);
    assert( (rc & p->db->errMask)==rc );
  }
  sqlitexVdbeDelete(p);
  return 0;
}

/*
** If parameter iOp is less than zero, then invoke the destructor for
** all auxiliary data pointers currently cached by the VM passed as
** the first argument.
**
** Or, if iOp is greater than or equal to zero, then the destructor is
** only invoked for those auxiliary data pointers created by the user 
** function invoked by the OP_Function opcode at instruction iOp of 
** VM pVdbe, and only then if:
**
**    * the associated function parameter is the 32nd or later (counting
**      from left to right), or
**
**    * the corresponding bit in argument mask is clear (where the first
**      function parameter corresponds to bit 0 etc.).
*/
void sqlitexVdbeDeleteAuxData(Vdbe *pVdbe, int iOp, int mask){
  AuxData **pp = &pVdbe->pAuxData;
  while( *pp ){
    AuxData *pAux = *pp;
    if( (iOp<0)
     || (pAux->iOp==iOp && (pAux->iArg>31 || !(mask & MASKBIT32(pAux->iArg))))
    ){
      testcase( pAux->iArg==31 );
      if( pAux->xDelete ){
        pAux->xDelete(pAux->pAux);
      }
      *pp = pAux->pNext;
      sqlitexDbFree(pVdbe->db, pAux);
    }else{
      pp= &pAux->pNext;
    }
  }
}

/*
** Free all memory associated with the Vdbe passed as the second argument,
** except for object itself, which is preserved.
**
** The difference between this function and sqlitexVdbeDelete() is that
** VdbeDelete() also unlinks the Vdbe from the list of VMs associated with
** the database connection and frees the object itself.
*/
void sqlitexVdbeClearObject(sqlitex *db, Vdbe *p){
  SubProgram *pSub, *pNext;
  int i;
  assert( p->db==0 || p->db==db );
  releaseMemArray(p->aVar, p->nVar);
  releaseMemArray(p->aColName, p->nResColumn*COLNAME_N);
  for(pSub=p->pProgram; pSub; pSub=pNext){
    pNext = pSub->pNext;
    vdbeFreeOpArray(db, pSub->aOp, pSub->nOp);
    sqlitexDbFree(db, pSub);
  }
  for(i=p->nzVar-1; i>=0; i--) sqlitexDbFree(db, p->azVar[i]);
  vdbeFreeOpArray(db, p->aOp, p->nOp);
  sqlitexDbFree(db, p->aColName);
  sqlitexDbFree(db, p->zSql);
#ifdef SQLITE_ENABLE_NORMALIZE
  sqlitexDbFree(db, p->zNormSql);
  {
    DblquoteStr *pThis, *pNext;
    for(pThis=p->pDblStr; pThis; pThis=pNext){
      pNext = pThis->pNextStr;
      sqlitexDbFree(db, pThis);
    }
  }
#endif
  sqlitexDbFree(db, p->pFree);
#ifdef SQLITE_ENABLE_STMT_SCANSTATUS
  for(i=0; i<p->nScan; i++){
    sqlitexDbFree(db, p->aScan[i].zName);
  }
  sqlitexDbFree(db, p->aScan);
#endif
}

/*
** Delete an entire VDBE.
*/
void sqlitexVdbeDelete(Vdbe *p){
  sqlitex *db;

  if( NEVER(p==0) ) return;
  db = p->db;
  assert( sqlitex_mutex_held(db->mutex) );
  sqlitexVdbeClearObject(db, p);
  if( p->pPrev ){
    p->pPrev->pNext = p->pNext;
  }else{
    assert( db->pVdbe==p );
    db->pVdbe = p->pNext;
  }
  if( p->pNext ){
    p->pNext->pPrev = p->pPrev;
  }
  p->magic = VDBE_MAGIC_DEAD;

  /* COMDB2 MODIFICATION */
  if( p->explainTrace ){
    free(p->explainTrace);
  }

  /* COMDB2 MODIFICATION */
  if( p->tbls){
    sqlitex_free( p->tbls );
    p->tbls = NULL;
    p->numTables = 0;
  }  
  if(p->lockInfo) {
    free(p->lockInfo);
    p->lockInfo = NULL;
  }
  if (p->trackIdentifiers) {
    sqlitexHashForeach(&p->identifiers, (void(*)(char*s)) free);
    sqlitexHashClear(&p->identifiers);
  }

  p->db = 0;

  sqlitexDbFree(db, p);
}

/* COMDB2 MODIFICATION */
char *sqlite3BtreeGetTblName(BtCursor *pCur);
int sqlite3BtreeIsEOF(BtCursor *pCur);
int sqlite3BtreeIsEmpty(BtCursor *pCur);
int sqlite3BtreeCursorRestore(BtCursor*, int*);
unsigned long long bdb_genid_to_host_order(unsigned long long genid);

/*
** The cursor "p" has a pending seek operation that has not yet been
** carried out.  Seek the cursor now.  If an error occurs, return
** the appropriate error code.
*/
static int SQLITE_NOINLINE handleDeferredMoveto(VdbeCursor *p){
  int res, rc;
#ifdef SQLITE_TEST
  extern int sqlitex_search_count;
#endif
  assert( p->deferredMoveto );
  assert( p->isTable );

  extern void abort_on_dta_error();

  /* COMDB2 MODIFICATION */
  /* NOTE: this will only be used for table direct lookup, which is not
     happening for remote cursors */
  rc = sqlite3BtreeMovetoUnpacked(p->pCursor, 0, p->movetoTarget, 
          /* COMDB2 MODIFICATION */ OP_Seek /* the only OP that gets you in this mode*/, &res);

  if( rc ) return rc;
  if( res!=0) {
       char errmsg[256];
       snprintf(errmsg, sizeof(errmsg),
          "Dta lookup lost the race for tbl %s genid=%llu (%llx) [new]\n",
          sqlite3BtreeGetTblName(p->pCursor),
          bdb_genid_to_host_order(p->movetoTarget), 
          bdb_genid_to_host_order(p->movetoTarget));
          fprintf( stderr, "%s EOF %d Empty %d\n",
                   errmsg, sqlite3BtreeIsEOF(p->pCursor), sqlite3BtreeIsEmpty(p->pCursor));

       abort_on_dta_error();

       return SQLITE_DEADLOCK;
    }
    /* COMDB2 MODIFICATION */
    /* this is a direct lookup for a genid;
     * genid might dissapear because of race or
     * data corruption
     * Make sure that what we found is what we're looking for */
    {
      i64 genid;
      sqlite3BtreeKey(p->pCursor, 0, sizeof(i64), &genid);
      if( p->movetoTarget != genid ){
        fprintf(stderr, 
          "Dta lookup lost the race for tbl %s genid=%llu found=%llu\n",
          sqlite3BtreeGetTblName(p->pCursor),
          p->movetoTarget, genid);
        abort_on_dta_error();
        return SQLITE_DEADLOCK;
      }
    }
#ifdef SQLITE_TEST
  sqlitex_search_count++;
#endif
  p->deferredMoveto = 0;
  p->cacheStatus = CACHE_STALE;
  return SQLITE_OK;
}

/*
** Something has moved cursor "p" out of place.  Maybe the row it was
** pointed to was deleted out from under it.  Or maybe the btree was
** rebalanced.  Whatever the cause, try to restore "p" to the place it
** is supposed to be pointing.  If the row was deleted out from under the
** cursor, set the cursor to point to a NULL row.
*/
static int SQLITE_NOINLINE handleMovedCursor(VdbeCursor *p){
  int isDifferentRow, rc;
  assert( p->pCursor!=0 );
  assert( sqlite3BtreeCursorHasMoved(p->pCursor) );
  rc = sqlite3BtreeCursorRestore(p->pCursor, &isDifferentRow);
  p->cacheStatus = CACHE_STALE;
  if( isDifferentRow ) p->nullRow = 1;
  return rc;
}

/*
** Check to ensure that the cursor is valid.  Restore the cursor
** if need be.  Return any I/O error from the restore operation.
*/
int sqlitexVdbeCursorRestore(VdbeCursor *p){
  if( sqlite3BtreeCursorHasMoved(p->pCursor) ){
    return handleMovedCursor(p);
  }
  return SQLITE_OK;
}

/*
** Make sure the cursor p is ready to read or write the row to which it
** was last positioned.  Return an error code if an OOM fault or I/O error
** prevents us from positioning the cursor to its correct position.
**
** If a MoveTo operation is pending on the given cursor, then do that
** MoveTo now.  If no move is pending, check to see if the row has been
** deleted out from under the cursor and if it has, mark the row as
** a NULL row.
**
** If the cursor is already pointing to the correct row and that row has
** not been deleted out from under the cursor, then this routine is a no-op.
*/
int sqlitexVdbeCursorMoveto(VdbeCursor **pp, int *piCol){
  VdbeCursor *p = *pp;
  if( p->deferredMoveto ){
    int iMap;
    if( p->aAltMap && (iMap = p->aAltMap[1+*piCol])>0 ){
      *pp = p->pAltCursor;
      *piCol = iMap - 1;
      return SQLITE_OK;
    }
    return handleDeferredMoveto(p);
  }
  if( p->pCursor && sqlite3BtreeCursorHasMoved(p->pCursor) ){
    return handleMovedCursor(p);
  }
  return SQLITE_OK;
}

#include <serialget.c>

/*
** The following functions:
**
** sqlitexVdbeSerialType()
** sqlitexVdbeSerialTypeLen()
** sqlitexVdbeSerialLen()
** sqlitexVdbeSerialPut()
** sqlitexVdbeSerialGet()
**
** encapsulate the code that serializes values for storage in SQLite
** data and index records. Each serialized value consists of a
** 'serial-type' and a blob of data. The serial type is an 8-byte unsigned
** integer, stored as a varint.
**
** In an SQLite index record, the serial type is stored directly before
** the blob of data that it corresponds to. In a table record, all serial
** types are stored at the start of the record, and the blobs of data at
** the end. Hence these functions allow the caller to handle the
** serial-type and data blob separately.
**
** The following table describes the various storage classes for data:
**
**   serial type        bytes of data      type
**   --------------     ---------------    ---------------
**      0                     0            NULL
**      1                     1            signed integer
**      2                     2            signed integer
**      3                     3            signed integer
**      4                     4            signed integer
**      5                     6            signed integer
**      6                     8            signed integer
**      7                     8            IEEE float
**      8                     0            Integer constant 0
**      9                     0            Integer constant 1
**     10                                  interval COMDB2 MODIFICATION
**     11                                  datetime COMDB2 MODIFICATION
**    N>=12 and even       (N-12)/2        BLOB
**    N>=13 and odd        (N-13)/2        text
**    SQLITE_MAX_U32-1                     intervaldsus COMDB2 MODIFICATION
**    SQLITE_MAX_U32                       datetimeus COMDB2 MODIFICATION
**
** The 8 and 9 types were added in 3.3.0, file format 4.  Prior versions
** of SQLite will not understand those serial types.
**
** COMDB2 MODIFICATION
** The (SQLITE_MAX_U32-1) and SQLITE_MAX_U32 were added in R6. Prior versions
** of Comdb2 will not understand those serial types.
** Note for future expansion:
** When introducing new serial types greater than 11, please update
** sqlitexIsFixedLengthSerialType accordingly.
*/

/*
** Return the serial-type for the value stored in pMem.
*/
u32 sqlitexVdbeSerialType(Mem *pMem, int file_format){
  int flags = pMem->flags;
  u32 n;

  if( flags&MEM_Null ){
    return 0;
  }
  if( flags&MEM_Int ){
    /* COMDB2 MODIFICATION */
    return 6;
#if 0
    /* Figure out whether to use 1, 2, 4, 6 or 8 bytes. */
#   define MAX_6BYTE ((((i64)0x00008000)<<32)-1)
    i64 i = pMem->u.i;
    u64 u;
    if( i<0 ){
      u = ~i;
    }else{
      u = i;
    }
    if( u<=127 ){
      return ((i&1)==i && file_format>=4) ? 8+(u32)u : 1;
    }
    if( u<=32767 ) return 2;
    if( u<=8388607 ) return 3;
    if( u<=2147483647 ) return 4;
    if( u<=MAX_6BYTE ) return 5;
    return 6;
#endif
  }
  if( flags&MEM_Real ){
    return 7;
  }

  /* COMDB2 MODIFICATION */
  if( flags & MEM_Interval ){
      /* use a rezerved serial to specify an interval */
      if (pMem->du.tv.type == INTV_DSUS_TYPE)
        return (SQLITE_MAX_U32-1);
      return 10;
  }

  /* COMDB2 MODIFICATION */
  if( flags & MEM_Datetime ){
      /* use a rezerved serial to specify a datetime */
      if (pMem->du.dt.dttz_prec == DTTZ_PREC_USEC)
        return SQLITE_MAX_U32;
      return 11;
  }

  assert( (pMem->db && pMem->db->mallocFailed) || flags&(MEM_Str|MEM_Blob) );
  assert( pMem->n>=0 );
  n = (u32)pMem->n;
  if( flags & MEM_Zero ){
    n += pMem->u.nZero;
  }
  return ((n*2) + 12 + ((flags&MEM_Str)!=0));
}

/*
** Return the length of the data corresponding to the supplied serial-type.
*/
u32 sqlitexVdbeSerialTypeLen(u32 serial_type){
  /* COMDB2 MODIFICATION */
  if( serial_type==(SQLITE_MAX_U32-1) ){
    return sizeof(intv_t);
  }else if( serial_type==SQLITE_MAX_U32 ){
    return sizeof(dttz_t);
  }else if( serial_type>=12 ){
    return (serial_type-12)/2;
  }else{
    static const u8 aSize[] = { 0, 1, 2, 3, 4, 6, 8, 8, 0, 0, 0, 0 };

    /* COMDB2 MODIFICATION */
    if( serial_type == 10 ) return offsetof(intv_t, u.ds.prec);
    if( serial_type == 11 ) return sizeof(dttz_t); 

    return aSize[serial_type];
  }
}

/*
** If we are on an architecture with mixed-endian floating 
** points (ex: ARM7) then swap the lower 4 bytes with the 
** upper 4 bytes.  Return the result.
**
** For most architectures, this is a no-op.
**
** (later):  It is reported to me that the mixed-endian problem
** on ARM7 is an issue with GCC, not with the ARM7 chip.  It seems
** that early versions of GCC stored the two words of a 64-bit
** float in the wrong order.  And that error has been propagated
** ever since.  The blame is not necessarily with GCC, though.
** GCC might have just copying the problem from a prior compiler.
** I am also told that newer versions of GCC that follow a different
** ABI get the byte order right.
**
** Developers using SQLite on an ARM7 should compile and run their
** application using -DSQLITE_DEBUG=1 at least once.  With DEBUG
** enabled, some asserts below will ensure that the byte order of
** floating point values is correct.
**
** (2007-08-30)  Frank van Vugt has studied this problem closely
** and has send his findings to the SQLite developers.  Frank
** writes that some Linux kernels offer floating point hardware
** emulation that uses only 32-bit mantissas instead of a full 
** 48-bits as required by the IEEE standard.  (This is the
** CONFIG_FPE_FASTFPE option.)  On such systems, floating point
** byte swapping becomes very complicated.  To avoid problems,
** the necessary byte swapping is carried out using a 64-bit integer
** rather than a 64-bit float.  Frank assures us that the code here
** works for him.  We, the developers, have no way to independently
** verify this, but Frank seems to know what he is talking about
** so we trust him.
*/
#define START_INLINE_SERIALGET
#ifdef SQLITE_MIXED_ENDIAN_64BIT_FLOAT
static u64 floatSwap(u64 in){
  union {
    u64 r;
    u32 i[2];
  } u;
  u32 t;

  u.r = in;
  t = u.i[0];
  u.i[0] = u.i[1];
  u.i[1] = t;
  return u.r;
}
# define swapMixedEndianFloat(X)  X = floatSwap(X)
#else
# define swapMixedEndianFloat(X)
#endif

#include <arpa/inet.h>
#include <flibc.h>

#define END_INLINE_SERIALGET

/*
** Write the serialized data blob for the value stored in pMem into 
** buf. It is assumed that the caller has allocated sufficient space.
** Return the number of bytes written.
**
** nBuf is the amount of space left in buf[].  The caller is responsible
** for allocating enough space to buf[] to hold the entire field, exclusive
** of the pMem->u.nZero bytes for a MEM_Zero value.
**
** Return the number of bytes actually written into buf[].  The number
** of bytes in the zero-filled tail is included in the return value only
** if those bytes were zeroed in buf[].
*/ 
u32 sqlitexVdbeSerialPut(u8 *buf, Mem *pMem, u32 serial_type){
  u32 len;

  if( serial_type == 6 )
  {
    int64_t *p=(int64_t *)buf;
    *p=flibc_htonll( pMem->u.i );
    return sizeof(long long);
  }

  /* Integer and Real */
  if( serial_type<=7 && serial_type>0 ){
    u64 v;
    u32 i;
    if( serial_type==7 ){
      assert( sizeof(v)==sizeof(pMem->u.r) );
      memcpy(&v, &pMem->u.r, sizeof(v));
      swapMixedEndianFloat(v);
    }else{
      v = pMem->u.i;
    }
    len = i = sqlitexVdbeSerialTypeLen(serial_type);
    assert( i>0 );
    do{
      buf[--i] = (u8)(v&0xFF);
      v >>= 8;
    }while( i );
    return len;
  }

  /* COMDB2 MODIFICATION */
  if( serial_type == 10 )
  {
    /* R5 interval */
    intv_t *p=(intv_t *)buf;

#ifdef _SUN_SOURCE
    intv_t scratch;
    p = &scratch;
#endif


    memset(p, 0, offsetof(intv_t, u.ds.prec));
    p->type = htonl(pMem->du.tv.type);
    p->sign = htonl(pMem->du.tv.sign);

    /* ds type */
    if( pMem->du.tv.type == INTV_DS_TYPE )
    {
        p->u.ds.days = htonl( pMem->du.tv.u.ds.days );
        p->u.ds.hours = htonl( pMem->du.tv.u.ds.hours );
        p->u.ds.mins = htonl( pMem->du.tv.u.ds.mins );
        p->u.ds.sec = htonl( pMem->du.tv.u.ds.sec );
        p->u.ds.frac = htonl( pMem->du.tv.u.ds.frac );
    }
    else if( pMem->du.tv.type == INTV_DSUS_TYPE )
    {
        /* For R5 millisecond interval compatiblity. */
        p->u.ds.days = htonl( pMem->du.tv.u.ds.days );
        p->u.ds.hours = htonl( pMem->du.tv.u.ds.hours );
        p->u.ds.mins = htonl( pMem->du.tv.u.ds.mins );
        p->u.ds.sec = htonl( pMem->du.tv.u.ds.sec );
        p->u.ds.frac = htonl( pMem->du.tv.u.ds.frac/1000 );
    }
    /* ym type */
    else if( pMem->du.tv.type == INTV_YM_TYPE)
    {
        p->u.ym.years = htonl( pMem->du.tv.u.ym.years );
        p->u.ym.months = htonl( pMem->du.tv.u.ym.months );
    }
    else
    {
       p->u.dec = pMem->du.tv.u.dec; /*TODO LINUX*/
    }

#ifdef _SUN_SOURCE
    memcpy(buf, &scratch, offsetof(intv_t, u.ds.prec));
#endif

    return offsetof(intv_t, u.ds.prec);
  } 

  if( serial_type == (SQLITE_MAX_U32 - 1) )
  {
    /* R6 variable precision interval */
    intv_t *p=(intv_t *)buf;

#ifdef _SUN_SOURCE
    intv_t scratch;
    p = &scratch;
#endif


    bzero(p, sizeof(*p));
    p->type = htonl(pMem->du.tv.type);
    p->sign = htonl(pMem->du.tv.sign);

    /* ds type */
    if( pMem->du.tv.type == INTV_DS_TYPE || pMem->du.tv.type == INTV_DSUS_TYPE )
    {
        p->u.ds.days = htonl( pMem->du.tv.u.ds.days );
        p->u.ds.hours = htonl( pMem->du.tv.u.ds.hours );
        p->u.ds.mins = htonl( pMem->du.tv.u.ds.mins );
        p->u.ds.sec = htonl( pMem->du.tv.u.ds.sec );
        p->u.ds.frac = htonl( pMem->du.tv.u.ds.frac );
        p->u.ds.prec = htons( pMem->du.tv.u.ds.prec );
        p->u.ds.conv = htons( pMem->du.tv.u.ds.conv );
    }
    /* ym type */
    else if( pMem->du.tv.type == INTV_YM_TYPE)
    {
        p->u.ym.years = htonl( pMem->du.tv.u.ym.years );
        p->u.ym.months = htonl( pMem->du.tv.u.ym.months );
    }
    else
    {
       p->u.dec = pMem->du.tv.u.dec; /*TODO LINUX*/
    }

#ifdef _SUN_SOURCE
    memcpy(buf, &scratch, sizeof(intv_t));
#endif

    return sizeof(intv_t);
  } 

  if( serial_type == 11 )
  {
    /* R5 datetime*/
    dttz_t *p=(dttz_t *)buf;


#ifdef _SUN_SOURCE
    dttz_t scratch;
    p = &scratch;
#endif


    bzero(p, sizeof(*p));
    p->dttz_sec = flibc_htonll( pMem->du.dt.dttz_sec );
    if( pMem->du.dt.dttz_prec == DTTZ_PREC_USEC )
      /* For R5 millisecond datetime compatiblity. */
      p->dttz_frac = htonl( pMem->du.dt.dttz_frac/1000 );
    else
      p->dttz_frac = htonl( pMem->du.dt.dttz_frac );
    p->dttz_prec = htons( DTTZ_PREC_MSEC );
    p->dttz_conv = htons( pMem->du.dt.dttz_conv );

#ifdef _SUN_SOURCE
    memcpy(buf, &scratch, sizeof(dttz_t));
#endif

    return sizeof(dttz_t);
  } 

  if( serial_type == SQLITE_MAX_U32 )
  {
    /* R6 variable precision datetime*/
    dttz_t *p=(dttz_t *)buf;


#ifdef _SUN_SOURCE
    dttz_t scratch;
    p = &scratch;
#endif


    bzero(p, sizeof(*p));
    p->dttz_sec = flibc_htonll( pMem->du.dt.dttz_sec );
    p->dttz_frac = htonl( pMem->du.dt.dttz_frac );
    p->dttz_prec = htons( pMem->du.dt.dttz_prec );
    p->dttz_conv = htons( pMem->du.dt.dttz_conv );

#ifdef _SUN_SOURCE
    memcpy(buf, &scratch, sizeof(dttz_t));
#endif

    return sizeof(dttz_t);
  } 

  /* String or blob */
  if( serial_type>=12 ){
    assert( pMem->n + ((pMem->flags & MEM_Zero)?pMem->u.nZero:0)
             == (int)sqlitexVdbeSerialTypeLen(serial_type) );
    len = pMem->n;
    memcpy(buf, pMem->z, len);
    return len;
  }

  /* NULL or constants 0 or 1 */
  return 0;
}

#ifndef SQLITE_BUILDING_FOR_COMDB2
/* keep the following here for reference when sth changes in sqlite */

#define START_INLINE_SERIALGET

/* Input "x" is a sequence of unsigned characters that represent a
** big-endian integer.  Return the equivalent native integer
*/
#define ONE_BYTE_INT(x)    ((i8)(x)[0])
#define TWO_BYTE_INT(x)    (256*(i8)((x)[0])|(x)[1])
#define THREE_BYTE_INT(x)  (65536*(i8)((x)[0])|((x)[1]<<8)|(x)[2])
#define FOUR_BYTE_UINT(x)  (((u32)(x)[0]<<24)|((x)[1]<<16)|((x)[2]<<8)|(x)[3])
#define FOUR_BYTE_INT(x) (16777216*(i8)((x)[0])|((x)[1]<<16)|((x)[2]<<8)|(x)[3])

/*
** Deserialize the data blob pointed to by buf as serial type serial_type
** and store the result in pMem.  Return the number of bytes read.
**
** This function is implemented as two separate routines for performance.
** The few cases that require local variables are broken out into a separate
** routine so that in most cases the overhead of moving the stack pointer
** is avoided.
*/ 
static u32 SQLITE_NOINLINE serialGet(
  const unsigned char *buf,     /* Buffer to deserialize from */
  u32 serial_type,              /* Serial type to deserialize */
  Mem *pMem                     /* Memory cell to write value into */
){
  u64 x = FOUR_BYTE_UINT(buf);
  u32 y = FOUR_BYTE_UINT(buf+4);
  x = (x<<32) + y;
  if( serial_type==6 ){
    /* EVIDENCE-OF: R-29851-52272 Value is a big-endian 64-bit
    ** twos-complement integer. */
    pMem->u.i = *(i64*)&x;
    pMem->flags = MEM_Int;
    testcase( pMem->u.i<0 );
  }else{
    /* EVIDENCE-OF: R-57343-49114 Value is a big-endian IEEE 754-2008 64-bit
    ** floating point number. */
#if !defined(NDEBUG) && !defined(SQLITE_OMIT_FLOATING_POINT)
    /* Verify that integers and floating point values use the same
    ** byte order.  Or, that if SQLITE_MIXED_ENDIAN_64BIT_FLOAT is
    ** defined that 64-bit floating point values really are mixed
    ** endian.
    */
    static const u64 t1 = ((u64)0x3ff00000)<<32;
    static const double r1 = 1.0;
    u64 t2 = t1;
    swapMixedEndianFloat(t2);
    assert( sizeof(r1)==sizeof(t2) && memcmp(&r1, &t2, sizeof(r1))==0 );
#endif
    assert( sizeof(x)==8 && sizeof(pMem->u.r)==8 );
    swapMixedEndianFloat(x);
    memcpy(&pMem->u.r, &x, sizeof(x));
    pMem->flags = sqlitexIsNaN(pMem->u.r) ? MEM_Null : MEM_Real;
  }
  return 8;
}
static inline u32 sqlitexVdbeSerialGet(
  const unsigned char *buf,     /* Buffer to deserialize from */
  u32 serial_type,              /* Serial type to deserialize */
  Mem *pMem                     /* Memory cell to write value into */
){
  switch( serial_type ){
    /* COMDB2 MODIFICATION */
    /* case 10:    Reserved for future use */
    /* case 11:    Reserved for future use */
    case 0: {  /* Null */
      /* EVIDENCE-OF: R-24078-09375 Value is a NULL. */
      pMem->flags = MEM_Null;
      break;
    }
    case 1: {
      /* EVIDENCE-OF: R-44885-25196 Value is an 8-bit twos-complement
      ** integer. */
      pMem->u.i = ONE_BYTE_INT(buf);
      pMem->flags = MEM_Int;
      testcase( pMem->u.i<0 );
      return 1;
    }
    case 2: { /* 2-byte signed integer */
      /* EVIDENCE-OF: R-49794-35026 Value is a big-endian 16-bit
      ** twos-complement integer. */
      pMem->u.i = TWO_BYTE_INT(buf);
      pMem->flags = MEM_Int;
      testcase( pMem->u.i<0 );
      return 2;
    }
    case 3: { /* 3-byte signed integer */
      /* EVIDENCE-OF: R-37839-54301 Value is a big-endian 24-bit
      ** twos-complement integer. */
      pMem->u.i = THREE_BYTE_INT(buf);
      pMem->flags = MEM_Int;
      testcase( pMem->u.i<0 );
      return 3;
    }
    case 4: { /* 4-byte signed integer */
      /* EVIDENCE-OF: R-01849-26079 Value is a big-endian 32-bit
      ** twos-complement integer. */
      pMem->u.i = FOUR_BYTE_INT(buf);
      pMem->flags = MEM_Int;
      testcase( pMem->u.i<0 );
      return 4;
    }
    case 5: { /* 6-byte signed integer */
      /* EVIDENCE-OF: R-50385-09674 Value is a big-endian 48-bit
      ** twos-complement integer. */
      pMem->u.i = FOUR_BYTE_UINT(buf+2) + (((i64)1)<<32)*TWO_BYTE_INT(buf);
      pMem->flags = MEM_Int;
      testcase( pMem->u.i<0 );
      return 6;
    }
    /* COMDB2 MODIFICATION */
    case 6: { /* 8-byte signed integer */
#ifdef _SUN_SOURCE
      memcpy(&pMem->u.i, buf, sizeof(int64_t));
#else
      pMem->u.i = *(int64_t *)buf;
#endif
      pMem->u.i = flibc_ntohll(pMem->u.i);
      pMem->flags = MEM_Int;
      return 8;
    }
    case 7: { /* IEEE floating point */
      /* COMDB2 MODIFICATION */
      /* These use local variables, so do them in a separate routine
      ** to avoid having to move the frame pointer in the common case 
      return serialGet(buf,serial_type,pMem); */


#if !defined(NDEBUG) && !defined(SQLITE_OMIT_FLOATING_POINT)
      /* Verify that integers and floating point values use the same
      ** byte order.  Or, that if SQLITE_MIXED_ENDIAN_64BIT_FLOAT is
      ** defined that 64-bit floating point values really are mixed
      ** endian.
      */
      static const u64 t1 = ((u64)0x3ff00000)<<32;
      static const double r1 = 1.0;
      u64 t2 = t1;
      swapMixedEndianFloat(t2);
      assert( sizeof(r1)==sizeof(t2) && memcmp(&r1, &t2, sizeof(r1))==0 );
#endif
      u64 x;
      u32 y;
      x = FOUR_BYTE_UINT(buf);
      y = FOUR_BYTE_UINT(buf+4);
      x = (x<<32) | y;
#if 0
      if( serial_type==6 ){
        pMem->u.i = *(i64*)&x;
        pMem->flags = MEM_Int;
        testcase( pMem->u.i<0 );
      }else{
#endif
      {
        assert( sizeof(x)==8 && sizeof(pMem->u.r)==8 );
        swapMixedEndianFloat(x);
        memcpy(&pMem->u.r, &x, sizeof(x));
        /* COMDB2 MODIFICATION */
        /* pMem->flags = sqlitexIsNaN(pMem->u.r) ? MEM_Null : MEM_Real; */
        pMem->flags = MEM_Real;
      }
      return 8;
    }
    case 8:    /* Integer 0 */
    case 9: {  /* Integer 1 */
      /* EVIDENCE-OF: R-12976-22893 Value is the integer 0. */
      /* EVIDENCE-OF: R-18143-12121 Value is the integer 1. */
      pMem->u.i = serial_type-8;
      pMem->flags = MEM_Int;
      return 0;
    }
    /* COMDB2 MODIFICATION */
    case 10: {
      /* R5 millisecond interval */
      intv_t *p=(intv_t *)buf;

#ifdef _SUN_SOURCE
      intv_t scratch;
      memcpy(&scratch, buf, offsetof(intv_t, u.ds.prec));
      p = &scratch;
#endif

      pMem->du.tv.type = ntohl(p->type);
      pMem->du.tv.sign = ntohl(p->sign);

      /* ds type */
      if( pMem->du.tv.type == INTV_DS_TYPE )
      {
        pMem->du.tv.u.ds.days = ntohl(p->u.ds.days);
        pMem->du.tv.u.ds.hours = ntohl(p->u.ds.hours);
        pMem->du.tv.u.ds.mins = ntohl(p->u.ds.mins);
        pMem->du.tv.u.ds.sec = ntohl(p->u.ds.sec);
        pMem->du.tv.u.ds.frac = ntohl(p->u.ds.frac);
        pMem->du.tv.u.ds.prec = DTTZ_PREC_MSEC;
        pMem->du.tv.u.ds.conv = 1;
      }
      else if( pMem->du.tv.type == INTV_DSUS_TYPE )
      {
        pMem->du.tv.u.ds.days = ntohl(p->u.ds.days);
        pMem->du.tv.u.ds.hours = ntohl(p->u.ds.hours);
        pMem->du.tv.u.ds.mins = ntohl(p->u.ds.mins);
        pMem->du.tv.u.ds.sec = ntohl(p->u.ds.sec);
        pMem->du.tv.u.ds.frac = ntohl(p->u.ds.frac) * 1000;
        pMem->du.tv.u.ds.prec = DTTZ_PREC_USEC;
        pMem->du.tv.u.ds.conv = 1;
      }
      /* ym type */
      else if( pMem->du.tv.type == INTV_YM_TYPE )
      {
        pMem->du.tv.u.ym.years = ntohl(p->u.ym.years);
        pMem->du.tv.u.ym.months = ntohl(p->u.ym.months);
      }
      else
      {
         pMem->du.tv.u.dec = p->u.dec;
      }
      /* mark ask interval */
      pMem->flags = MEM_Interval;
      return offsetof(intv_t, u.ds.prec);
    }
    case (SQLITE_MAX_U32-1): {
      /* R5 variable precision interval */
      intv_t *p=(intv_t *)buf;

#ifdef _SUN_SOURCE
      intv_t scratch;
      memcpy(&scratch, buf, sizeof(intv_t));
      p = &scratch;
#endif

      pMem->du.tv.type = ntohl(p->type);
      pMem->du.tv.sign = ntohl(p->sign);

      /* ds type */
      if( pMem->du.tv.type == INTV_DS_TYPE || pMem->du.tv.type == INTV_DSUS_TYPE )
      {
        pMem->du.tv.u.ds.days = ntohl(p->u.ds.days);
        pMem->du.tv.u.ds.hours = ntohl(p->u.ds.hours);
        pMem->du.tv.u.ds.mins = ntohl(p->u.ds.mins);
        pMem->du.tv.u.ds.sec = ntohl(p->u.ds.sec);
        pMem->du.tv.u.ds.frac = ntohl(p->u.ds.frac);
        pMem->du.tv.u.ds.prec = ntohs(p->u.ds.prec);
        pMem->du.tv.u.ds.conv = ntohs(p->u.ds.conv);
      }
      /* ym type */
      else if( pMem->du.tv.type == INTV_YM_TYPE )
      {
        pMem->du.tv.u.ym.years = ntohl(p->u.ym.years);
        pMem->du.tv.u.ym.months = ntohl(p->u.ym.months);
      }
      else
      {
         pMem->du.tv.u.dec = p->u.dec;
      }
      /* mark ask interval */
      pMem->flags = MEM_Interval;
      return sizeof(intv_t);
    }
    case 11: {  /* datetime blob */
      /* R5 millisecond datetime */
      dttz_t *p=(dttz_t *)buf;

#ifdef _SUN_SOURCE
      dttz_t scratch;
      memcpy(&scratch, buf, sizeof(dttz_t));
      p = &scratch;
#endif

      /* datetime */
      pMem->du.dt.dttz_sec = flibc_ntohll( p->dttz_sec );
      pMem->du.dt.dttz_frac = ntohl( p->dttz_frac );
      pMem->du.dt.dttz_prec = ntohs( p->dttz_prec );
      pMem->du.dt.dttz_conv = ntohs( p->dttz_conv );
      if( pMem->du.dt.dttz_prec != DTTZ_PREC_MSEC ) /* data from R5 */
        pMem->du.dt.dttz_conv = 1;
      pMem->flags = MEM_Datetime;
      pMem->tz = NULL;  /* make sure it's not garbage */
      return sizeof(dttz_t);
    }
    case SQLITE_MAX_U32: {  /* datetime blob */
      /* R5 variable precision datetime */
      dttz_t *p=(dttz_t *)buf;

#ifdef _SUN_SOURCE
      dttz_t scratch;
      memcpy(&scratch, buf, sizeof(dttz_t));
      p = &scratch;
#endif

      /* datetime */
      pMem->du.dt.dttz_sec = flibc_ntohll( p->dttz_sec );
      pMem->du.dt.dttz_frac = ntohl( p->dttz_frac );
      pMem->du.dt.dttz_prec = ntohs( p->dttz_prec );
      pMem->du.dt.dttz_conv = ntohs( p->dttz_conv );
      pMem->flags = MEM_Datetime;
      pMem->tz = NULL;  /* make sure it's not garbage */
      return sizeof(dttz_t);
    }
    default: {
      /* EVIDENCE-OF: R-14606-31564 Value is a BLOB that is (N-12)/2 bytes in
      ** length.
      ** EVIDENCE-OF: R-28401-00140 Value is a string in the text encoding and
      ** (N-13)/2 bytes in length. */
      static const u16 aFlag[] = { MEM_Blob|MEM_Ephem, MEM_Str|MEM_Ephem };
      pMem->z = (char *)buf;
      pMem->n = (serial_type-12)/2;
      pMem->flags = aFlag[serial_type&1];
      return pMem->n;
    }
  }
  return 0;
}
#define END_INLINE_SERIALGET
#endif //SQLITE_BUILDING_FOR_COMDB2

/*
** This routine is used to allocate sufficient space for an UnpackedRecord
** structure large enough to be used with sqlitexVdbeRecordUnpack() if
** the first argument is a pointer to KeyInfo structure pKeyInfo.
**
** The space is either allocated using sqlitexDbMallocRaw() or from within
** the unaligned buffer passed via the second and third arguments (presumably
** stack space). If the former, then *ppFree is set to a pointer that should
** be eventually freed by the caller using sqlitexDbFree(). Or, if the 
** allocation comes from the pSpace/szSpace buffer, *ppFree is set to NULL
** before returning.
**
** If an OOM error occurs, NULL is returned.
*/
UnpackedRecord *sqlitexVdbeAllocUnpackedRecord(
  KeyInfo *pKeyInfo,              /* Description of the record */
  char *pSpace,                   /* Unaligned space available */
  int szSpace,                    /* Size of pSpace[] in bytes */
  char **ppFree                   /* OUT: Caller should free this pointer */
){
  UnpackedRecord *p;              /* Unpacked record to return */
  int nOff;                       /* Increment pSpace by nOff to align it */
  int nByte;                      /* Number of bytes required for *p */

  /* We want to shift the pointer pSpace up such that it is 8-byte aligned.
  ** Thus, we need to calculate a value, nOff, between 0 and 7, to shift 
  ** it by.  If pSpace is already 8-byte aligned, nOff should be zero.
  */
  nOff = (8 - (SQLITE_PTR_TO_INT(pSpace) & 7)) & 7;
  nByte = ROUND8(sizeof(UnpackedRecord)) + sizeof(Mem)*(pKeyInfo->nField+1);
  if( nByte>szSpace+nOff ){
    p = (UnpackedRecord *)sqlitexDbMallocRaw(pKeyInfo->db, nByte);
    *ppFree = (char *)p;
    if( !p ) return 0;
  }else{
    p = (UnpackedRecord*)&pSpace[nOff];
    *ppFree = 0;
  }

  p->aMem = (Mem*)&((char*)p)[ROUND8(sizeof(UnpackedRecord))];
  assert( pKeyInfo->aSortOrder!=0 );
  p->pKeyInfo = pKeyInfo;
  p->nField = pKeyInfo->nField + 1;
  return p;
}

/*
** Given the nKey-byte encoding of a record in pKey[], populate the 
** UnpackedRecord structure indicated by the fourth argument with the
** contents of the decoded record.
*/ 
void sqlitexVdbeRecordUnpack(
  KeyInfo *pKeyInfo,     /* Information about the record format */
  int nKey,              /* Size of the binary record */
  const void *pKey,      /* The binary record */
  UnpackedRecord *p      /* Populate this structure before returning. */
){
  const unsigned char *aKey = (const unsigned char *)pKey;
  int d; 
  u32 idx;                        /* Offset in aKey[] to read from */
  u16 u;                          /* Unsigned loop counter */
  u32 szHdr;
  Mem *pMem = p->aMem;

  p->default_rc = 0;
  assert( EIGHT_BYTE_ALIGNMENT(pMem) );
  idx = getVarint32(aKey, szHdr);
  d = szHdr;
  u = 0;
  while( idx<szHdr && d<=nKey ){
    u32 serial_type;

    idx += getVarint32(&aKey[idx], serial_type);
    pMem->enc = pKeyInfo->enc;
    pMem->db = pKeyInfo->db;
    /* pMem->flags = 0; // sqlitexVdbeSerialGet() will set this for us */
    pMem->szMalloc = 0;
    d += sqlitexVdbeSerialGet(&aKey[d], serial_type, pMem);
    pMem++;
    if( (++u)>=p->nField ) break;
  }
  assert( u<=pKeyInfo->nField + 1 );
  p->nField = u;
}

#ifndef SQLITE_BUILDING_FOR_COMDB2
/* keep the following here for reference when sth changes in sqlite */


#define START_INLINE_MEMCOMPARE

static inline int compareDateTimeInterval(int f1, int f2, int combined_flags, const Mem *pMem1, const Mem *pMem2)
{
  /* datetime and interval 
   * since casting from numerics to datetime/interval preserves the numeric flag,
   * try first to see if these are converted to datetime/interval and only a
   * afterwards check for numerics */
  if(combined_flags&MEM_Datetime) {
     if(!(f1&MEM_Datetime))
     {
        if(pMem1->tz == NULL)
        {
            /* either embedded in the string, or use the tz of the other member */
           sqlitexVdbeMemDatetimefyTz((Mem *)pMem1, pMem2->tz);
        }
        else
        {
           sqlitexVdbeMemDatetimefy((Mem *)pMem1);
        }
     }
     else if(!(f2&MEM_Datetime))
     {
        if(pMem2->tz == NULL)
        {
           /* either embedded in the string, or use the tz of the other member */
           sqlitexVdbeMemDatetimefyTz((Mem *)pMem2, pMem1->tz);
        }
        else
        {
           sqlitexVdbeMemDatetimefy((Mem *)pMem2);
        }
     }
     return dttz_cmp(&pMem1->du.dt, &pMem2->du.dt);
  }
  if(combined_flags&MEM_Interval) {
     if (pMem1->du.tv.type == INTV_DECIMAL_TYPE)
     {
        decQuad result;
        decContext   ctx;

        dec_ctx_init(&ctx, DEC_INIT_DECQUAD, gbl_decimal_rounding);
        
        if (decQuadIsZero((decQuad*)&pMem1->du.tv.u.dec) && 
            decQuadIsZero((decQuad*)&pMem2->du.tv.u.dec)) 
        {
          return 0;
        }

        decQuadCompare( &result, 
            (decQuad*)&pMem1->du.tv.u.dec, (decQuad*)&pMem2->du.tv.u.dec, &ctx);

        if(decQuadIsZero(&result))
          return 0;
        if (decQuadIsSigned( &result))
          return -1;
        return 1;
     }
     else
     {
	     return interval_cmp(&pMem1->du.tv, &pMem2->du.tv);
     }
  }
  printf("ERROR: called with bad flags\n");
  return 0;
}

#define END_INLINE_MEMCOMPARE

#define START_INLINE_VDBECOMPARE
#if SQLITE_DEBUG
/*
** This function compares two index or table record keys in the same way
** as the sqlitexVdbeRecordCompare() routine. Unlike VdbeRecordCompare(),
** this function deserializes and compares values using the
** sqlitexVdbeSerialGet() and sqlitexMemCompare() functions. It is used
** in assert() statements to ensure that the optimized code in
** sqlitexVdbeRecordCompare() returns results with these two primitives.
**
** Return true if the result of comparison is equivalent to desiredResult.
** Return false if there is a disagreement.
*/
static inline int vdbeRecordCompareDebug(
  int nKey1, const void *pKey1, /* Left key */
  const UnpackedRecord *pPKey2, /* Right key */
  int desiredResult             /* Correct answer */
){
  u32 d1;            /* Offset into aKey[] of next data element */
  u32 idx1;          /* Offset into aKey[] of next header element */
  u32 szHdr1;        /* Number of bytes in header */
  int i = 0;
  int rc = 0;
  const unsigned char *aKey1 = (const unsigned char *)pKey1;
  KeyInfo *pKeyInfo;
  Mem mem1;

  pKeyInfo = pPKey2->pKeyInfo;
  if( pKeyInfo->db==0 ) return 1;
  mem1.enc = pKeyInfo->enc;
  mem1.db = pKeyInfo->db;
  /* mem1.flags = 0;  // Will be initialized by sqlitexVdbeSerialGet() */
  VVA_ONLY( mem1.szMalloc = 0; ) /* Only needed by assert() statements */

  /* Compilers may complain that mem1.u.i is potentially uninitialized.
  ** We could initialize it, as shown here, to silence those complaints.
  ** But in fact, mem1.u.i will never actually be used uninitialized, and doing 
  ** the unnecessary initialization has a measurable negative performance
  ** impact, since this routine is a very high runner.  And so, we choose
  ** to ignore the compiler warnings and leave this variable uninitialized.
  */
  /*  mem1.u.i = 0;  // not needed, here to silence compiler warning */
  
  idx1 = getVarint32(aKey1, szHdr1);
  d1 = szHdr1;
  assert( pKeyInfo->nField+pKeyInfo->nXField>=pPKey2->nField || CORRUPT_DB );
  assert( pKeyInfo->aSortOrder!=0 );
  assert( pKeyInfo->nField>0 );
  assert( idx1<=szHdr1 || CORRUPT_DB );
  do{
    u32 serial_type1;

    /* Read the serial types for the next element in each key. */
    idx1 += getVarint32( aKey1+idx1, serial_type1 );

    /* Verify that there is enough key space remaining to avoid
    ** a buffer overread.  The "d1+serial_type1+2" subexpression will
    ** always be greater than or equal to the amount of required key space.
    ** Use that approximation to avoid the more expensive call to
    ** sqlitexVdbeSerialTypeLen() in the common case.
    */
    if( d1+serial_type1+2>(u32)nKey1
     && d1+sqlitexVdbeSerialTypeLen(serial_type1)>(u32)nKey1 
    ){
      break;
    }

    /* Extract the values to be compared.
    */
    d1 += sqlitexVdbeSerialGet(&aKey1[d1], serial_type1, &mem1);

    /* Do the comparison
    */
    rc = sqlitexMemCompare(&mem1, &pPKey2->aMem[i], pKeyInfo->aColl[i]);
    if( rc!=0 ){
      assert( mem1.szMalloc==0 );  /* See comment below */
      if( pKeyInfo->aSortOrder[i] ){
        rc = -rc;  /* Invert the result for DESC sort order. */
      }
      goto debugCompareEnd;
    }
    i++;
  }while( idx1<szHdr1 && i<pPKey2->nField );

  /* No memory allocation is ever used on mem1.  Prove this using
  ** the following assert().  If the assert() fails, it indicates a
  ** memory leak and a need to call sqlitexVdbeMemRelease(&mem1).
  */
  assert( mem1.szMalloc==0 );

  /* rc==0 here means that one of the keys ran out of fields and
  ** all the fields up to that point were equal. Return the default_rc
  ** value.  */
  rc = pPKey2->default_rc;

debugCompareEnd:
  if( desiredResult==0 && rc==0 ) return 1;
  if( desiredResult<0 && rc<0 ) return 1;
  if( desiredResult>0 && rc>0 ) return 1;
  if( CORRUPT_DB ) return 1;
  if( pKeyInfo->db->mallocFailed ) return 1;
  return 0;
}
#endif
#define END_INLINE_VDBECOMPARE



#define START_INLINE_MEMCOMPARE
#if SQLITE_DEBUG
/*
** Count the number of fields (a.k.a. columns) in the record given by
** pKey,nKey.  The verify that this count is less than or equal to the
** limit given by pKeyInfo->nField + pKeyInfo->nXField.
**
** If this constraint is not satisfied, it means that the high-speed
** vdbeRecordCompareInt() and vdbeRecordCompareString() routines will
** not work correctly.  If this assert() ever fires, it probably means
** that the KeyInfo.nField or KeyInfo.nXField values were computed
** incorrectly.
*/
static void vdbeAssertFieldCountWithinLimits(
  int nKey, const void *pKey,   /* The record to verify */ 
  const KeyInfo *pKeyInfo       /* Compare size with this KeyInfo */
){
  int nField = 0;
  u32 szHdr;
  u32 idx;
  u32 notUsed;
  const unsigned char *aKey = (const unsigned char*)pKey;

  if( CORRUPT_DB ) return;
  idx = getVarint32(aKey, szHdr);
  assert( nKey>=0 );
  assert( szHdr<=(u32)nKey );
  while( idx<szHdr ){
    idx += getVarint32(aKey+idx, notUsed);
    nField++;
  }
  assert( nField <= pKeyInfo->nField+pKeyInfo->nXField );
}
#else
# define vdbeAssertFieldCountWithinLimits(A,B,C)
#endif

/*
** Both *pMem1 and *pMem2 contain string values. Compare the two values
** using the collation sequence pColl. As usual, return a negative , zero
** or positive value if *pMem1 is less than, equal to or greater than 
** *pMem2, respectively. Similar in spirit to "rc = (*pMem1) - (*pMem2);".
*/
static inline int vdbeCompareMemString(
  const Mem *pMem1,
  const Mem *pMem2,
  const CollSeq *pColl,
  u8 *prcErr                      /* If an OOM occurs, set to SQLITE_NOMEM */
){
  if( pMem1->enc==pColl->enc ){
    /* The strings are already in the correct encoding.  Call the
     ** comparison function directly */
    return pColl->xCmp(pColl->pUser,pMem1->n,pMem1->z,pMem2->n,pMem2->z);
  }else{
    int rc;
    const void *v1, *v2;
    int n1, n2;
    Mem c1;
    Mem c2;
    sqlitexVdbeMemInit(&c1, pMem1->db, MEM_Null);
    sqlitexVdbeMemInit(&c2, pMem1->db, MEM_Null);
    sqlitexVdbeMemShallowCopy(&c1, pMem1, MEM_Ephem);
    sqlitexVdbeMemShallowCopy(&c2, pMem2, MEM_Ephem);
    v1 = sqlitexValueText((sqlitex_value*)&c1, pColl->enc);
    n1 = v1==0 ? 0 : c1.n;
    v2 = sqlitexValueText((sqlitex_value*)&c2, pColl->enc);
    n2 = v2==0 ? 0 : c2.n;
    rc = pColl->xCmp(pColl->pUser, n1, v1, n2, v2);
    sqlitexVdbeMemRelease(&c1);
    sqlitexVdbeMemRelease(&c2);
    if( (v1==0 || v2==0) && prcErr ) *prcErr = SQLITE_NOMEM;
    return rc;
  }
}

/*
** Compare two blobs.  Return negative, zero, or positive if the first
** is less than, equal to, or greater than the second, respectively.
** If one blob is a prefix of the other, then the shorter is the lessor.
*/
static SQLITE_NOINLINE int sqlitexBlobCompare(const Mem *pB1, const Mem *pB2){
  int c = memcmp(pB1->z, pB2->z, pB1->n>pB2->n ? pB2->n : pB1->n);
  if( c ) return c;
  return pB1->n - pB2->n;
}


/*
** Compare the values contained by the two memory cells, returning
** negative, zero or positive if pMem1 is less than, equal to, or greater
** than pMem2. Sorting order is NULL's first, followed by numbers (integers
** and reals) sorted numerically, followed by text ordered by the collating
** sequence pColl and finally blob's ordered by memcmp().
**
** Two NULL values are considered equal by this function.
*/
static inline int sqlitexMemCompare(const Mem *pMem1, const Mem *pMem2,
        const CollSeq *pColl){
  int rc;
  int f1, f2;
  int combined_flags;

  f1 = pMem1->flags;
  f2 = pMem2->flags;
  combined_flags = f1|f2;
  assert( (combined_flags & MEM_RowSet)==0 );
 
  /* If one value is NULL, it is less than the other. If both values
   * are NULL, return 0. */
  if( combined_flags&MEM_Null ){
    return (f2&MEM_Null) - (f1&MEM_Null);
  }

  /* COMDB2 MODIFICATION */
  if(combined_flags&MEM_Datetime || combined_flags&MEM_Interval) {
    return compareDateTimeInterval(f1, f2, combined_flags, pMem1, pMem2);
  }


  /* If one value is a number and the other is not, the number is less.
  ** If both are numbers, compare as reals if one is a real, or as integers
  ** if both values are integers.
  */
  if( combined_flags&(MEM_Int|MEM_Real|MEM_Small) ){
#define AKSCMP(x, y) do {      \
  if ( (x) < (y) ) return -1; \
  if ( (x) > (y) ) return  1; \
  return 0;                   \
} while (0);
    if( (f1 & f2 & MEM_Int)!=0 ){
      AKSCMP(pMem1->u.i, pMem2->u.i);
    }
    if( (combined_flags&MEM_Small)!=0 ){
      float r1 = pMem1->u.r;
      float r2 = pMem2->u.r;
      if( f1 & MEM_Int )
        r1 = pMem1->u.i;
      if( f2 & MEM_Int )
        r2 = pMem2->u.i;
      AKSCMP(r1, r2);
    }
    double r1, r2;
    if( (f1&MEM_Real)!=0 ){
      r1 = pMem1->u.r;
    }else if( (f1&MEM_Int)!=0 ){
      r1 = (double)pMem1->u.i;
    }else{
      return 1;
    }
    if( (f2&MEM_Real)!=0 ){
      r2 = pMem2->u.r;
    }else if( (f2&MEM_Int)!=0 ){
      r2 = (double)pMem2->u.i;
    }else{
      return -1;
    }
    AKSCMP(r1, r2);
  }

  /* If one value is a string and the other is a blob, the string is less.
  ** If both are strings, compare using the collating functions.
  */
  if( combined_flags&MEM_Str ){
    if( (f1 & MEM_Str)==0 ){
      return 1;
    }
    if( (f2 & MEM_Str)==0 ){
      return -1;
    }

    assert( pMem1->enc==pMem2->enc );
    assert( pMem1->enc==SQLITE_UTF8 || 
            pMem1->enc==SQLITE_UTF16LE || pMem1->enc==SQLITE_UTF16BE );

    /* The collation sequence must be defined at this point, even if
    ** the user deletes the collation sequence after the vdbe program is
    ** compiled (this was not always the case).
    */
    assert( !pColl || pColl->xCmp );

    if( pColl ){
      return vdbeCompareMemString(pMem1, pMem2, pColl, 0);
    }
    /* If a NULL pointer was passed as the collate function, fall through
    ** to the blob case and use memcmp().  */
  }
 
  /* Both values must be blobs.  Compare using memcmp().  */
  return sqlitexBlobCompare(pMem1, pMem2);
}

#define END_INLINE_MEMCOMPARE



#define START_INLINE_VDBECOMPARE
/*
** The first argument passed to this function is a serial-type that
** corresponds to an integer - all values between 1 and 9 inclusive 
** except 7. The second points to a buffer containing an integer value
** serialized according to serial_type. This function deserializes
** and returns the value.
*/
static inline i64 vdbeRecordDecodeInt(u32 serial_type, const u8 *aKey){
  u32 y;
  assert( CORRUPT_DB || (serial_type>=1 && serial_type<=9 && serial_type!=7) );
  switch( serial_type ){
    case 0:
    case 1:
      testcase( aKey[0]&0x80 );
      return ONE_BYTE_INT(aKey);
    case 2:
      testcase( aKey[0]&0x80 );
      return TWO_BYTE_INT(aKey);
    case 3:
      testcase( aKey[0]&0x80 );
      return THREE_BYTE_INT(aKey);
    case 4: {
      testcase( aKey[0]&0x80 );
      y = FOUR_BYTE_UINT(aKey);
      return (i64)*(int*)&y;
    }
    case 5: {
      testcase( aKey[0]&0x80 );
      return FOUR_BYTE_UINT(aKey+2) + (((i64)1)<<32)*TWO_BYTE_INT(aKey);
    }
    case 6: {
      u64 x = FOUR_BYTE_UINT(aKey);
      testcase( aKey[0]&0x80 );
      x = (x<<32) | FOUR_BYTE_UINT(aKey+4);
      return (i64)*(i64*)&x;
    }
  }

  return (serial_type - 8);
}

/*
** This function compares the two table rows or index records
** specified by {nKey1, pKey1} and pPKey2.  It returns a negative, zero
** or positive integer if key1 is less than, equal to or 
** greater than key2.  The {nKey1, pKey1} key must be a blob
** created by the OP_MakeRecord opcode of the VDBE.  The pPKey2
** key must be a parsed key such as obtained from
** sqlitexVdbeParseRecord.
**
** If argument bSkip is non-zero, it is assumed that the caller has already
** determined that the first fields of the keys are equal.
**
** Key1 and Key2 do not have to contain the same number of fields. If all 
** fields that appear in both keys are equal, then pPKey2->default_rc is 
** returned.
**
** If database corruption is discovered, set pPKey2->errCode to 
** SQLITE_CORRUPT and return 0. If an OOM error is encountered, 
** pPKey2->errCode is set to SQLITE_NOMEM and, if it is not NULL, the
** malloc-failed flag set on database handle (pPKey2->pKeyInfo->db).
*/
static inline int vdbeRecordCompareWithSkip(
  int nKey1, const void *pKey1,   /* Left key */
  UnpackedRecord *pPKey2,         /* Right key */
  int bSkip                       /* If true, skip the first field */
){
  u32 d1;                         /* Offset into aKey[] of next data element */
  int i;                          /* Index of next field to compare */
  u32 szHdr1;                     /* Size of record header in bytes */
  u32 idx1;                       /* Offset of first type in header */
  int rc = 0;                     /* Return value */
  Mem *pRhs = pPKey2->aMem;       /* Next field of pPKey2 to compare */
  KeyInfo *pKeyInfo = pPKey2->pKeyInfo;
  const unsigned char *aKey1 = (const unsigned char *)pKey1;
  Mem mem1 = {{0}};

  /* If bSkip is true, then the caller has already determined that the first
  ** two elements in the keys are equal. Fix the various stack variables so
  ** that this routine begins comparing at the second field. */
  if( bSkip ){
    u32 s1;
    idx1 = 1 + getVarint32(&aKey1[1], s1);
    szHdr1 = aKey1[0];
    d1 = szHdr1 + sqlitexVdbeSerialTypeLen(s1);
    i = 1;
    pRhs++;
  }else{
    idx1 = getVarint32(aKey1, szHdr1);
    d1 = szHdr1;
    if( d1>(unsigned)nKey1 ){ 
      pPKey2->errCode = (u8)SQLITE_CORRUPT_BKPT;
      return 0;  /* Corruption */
    }
    i = 0;
  }

  VVA_ONLY( mem1.szMalloc = 0; ) /* Only needed by assert() statements */
  assert( pPKey2->pKeyInfo->nField+pPKey2->pKeyInfo->nXField>=pPKey2->nField 
       || CORRUPT_DB );
  assert( pPKey2->pKeyInfo->aSortOrder!=0 );
  assert( pPKey2->pKeyInfo->nField>0 );
  assert( idx1<=szHdr1 || CORRUPT_DB );
  do{
    u32 serial_type;

    /* COMDB2 MODIFICATION */
    getVarint32(&aKey1[idx1], serial_type);
    sqlitexVdbeSerialGet(&aKey1[d1], serial_type, &mem1);
    int  f1 = mem1.flags;
    int  f2 = pRhs->flags;
    int  combined_flags = f1|f2;
    if( combined_flags&MEM_Null ){
      rc = (f2&MEM_Null) - (f1&MEM_Null);
    }
    else if(combined_flags&MEM_Datetime || combined_flags&MEM_Interval) {
      rc = compareDateTimeInterval(f1, f2, combined_flags, &mem1, pRhs);
    }
    /* RHS is an integer */
    else if( pRhs->flags & MEM_Int ){
      serial_type = aKey1[idx1];
      testcase( serial_type==12 );
      if( !sqlitexIsFixedLengthSerialType(serial_type) ){
        rc = +1;
      }else if( serial_type==0 ){
        rc = -1;
      }else if( serial_type==7 ){
        double rhs = (double)pRhs->u.i;
        sqlitexVdbeSerialGet(&aKey1[d1], serial_type, &mem1);
        if( mem1.u.r<rhs ){
          rc = -1;
        }else if( mem1.u.r>rhs ){
          rc = +1;
        }
      }else{
        i64 lhs = vdbeRecordDecodeInt(serial_type, &aKey1[d1]);
        i64 rhs = pRhs->u.i;
        if( lhs<rhs ){
          rc = -1;
        }else if( lhs>rhs ){
          rc = +1;
        }
      }
    }

    /* RHS is real */
    else if( pRhs->flags & MEM_Real ){
      serial_type = aKey1[idx1];
      if( !sqlitexIsFixedLengthSerialType(serial_type) ){
        rc = +1;
      }else if( serial_type==0 ){
        rc = -1;
      }else{
        double rhs = pRhs->u.r;
        double lhs;
        sqlitexVdbeSerialGet(&aKey1[d1], serial_type, &mem1);
        if( serial_type==7 ){
          lhs = mem1.u.r;
        }else{
          lhs = (double)mem1.u.i;
        }
        if( lhs<rhs ){
          rc = -1;
        }else if( lhs>rhs ){
          rc = +1;
        }
      }
    }

    /* RHS is a string */
    else if( pRhs->flags & MEM_Str ){
      getVarint32(&aKey1[idx1], serial_type);
      testcase( serial_type==12 );
      if( sqlitexIsFixedLengthSerialType(serial_type) ){
        rc = -1;
      }else if( !(serial_type & 0x01) ){
        rc = +1;
      }else{
        mem1.n = (serial_type - 12) / 2;
        testcase( (d1+mem1.n)==(unsigned)nKey1 );
        testcase( (d1+mem1.n+1)==(unsigned)nKey1 );
        if( (d1+mem1.n) > (unsigned)nKey1 ){
          pPKey2->errCode = (u8)SQLITE_CORRUPT_BKPT;
          return 0;                /* Corruption */
        }else if( pKeyInfo->aColl[i] ){
          mem1.enc = pKeyInfo->enc;
          mem1.db = pKeyInfo->db;
          mem1.flags = MEM_Str;
          mem1.z = (char*)&aKey1[d1];
          rc = vdbeCompareMemString(
              &mem1, pRhs, pKeyInfo->aColl[i], &pPKey2->errCode
          );
        }else{
          int nCmp = MIN(mem1.n, pRhs->n);
          rc = memcmp(&aKey1[d1], pRhs->z, nCmp);
          if( rc==0 ) rc = mem1.n - pRhs->n; 
        }
      }
    }

    /* RHS is a blob */
    else if( pRhs->flags & MEM_Blob ){
      getVarint32(&aKey1[idx1], serial_type);
      testcase( serial_type==12 );
      if( sqlitexIsFixedLengthSerialType(serial_type)
          || (serial_type & 0x01) ){
        rc = -1;
      }else{
        int nStr = (serial_type - 12) / 2;
        testcase( (d1+nStr)==(unsigned)nKey1 );
        testcase( (d1+nStr+1)==(unsigned)nKey1 );
        if( (d1+nStr) > (unsigned)nKey1 ){
          pPKey2->errCode = (u8)SQLITE_CORRUPT_BKPT;
          return 0;                /* Corruption */
        }else{
          int nCmp = MIN(nStr, pRhs->n);
          rc = memcmp(&aKey1[d1], pRhs->z, nCmp);
          if( rc==0 ) rc = nStr - pRhs->n;
        }
      }
    }

    /* RHS is null */
    else{
      serial_type = aKey1[idx1];
      rc = (serial_type!=0);
    }

    if( rc!=0 ){
      if( pKeyInfo->aSortOrder[i] ){
        rc = -rc;
      }
      assert( vdbeRecordCompareDebug(nKey1, pKey1, pPKey2, rc) );
      assert( mem1.szMalloc==0 );  /* See comment below */
      return rc;
    }

    i++;
    pRhs++;
    d1 += sqlitexVdbeSerialTypeLen(serial_type);
    idx1 += sqlitexVarintLen(serial_type);
  }while( idx1<(unsigned)szHdr1 && i<pPKey2->nField && d1<=(unsigned)nKey1 );

  /* No memory allocation is ever used on mem1.  Prove this using
  ** the following assert().  If the assert() fails, it indicates a
  ** memory leak and a need to call sqlitexVdbeMemRelease(&mem1).  */
  assert( mem1.szMalloc==0 );

  /* rc==0 here means that one or both of the keys ran out of fields and
  ** all the fields up to that point were equal. Return the default_rc
  ** value.  */
  assert( CORRUPT_DB 
       || vdbeRecordCompareDebug(nKey1, pKey1, pPKey2, pPKey2->default_rc) 
       || pKeyInfo->db->mallocFailed
  );
  return pPKey2->default_rc;
}

static inline int sqlitexVdbeRecordCompare(
  int nKey1, const void *pKey1,   /* Left key */
  UnpackedRecord *pPKey2          /* Right key */
){
  return vdbeRecordCompareWithSkip(nKey1, pKey1, pPKey2, 0);
}


#define END_INLINE_VDBECOMPARE



//not used by COMDB2
/*
** This function is an optimized version of sqlitexVdbeRecordCompare() 
** that (a) the first field of pPKey2 is an integer, and (b) the 
** size-of-header varint at the start of (pKey1/nKey1) fits in a single
** byte (i.e. is less than 128).
**
** To avoid concerns about buffer overreads, this routine is only used
** on schemas where the maximum valid header size is 63 bytes or less.
*/
static int vdbeRecordCompareInt(
  int nKey1, const void *pKey1, /* Left key */
  UnpackedRecord *pPKey2        /* Right key */
){
  const u8 *aKey = &((const u8*)pKey1)[*(const u8*)pKey1 & 0x3F];
  int serial_type = ((const u8*)pKey1)[1];
  int res;
  u32 y;
  u64 x;
  i64 v = pPKey2->aMem[0].u.i;
  i64 lhs;

  vdbeAssertFieldCountWithinLimits(nKey1, pKey1, pPKey2->pKeyInfo);
  assert( (*(u8*)pKey1)<=0x3F || CORRUPT_DB );
  switch( serial_type ){
    case 1: { /* 1-byte signed integer */
      lhs = ONE_BYTE_INT(aKey);
      testcase( lhs<0 );
      break;
    }
    case 2: { /* 2-byte signed integer */
      lhs = TWO_BYTE_INT(aKey);
      testcase( lhs<0 );
      break;
    }
    case 3: { /* 3-byte signed integer */
      lhs = THREE_BYTE_INT(aKey);
      testcase( lhs<0 );
      break;
    }
    case 4: { /* 4-byte signed integer */
      y = FOUR_BYTE_UINT(aKey);
      lhs = (i64)*(int*)&y;
      testcase( lhs<0 );
      break;
    }
    case 5: { /* 6-byte signed integer */
      lhs = FOUR_BYTE_UINT(aKey+2) + (((i64)1)<<32)*TWO_BYTE_INT(aKey);
      testcase( lhs<0 );
      break;
    }
    case 6: { /* 8-byte signed integer */
      x = FOUR_BYTE_UINT(aKey);
      x = (x<<32) | FOUR_BYTE_UINT(aKey+4);
      lhs = *(i64*)&x;
      testcase( lhs<0 );
      break;
    }
    case 8: 
      lhs = 0;
      break;
    case 9:
      lhs = 1;
      break;

    /* This case could be removed without changing the results of running
    ** this code. Including it causes gcc to generate a faster switch 
    ** statement (since the range of switch targets now starts at zero and
    ** is contiguous) but does not cause any duplicate code to be generated
    ** (as gcc is clever enough to combine the two like cases). Other 
    ** compilers might be similar.  */ 
    case 0: case 7:
      return sqlitexVdbeRecordCompare(nKey1, pKey1, pPKey2);

    default:
      return sqlitexVdbeRecordCompare(nKey1, pKey1, pPKey2);
  }

  if( v>lhs ){
    res = pPKey2->r1;
  }else if( v<lhs ){
    res = pPKey2->r2;
  }else if( pPKey2->nField>1 ){
    /* The first fields of the two keys are equal. Compare the trailing 
    ** fields.  */
    res = vdbeRecordCompareWithSkip(nKey1, pKey1, pPKey2, 1);
  }else{
    /* The first fields of the two keys are equal and there are no trailing
    ** fields. Return pPKey2->default_rc in this case. */
    res = pPKey2->default_rc;
  }

  assert( vdbeRecordCompareDebug(nKey1, pKey1, pPKey2, res) );
  return res;
}

/*
** This function is an optimized version of sqlitexVdbeRecordCompare() 
** that (a) the first field of pPKey2 is a string, that (b) the first field
** uses the collation sequence BINARY and (c) that the size-of-header varint 
** at the start of (pKey1/nKey1) fits in a single byte.
*/
static int vdbeRecordCompareString(
  int nKey1, const void *pKey1, /* Left key */
  UnpackedRecord *pPKey2        /* Right key */
){
  const u8 *aKey1 = (const u8*)pKey1;
  int serial_type;
  int res;

  vdbeAssertFieldCountWithinLimits(nKey1, pKey1, pPKey2->pKeyInfo);
  getVarint32(&aKey1[1], serial_type);
  if( sqlitexIsFixedLengthSerialType(serial_type) ){
    res = pPKey2->r1;      /* (pKey1/nKey1) is a number or a null */
  }else if( !(serial_type & 0x01) ){ 
    res = pPKey2->r2;      /* (pKey1/nKey1) is a blob */
  }else{
    int nCmp;
    int nStr;
    int szHdr = aKey1[0];

    nStr = (serial_type-12) / 2;
    if( (szHdr + nStr) > nKey1 ){
      pPKey2->errCode = (u8)SQLITE_CORRUPT_BKPT;
      return 0;    /* Corruption */
    }
    nCmp = MIN( pPKey2->aMem[0].n, nStr );
    res = memcmp(&aKey1[szHdr], pPKey2->aMem[0].z, nCmp);

    if( res==0 ){
      res = nStr - pPKey2->aMem[0].n;
      if( res==0 ){
        if( pPKey2->nField>1 ){
          res = vdbeRecordCompareWithSkip(nKey1, pKey1, pPKey2, 1);
        }else{
          res = pPKey2->default_rc;
        }
      }else if( res>0 ){
        res = pPKey2->r2;
      }else{
        res = pPKey2->r1;
      }
    }else if( res>0 ){
      res = pPKey2->r2;
    }else{
      res = pPKey2->r1;
    }
  }

  assert( vdbeRecordCompareDebug(nKey1, pKey1, pPKey2, res)
       || CORRUPT_DB
       || pPKey2->pKeyInfo->db->mallocFailed
  );
  return res;
}

//not used by COMDB2
/*
** Return a pointer to an sqlitexVdbeRecordCompare() compatible function
** suitable for comparing serialized records to the unpacked record passed
** as the only argument.
*/
RecordCompare sqlitexVdbeFindCompare(UnpackedRecord *p){
  /* varintRecordCompareInt() and varintRecordCompareString() both assume
  ** that the size-of-header varint that occurs at the start of each record
  ** fits in a single byte (i.e. is 127 or less). varintRecordCompareInt()
  ** also assumes that it is safe to overread a buffer by at least the 
  ** maximum possible legal header size plus 8 bytes. Because there is
  ** guaranteed to be at least 74 (but not 136) bytes of padding following each
  ** buffer passed to varintRecordCompareInt() this makes it convenient to
  ** limit the size of the header to 64 bytes in cases where the first field
  ** is an integer.
  **
  ** The easiest way to enforce this limit is to consider only records with
  ** 13 fields or less. If the first field is an integer, the maximum legal
  ** header size is (12*5 + 1 + 1) bytes.  */
  if( (p->pKeyInfo->nField + p->pKeyInfo->nXField)<=13 ){
    int flags = p->aMem[0].flags;
    if( p->pKeyInfo->aSortOrder[0] ){
      p->r1 = 1;
      p->r2 = -1;
    }else{
      p->r1 = -1;
      p->r2 = 1;
    }
    if( (flags & MEM_Int) ){
      return vdbeRecordCompareInt;
    }
    testcase( flags & MEM_Real );
    testcase( flags & MEM_Null );
    testcase( flags & MEM_Blob );
    if( (flags & (MEM_Real|MEM_Null|MEM_Blob))==0 && p->pKeyInfo->aColl[0]==0 ){
      assert( flags & MEM_Str );
      return vdbeRecordCompareString;
    }
  }

  return sqlitexVdbeRecordCompare;
}

#endif //SQLITE_BUILDING_FOR_COMDB2

/*
** pCur points at an index entry created using the OP_MakeRecord opcode.
** Read the rowid (the last field in the record) and store it in *rowid.
** Return SQLITE_OK if everything works, or an error code otherwise.
**
** pCur might be pointing to text obtained from a corrupt database file.
** So the content cannot be trusted.  Do appropriate checks on the content.
*/
int sqlitexVdbeIdxRowid(sqlitex *db, BtCursor *pCur, i64 *rowid){
  i64 nCellKey = 0;
  int rc;
  u32 szHdr;        /* Size of the header */
  u32 typeRowid;    /* Serial type of the rowid */
  u32 lenRowid;     /* Size of the rowid */
  Mem m, v;

  /* Get the size of the index entry.  Only indices entries of less
  ** than 2GiB are support - anything large must be database corruption.
  ** Any corruption is detected in sqlite3BtreeParseCellPtr(), though, so
  ** this code can safely assume that nCellKey is 32-bits  
  */
  assert( sqlite3BtreeCursorIsValid(pCur) );
  nCellKey = sqlite3BtreeIntegerKey(pCur);
  assert( (nCellKey & SQLITE_MAX_U32)==(u64)nCellKey );

  /* Read in the complete content of the index entry */
  sqlitexVdbeMemInit(&m, db, 0);
  rc = sqlitexVdbeMemFromBtree(pCur, 0, (u32)nCellKey, 1, &m);
  if( rc ){
    return rc;
  }

  /* The index entry must begin with a header size */
  (void)getVarint32((u8*)m.z, szHdr);
  testcase( szHdr==3 );
  testcase( szHdr==m.n );
  /* COMDB2 MODIFICATION */
  /* Raw index optimization will cause this test to fail */
  /*
  if( unlikely(szHdr<3 || (int)szHdr>m.n) ){
    goto idx_rowid_corruption;
  }
  */

  /* The last field of the index should be an integer - the ROWID.
  ** Verify that the last entry really is an integer. */
  (void)getVarint32((u8*)&m.z[szHdr-1], typeRowid);
  testcase( typeRowid==1 );
  testcase( typeRowid==2 );
  testcase( typeRowid==3 );
  testcase( typeRowid==4 );
  testcase( typeRowid==5 );
  testcase( typeRowid==6 );
  testcase( typeRowid==8 );
  testcase( typeRowid==9 );
  if( unlikely(typeRowid<1 || typeRowid>9 || typeRowid==7) ){
    goto idx_rowid_corruption;
  }
  lenRowid = sqlitexVdbeSerialTypeLen(typeRowid);
  testcase( (u32)m.n==szHdr+lenRowid );
  if( unlikely((u32)m.n<szHdr+lenRowid) ){
    goto idx_rowid_corruption;
  }

  /* Fetch the integer off the end of the index record */
  sqlitexVdbeSerialGet((u8*)&m.z[m.n-lenRowid], typeRowid, &v);
  *rowid = v.u.i;
  sqlitexVdbeMemRelease(&m);
  return SQLITE_OK;

  /* Jump here if database corruption is detected after m has been
  ** allocated.  Free the m object and return SQLITE_CORRUPT. */
idx_rowid_corruption:
  testcase( m.szMalloc!=0 );
  sqlitexVdbeMemRelease(&m);
  return SQLITE_CORRUPT_BKPT;
}

#ifndef SQLITE_BUILDING_FOR_COMDB2
/* keep the following here for reference when sth changes in sqlite */

#define START_INLINE_VDBECOMPARE
/*
** Compare the key of the index entry that cursor pC is pointing to against
** the key string in pUnpacked.  Write into *pRes a number
** that is negative, zero, or positive if pC is less than, equal to,
** or greater than pUnpacked.  Return SQLITE_OK on success.
**
** pUnpacked is either created without a rowid or is truncated so that it
** omits the rowid at the end.  The rowid at the end of the index entry
** is ignored as well.  Hence, this routine only compares the prefixes 
** of the keys prior to the final rowid, not the entire key.
*/
static inline int sqlitexVdbeIdxKeyCompare(
  sqlitex *db,                     /* Database connection */
  VdbeCursor *pC,                  /* The cursor to compare against */
  UnpackedRecord *pUnpacked,       /* Unpacked version of key */
  int *res                         /* Write the comparison result here */
){
  i64 nCellKey = 0;
  int rc;
  BtCursor *pCur = pC->pCursor;
  Mem m;

  assert( sqlite3BtreeCursorIsValid(pCur) );
  nCellKey = sqlite3BtreeIntegerKey(pCur);
  /* nCellKey will always be between 0 and 0xffffffff because of the way
  ** that btreeParseCellPtr() and sqlitexGetVarint32() are implemented */
  if( nCellKey<=0 || nCellKey>0x7fffffff ){
    *res = 0;
    return SQLITE_CORRUPT_BKPT;
  }
  sqlitexVdbeMemInit(&m, db, 0);
  rc = sqlitexVdbeMemFromBtree(pC->pCursor, 0, (u32)nCellKey, 1, &m);
  if( rc ){
    return rc;
  }
  *res = sqlitexVdbeRecordCompare(m.n, m.z, pUnpacked);
  sqlitexVdbeMemRelease(&m);
  return SQLITE_OK;
}

#define END_INLINE_VDBECOMPARE

#endif //SQLITE_BUILDING_FOR_COMDB2

/*
** This routine sets the value to be returned by subsequent calls to
** sqlitex_changes() on the database handle 'db'. 
*/
void sqlitexVdbeSetChanges(sqlitex *db, int nChange){
  assert( sqlitex_mutex_held(db->mutex) );
  db->nChange = nChange;
  db->nTotalChange += nChange;
}

/*
** Set a flag in the vdbe to update the change counter when it is finalised
** or reset.
*/
void sqlitexVdbeCountChanges(Vdbe *v){
  v->changeCntOn = 1;
}

/*
** Mark every prepared statement associated with a database connection
** as expired.
**
** An expired statement means that recompilation of the statement is
** recommend.  Statements expire when things happen that make their
** programs obsolete.  Removing user-defined functions or collating
** sequences, or changing an authorization function are the types of
** things that make prepared statements obsolete.
*/
void sqlitexExpirePreparedStatements(sqlitex *db){
  Vdbe *p;
  for(p = db->pVdbe; p; p=p->pNext){
    p->expired = 1;
  }
}

/*
** Return the database associated with the Vdbe.
*/
sqlitex *sqlitexVdbeDb(Vdbe *v){
  return v->db;
}

/*
** Return a pointer to an sqlitex_value structure containing the value bound
** parameter iVar of VM v. Except, if the value is an SQL NULL, return 
** 0 instead. Unless it is NULL, apply affinity aff (one of the SQLITE_AFF_*
** constants) to the value before returning it.
**
** The returned value must be freed by the caller using sqlitexValueFree().
*/
sqlitex_value *sqlitexVdbeGetBoundValue(Vdbe *v, int iVar, u8 aff){
  assert( iVar>0 );
  if( v ){
    Mem *pMem = &v->aVar[iVar-1];
    if( 0==(pMem->flags & MEM_Null) ){
      sqlitex_value *pRet = sqlitexValueNew(v->db);
      if( pRet ){
        sqlitexVdbeMemCopy((Mem *)pRet, pMem);
        sqlitexValueApplyAffinity(pRet, aff, SQLITE_UTF8);
      }
      return pRet;
    }
  }
  return 0;
}

/*
** Configure SQL variable iVar so that binding a new value to it signals
** to sqlitex_reoptimize() that re-preparing the statement may result
** in a better query plan.
*/
void sqlitexVdbeSetVarmask(Vdbe *v, int iVar){
  assert( iVar>0 );
  if( iVar>32 ){
    v->expmask = 0xffffffff;
  }else{
    v->expmask |= ((u32)1 << (iVar-1));
  }
}

#ifndef SQLITE_OMIT_VIRTUALTABLE
/*
** Transfer error message text from an sqlitex_vtab.zErrMsg (text stored
** in memory obtained from sqlitex_malloc) into a Vdbe.zErrMsg (text stored
** in memory obtained from sqlitexDbMalloc).
*/
void sqlitexVtabImportErrmsg(Vdbe *p, sqlitex_vtab *pVtab){
  sqlitex *db = p->db;
  sqlitexDbFree(db, p->zErrMsg);
  p->zErrMsg = sqlitexDbStrDup(db, pVtab->zErrMsg);
  sqlitex_free(pVtab->zErrMsg);
  pVtab->zErrMsg = 0;
}
#endif /* SQLITE_OMIT_VIRTUALTABLE */

Mem* sqlitexGetCachedResultRow(sqlitex_stmt *pStmt, int *nColumns)
{
  Vdbe *p = (Vdbe *)pStmt;
 
  if (p) 
  { 
    if (nColumns)
      *nColumns = p->nResColumn;
    return p->pResultSet;
  }

  return NULL;
}
/* vim: set ts=2 sw=2 et: */
