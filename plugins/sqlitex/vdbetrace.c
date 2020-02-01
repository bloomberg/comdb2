/*
** 2009 November 25
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
**
** This file contains code used to insert the values of host parameters
** (aka "wildcards") into the SQL text output by sqlitex_trace().
**
** The Vdbe parse-tree explainer is also found here.
*/
#include "sqliteInt.h"
#include "vdbeInt.h"

#ifndef SQLITE_OMIT_TRACE

/*
** zSql is a zero-terminated string of UTF-8 SQL text.  Return the number of
** bytes in this text up to but excluding the first character in
** a host parameter.  If the text contains no host parameters, return
** the total number of bytes in the text.
*/
static int findNextHostParameter(const char *zSql, int *pnToken){
  int tokenType;
  int nTotal = 0;
  int n;

  *pnToken = 0;
  while( zSql[0] ){
    n = sqlitexGetToken((u8*)zSql, &tokenType);
    assert( n>0 && tokenType!=TK_ILLEGAL );
    if( tokenType==TK_VARIABLE ){
      *pnToken = n;
      break;
    }
    nTotal += n;
    zSql += n;
  }
  return nTotal;
}

/*
** This function returns a pointer to a nul-terminated string in memory
** obtained from sqlitexDbMalloc(). If sqlitex.nVdbeExec is 1, then the
** string contains a copy of zRawSql but with host parameters expanded to 
** their current bindings. Or, if sqlitex.nVdbeExec is greater than 1, 
** then the returned string holds a copy of zRawSql with "-- " prepended
** to each line of text.
**
** If the SQLITE_TRACE_SIZE_LIMIT macro is defined to an integer, then
** then long strings and blobs are truncated to that many bytes.  This
** can be used to prevent unreasonably large trace strings when dealing
** with large (multi-megabyte) strings and blobs.
**
** The calling function is responsible for making sure the memory returned
** is eventually freed.
**
** ALGORITHM:  Scan the input string looking for host parameters in any of
** these forms:  ?, ?N, $A, @A, :A.  Take care to avoid text within
** string literals, quoted identifier names, and comments.  For text forms,
** the host parameter index is found by scanning the prepared
** statement for the corresponding OP_Variable opcode.  Once the host
** parameter index is known, locate the value in p->aVar[].  Then render
** the value as a literal in place of the host parameter name.
*/
char *sqlitexVdbeExpandSql(
  Vdbe *p,                 /* The prepared statement being evaluated */
  const char *zRawSql      /* Raw text of the SQL statement */
){
  sqlitex *db;             /* The database connection */
  int idx = 0;             /* Index of a host parameter */
  int nextIndex = 1;       /* Index of next ? host parameter */
  int n;                   /* Length of a token prefix */
  int nToken;              /* Length of the parameter token */
  int i;                   /* Loop counter */
  Mem *pVar;               /* Value of a host parameter */
  StrAccum out;            /* Accumulate the output here */
  char zBase[100];         /* Initial working space */

  db = p->db;
  sqlitexStrAccumInit(&out, zBase, sizeof(zBase), 
                      db->aLimit[SQLITE_LIMIT_LENGTH]);
  out.db = db;
  if( db->nVdbeExec>1 ){
    while( *zRawSql ){
      const char *zStart = zRawSql;
      while( *(zRawSql++)!='\n' && *zRawSql );
      sqlitexStrAccumAppend(&out, "-- ", 3);
      assert( (zRawSql - zStart) > 0 );
      sqlitexStrAccumAppend(&out, zStart, (int)(zRawSql-zStart));
    }
  }else{
    while( zRawSql[0] ){
      n = findNextHostParameter(zRawSql, &nToken);
      assert( n>0 );
      sqlitexStrAccumAppend(&out, zRawSql, n);
      zRawSql += n;
      assert( zRawSql[0] || nToken==0 );
      if( nToken==0 ) break;
      if( zRawSql[0]=='?' ){
        if( nToken>1 ){
          assert( sqlitexIsdigit(zRawSql[1]) );
          sqlitexGetInt32(&zRawSql[1], &idx);
        }else{
          idx = nextIndex;
        }
      }else{
        assert( zRawSql[0]==':' || zRawSql[0]=='$' || zRawSql[0]=='@' );
        testcase( zRawSql[0]==':' );
        testcase( zRawSql[0]=='$' );
        testcase( zRawSql[0]=='@' );
        idx = sqlitexVdbeParameterIndex(p, zRawSql, nToken);
        assert( idx>0 );
      }
      zRawSql += nToken;
      nextIndex = idx + 1;
      assert( idx>0 && idx<=p->nVar );
      pVar = &p->aVar[idx-1];
      if( pVar->flags & MEM_Null ){
        sqlitexStrAccumAppend(&out, "NULL", 4);
      }else if( pVar->flags & MEM_Int ){
        sqlitexXPrintf(&out, 0, "%lld", pVar->u.i);
      }else if( pVar->flags & MEM_Real ){
        sqlitexXPrintf(&out, 0, "%!.15g", pVar->u.r);
      }else if( pVar->flags & MEM_Str ){
        int nOut;  /* Number of bytes of the string text to include in output */
#ifndef SQLITE_OMIT_UTF16
        u8 enc = ENC(db);
        Mem utf8;
        if( enc!=SQLITE_UTF8 ){
          memset(&utf8, 0, sizeof(utf8));
          utf8.db = db;
          sqlitexVdbeMemSetStr(&utf8, pVar->z, pVar->n, enc, SQLITEX_STATIC);
          sqlitexVdbeChangeEncoding(&utf8, SQLITE_UTF8);
          pVar = &utf8;
        }
#endif
        nOut = pVar->n;
#ifdef SQLITE_TRACE_SIZE_LIMIT
        if( nOut>SQLITE_TRACE_SIZE_LIMIT ){
          nOut = SQLITE_TRACE_SIZE_LIMIT;
          while( nOut<pVar->n && (pVar->z[nOut]&0xc0)==0x80 ){ nOut++; }
        }
#endif    
        sqlitexXPrintf(&out, 0, "'%.*q'", nOut, pVar->z);
#ifdef SQLITE_TRACE_SIZE_LIMIT
        if( nOut<pVar->n ){
          sqlitexXPrintf(&out, 0, "/*+%d bytes*/", pVar->n-nOut);
        }
#endif
#ifndef SQLITE_OMIT_UTF16
        if( enc!=SQLITE_UTF8 ) sqlitexVdbeMemRelease(&utf8);
#endif
      }else if( pVar->flags & MEM_Zero ){
        sqlitexXPrintf(&out, 0, "zeroblob(%d)", pVar->u.nZero);
      }else{
        int nOut;  /* Number of bytes of the blob to include in output */
        assert( pVar->flags & MEM_Blob );
        sqlitexStrAccumAppend(&out, "x'", 2);
        nOut = pVar->n;
#ifdef SQLITE_TRACE_SIZE_LIMIT
        if( nOut>SQLITE_TRACE_SIZE_LIMIT ) nOut = SQLITE_TRACE_SIZE_LIMIT;
#endif
        for(i=0; i<nOut; i++){
          sqlitexXPrintf(&out, 0, "%02x", pVar->z[i]&0xff);
        }
        sqlitexStrAccumAppend(&out, "'", 1);
#ifdef SQLITE_TRACE_SIZE_LIMIT
        if( nOut<pVar->n ){
          sqlitexXPrintf(&out, 0, "/*+%d bytes*/", pVar->n-nOut);
        }
#endif
      }
    }
  }
  return sqlitexStrAccumFinish(&out);
}

#endif /* #ifndef SQLITE_OMIT_TRACE */
