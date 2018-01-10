/*
** Copyright (c) 2008 D. Richard Hipp
**
** This program is free software; you can redistribute it and/or
** modify it under the terms of the GNU General Public
** License version 2 as published by the Free Software Foundation.
**
** This program is distributed in the hope that it will be useful,
** but WITHOUT ANY WARRANTY; without even the implied warranty of
** MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
** General Public License for more details.
** 
** You should have received a copy of the GNU General Public
** License along with this library; if not, write to the
** Free Software Foundation, Inc., 59 Temple Place - Suite 330,
** Boston, MA  02111-1307, USA.
**
** Author contact information:
**   drh@hwaci.com
**   http://www.hwaci.com/drh/
**
*******************************************************************************
**
** This main driver for the sqllogictest program.
*/
#ifdef WIN32
#define _CRT_SECURE_NO_WARNINGS
#endif

#include "sqllogictest.h"
#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#ifndef WIN32
#include <unistd.h>
#define stricmp strcasecmp
#endif
#include <string.h>
#include <strings.h>
#include <signal.h>

/*#include "slt_comdb2.c"*/

#define DEFAULT_HASH_THRESHOLD 8

void registerComdb2(void);
void registerSqlite(void);
/*
** An array of registered database engines
*/
static int nEngine = 0;
static const DbEngine **apEngine = 0;

/*
** Register a new database engine.
*/
void sqllogictestRegisterEngine(const DbEngine *p){
  nEngine++;
  apEngine = realloc(apEngine, nEngine*sizeof(apEngine[0]));
  if( apEngine==0 ){
    fprintf(stderr, "out of memory at %s:%d\n", __FILE__,__LINE__);
    exit(1);
  }
  apEngine[nEngine-1] = (DbEngine *)p;
}

/*
** Print a usage comment and die
*/
static void usage(const char *argv0){
#if 0  /* Obsolete code */
  fprintf(stdout,
    "Usage: %s [-verify] [-engine DBENGINE] [-connection STR] [-ht NUM] script\n",
    argv0);
#else
  fprintf(stdout,
    "Usage: %s [-verify] [-odbc STR] [-ht NUM] script\n",
    argv0);
#endif
  exit(1);
}

/*
** A structure to keep track of the state of scanning the input script.
*/
typedef struct Script Script;
struct Script {
  char *zScript;       /* Complete text of the input script */
  int iCur;            /* Index in zScript of start of current line */
  char *zLine;         /* Pointer to start of current line */
  int len;             /* Length of current line */
  int iNext;           /* index of start of next line */
  int nLine;           /* line number for the current line */
  int iEnd;            /* Index in zScript of '\000' at end of script */
  int startLine;       /* Line number of start of current record */
  int copyFlag;        /* If true, copy lines to output as they are read */
  char azToken[4][200]; /* tokenization of a line */
};

/*
** Advance the cursor to the start of the next non-comment line of the
** script.  Make p->zLine point to the start of the line.  Make p->len
** be the length of the line.  Zero-terminate the line.  Any \r at the
** end of the line is removed.
**
** Return 1 on success.  Return 0 and no-op at end-of-file.
*/
static int nextLine(Script *p){
  int i;

  /* Loop until a non-comment line is found, or until end-of-file */
  while(1){

    /* When we reach end-of-file, return 0 */
    if( p->iNext>=p->iEnd ){
      p->iCur = p->iEnd;
      p->zLine = &p->zScript[p->iEnd];
      p->len = 0;
      return 0;
    }

    /* Advance the cursor to the next line */
    p->iCur = p->iNext;
    p->nLine++;
    p->zLine = &p->zScript[p->iCur];
    for(i=p->iCur; i<p->iEnd && p->zScript[i]!='\n'; i++){}
    p->zScript[i] = 0;
    p->len = i - p->iCur;
    p->iNext = i+1;

    /* If the current line ends in a \r then remove the \r. */
    if( p->len>0 && p->zScript[i-1]=='\r' ){
      p->len--;
      i--;
      p->zScript[i] = 0;
    }

    /* If the line consists of all spaces, make it an empty line */
    for(i=i-1; i>=p->iCur && isspace(p->zScript[i]); i--){}
    if( i<p->iCur ){
      p->zLine[0] = 0;
    }

    /* If the copy flag is set, write the line to standard output */
    if( p->copyFlag ){
      printf("%s\n", p->zLine);
    }

    /* If the line is not a comment line, then we are finished, so break
    ** out of the loop.  If the line is a comment, the loop will repeat in
    ** order to skip this line. */
    if( p->zLine[0]!='#' ) break;
  }
  return 1;
}

/*
** Look ahead to the next line and return TRUE if it is a blank line.
** But do not advance to the next line yet.
*/
static int nextIsBlank(Script *p){
  int i = p->iNext;
  if( i>=p->iEnd ) return 1;
  while( i<p->iEnd && isspace(p->zScript[i]) ){
    if( p->zScript[i]=='\n' ) return 1;
    i++;
  }
  return 0;
}

/*
** Advance the cursor to the start of the next record.  To do this,
** first skip over the tail section of the record in which we are
** currently located, then skip over blank lines.
**
** Return 1 on success.  Return 0 at end-of-file.
*/
static int findStartOfNextRecord(Script *p){

  /* Skip over any existing content to find a blank line */
  if( p->iCur>0 ){
    while( p->zLine[0] && p->iCur<p->iEnd ){
      nextLine(p);
    }
  }else{
    nextLine(p);
  }

  /* Skip over one or more blank lines to find the first line of the
  ** new record */
  while( p->zLine[0]==0 && p->iCur<p->iEnd ){
    nextLine(p);
  }

  /* Return 1 if we have not reached end of file. */
  return p->iCur<p->iEnd;
}

/*
** Find a single token in a string.  Return the index of the start
** of the token and the length of the token.
*/
static void findToken(const char *z, int *piStart, int *pLen){
  int i;
  int iStart;
  for(i=0; isspace(z[i]); i++){}
  *piStart = iStart = i;
  while( z[i] && !isspace(z[i]) ){ i++; }
  *pLen = i - iStart;
}

#define count(X)  (sizeof(X)/sizeof(X[0]))

/*
** tokenize the current line in up to 3 tokens and store those values
** into p->azToken[0], p->azToken[1], and p->azToken[2].  Record the
** current line in p->startLine.
*/
static void tokenizeLine(Script *p){
  int i, j, k;
  int len, n;
  for(i=0; i<count(p->azToken); i++) p->azToken[i][0] = 0;
  p->startLine = p->nLine;
  for(i=j=0; j<p->len && i<count(p->azToken); i++){
    findToken(&p->zLine[j], &k, &len);
    j += k;
    n = len;
    if( n>=sizeof(p->azToken[0]) ){
      n = sizeof(p->azToken[0])-1;
    }
    memcpy(p->azToken[i], &p->zLine[j], n);
    p->azToken[i][n] = 0;
    j += n+1;
  }
}

/*
** The number columns in a row of the current result set
*/
static int nColumn = 0;

/*
** Comparison function for sorting the result set.
*/
static int rowCompare(const void *pA, const void *pB){
  const char **azA = (const char**)pA;
  const char **azB = (const char**)pB;
  int c = 0, i;
  for(i=0; c==0 && i<nColumn; i++){
    c = strcmp(azA[i], azB[i]);
  }
  return c;
}

/*
** Entry in a hash table of prior results
*/
typedef struct HashEntry HashEntry;
struct HashEntry {
  char zKey[24];      /* The search key */
  char zHash[33];     /* The hash value stored */
  HashEntry *pNext;   /* Next with same hash */
  HashEntry *pAll;    /* Next overall */
};

/*
** The hash table
*/
#define NHASH 1009
static HashEntry *aHash[NHASH];
static HashEntry *pAll;

/*
** Try to look up the value zKey in the hash table.  If the value
** does not exist, create it and return 0.  If the value does already
** exist return 0 if hash matches and 1 if the hash is different.
*/
static int checkValue(const char *zKey, const char *zHash){
  unsigned int h;
  HashEntry *p;
  unsigned int i;

  h = 0;
  for(i=0; zKey[i] && i<sizeof(p->zKey); i++){ h = h<<3 ^ h ^ zKey[i]; }
  h = h % NHASH;
  for(p = aHash[h]; p; p=p->pNext){
    if( strcmp(p->zKey, zKey)==0 ){
      return strcmp(p->zHash,zHash)!=0;
    }
  }
  p = malloc( sizeof(*p) );
  if( p==0 ){
    fprintf(stderr, "out of memory at %s:%d\n", __FILE__,__LINE__);
    exit(1);
  }
  for(i=0; zKey[i] && i<sizeof(p->zKey)-1; i++){ p->zKey[i] = zKey[i]; }
  p->zKey[i] = 0;
  for(i=0; zHash[i] && i<sizeof(p->zHash)-1; i++){ p->zHash[i] = zHash[i]; }
  p->zHash[i] = 0;
  p->pAll = pAll;
  pAll = p;
  p->pNext = aHash[h];
  aHash[h] = p;
  return 0;
}

char *dbname = NULL;

/*
** This is the main routine.  This routine runs first.  It processes
** command-line arguments then runs the test.
*/
int main(int argc, char **argv){
  int verifyMode = 0;                  /* True if in -verify mode */
  const char *zScriptFile = 0;         /* Input script filename */
  const char *zDbEngine = "SQLite";    /* Name of database engine */
  const char *zConnection = 0;         /* Connection string on DB engine */
  const DbEngine *pEngine = 0;         /* Pointer to DbEngine object */
  int i;                               /* Loop counter */
  char *zScript;                       /* Content of the script */
  long nScript;                        /* Size of the script in bytes */
  void *pConn;                         /* Connection to the database engine */
  int rc;                              /* Result code from subroutine call */
  int nErr = 0;                        /* Number of errors */
  int nCmd = 0;                        /* Number of SQL statements processed */
  int nSkipped = 0;                    /* Number of SQL statements skipped */
  int nResult;                         /* Number of query results */
  char **azResult;                     /* Query result vector */
  Script sScript;                      /* Script parsing status */
  FILE *in;                            /* For reading script */
  char zHash[100];                     /* Storage space for hash results */
  int hashThreshold = DEFAULT_HASH_THRESHOLD;  /* Threshold for hashing res */
  int bHt = 0;                         /* True if -ht command-line option */

  /* Add calls to the registration procedures for new database engine
  ** interfaces here
  */
  signal(SIGPIPE, SIG_IGN);

  registerComdb2();
  registerSqlite();

  /* Report an error if no registered engines
  */
  if( nEngine == 0 ){
    fprintf(stderr, "%s: No registered database engines\n", argv[0]);
    usage(argv[0]);
  }

  /* Default to first registered engine 
  */
  if( zDbEngine == NULL ){
    zDbEngine = apEngine[0]->zName;
  }

  /* Scan the command-line and process arguments
  */
  for(i=1; i<argc; i++){
    int n = (int)strlen(argv[i]);
    if( strncmp(argv[i], "-verify",n)==0 ){
      verifyMode = 1;
    }else if( strncmp(argv[i], "-engine",n)==0 ){
      zDbEngine = argv[++i];
#if 0
    }else if( strncmp(argv[i], "-connection",n)==0 ){
      zConnection = argv[++i];
#endif
    }else if( strncmp(argv[i], "-odbc",n)==0 ){
      zDbEngine = "ODBC3";
      zConnection = argv[++i];
    }else if( strncmp(argv[i], "-ht",n)==0 ){
      hashThreshold = atoi(argv[++i]);
      bHt = -1;
    }else if( strncmp(argv[i], "-db",n)==0 ) {
        dbname = argv[++i];
    }else if( zScriptFile==0 ){
      zScriptFile = argv[i];
    }else{
      fprintf(stderr, "%s: unknown argument: %s\n", argv[0], argv[i]);
      usage(argv[0]);
    }
  }

  /* Check for errors and missing arguments.  Find the database engine
  ** to use for this run.
  */
  if( zScriptFile==0 ){
    fprintf(stderr, "%s: no input script specified\n", argv[0]);
    usage(argv[0]);
  }
  for(i=0; i<nEngine; i++){
    if( stricmp(zDbEngine, apEngine[i]->zName)==0 ){
      pEngine = apEngine[i];
      break;
    }
  }
  if( pEngine==0 ){
    fprintf(stderr, "%s: unknown database engine: %s\n", argv[0], zDbEngine);
    fprintf(stdout, "Choices are:");
    for(i=0; i<nEngine; i++) fprintf(stdout, " %s", apEngine[i]->zName);
    fprintf(stdout, "\n");
    exit(1);
  }

  /*
  ** Read the entire script file contents into memory
  */
  in = fopen(zScriptFile, "rb");
  if( in==0 ){
    fprintf(stderr, "%s: cannot open for reading\n", zScriptFile);
    exit(1);
  }
  fseek(in, 0L, SEEK_END);
  nScript = ftell(in);
  zScript = malloc( nScript+1 );
  if( zScript==0 ){
    fprintf(stderr, "out of memory at %s:%d\n", __FILE__,__LINE__);
    exit(1);
  }
  fseek(in, 0L, SEEK_SET);
  fread(zScript, 1, nScript, in);
  fclose(in);
  zScript[nScript] = 0;

  /* Initialize the sScript structure so that the cursor will be pointing
  ** to the start of the first line in the file after nextLine() is called
  ** once. */
  memset(&sScript, 0, sizeof(sScript));
  sScript.zScript = zScript;
  sScript.zLine = zScript;
  sScript.iEnd = nScript;
  sScript.copyFlag = !verifyMode;

  /* Open the database engine under test
  */
  rc = pEngine->xConnect(pEngine->pAuxData, zConnection, &pConn);
  if( rc ){
    fprintf(stderr, "%s: unable to connect to database\n", argv[0]);
    exit(1);
  }

  /* Get the "real" db name
  */
  rc = pEngine->xGetEngineName(pConn, &zDbEngine);
  if( rc ){
    fprintf(stderr, "%s: unable to get DB name from connection\n", argv[0]);
    exit(1);
  }

  /* Loop over all records in the file */
  while( findStartOfNextRecord(&sScript) ){
    int bSkip = 0; /* True if we should skip the current record. */

    /* Tokenizer the first line of the record.  This also records the
    ** line number of the first record in sScript.startLine */
    tokenizeLine(&sScript);

    bSkip = 0;
    while( strcmp(sScript.azToken[0],"skipif")==0 
        || strcmp(sScript.azToken[0],"onlyif")==0
    ){
      int bMatch;
      /* The "skipif" and "onlyif" modifiers allow skipping or using
      ** statement or query record for a particular database engine.
      ** In this way, SQL features implmented by a majority of the
      ** engines can be tested without causing spurious errors for
      ** engines that don't support it.
      **
      ** Once this record is encountered, an the current selected
      ** db interface matches the db engine specified in the record,
      ** the we skip this rest of this record for "skipif" or for
      ** "onlyif" we skip the record if the record does not match.
      */
      bMatch = stricmp(sScript.azToken[1], zDbEngine)==0;
      if( sScript.azToken[0][0]=='s' ){
        if( bMatch ) bSkip = -1;
      }else{
        if( !bMatch ) bSkip = -1;
      }
      nextLine(&sScript);
      tokenizeLine(&sScript);
    }
    if( bSkip ) {
      int n;
      nSkipped++;
      if( !verifyMode ) continue;
      if( strcmp(sScript.azToken[0],"query")!=0 ) continue;
      if( sScript.azToken[3][0]==0 ) continue;

      /* We are skipping this record.  But we observe that it is a query
      ** with a named hash value and we are in verify mode.  Even though 
      ** we are going to skip the SQL evaluation, we might as well check
      ** the hash of the result.
      */
      while( !nextIsBlank(&sScript) && nextLine(&sScript)
             && strcmp(sScript.zLine,"----")!=0 ){
        /* Skip over the SQL text */
      }
      if( strcmp(sScript.zLine, "----")==0 ) nextLine(&sScript);
      if( sScript.zLine[0]==0 ) continue;
      n = sscanf(sScript.zLine, "%*d values hashing to %32s", zHash);
      if( n!=1 ){
        md5_add(sScript.zLine);
        md5_add("\n");
        while( !nextIsBlank(&sScript) && nextLine(&sScript) ){
          md5_add(sScript.zLine);
          md5_add("\n");
        }
        strcpy(zHash, md5_finish());
      }
      if( checkValue(sScript.azToken[3], zHash) ){
        fprintf(stderr, "%s:%d: labeled result [%s] does not agree with "
                        "previous values\n", zScriptFile, 
                        sScript.startLine, sScript.azToken[3]);
        nErr++;
      }
      continue;
    }

    /* Figure out the record type and do appropriate processing */
    if( strcmp(sScript.azToken[0],"statement")==0 ){
      int k = 0;

      /* Extract the SQL from second and subsequent lines of the
      ** record.  Copy the SQL into contiguous memory at the beginning
      ** of zScript - we are guaranteed to have enough space there. */
      while( nextLine(&sScript) && sScript.zLine[0] ){
        if( k>0 ) zScript[k++] = '\n';
        memmove(&zScript[k], sScript.zLine, sScript.len);
        k += sScript.len;
      }
      zScript[k] = 0;

      /* Run the statement.  Remember the results */
      rc = pEngine->xStatement(pConn, zScript);
      nCmd++;

      /* Check to see if we are expecting success or failure */
      if( strcmp(sScript.azToken[1],"ok")==0 ){
        /* do nothing if we expect success */
      }else if( strcmp(sScript.azToken[1],"error")==0 ){
        /* Invert the result if we expect failure */
        rc = !rc;
      }else{
        fprintf(stderr, "%s:%d: statement argument should be 'ok' or 'error'\n",
                zScriptFile, sScript.startLine);
        nErr++;
        rc = 0;
      }
    
      /* Report an error if the results do not match expectation */
      if( rc ){
        fprintf(stderr, "%s:%d: statement error\n",
                zScriptFile, sScript.startLine);
        nErr++;
      }
    }else if( strcmp(sScript.azToken[0],"query")==0 ){
      int k = 0;
      int c;

      /* Verify that the type string consists of one or more characters
      ** from the set "TIR". */
      for(k=0; (c = sScript.azToken[1][k])!=0; k++){
        if( c!='T' && c!='I' && c!='R' ){
          fprintf(stderr, "%s:%d: unknown type character '%c' in type string\n",
                  zScriptFile, sScript.startLine, c);
          nErr++;
          break;
        }
      }
      if( c!=0 ) continue;
      if( k<=0 ){
        fprintf(stderr, "%s:%d: missing type string\n",
                zScriptFile, sScript.startLine);
        nErr++;
        break;
      }

      /* Extract the SQL from second and subsequent lines of the record
      ** until the first "----" line or until end of record.
      */
      k = 0;
      while( !nextIsBlank(&sScript) && nextLine(&sScript) && sScript.zLine[0]
             && strcmp(sScript.zLine,"----")!=0 ){
        if( k>0 ) zScript[k++] = '\n';
        memmove(&zScript[k], sScript.zLine, sScript.len);
        k += sScript.len;
      }
      zScript[k] = 0;

      /* Run the query */
      nResult = 0;
      azResult = 0;
      rc = pEngine->xQuery(pConn, zScript, sScript.azToken[1],
                           &azResult, &nResult);
      nCmd++;
      if( rc ){
        fprintf(stderr, "%s:%d: query failed\n",
                zScriptFile, sScript.startLine);
        pEngine->xFreeResults(pConn, azResult, nResult);
        nErr++;
        continue;
      }

      /* Do any required sorting of query results */
      if( sScript.azToken[2][0]==0 || strcmp(sScript.azToken[2],"nosort")==0 ){
        /* Do no sorting */
      }else if( strcmp(sScript.azToken[2],"rowsort")==0 ){
        /* Row-oriented sorting */
        nColumn = (int)strlen(sScript.azToken[1]);
        qsort(azResult, nResult/nColumn, sizeof(azResult[0])*nColumn,
              rowCompare);
      }else if( strcmp(sScript.azToken[2],"valuesort")==0 ){
        /* Sort all values independently */
        nColumn = 1;
        qsort(azResult, nResult, sizeof(azResult[0]), rowCompare);
      }else{
        fprintf(stderr, "%s:%d: unknown sort method: '%s'\n",
                zScriptFile, sScript.startLine, sScript.azToken[2]);
        nErr++;
      }

      /* Hash the results if we are over the hash threshold or if we 
      ** there is a hash label */
      if( sScript.azToken[3][0]
       || (hashThreshold>0 && nResult>hashThreshold)
      ){
        md5_add("");  /* make sure md5 is reset, even if no results */
        for(i=0; i<nResult; i++){
          md5_add(azResult[i]);
          md5_add("\n");
        }
        snprintf(zHash, sizeof(zHash),
                         "%d values hashing to %s", nResult, md5_finish());
        sScript.azToken[3][20] = 0;
        if( sScript.azToken[3][0]
         && checkValue(sScript.azToken[3], md5_finish())
        ){
          fprintf(stderr, "%s:%d: labeled result [%s] does not agree with "
                          "previous values\n", zScriptFile, 
                          sScript.startLine, sScript.azToken[3]);
          nErr++;
        }
      }
      
      if( verifyMode ){
        /* In verify mode, first skip over the ---- line if we are still
        ** pointing at it. */
        if( strcmp(sScript.zLine, "----")==0 ) nextLine(&sScript);

        /* Compare subsequent lines of the script against the results
        ** from the query.  Report an error if any differences are found.
        */
        if( hashThreshold==0 || nResult<=hashThreshold ){
          for(i=0; i<nResult && sScript.zLine[0]; nextLine(&sScript), i++){
            if( strcmp(sScript.zLine, azResult[i])!=0 ){
              fprintf(stdout,"%s:%d: wrong result\n", zScriptFile,
                      sScript.nLine);
              nErr++;
              break;
            }
          }
        }else{
          if( strcmp(sScript.zLine, zHash)!=0 ){
            fprintf(stderr, "%s:%d: wrong result hash\n",
                    zScriptFile, sScript.nLine);
            nErr++;
          }
        }
      }else{
        /* In completion mode, first make sure we have output an ---- line.
        ** Output such a line now if we have not already done so.
        */
        if( strcmp(sScript.zLine, "----")!=0 ){
          printf("----\n");
        }

        /* Output the results obtained by running the query
        */
        if( hashThreshold==0 || nResult<=hashThreshold ){
          for(i=0; i<nResult; i++){
            printf("%s\n", azResult[i]);
          }
        }else{
          printf("%s\n", zHash);
        }
        printf("\n");

        /* Skip over any existing results.  They will be ignored.
        */
        sScript.copyFlag = 0;
        while( sScript.zLine[0]!=0 && sScript.iCur<sScript.iEnd ){
          nextLine(&sScript);
        }
        sScript.copyFlag = 1;
      }

      /* Free the query results */
      pEngine->xFreeResults(pConn, azResult, nResult);
    }else if( strcmp(sScript.azToken[0],"hash-threshold")==0 ){
      /* Set the maximum number of result values that will be accepted
      ** for a query.  If the number of result values exceeds this number,
      ** then an MD5 hash is computed of all values, and the resulting hash
      ** is the only result.
      **
      ** If the threshold is 0, then hashing is never used.
      **
      ** If a threshold was specified on the command line, ignore 
      ** any specifed in the script.
      */
      if( !bHt ){
        hashThreshold = atoi(sScript.azToken[1]);
      }
    }else if( strcmp(sScript.azToken[0],"halt")==0 ){
      /* Used for debugging.  Stop reading the test script and shut down.
      ** A "halt" record can be inserted in the middle of a test script in
      ** to run the script up to a particular point that is giving a
      ** faulty result, then terminate at that point for analysis.
      */
      fprintf(stdout, "%s:%d: halt\n", zScriptFile, sScript.startLine);
      break;
    }else if ( strcmp(sScript.azToken[0],"schemachange") == 0) {
        int k = 0;
        FILE *f;
        char cmd[255];
        while( nextLine(&sScript) && sScript.zLine[0] ){
            if( k>0 ) zScript[k++] = '\n';
            memmove(&zScript[k], sScript.zLine, sScript.len);
            k += sScript.len;
        }
        zScript[k] = 0;
        f = fopen("/bb/data/tmp/sqltest.csc2", "w");
        if (f == NULL) {
            fprintf(stderr, "%s:%d can't write schema file\n", zScriptFile, sScript.startLine);
            break;
        }
        fprintf(f, "%s", zScript);
        fclose(f);

        if (sScript.azToken[2] && strcmp(sScript.azToken[2], "init") == 0) {
            /* init first */
            snprintf(cmd, sizeof(cmd), "comdb2sc.tsk %s fastinit %s", dbname, sScript.azToken[1]);
            rc = system(cmd);
            if (rc) {
                fprintf(stderr, "%s:%d fastinit failed rc %d\n", zScriptFile, sScript.startLine, rc);
                break;
            }
        }
        snprintf(cmd, sizeof(cmd), "comdb2sc.tsk %s alter %s /bb/data/tmp/sqltest.csc2", dbname, sScript.azToken[1]);
        rc = system(cmd);
        if (rc) {
            fprintf(stderr, "%s:%d schema change failed rc %d\n", zScriptFile, sScript.startLine, rc);
            break;
        }
        sleep(1);
    }else{
      /* An unrecognized record type is an error */
      fprintf(stderr, "%s:%d: unknown record type: '%s'\n",
              zScriptFile, sScript.startLine, sScript.azToken[0]);
      nErr++;
      break;
    }
  }


  /* Shutdown the database connection.
  */
  rc = pEngine->xDisconnect(pConn);
  if( rc ){
    fprintf(stderr, "%s: disconnection from database failed\n", argv[0]);
    nErr++;
  }

  /* Report the number of errors and quit.
  */
  if( verifyMode || nErr || nSkipped){
    fprintf(stderr, "%s: %d errors out of %d SQL statement.  %d skipped.\n",
           zScriptFile, nErr, nCmd, nSkipped);
  }
  free(zScript);
  return nErr; 
}
