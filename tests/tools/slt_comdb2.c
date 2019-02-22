#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <cdb2api.h>

#include "sqllogictest.h"

extern char *dbname;

/*
** Structure used to accumulate a result set.
*/
typedef struct ResAccum ResAccum;
struct ResAccum {
  char **azValue;   /* Array of pointers to values, each malloced separately */
  int nAlloc;       /* Number of slots allocated in azValue */
  int nUsed;        /* Number of slots in azValue used */
};


/*
** Append a value to a result set.  zValue is copied into memory obtained
** from malloc.  Or if zValue is NULL, then a NULL pointer is appended.
*/
static void appendValue(ResAccum *p, const char *zValue){
  char *z;
  if( zValue ){
    z = strdup(zValue);
    if( z==0 ){
      fprintf(stderr, "out of memory at %s:%d\n", __FILE__,__LINE__);
      exit(1);
    }
  }else{
    z = 0;
  }
  if( p->nUsed>=p->nAlloc ){
    char **az;
    p->nAlloc += 200;
    az = realloc(p->azValue, p->nAlloc*sizeof(p->azValue[0]));
    if( az==0 ){
      fprintf(stderr, "out of memory at %s:%d num %ds\n", __FILE__,__LINE__, p->nAlloc);
      exit(1);
    }
    p->azValue = az;
  }
  p->azValue[p->nUsed++] = z;
}


/*
** This interface is called to free the memory that was returned
** by xQuery.
**
** It might be the case that nResult==0 or azResult==0.
*/
static int comdb2FreeResults(
  void *pConn,                /* Connection created by xConnect */
  char **azResult,            /* The results to be freed */
  int nResult                 /* Number of rows of result */
){
  int i;
  for(i=0; i<nResult; i++){
    free(azResult[i]);
  }
  free(azResult);
  return 0;
}

/*
** This routine is called to close a connection previously opened
** by xConnect.
**
** This routine may or may not delete the database.  Whichever way
** it works, steps should be taken to avoid an accumulation of left-over
** database files.  If the database is deleted here, that is one approach.
** The other approach is to delete left-over databases in the xConnect
** method.  The SQLite interface takes the latter approach.
*/
static int comdb2Disconnect(
  void *pConn                 /* Connection created by xConnect */
) {
    cdb2_hndl_tp * sqlh = pConn;
    cdb2_close(sqlh);
    return 0;
}

static int comdb2Connect(
  void *NotUsed,              /* Argument from DbEngine object.  Not used */
  const char *zConnectStr,    /* Connection string */
  void **ppConn               /* Write completed connection here */
){
    int rc;

    if (dbname == NULL)
        dbname = "mikedb";

    char *conf = getenv("CDB2_CONFIG");
    if (conf)
        cdb2_set_comdb2db_config(conf);

    cdb2_hndl_tp *sqlh=NULL;
    rc = cdb2_open(&sqlh, dbname, "default", 0);
    if(0 != rc)
    {
        fprintf(stderr,"error allocating sqlhandle\n");
        exit(1);
    }
    *ppConn = sqlh;
  return 0;  
}

/*
** This routine is called to return the name of the DB engine
** used by the connection pConn.  This name may or may not
** be the same as specified in the DbEngine structure.
**
** Then returned DB name does not have to be freed by the called.
**
** This routine should be called only after a valid connection
** has been establihed with xConnect.
**
** For ODBC connections, the engine name is resolved by the 
** driver manager after a connection is made.
*/
static int comdb2GetEngineName(
  void *pConn,                /* Connection created by xConnect */
  const char **zName          /* SQL statement to evaluate */
){
  static char *zDmbsName = "comdb2";
  *zName = zDmbsName;
  return 0;
}

/*
** Evaluate the single SQL statement given in zSql.  Return 0 on success.
** return non-zero if any error occurs.
*/
static int comdb2Statement(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql            /* SQL statement to evaluate */
){
  int rc;

  cdb2_hndl_tp * sqlh = pConn;
  rc = cdb2_run_statement( sqlh, zSql );

  return rc!=0;
}

/*
** This interface runs a query and accumulates the results into an array
** of pointers to strings.  *pazResult is made to point to the resulting
** array and *pnResult is set to the number of elements in the array.
**
** NULL values in the result set should be represented by a string "NULL".
** Empty strings should be shown as "(empty)".  Unprintable and
** control characters should be rendered as "@".
**
** Return 0 on success and 1 if there is an error.  It is not necessary
** to initialize *pazResult or *pnResult if an error occurs.
*/
static int comdb2Query(
  void *pConn,                /* Connection created by xConnect */
  const char *zSql,           /* SQL statement to evaluate */
  const char *zType,          /* One character for each column of result */
  char ***pazResult,          /* RETURN:  Array of result values */
  int *pnResult               /* RETURN:  Number of result values */
){
    int rc;
    cdb2_hndl_tp * sqlh = pConn;
    int ncols = strlen(zType);
    int *types;
    int col;
    char val[20];
    ResAccum res;
    int i;
    double d;
    char *s;
    char *tmp;

    memset(&res, 0, sizeof(res));
    types = malloc(ncols * sizeof(int));
    for (col = 0; col < ncols; col++) {
        switch (zType[col]) {
            case 'T':
                types[col] = CDB2_CSTRING;
                break;
            case 'I':
                types[col] = CDB2_INTEGER;
                break;
            case 'R':
                types[col] = CDB2_REAL;
                break;

            default:
                free(types);
                fprintf(stderr, "unknown character in type-string: %c\n", zType[col]);
                return 1;
        }
    }

    rc = cdb2_run_statement_typed( sqlh, zSql, ncols, types );
    free(types);
    if (rc)
        return rc;
    rc = cdb2_next_record( sqlh );
    while (rc == CDB2_OK) {
        for (col = 0; col < ncols; col++) {
            if (cdb2_column_value(sqlh, col) == NULL)
                appendValue(&res, "NULL");
            else {
                switch (zType[col]) {
                    case 'T':
                        s = cdb2_column_value(sqlh, col);
                        if (s[0] == 0)
                            s = "(empty)";
                        tmp = s;
                        while (*tmp) {
                            if (*tmp < ' ' || *tmp > '~')
                                *tmp = '@';
                            tmp++;
                        }
                        appendValue(&res, s);
                        break;

                    case 'I':
                        i = *(long long*) cdb2_column_value(sqlh, col);
                        snprintf(val, sizeof(val), "%d", i);
                        appendValue(&res, val);
                        break;

                    case 'R':
                        d = *(double*) cdb2_column_value(sqlh, col);
                        snprintf(val, sizeof(val), "%.3f", d);
                        appendValue(&res, val);
                }
            }
        }
        rc = cdb2_next_record( sqlh );
    }
    if (rc != CDB2_OK_DONE)
        return rc;

    *pazResult = res.azValue;
    *pnResult = res.nUsed;

    return 0;
}

/*
** This routine registers the SQLite database engine with the main
** driver.  New database engine interfaces should have a single
** routine similar to this one.  The main() function below should be
** modified to call that routine upon startup.
*/
void registerComdb2(void){
  /*
  ** This is the object that defines the database engine interface.
  */
  static const DbEngine sqliteDbEngine = {
     "Comdb2",             /* zName */
     0,                    /* pAuxData */
     comdb2Connect,        /* xConnect */
     comdb2GetEngineName,  /* xGetEngineName */
     comdb2Statement,      /* xStatement */
     comdb2Query,          /* xQuery */
     comdb2FreeResults,    /* xFreeResults */
     comdb2Disconnect      /* xDisconnect */
  };
  sqllogictestRegisterEngine(&sqliteDbEngine);
}

/*
**************** End of the SQLite database engine interface *****************
*****************************************************************************/
