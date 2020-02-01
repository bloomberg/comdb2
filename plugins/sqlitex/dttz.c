/*
** 2007 Octomber 4
**
**
*************************************************************************
** This file contains code to operate on the datetime field.
**
** Note: all functions in this file take input/give output in host byte order.
**
** $Id: shell.c,v 1.138 2006/06/06 12:32:21 drh Exp $
*/

#include <time.h>
#include <strings.h>
#include <alloca.h>
#include "comdb2.h"
#include "sql.h"

#include <sys/types.h>
#include <inttypes.h>
#include <arpa/inet.h>

#include "sqliteInt.h"
#include "vdbeInt.h"
#include <types.h>

/* forwards from type.h|c; this is damn stupid */
struct field_conv_opts;
struct blob_buffer_t;
typedef struct blob_buffer blob_buffer_t;
int SERVER_DATETIME_to_CLIENT_CSTR( const void *in, int inlen,
        const struct field_conv_opts *inopts, blob_buffer_t *inblob,
        void *out, int outlen, int *outnull, int * outdtsz,
        const struct field_conv_opts *outopts, blob_buffer_t *outblob);
int SERVER_DATETIME_to_CLIENT_DATETIME ( const void *in, int inlen,
        const struct field_conv_opts *inopts, blob_buffer_t *inblob,
        void *out, int outlen, int *outnull, int * outdtsz,
        const struct field_conv_opts *outopts, blob_buffer_t *outblob);
const uint8_t *client_datetime_get(cdb2_client_datetime_t *p_client_datetime,
        const uint8_t *p_buf, const uint8_t *p_buf_end);
//uint64_t flibc_htonll(uint64_t host_order);


static void yearFunc(sqlitex_context *context, int argc, sqlitex_value **argv);
static void monthFunc(sqlitex_context *context, int argc, sqlitex_value **argv);
static void dayFunc(sqlitex_context *context, int argc, sqlitex_value **argv);
static void hourFunc(sqlitex_context *context, int argc, sqlitex_value **argv);
static void daysFunc(sqlitex_context *context, int argc, sqlitex_value **argv);
static void monthsFunc(sqlitex_context *context, int argc, sqlitex_value **argv);
static void minuteFunc(sqlitex_context *context, int argc, sqlitex_value **argv);
static void secondFunc(sqlitex_context *context, int argc, sqlitex_value **argv);
static void nowFunc(sqlitex_context *context, int argc, sqlitex_value **argv);

void register_date_functionsX(sqlitex * db) {
    static const struct {
        char *zName;
        int nArg;
        void (*xFunc)(sqlitex_context*,int,sqlitex_value**);
        void (*xStep)(sqlitex_context*,int,sqlitex_value**);
        void (*xFinal)(sqlitex_context*);
    } aFuncs[] = {
        { "year",      1, yearFunc         , NULL, NULL},
        { "month",     1, monthFunc        , NULL, NULL},
        { "day",       1, dayFunc          , NULL, NULL},
        { "hour",      1, hourFunc         , NULL, NULL},
        { "minute",    1, minuteFunc       , NULL, NULL},
        { "second",    1, secondFunc       , NULL, NULL},

        { "days",      2, daysFunc         , NULL, NULL},

        { "now",       0, nowFunc          , NULL, NULL},
        { "now",       1, nowFunc          , NULL, NULL},
        { "months",    1, monthsFunc       , NULL, NULL},
    };
    int rc;
    int i;

    for(i=0;i<sizeof(aFuncs)/sizeof(aFuncs[0]); i++){
        rc = sqlitex_create_function(db, aFuncs[i].zName, aFuncs[i].nArg, 
                SQLITE_ANY|SQLITE_DETERMINISTIC, 0, aFuncs[i].xFunc, aFuncs[i].xStep, aFuncs[i].xFinal);
    }
}

static int _convMem2ClientDatetimeX(Mem *pMem, void *out, int outlen,
        int *outdtsz, int isstring)
{
    int ret;
    if (!(pMem->flags & MEM_Datetime))   /* ugly, arghh */
        return SQLITE_ERROR;
    if (!pMem->tz)
        return SQLITE_ERROR;
    if (isstring) {
        ret = dttz_to_str(&pMem->du.dt, out, outlen, outdtsz, pMem->tz);
    } else {
        if (outlen != sizeof(cdb2_client_datetime_t))
            return SQLITE_ERROR;
        *outdtsz = sizeof(cdb2_client_datetime_t);
        ret = dttz_to_client_datetime(&pMem->du.dt, pMem->tz, out);
    }
    return ret ? SQLITE_ERROR : SQLITE_OK;
}

static int _convMem2ClientDatetimeus(Mem *pMem, void *out, int outlen,
        int *outdtsz, int isstring)
{
    int ret;
    if (!(pMem->flags & MEM_Datetime))   /* ugly, arghh */
        return SQLITE_ERROR;
    if (!pMem->tz)
        return SQLITE_ERROR;
    if (isstring) {
        ret = dttz_to_str(&pMem->du.dt, out, outlen, outdtsz, pMem->tz);
    } else {
        if (outlen != sizeof(cdb2_client_datetime_t))
            return SQLITE_ERROR;
        *outdtsz = sizeof(cdb2_client_datetime_t);
        ret = dttz_to_client_datetimeus(&pMem->du.dt, pMem->tz, out);
    }
    return ret ? SQLITE_ERROR : SQLITE_OK;
}

int convMem2ClientDatetimeX(Mem *pMem, void *out) {

    int     outdtsz;

    return (pMem->du.dt.dttz_prec == DTTZ_PREC_USEC)?
        _convMem2ClientDatetimeus(pMem, out, sizeof(cdb2_client_datetime_t), &outdtsz, 0):
        _convMem2ClientDatetimeX(pMem, out, sizeof(cdb2_client_datetime_t), &outdtsz, 0);
}

int convDttz2ClientDatetimeX(dttz_t *dttz, const char *tzname, void *out, int sqltype) {

    int     outdtsz;
    Mem     mem;

    bzero(&mem, sizeof(mem));
    mem.flags = MEM_Datetime;
    mem.du.dt.dttz_sec = dttz->dttz_sec;
    mem.du.dt.dttz_frac = dttz->dttz_frac;
    mem.du.dt.dttz_prec = dttz->dttz_prec;
    mem.tz = tzname;

    return (sqltype == SQLITE_DATETIMEUS)?
        _convMem2ClientDatetimeus(&mem, out, sizeof(cdb2_client_datetime_t), &outdtsz, 0):
        _convMem2ClientDatetimeX(&mem, out, sizeof(cdb2_client_datetime_t), &outdtsz, 0);
}

int convMem2ClientDatetimeStrX(Mem *pMem, void *out, int outlen, int *outdtsz) {

    return (pMem->du.dt.dttz_prec == DTTZ_PREC_USEC)?
        _convMem2ClientDatetimeus(pMem, out, outlen, outdtsz, 1):
        _convMem2ClientDatetimeX(pMem, out, outlen, outdtsz, 1);
}

static void yearFunc(sqlitex_context *context, int argc, sqlitex_value **argv){

  assert(argc == 1);

  if( SQLITE_NULL!=sqlitex_value_type(argv[0]) ){

      if(sqlitexVdbeMemDatetimefy(argv[0]) == SQLITE_OK) {
            cdb2_client_datetime_t   cdt;

            if(convMem2ClientDatetimeX(argv[0], &cdt) != SQLITE_OK) {
                sqlitex_result_null(context);
                return;
            }

            sqlitex_result_int(context, cdt.tm.tm_year+1900);
      }

  } else 
      sqlitex_result_null(context);
}

static void monthFunc(sqlitex_context *context, int argc, sqlitex_value **argv) {

  assert(argc == 1);

  if( SQLITE_NULL!=sqlitex_value_type(argv[0]) ){

      if(sqlitexVdbeMemDatetimefy(argv[0]) == SQLITE_OK) {
            cdb2_client_datetime_t   cdt;

            if(convMem2ClientDatetimeX(argv[0], &cdt) != SQLITE_OK) {
                sqlitex_result_null(context);
                return;
            }

            sqlitex_result_int(context, cdt.tm.tm_mon+1);
      }

  } else 
      sqlitex_result_null(context);
}

static void dayFunc(sqlitex_context *context, int argc, sqlitex_value **argv) {

  assert(argc == 1);

  if( SQLITE_NULL!=sqlitex_value_type(argv[0]) ){

      if(sqlitexVdbeMemDatetimefy(argv[0]) == SQLITE_OK) {
            cdb2_client_datetime_t   cdt;

            if(convMem2ClientDatetimeX(argv[0], &cdt) != SQLITE_OK) {
                sqlitex_result_null(context);
                return;
            }

            sqlitex_result_int(context, cdt.tm.tm_mday);
      }

  } else 
      sqlitex_result_null(context);
}

static void hourFunc(sqlitex_context *context, int argc, sqlitex_value **argv) {

  assert(argc == 1);

  if( SQLITE_NULL!=sqlitex_value_type(argv[0]) ){

      if(sqlitexVdbeMemDatetimefy(argv[0]) == SQLITE_OK) {
            cdb2_client_datetime_t   cdt;

            if(convMem2ClientDatetimeX(argv[0], &cdt) != SQLITE_OK) {
                sqlitex_result_null(context);
                return;
            }

            sqlitex_result_int(context, cdt.tm.tm_hour);
      }

  } else 
      sqlitex_result_null(context);
}

static void minuteFunc(sqlitex_context *context, int argc, sqlitex_value **argv) {

  assert(argc == 1);

  if( SQLITE_NULL!=sqlitex_value_type(argv[0]) ){

      if(sqlitexVdbeMemDatetimefy(argv[0]) == SQLITE_OK) {
            cdb2_client_datetime_t   cdt;

            if(convMem2ClientDatetimeX(argv[0], &cdt) != SQLITE_OK) {
                sqlitex_result_null(context);
                return;
            }

            sqlitex_result_int(context, cdt.tm.tm_min);
      }

  } else 
      sqlitex_result_null(context);
}

static void secondFunc(sqlitex_context *context, int argc, sqlitex_value **argv) {

  assert(argc == 1);

  if( SQLITE_NULL!=sqlitex_value_type(argv[0]) ){

      if(sqlitexVdbeMemDatetimefy(argv[0]) == SQLITE_OK) {
            cdb2_client_datetime_t   cdt;

            if(convMem2ClientDatetimeX(argv[0], &cdt) != SQLITE_OK) {
                sqlitex_result_null(context);
                return;
            }

            sqlitex_result_int(context, cdt.tm.tm_sec);
      }

  } else 
      sqlitex_result_null(context);
}



static void daysFunc(sqlitex_context *context, int argc, sqlitex_value **argv) {

  assert(argc == 2);

  if( SQLITE_NULL!=sqlitex_value_type(argv[0]) ){

      const unsigned char *z = sqlitex_value_text(argv[1]);

      if(z && sqlitexVdbeMemDatetimefy(argv[0]) == SQLITE_OK) {
            cdb2_client_datetime_t   cdt;

            if(convMem2ClientDatetimeX(argv[0], &cdt) != SQLITE_OK) {
                sqlitex_result_null(context);
                return;
            }
/*
            if(!strncmp(z, "week", 4)) {
                sqlitex_result_int(context, cdt.tm.tm_wday);
            } else if(!strncmp(z, "month", 5)) {
                sqlitex_result_int(context, cdt.tm.tm_mday);
            } else if(!strncmp(z, "year", 4)) {
*/
            if(z[0] == 'w') {
                sqlitex_result_int(context, cdt.tm.tm_wday);
            } else if(z[0] == 'm') {
                /* days since start of month, not day of month */
                sqlitex_result_int(context, cdt.tm.tm_mday-1); 
            } else if(z[0] == 'y') {
                sqlitex_result_int(context, cdt.tm.tm_yday);
            } else {
                /* default to dayFunc*/
                sqlitex_result_int(context, cdt.tm.tm_mday-1);
            }
      }

  } else 
      sqlitex_result_null(context);
}

extern pthread_key_t query_info_key;

static void nowFunc(sqlitex_context *context, int argc, sqlitex_value **argv)
{
   struct timespec *tspec;
   dttz_t dt;
   int precision = 0;
   assert(context->pVdbe);

   if (argc == 0) {
       precision = context->pVdbe->dtprec;
       if (precision != DTTZ_PREC_MSEC
               && precision != DTTZ_PREC_USEC) {
           struct sql_thread *thd =pthread_getspecific(query_info_key);

           if(thd && thd->clnt)
               precision = thd->clnt->dtprec;
       }
   }
   else if (SQLITE_INTEGER == sqlitex_value_type(argv[0]))
       precision = sqlitex_value_int(argv[0]);
   else if (SQLITE3_TEXT == sqlitex_value_type(argv[0])) {
       const unsigned char *msus = sqlitex_value_text(argv[0]);
       DTTZ_TEXT_TO_PREC((char *)msus, precision, 0, goto err);
   }

   if (precision != DTTZ_PREC_MSEC
           && precision != DTTZ_PREC_USEC) {
err:
       sqlitex_result_error(context, "incorrect precision", -1);
       return;
   }

   timespec_to_dttz(&context->pVdbe->tspec, &dt, precision);
   /*
      there is no way yet to provide the timezone here w/out a gross hack
      instead we let the next function or cast applied to this to set the right
      timezone
    */

   /* TODO: figure out why we're memsetting this.  context->pOut is supposed to 
    * be re-used, and sqlitexVdbeMemSetDatetime doesn't allocate memory. For
    * now, just plug the memory leak. */
   if (context->pOut->szMalloc)  {
       sqlitexDbFree(context->pOut->db, context->pOut->zMalloc);
       context->pOut->szMalloc = 0;
       /* we bzero this below - if we stop, be sure to
        * set context->pOut->zMalloc to NULL */
   }

   bzero(context->pOut, sizeof(Mem));
   sqlitexVdbeMemSetDatetime(context->pOut, &dt, NULL);
}


static void monthsFunc(sqlitex_context *context, int argc, sqlitex_value **argv) {

   assert(argc == 1);

   if( SQLITE_NULL!=sqlitex_value_type(argv[0]) ){

      if(sqlitexVdbeMemDatetimefy(argv[0]) == SQLITE_OK) {
         cdb2_client_datetime_t   cdt;

         if(convMem2ClientDatetimeX(argv[0], &cdt) != SQLITE_OK) {
            sqlitex_result_null(context);
            return;
         }

         sqlitex_result_int(context, (cdt.tm.tm_year+1900)*12+cdt.tm.tm_mon);
      }

   } else 
      sqlitex_result_null(context);
}

/* vim: set et sw=4 ts=4: */
