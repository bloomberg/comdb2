#include <stdio.h>
#include "sqliteInt.h"
#include <vdbeInt.h>
#include "comdb2build.h"
#include "comdb2vdbe.h"
#include <stdlib.h>
#include <stdarg.h>
#include <string.h>
#include <schemachange.h>
#include <sc_lua.h>
#include <comdb2.h>
#include <bdb_api.h>
#include <osqlsqlthr.h>
#include <sqloffload.h>
#include <analyze.h>
#include <bdb_access.h>
#include <bpfunc.h>
#include <bpfunc.pb-c.h>
#include <osqlcomm.h>
#include <net_types.h>
#include <views.h>
#include <logmsg.h>
#include <str0.h>
#include <zlib.h>
#include <shard_range.h>
#include <logical_cron.h>
#include "cdb2_constants.h"
#include "db_access.h" /* gbl_check_access_controls */
#include "comdb2_atomic.h"

#define COMDB2_INVALID_AUTOINCREMENT "invalid datatype for autoincrement"

extern pthread_key_t query_info_key;
extern int gbl_commit_sleep;
extern int gbl_convert_sleep;
extern int gbl_allow_user_schema;
extern int gbl_ddl_cascade_drop;
extern int gbl_legacy_schema;
extern int gbl_permit_small_sequences;
extern int gbl_lightweight_rename;

int gbl_view_feature = 1;

extern int sqlite3GetToken(const unsigned char *z, int *tokenType);
extern int sqlite3ParserFallback(int iToken);
extern int comdb2_save_ddl_context(char *name, void *ctx, comdb2ma mem);
extern void *comdb2_get_ddl_context(char *name);
/******************* Utility ****************************/

static inline int setError(Parse *pParse, int rc, const char *msg)
{
    sqlite3ErrorMsg(pParse, "%s", msg);
    pParse->rc = rc;
    return rc;
}

// return 0 on failure to parse
int readIntFromToken(Token* t, int *rst)
{
    char nptr[t->n + 1];
    memcpy(nptr, t->z, t->n);
    nptr[t->n] = '\0';
    char *endptr;

    errno = 0;
    long int l = strtol(nptr, &endptr, 10);
    int i = l; // to detect int overflow

    if (errno || *endptr != '\0' || i != l)
        return 0;

    *rst = i;
    return 1;
}

/*
  Check whether a remote schema name is specified.

  If the schema name is specified and is either "main" or local
  schema, we copy it to the first argument and return 0.
*/
static inline int isRemote(Parse *pParse, Token **t1, Token **t2)
{
    /* Check if the second token is empty. */
    if ((*t2)->n == 0) {
        /* No schema name. */
        return 0;
    } else {
        /* We must have both the tokens set. */
        assert((*t1)->n > 0 && (*t2)->n > 0);

        /* t1 must be a local schema name. */
        if ((strncasecmp((*t1)->z, thedb->envname, (*t1)->n) == 0) ||
            (strncasecmp((*t1)->z, "main", (*t1)->n) == 0)) {
            /*
              Its a local schema. Let's move the table/index name in
              t2 to t1. Doing so will ease the callers by allowing
              them to simply refer to t1 for table/index name.
            */
            (*t1)->n = (*t2)->n;
            (*t1)->z = (*t2)->z;
            (*t2)->n = 0;
            (*t2)->z = 0;
            return 0;
        }
    }
    return setError(pParse, SQLITE_MISUSE,
                    "DDL commands operate on local schema only.");
}

enum table_chk_flags {
    ERROR_ON_TBL_FOUND = 0,
    ERROR_ON_TBL_NOT_FOUND = 1,
    ERROR_IGNORE = 2,
};

static int authenticateSC(const char *table, Parse *pParse);
/* chkAndCopyTable expects the dst (OUT) buffer to be of MAXTABLELEN size. */
static inline int chkAndCopyTable(Parse *pParse, char *dst, const char *name,
                                  size_t name_len, enum table_chk_flags error_flag,
                                  int check_shard, int *table_exists,
                                  char **partition_first_shard)
{
    int rc = 0;
    char *table_name;
    struct sqlclntstate *clnt = get_sql_clnt();

    table_name = strndup(name, name_len);
    if (table_name == NULL) {
        return setError(pParse, SQLITE_NOMEM, "System out of memory");
    }

    /* Remove quotes (if any). */
    sqlite3Dequote(table_name);

    /* Check whether table name length is valid. */
    if (strlen(table_name) >= MAXTABLELEN) {
        rc = setError(pParse, SQLITE_MISUSE, "Table name is too long");
        goto cleanup;
    }

    if (gbl_allow_user_schema && clnt->current_user.have_name &&
        strcasecmp(clnt->current_user.name, DEFAULT_USER) != 0) {
        /* Check whether table_name contains user name. */
        char* username = strchr(table_name, '@');
        if (username) {
            /* Do nothing. */
            strncpy0(dst, table_name, MAXTABLELEN);
        } else { /* Add user nmame. */
            /* Make it part of user schema. */
            char userschema[MAXTABLELEN];
            int bdberr;
            int bytes_written;
            bdb_state_type *bdb_state = thedb->bdb_env;
            tran_type *tran = curtran_gettran();
            int rc = bdb_tbl_access_userschema_get(bdb_state, tran, clnt->current_user.name, userschema, &bdberr);
            curtran_puttran(tran);
            if (rc == 0) {
              if (userschema[0] == '\0') {
                snprintf(dst, MAXTABLELEN, "%s", table_name);
              } else {
                bytes_written = snprintf(dst, MAXTABLELEN, "%s@%s", table_name,
                                         userschema);
                if (bytes_written >= MAXTABLELEN) {
                  rc = setError(pParse, SQLITE_MISUSE, "User-schema name is "
                                                       "too long");
                  goto cleanup;
                }
              }
            } else {
              bytes_written = snprintf(dst, MAXTABLELEN, "%s@%s", table_name,
                                       clnt->current_user.name);
              if (bytes_written >= MAXTABLELEN) {
                rc = setError(pParse, SQLITE_MISUSE, "User-schema name is "
                                                     "too long");
                goto cleanup;
              }
            }
        }
    } else {
       strncpy0(dst, table_name, MAXTABLELEN);
    }

    // Check whether the user is allowed perform this schema change.
    if (authenticateSC(dst, pParse))
        goto cleanup;

    char *firstshard = timepart_shard_name(dst, 0, 0, NULL);
    if(!firstshard) {
        struct dbtable *db = get_dbtable_by_name(dst);

        if (table_exists) {
            *table_exists = (db) ? 1 : 0;
        }

        if (db == NULL && (error_flag == ERROR_ON_TBL_NOT_FOUND)) {
            rc = setError(pParse, SQLITE_ERROR, "Table not found");
            goto cleanup;
        }

        struct dbview *view = get_view_by_name(dst);
        if ((db != NULL || view != NULL) &&
            (error_flag == ERROR_ON_TBL_FOUND)) {
            rc = setError(pParse, SQLITE_ERROR, "Table already exists");
            goto cleanup;
        }

        if (check_shard && db && db->timepartition_name)
        {
            setError(pParse, SQLITE_ERROR, "Shards cannot be schema changed independently");
            rc = SQLITE_ERROR;
            goto cleanup;
        }

        if (db) {
            /* use original tablename */
            strncpy0(dst, db->tablename, MAXTABLELEN);
        }
        if (partition_first_shard)
            *partition_first_shard = 0;
    }
    else
    {
        if (partition_first_shard)
            *partition_first_shard = firstshard;
        else
            free(firstshard);
    }

    rc = SQLITE_OK;

cleanup:
    free(table_name);
    return rc;
}

static inline
int create_string_from_token(Vdbe* v, Parse* pParse, char** dst, Token* t)
{
    *dst = (char*) malloc (t->n + 1);

    if (*dst == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return SQLITE_NOMEM;
    }

    if (t->z[0] == '\'') {
      strncpy(*dst, t->z+1, t->n-2);
      (*dst)[t->n - 2] = '\0';
    } else {
      strncpy(*dst, t->z, t->n);
      (*dst)[t->n] = '\0';
    }

    return SQLITE_OK;
}

static inline int copyNoSqlToken(
  Vdbe* v,
  Parse *pParse,
  char** buf,
  Token *t
){
  size_t nByte = sizeof(char) * (t->n + 1);
  assert( *buf==NULL );
  *buf = (char *)malloc(nByte);
  if( *buf==NULL ){
    setError(pParse, SQLITE_NOMEM, "System out of memory");
    return SQLITE_NOMEM;
  }
  memset(*buf, 0, nByte);
  if( t->n>=3 ){
    const char *z = t->z;
    size_t n = (size_t)t->n;
    if( z[0]=='{' && z[n-1]=='}' ){ z++; n -= 2; }
    strncpy(*buf, z, n);
  }
  return SQLITE_OK;
}

static inline int chkAndCopyTableTokens(Parse *pParse, char *dst, Token *t1,
                                        Token *t2, enum table_chk_flags error_flag,
                                        int check_shard, int *table_exists,
                                        char **partition_first_shard)
{
    int rc;

    if (t1 == NULL)
        return SQLITE_OK;

    if (t2 && (rc = isRemote(pParse, &t1, &t2)))
        return rc;

    if ((rc = chkAndCopyTable(pParse, dst, t1->z, t1->n, error_flag, check_shard,
                              table_exists, partition_first_shard))) {
        return rc;
    }

    return SQLITE_OK;
}

static inline int chkAndCopyPartitionTokens(Parse *pParse, char *dst, Token *t1,
                                            Token *t2)
{
    char *table_name;
    int rc = SQLITE_OK;

    if (t1 == NULL)
        return SQLITE_OK;

    if (t2 && t2->n>0)
        return setError(pParse, SQLITE_MISUSE, "Local counters only");

    table_name = strndup(t1->z, t1->n);
    if (table_name == NULL) {
        return setError(pParse, SQLITE_NOMEM, "System out of memory");
    }

    sqlite3Dequote(table_name);

    if (strlen(table_name) >= MAXTABLELEN) {
        rc = setError(pParse, SQLITE_MISUSE, "Table name is too long");
        goto cleanup;
    }

    strncpy0(dst, table_name, MAXTABLELEN);

cleanup:
    free(table_name);

    return rc;
}

static void fillTableOption(struct schema_change_type* sc, int opt)
{
    if (OPT_ON(opt, ODH_OFF))
        sc->headers = 0;
    else if (OPT_ON(opt, ODH_ON)) 
        sc->headers = 1;

    if (OPT_ON(opt, IPU_OFF))
        sc->ip_updates = 0;
    else if (OPT_ON(opt, IPU_ON))
        sc->ip_updates = 1;

    if (OPT_ON(opt, ISC_OFF))
        sc->instant_sc = 0;
    else if (OPT_ON(opt, ISC_ON))
        sc->instant_sc = 1;

    if (OPT_ON(opt, BLOB_NONE))
        sc->compress_blobs = BDB_COMPRESS_NONE;
    else if (OPT_ON(opt, BLOB_RLE))
        sc->compress_blobs = BDB_COMPRESS_RLE8;
    else if (OPT_ON(opt, BLOB_ZLIB))
        sc->compress_blobs = BDB_COMPRESS_ZLIB;
    else if (OPT_ON(opt, BLOB_LZ4))
        sc->compress_blobs = BDB_COMPRESS_LZ4;

    if (OPT_ON(opt, REC_NONE))
        sc->compress = BDB_COMPRESS_NONE;
    else if (OPT_ON(opt, REC_RLE))
        sc->compress = BDB_COMPRESS_RLE8;
    else if (OPT_ON(opt, REC_CRLE))
        sc->compress = BDB_COMPRESS_CRLE;
    else if (OPT_ON(opt, REC_ZLIB))
        sc->compress = BDB_COMPRESS_ZLIB;
    else if (OPT_ON(opt, REC_LZ4))
        sc->compress = BDB_COMPRESS_LZ4;

    sc->commit_sleep = gbl_commit_sleep;
    sc->convert_sleep = gbl_convert_sleep;
}

int comdb2PrepareSC(Vdbe *v, Parse *pParse, int int_arg,
                    struct schema_change_type *arg, vdbeFunc func,
                    vdbeFuncArgFree freeFunc)
{
    comdb2WriteTransaction(pParse);
    Table *t = sqlite3LocateTable(pParse, LOCATE_NOERR, arg->tablename, NULL);
    if (t) {
        sqlite3VdbeAddTable(v, t);
    }
    return comdb2prepareNoRows(v, pParse, int_arg, arg, func, freeFunc);
}

int (*externalComdb2AuthenticateUserDDL)(void*, const char *tablename) = NULL;
int (*externalComdb2CheckOpAccess)(void *) = 0;

static int comdb2AuthenticateUserDDL(const char *tablename)
{
     struct sqlclntstate *clnt = get_sql_clnt();

     if (gbl_uses_externalauth && externalComdb2AuthenticateUserDDL && !clnt->admin) {
         clnt->authdata = get_authdata(clnt);
         if (gbl_externalauth_warn && !clnt->authdata)
            logmsg(LOGMSG_INFO, "Client %s pid:%d mach:%d is missing authentication data\n",
                   clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
         else if (externalComdb2AuthenticateUserDDL(clnt->authdata, tablename)) {
             ATOMIC_ADD64(gbl_num_auth_denied, 1);
             return SQLITE_AUTH;
         }
         ATOMIC_ADD64(gbl_num_auth_allowed, 1);
         return SQLITE_OK;
     }

     bdb_state_type *bdb_state = thedb->bdb_env;
     tran_type *tran = curtran_gettran();
     int bdberr; 
     int authOn = bdb_authentication_get(bdb_state, tran, &bdberr); 
    
     if (authOn != 0) {
        curtran_puttran(tran);
        return SQLITE_OK;
     }

     if (clnt && tablename)
     {
        int rc = bdb_tbl_op_access_get(bdb_state, tran, 0, tablename, clnt->current_user.name, &bdberr);
        curtran_puttran(tran);
        if (rc) {
          ATOMIC_ADD64(gbl_num_auth_denied, 1);
          return SQLITE_AUTH;
        } else {
            ATOMIC_ADD64(gbl_num_auth_allowed, 1);
            return SQLITE_OK;
        }
     }
     curtran_puttran(tran);

     ATOMIC_ADD64(gbl_num_auth_denied, 1);
     return SQLITE_AUTH;
}

static int comdb2CheckOpAccess(void) {
    struct sqlclntstate *clnt = get_sql_clnt();
    if (gbl_uses_externalauth && externalComdb2CheckOpAccess && !clnt->admin) {
         clnt->authdata = get_authdata(clnt);
         if (gbl_externalauth_warn && !clnt->authdata) {
            logmsg(LOGMSG_INFO, "Client %s pid:%d mach:%d is missing authentication data\n",
                   clnt->argv0 ? clnt->argv0 : "???", clnt->conninfo.pid, clnt->conninfo.node);
            return SQLITE_OK;
         } else if (externalComdb2CheckOpAccess(clnt->authdata)) {
             ATOMIC_ADD64(gbl_num_auth_denied, 1);
             return SQLITE_AUTH;
         }
         ATOMIC_ADD64(gbl_num_auth_allowed, 1);
         return SQLITE_OK;
    }
    if (comdb2AuthenticateUserDDL(""))
        return SQLITE_AUTH;
    return SQLITE_OK;
}

int comdb2IsPrepareOnly(Parse* pParse)
{
    return pParse==NULL || (pParse->prepFlags & SQLITE_PREPARE_ONLY);
}

int comdb2IsDryrun(Parse* pParse)
{
   if(!pParse || !pParse->isDryrun)
       return 0;
   return 1;
}

int comdb2SCIsDryRunnable(struct schema_change_type *s){
    switch(s->kind){
        case SC_ADDTABLE:
        case SC_DROPTABLE:
        case SC_TRUNCATETABLE:
        case SC_ALTERTABLE:
        case SC_ALTERTABLE_PENDING:
        case SC_REBUILDTABLE:
        case SC_ALTERTABLE_INDEX:
        case SC_REBUILDTABLE_INDEX:
            return 1;
        default:
            return 0;
    }
}

int comdb2AuthenticateUserOp(Parse* pParse)
{
    int rc;
    rc = comdb2CheckOpAccess();
    if (rc != SQLITE_OK) {
        setError(pParse, rc, "User does not have OP credentials");
    }
    return rc;
}

/* Only an op user can turn authentication on. */
static int comdb2AuthenticateOpPassword(Parse* pParse)
{
     struct sqlclntstate *clnt = get_sql_clnt();
     tran_type *tran = curtran_gettran();
     bdb_state_type *bdb_state = thedb->bdb_env;
     int bdberr; 

     if (clnt)
     {
         /* Authenticate the password first, as we haven't been doing it so far. */
         if (bdb_user_password_check(tran, clnt->current_user.name,
                                     clnt->current_user.password, NULL))
         {
            curtran_puttran(tran);
            return SQLITE_AUTH;
         }
         
         /* Check if the user is OP user. */
         int rc = bdb_tbl_op_access_get(bdb_state, tran, 0, "",
                                   clnt->current_user.name, &bdberr);
         curtran_puttran(tran);
         if (rc)
             return SQLITE_AUTH;
         else
             return SQLITE_OK;
     }
     curtran_puttran(tran);

     return SQLITE_AUTH;
}

int comdb2SqlDryrunSchemaChange(OpFunc *f)
{
    struct sqlclntstate *clnt = get_sql_clnt();
    struct schema_change_type *s = (struct schema_change_type*)f->arg;
    struct ireq iq;
    init_fake_ireq(thedb, &iq);
    iq.errstrused = 1;
    s->iq = &iq;
    FILE *fl = tmpfile();
    if (!fl) {
        fprintf(stderr, "%s:%d SYSTEM RAN OUT OF FILE DESCRIPTORS!!! EXITING\n",
                __FILE__, __LINE__);
        exit(1);
    }
    SBUF2 *sb = sbuf2open( fileno(fl), SBUF2_NO_CLOSE_FD); /* sbuf pointed at f */
    s->sb = sb;
    int sv_onstack = s->onstack;
    s->onstack = 1;
    f->rc = do_dryrun(s);
    s->onstack = sv_onstack;
    sbuf2close(sb);

    rewind(fl);
    char buf[1024] = {0};
    while (f->rc == 0 && fgets(buf, sizeof(buf), fl)) {
#ifdef DEBUG
        printf("%s\n", buf);
#endif
        char *sptr = strchr(buf, '\n');
        if (sptr) *sptr = 0;
        if(buf[0] != '>' && buf[0] != '?')  continue; //filter out extra lines
        sptr = &buf[1];
        printf("%s\n", sptr);
        opFuncPrintf(f, "%s", sptr);
    }
    fclose(fl);

    osqlstate_t *osql = &clnt->osql;
    osql->xerr.errval = errstat_get_rc(&iq.errstat);
    memcpy(osql->xerr.errstr, errstat_get_str(&iq.errstat), strlen(errstat_get_str(&iq.errstat)));
    //f->errorMsg = (char *)errstat_get_str(&iq.errstat);
    f->errorMsg = osql->xerr.errstr;
    s->iq = NULL;
    return f->rc;
}

static int comdb2SqlSchemaChange_int(OpFunc *f, int usedb)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct schema_change_type *s = (struct schema_change_type*)f->arg;
    f->rc = osql_schemachange_logic(s, thd, usedb);
    if (f->rc == SQLITE_DDL_MISUSE)
        f->errorMsg = "Transactional DDL Error: Overlapping Tables";
    else if (f->rc)
        f->errorMsg = "Transactional DDL Error: Internal Errors";
    return f->rc;
}

static int comdb2SqlSchemaChange_usedb(OpFunc *f)
{
    return comdb2SqlSchemaChange_int(f, 1);
}

int comdb2SqlSchemaChange(OpFunc *f)
{
    return comdb2SqlSchemaChange_int(f, 0);
}

int comdb2SqlSchemaChange_tran(OpFunc *f)
{
    struct sqlclntstate *clnt = get_sql_clnt();
    osqlstate_t *osql = &clnt->osql;
    int rc = 0;
    int sentops = 0;
    int bdberr = 0;
    osql_sock_start(clnt, OSQL_SOCK_REQ ,0);
    comdb2SqlSchemaChange(f);
    if (clnt->dbtran.mode != TRANLEVEL_SOSQL) {
        rc = osql_shadtbl_process(clnt, &sentops, &bdberr, 0);
        if (rc) {
            logmsg(LOGMSG_ERROR,
                   "%s:%d failed to process shadow table, rc %d, bdberr %d\n",
                   __func__, __LINE__, rc, bdberr);
            osql_sock_abort(clnt, OSQL_SOCK_REQ);
            f->rc = osql->xerr.errval = ERR_INTERNAL;
            f->errorMsg = "Failed to process shadow table";
            return ERR_INTERNAL;
        }
    }
    rc = osql_sock_commit(clnt, OSQL_SOCK_REQ, TRANS_CLNTCOMM_NORMAL);
    if (osql->xerr.errval == COMDB2_SCHEMACHANGE_OK) {
        osql->xerr.errval = 0;
    }
    f->rc = osql->xerr.errval;
    f->errorMsg = osql->xerr.errstr;
    return f->rc;
}

static int comdb2ProcSchemaChange(OpFunc *f)
{
    int rc = comdb2SqlSchemaChange_tran(f);
    if (rc == 0) {
        opFuncPrintf(f, "%s", f->errorMsg);
    }
    return rc;
}

void free_rstMsg(struct rstMsg* rec)
{
    if (rec)
    {
        if (!rec->staticMsg && rec->msg)
            free(rec->msg);
        free(rec);
    }
}

/* 
* Send a BPFUNC to the master
* Returns SQLITE_OK if successful.
*
*/
int comdb2SendBpfunc(OpFunc *f)
{
   struct sql_thread *thd = pthread_getspecific(query_info_key);
   int rc = 0;
   BpfuncArg *arg = (BpfuncArg*)f->arg;

   rc = osql_bpfunc_logic(thd, arg);
    
   if (rc) {
       f->rc = rc;
       f->errorMsg = "FAIL"; // TODO This must be translated to a description
   } else {
       f->rc = SQLITE_OK;
       f->errorMsg = "";
   }
   return f->rc;
}




/* ######################### Parser Reductions ############################ */

/**************************** Function prototypes ***************************/

static void comdb2Rebuild(Parse *p, Token* nm, Token* lnm, int opt);

/************************** Function definitions ****************************/

static int authenticateSC(const char * table,  Parse *pParse)
{
    char *username = strstr(table, "@");
    struct sqlclntstate *clnt = get_sql_clnt();
    if (username && strcmp(username+1, clnt->current_user.name) == 0) {
        return 0;
    } else if (comdb2AuthenticateUserDDL(table) == 0) {
        return 0;
    } else if (comdb2AuthenticateUserOp(pParse) == 0) {
        return 0;
    }
    return -1;
}

void comdb2CreateTableCSC2(
  Parse *pParse,   /* Parser context */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int opt,         /* Various options for create (compress, etc) */
  Token *csc2,
  int temp,
  int noErr
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_CREATE_TABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v  = sqlite3GetVdbe(pParse);
    int table_exists = 0;

    if (temp) {
        setError(pParse, SQLITE_MISUSE, "Can't create temporary csc2 table");
        return;
    }
    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(pParse, sc->tablename, pName1, pName2,
                              (noErr) ? ERROR_IGNORE : ERROR_ON_TBL_FOUND, 1,
                              &table_exists, NULL))
        goto out;

    if (noErr && table_exists) {
        logmsg(LOGMSG_DEBUG, "Table '%s' already exists.", sc->tablename);
        goto out;
    }

    sc->kind = SC_ADDTABLE;
    sc->nothrevent = 1;
    sc->live = 1;

    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto out;
        }
    }

    fillTableOption(sc, opt);
    copyNoSqlToken(v, pParse, &sc->newcsc2, csc2);

    if(sc->dryrun)
        comdb2prepareSString(v, pParse, 0,  sc, &comdb2SqlDryrunSchemaChange,
                            (vdbeFuncArgFree)  &free_schema_change_type);
    else
        comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
    return;
}

void comdb2AlterTableCSC2(
  Parse *pParse,   /* Parser context */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int opt,         /* Various options for alter (compress, etc) */
  Token *csc2
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_ALTER_TABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v = sqlite3GetVdbe(pParse);

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(pParse, sc->tablename, pName1, pName2,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto out;

    sc->kind = SC_ALTERTABLE;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto out;
        }
    }
    fillTableOption(sc, opt);
    if(OPT_ON(opt, FORCE_SC)){
        sc->force = 1;
    }
    copyNoSqlToken(v, pParse, &sc->newcsc2, csc2);
    if(sc->dryrun)
        comdb2prepareSString(v, pParse, 0,  sc, &comdb2SqlDryrunSchemaChange,
                            (vdbeFuncArgFree)  &free_schema_change_type);
    else
        comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

void comdb2DropTable(Parse *pParse, SrcList *pName)
{
    char *partition_first_shard = NULL;

    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_DROP_TABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v = sqlite3GetVdbe(pParse);

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    Token table = {pName->a[0].zName, strlen(pName->a[0].zName)};
    if (chkAndCopyTableTokens(pParse, sc->tablename, &table, 0,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0,
                              &partition_first_shard))
        goto out;

    sc->same_schema = 1;
    sc->kind = SC_DROPTABLE;
    sc->nothrevent = 1;
    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto out;
        }
    }

    if (partition_first_shard)
        sc->partition.type = PARTITION_REMOVE;

    tran_type *tran = curtran_gettran();
    int rc = get_csc2_file_tran(partition_first_shard ? partition_first_shard :
                                sc->tablename, -1 , &sc->newcsc2, NULL, tran);
    curtran_puttran(tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: %s schema not found: %s\n", __func__,
               partition_first_shard ? "shard" : "table",
               partition_first_shard ? partition_first_shard : sc->tablename);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }
    if(sc->dryrun)
        comdb2prepareSString(v, pParse, 0,  sc, &comdb2SqlDryrunSchemaChange,
                            (vdbeFuncArgFree)  &free_schema_change_type);
    else
        comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                        (vdbeFuncArgFree)&free_schema_change_type);
    free(partition_first_shard);
    return;

out:
    free_schema_change_type(sc);
    free(partition_first_shard);
}

static inline void comdb2Rebuild(Parse *pParse, Token* nm, Token* lnm, int opt)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    struct schema_change_type* sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(pParse, sc->tablename, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto out;

    fillTableOption(sc, opt);
    sc->nothrevent = 1;
    sc->live = 1;
    sc->scanmode = gbl_default_sc_scanmode;

    if (OPT_ON(opt, REBUILD_ALL))
        sc->force_rebuild = 1;

    if (OPT_ON(opt, REBUILD_DATA))
        sc->force_dta_rebuild = 1;
    
    if (OPT_ON(opt, REBUILD_BLOB)) {
        sc->force_dta_rebuild = 1;
        sc->force_blob_rebuild = 1;
    }

    if (!sc->force_rebuild)
        sc->use_plan = 1;

    if (OPT_ON(opt, PAGE_ORDER))
        sc->scanmode = SCAN_PAGEORDER;

    if (OPT_ON(opt, READ_ONLY))
        sc->live = 0;
    else
        sc->live = 1;

    sc->kind = SC_REBUILDTABLE;
    sc->commit_sleep = gbl_commit_sleep;
    sc->convert_sleep = gbl_convert_sleep;

    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto out;
        }
    }
    sc->same_schema = 1;
    tran_type *tran = curtran_gettran();
    int rc = get_csc2_file_tran(sc->tablename, -1 , &sc->newcsc2, NULL, tran);
    curtran_puttran(tran);
    if (rc)
    {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: %s\n", __func__,
               sc->tablename);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }
    if(sc->dryrun){
        comdb2prepareSString(v, pParse, 0,  sc, &comdb2SqlDryrunSchemaChange,
                            (vdbeFuncArgFree)  &free_schema_change_type);
    } else {
        comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    }
    return;

out:
    free_schema_change_type(sc);
}


void comdb2RebuildFull(Parse* p, Token* nm,Token* lnm, int opt)
{
    if (comdb2IsPrepareOnly(p))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(p, SQLITE_REBUILD_TABLE, 0, 0, 0) ){
            setError(p, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    comdb2Rebuild(p, nm,lnm, REBUILD_ALL + REBUILD_DATA + REBUILD_BLOB + opt);
}


void comdb2RebuildData(Parse* p, Token* nm, Token* lnm, int opt)
{
    if (comdb2IsPrepareOnly(p))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(p, SQLITE_REBUILD_DATA, 0, 0, 0) ){
            setError(p, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    comdb2Rebuild(p,nm,lnm,REBUILD_DATA + opt);
}

void comdb2RebuildDataBlob(Parse* p,Token* nm, Token* lnm, int opt)
{
    if (comdb2IsPrepareOnly(p))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(p, SQLITE_REBUILD_DATABLOB, 0, 0, 0) ){
            setError(p, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    comdb2Rebuild(p, nm, lnm, REBUILD_BLOB + opt);
}

void comdb2Truncate(Parse* pParse, Token* nm, Token* lnm)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_TRUNCATE_TABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v  = sqlite3GetVdbe(pParse);

    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(pParse, sc->tablename, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto out;

    sc->kind = SC_TRUNCATETABLE;
    sc->nothrevent = 1;
    sc->same_schema = 1;
    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto out;
        }
    }

    tran_type *tran = curtran_gettran();
    int rc = get_csc2_file_tran(sc->tablename, -1, &sc->newcsc2, NULL, tran);
    curtran_puttran(tran);
    if(rc)
    {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: %s\n", __func__,
               sc->tablename);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }

    if(sc->dryrun)
        comdb2prepareSString(v, pParse, 0,  sc, &comdb2SqlDryrunSchemaChange,
                            (vdbeFuncArgFree)  &free_schema_change_type);
    else
        comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}


void comdb2RebuildIndex(Parse* pParse, Token* nm, Token* lnm, Token* index, int opt)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_REBUILD_INDEX, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v  = sqlite3GetVdbe(pParse);

    char* indexname;
    int index_num;

    struct schema_change_type* sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(pParse, sc->tablename, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto out;

    sc->same_schema = 1;
    tran_type *tran = curtran_gettran();
    int rc = get_csc2_file_tran(sc->tablename, -1 , &sc->newcsc2, NULL, tran);
    curtran_puttran(tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: %s\n", __func__,
               sc->tablename);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }

    if (create_string_from_token(v, pParse, &indexname, index))
        goto out;

    rc = getidxnumbyname(get_dbtable_by_name(sc->tablename), indexname, &index_num);
    if( rc ){
        logmsg(LOGMSG_ERROR, "!table:index '%s:%s' not found\n", sc->tablename, indexname);
        setError(pParse, SQLITE_ERROR, "Index not found");
        goto out;
    }

    free(indexname);

    sc->kind = SC_REBUILDTABLE_INDEX;
    sc->nothrevent = 1;
    sc->rebuild_index = 1;
    sc->index_to_rebuild = index_num;
    sc->use_plan = 1;
    sc->scanmode = gbl_default_sc_scanmode;
    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto out;
        }
    }

    if (OPT_ON(opt, PAGE_ORDER))
        sc->scanmode = SCAN_PAGEORDER;

    if (OPT_ON(opt, READ_ONLY))
        sc->live = 0;
    else
        sc->live = 1;

    sc->commit_sleep = gbl_commit_sleep;
    sc->convert_sleep = gbl_convert_sleep;

    sc->same_schema = 1;

    if(sc->dryrun){
        comdb2prepareSString(v, pParse, 0,  sc, &comdb2SqlDryrunSchemaChange,
                            (vdbeFuncArgFree)  &free_schema_change_type);
    } else {
        comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                        (vdbeFuncArgFree)&free_schema_change_type);
    }
    return;

out:
    free_schema_change_type(sc);
}

/********************** STORED PROCEDURES ****************************************/

void comdb2CreateProcedure(Parse* pParse, Token* nm, Token* ver, Token* proc)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_CREATE_PROC, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    char spname[MAX_SPNAME];
    char sp_version[MAX_SPVERSION_LEN];

    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2TokenToStr(nm, spname, sizeof(spname))) {
        setError(pParse, SQLITE_MISUSE, "Procedure name is too long");
        return;
    }

    struct schema_change_type *sc = new_schemachange_type();
    strcpy(sc->tablename, spname);
    sc->kind = SC_ADDSP;

    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto cleanup;
        }
    }
    if (ver) {
        if (comdb2TokenToStr(ver, sp_version, sizeof(sp_version))) {
            setError(pParse, SQLITE_MISUSE, "Procedure version is too long");
            goto cleanup;
        }
        strcpy(sc->fname, sp_version);
    }
    copyNoSqlToken(v, pParse, &sc->newcsc2, proc);
    const char* colname[] = {"version"};
    const int coltype = OPFUNC_STRING_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 256};
    comdb2prepareOpFunc(v, pParse, 1, sc, &comdb2ProcSchemaChange,
                        (vdbeFuncArgFree)&free_schema_change_type, &stp);
    return;

cleanup:
    free_schema_change_type(sc);
    return;
}

void comdb2DefaultProcedure(Parse *pParse, Token *nm, Token *ver, int str)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    char spname[MAX_SPNAME];
    char sp_version[MAX_SPVERSION_LEN];

    Vdbe *v = sqlite3GetVdbe(pParse);

    if (comdb2TokenToStr(nm, spname, sizeof(spname))) {
        setError(pParse, SQLITE_MISUSE, "Procedure name is too long");
        return;
    }

    struct schema_change_type *sc = new_schemachange_type();
    strcpy(sc->tablename, spname);

    if (str) {
        if (comdb2TokenToStr(ver, sp_version, sizeof(sp_version))) {
            setError(pParse, SQLITE_MISUSE, "Procedure version is too long");
            goto cleanup;
        }
        strcpy(sc->fname, sp_version);
    } else {
        sc->newcsc2 = malloc(ver->n + 1);
        strncpy(sc->newcsc2, ver->z, ver->n);
        sc->newcsc2[ver->n] = '\0';
    }
    sc->kind = SC_DEFAULTSP;

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

cleanup:
    free_schema_change_type(sc);
    return;
}

void comdb2DropProcedure(Parse *pParse, Token *nm, Token *ver, int str)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_DROP_PROC, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    char spname[MAX_SPNAME];
    char sp_version[MAX_SPVERSION_LEN];

    Vdbe *v = sqlite3GetVdbe(pParse);

    if (comdb2TokenToStr(nm, spname, sizeof(spname))) {
        setError(pParse, SQLITE_MISUSE, "Procedure name is too long");
        return;
    }

    // Note: Even though we know that we can't drop the stored proce here
    // , we let this go through and check the error in do_del_sp to have
    // a homogenous rcode
#ifdef SFUNC_USAGE_CHECK_WHEN_PARSE
    char *tbl = 0;
    if (lua_sfunc_used(spname, &tbl)) {
        char *errMsg = comdb2_asprintf("Can't drop. %s is in use by %s", spname, tbl);
        setError(pParse, SQLITE_ERROR, errMsg);
        free(errMsg);
        return;
    }
#endif

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }
    strcpy(sc->tablename, spname);

    if (str) {
        if (comdb2TokenToStr(ver, sp_version, sizeof(sp_version))) {
            setError(pParse, SQLITE_MISUSE, "Procedure version is too long");
            goto cleanup;
        }
        strcpy(sc->fname, sp_version);
    } else {
        sc->newcsc2 = malloc(ver->n + 1);
        strncpy(sc->newcsc2, ver->z, ver->n);
        sc->newcsc2[ver->n] = '\0';
    }
    sc->kind = SC_DELSP;

    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto cleanup;
        }
    }
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange_tran,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

cleanup:
    free_schema_change_type(sc);
    return;
}

/********************* PARTITIONS  **********************************************/

static int _get_integer(Token *tok, int32_t *oInt)
{
    char str[10];
    if (tok->n >= sizeof(str)) {
        return -1;
    }
    strncpy0(str, tok->z, tok->n + 1);
    *oInt = atoi(str);
    return 0;
}

static int comdb2GetTimePartitionParams(Parse* pParse, Token *period,
                                        Token *retention, Token *start,
                                        int32_t *oPeriod, int32_t *oRetention,
                                        int64_t *oStart)
{
    char period_str[50];

    assert (*period->z == '\'' || *period->z == '\"');
    period->z++;
    period->n -= 2;

    if (period->n >= sizeof(period_str)) {
        setError(pParse, SQLITE_MISUSE, "Invalid period name");
        return -1;
    }
    strncpy0(period_str, period->z, period->n + 1);
    *oPeriod = name_to_period(period_str);
    if (*oPeriod == VIEW_PARTITION_INVALID) {
        setError(pParse, SQLITE_ERROR, "Invalid period name");
        return -1;
    }

    if (_get_integer(retention, oRetention)) {
        setError(pParse, SQLITE_MISUSE, "Invalid retention");
        return -1;
    }

    char start_str[200];
    assert (*start->z == '\'' || *start->z == '\"');
    start->z++;
    start->n -= 2;
    if (start->n >= sizeof(start_str)) {
        setError(pParse, SQLITE_MISUSE, "Invalid start date");
        return -1;    
    }
    strncpy0(start_str, start->z, start->n + 1);
    *oStart = convert_from_start_string(*oPeriod, start_str);
    if (*oStart == -1 ) {
        setError(pParse, SQLITE_ERROR, "Invalid start date");
        return -1;
    }

    return 0;
}

static int comdb2GetManualPartitionParams(Parse* pParse, Token *retention,
                                          Token *start, int32_t *oRetention,
                                          int32_t *oStart)
{
    if (_get_integer(retention, oRetention)) {
        setError(pParse, SQLITE_MISUSE, "Invalid manual retention");
        return -1;
    }
    if (!start)  {
        *oStart = 0;
    } else if (_get_integer(start, oStart)) {
        setError(pParse, SQLITE_MISUSE, "Invalid manual start");
        return -1;
    }
    return 0;
}

void comdb2CreatePartition(Parse* pParse, Token* table,
                           Token* partition_name, Token* period,
                           Token* retention, Token* start)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    if (comdb2IsDryrun(pParse)) {
        setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
         return;
    }
#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_CREATE_PART, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v  = sqlite3GetVdbe(pParse);

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    if (!arg) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }
    bpfunc_arg__init(arg);

    BpfuncCreateTimepart *tp = malloc(sizeof(BpfuncCreateTimepart));
    if (!tp) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }
    bpfunc_create_timepart__init(tp);
    
    arg->crt_tp = tp;
    arg->type = BPFUNC_CREATE_TIMEPART;
    tp->tablename = (char*) malloc(MAXTABLELEN);
    if (!tp->tablename) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }
    memset(tp->tablename, '\0', MAXTABLELEN);
    if (table &&
        chkAndCopyTableTokens(pParse, tp->tablename, table, 0,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto clean_arg;

    tp->partition_name = (char*) malloc(MAXTABLELEN);
    if (!tp->partition_name) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }
    if (partition_name->n >= MAXTABLELEN) {
        setError(pParse, SQLITE_MISUSE, "Partition name is too long");
        goto clean_arg;
    }
    strncpy0(tp->partition_name, partition_name->z, partition_name->n + 1);

    if (comdb2GetTimePartitionParams(pParse, period, retention, start,
                                     &tp->period, &tp->retention, &tp->start)) {
        goto clean_arg;
    }

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc,
                        (vdbeFuncArgFree) &free_bpfunc_arg);
    return;

clean_arg:
    if (arg)
        free_bpfunc_arg(arg);
}


void comdb2DropPartition(Parse* pParse, Token* partition_name)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    if(comdb2IsDryrun(pParse)) {
        setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
        return;
    }
#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_DROP_PART, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v  = sqlite3GetVdbe(pParse);

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    if (!arg) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    bpfunc_arg__init(arg);

    BpfuncDropTimepart *tp = malloc(sizeof(BpfuncDropTimepart));
    if (!tp) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }
    bpfunc_drop_timepart__init(tp);

    arg->drop_tp = tp;
    arg->type = BPFUNC_DROP_TIMEPART;
    tp->partition_name = (char*) malloc(MAXTABLELEN);
    if (!tp->partition_name) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }
    if (partition_name->n >= MAXTABLELEN) {
        setError(pParse, SQLITE_MISUSE, "Partition name is too long");
        goto clean_arg;
    }
    strncpy0(tp->partition_name, partition_name->z, partition_name->n + 1);

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc,
                        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    if(arg)
        free_bpfunc_arg(arg);
}


/********************* BULK IMPORT ***********************************************/

void comdb2bulkimport(Parse* pParse, Token* nm,Token* lnm, Token* nm2, Token* lnm2)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    setError(pParse, SQLITE_INTERNAL, "Not Implemented");
    logmsg(LOGMSG_DEBUG, "Bulk import from %.*s to %.*s ", nm->n + lnm->n,
           nm->z, nm2->n +lnm2->n, nm2->z);
}

/********************* ANALYZE ***************************************************/

int comdb2vdbeAnalyze(OpFunc *f)
{
    if ((f->rc = do_analyze(f->arg, f->int_arg)) != SQLITE_OK)
        f->errorMsg = "Analyze could not run because of internal problems";
    return f->rc;
}


void comdb2analyze(Parse* pParse, int opt, Token* nm, Token* lnm, int pc)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_ANALYZE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);
    int threads = GET_ANALYZE_THREAD(opt);
    int sum_threads = GET_ANALYZE_SUMTHREAD(opt);

    if (threads > 0)
        analyze_set_max_table_threads(NULL, &threads);
    if (sum_threads)
        analyze_set_max_sampling_threads(NULL, &sum_threads);

    if (nm == NULL) {
        comdb2prepareNoRows(v, pParse, pc, NULL, &comdb2vdbeAnalyze,
                            (vdbeFuncArgFree) &free);
    } else {
        char *tablename = (char*) malloc(MAXTABLELEN);
        if (!tablename)
            goto err;

        if (chkAndCopyTableTokens(pParse, tablename, nm, lnm,
                                  ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL)) {
            free(tablename);
            goto err;
        }
        else
            comdb2prepareNoRows(v, pParse, pc, tablename, &comdb2vdbeAnalyze,
                                (vdbeFuncArgFree) &free);
    }

    return;

err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
}

/*
  Implementation of PUT ANALYZE COVERAGE ...
 */
void comdb2analyzeCoverage(Parse* pParse, Token* nm, Token* lnm, int newscale)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_ANALYZE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (newscale < -1 || newscale > 100) {
        setError(pParse, SQLITE_ERROR, "Coverage must be between 0 and 100");
        return;
    }

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    if (!arg) goto err;
    bpfunc_arg__init(arg);

    BpfuncAnalyzeCoverage *ancov_f = (BpfuncAnalyzeCoverage*) malloc(sizeof(BpfuncAnalyzeCoverage));
    if (!ancov_f) goto err;
    bpfunc_analyze_coverage__init(ancov_f);

    arg->an_cov = ancov_f;
    arg->type = BPFUNC_ANALYZE_COVERAGE;
    ancov_f->tablename = (char*) malloc(MAXTABLELEN);
    if (!ancov_f->tablename) goto err;

    if (chkAndCopyTableTokens(pParse, ancov_f->tablename, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto clean_arg;

    ancov_f->newvalue = newscale;
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);
    return;

err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg) free_bpfunc_arg(arg);
}


void comdb2setSkipscan(Parse* pParse, Token* nm, Token* lnm, int enable)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (enable != 0 && enable != 1) {
        setError(pParse, SQLITE_ERROR, "Can only enable or disable skipscan");
        return;
    }

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    if (!arg) goto err;
    bpfunc_arg__init(arg);

    BpfuncAnalyzeCoverage *ancov_f = (BpfuncAnalyzeCoverage*) malloc(sizeof(BpfuncAnalyzeCoverage));
    if (!ancov_f) goto err;
    bpfunc_analyze_coverage__init(ancov_f);

    arg->an_cov = ancov_f;
    arg->type = BPFUNC_SET_SKIPSCAN;
    ancov_f->tablename = (char*) malloc(MAXTABLELEN);
    if (!ancov_f->tablename) goto err;

    if (chkAndCopyTableTokens(pParse, ancov_f->tablename, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto clean_arg;

    ancov_f->newvalue = enable;
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);
    return;

err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg) free_bpfunc_arg(arg);
}


void comdb2enableGenid48(Parse* pParse, int enable)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));

    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err; 

    BpfuncGenid48Enable *gn = malloc(sizeof(BpfuncGenid48Enable));
    
    if (gn)
        bpfunc_genid48_enable__init(gn);
    else
        goto err;

    arg->gn_enable = gn;
    arg->type = BPFUNC_GENID48_ENABLE;
    gn->enable = enable;
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);
    return;

err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
    if (arg)
        free_bpfunc_arg(arg);   
}

void comdb2enableRowlocks(Parse* pParse, int enable)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));

    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err; 

    BpfuncRowlocksEnable *rl = malloc(sizeof(BpfuncRowlocksEnable));
    
    if (rl)
        bpfunc_rowlocks_enable__init(rl);
    else
        goto err;

    arg->rl_enable = rl;
    arg->type = BPFUNC_ROWLOCKS_ENABLE;
    rl->enable = enable;
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);
    return;

err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
    if (arg)
        free_bpfunc_arg(arg);   
}

void comdb2analyzeThreshold(Parse* pParse, Token* nm, Token* lnm, int newthreshold)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_ANALYZE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (newthreshold < -1 || newthreshold > 100) {
        setError(pParse, SQLITE_ERROR, "Threshold must be between 0 and 100");
        return;
    }
    
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    if (!arg) goto err;
    bpfunc_arg__init(arg);

    BpfuncAnalyzeThreshold *anthr_f = (BpfuncAnalyzeThreshold*) malloc(sizeof(BpfuncAnalyzeThreshold));
    if (!anthr_f) goto err;
    bpfunc_analyze_threshold__init(anthr_f);

    arg->an_thr = anthr_f;
    arg->type = BPFUNC_ANALYZE_THRESHOLD;
    anthr_f->tablename = (char*) malloc(MAXTABLELEN);

    if (!anthr_f->tablename)
        goto err;

    if (chkAndCopyTableTokens(pParse, anthr_f->tablename, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        return;

    anthr_f->newvalue = newthreshold;
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
    if (arg)
        free_bpfunc_arg(arg);
}

/********************* ALIAS **************************************************/

void comdb2setAlias(Parse* pParse, Token* name, Token* url)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));

    if (arg)
    {
        bpfunc_arg__init(arg);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }

    BpfuncAlias *alias_f = (BpfuncAlias*) malloc(sizeof(BpfuncAlias));

    if (alias_f)
    {
        bpfunc_alias__init(alias_f);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->alias = alias_f;
    arg->type = BPFUNC_ALIAS;
    alias_f->name = (char*) malloc(MAXTABLELEN);

    if (chkAndCopyTableTokens(pParse, alias_f->name, name, 0,
                              ERROR_ON_TBL_FOUND, 1, 0, NULL))
        goto clean_arg;

    assert (*url->z == '\'' || *url->z == '\"');
    url->z++;
    url->n -= 2;

    if (create_string_from_token(v, pParse, &alias_f->remote, url))
        goto clean_arg;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);
}

void comdb2getAlias(Parse* pParse, Token* t1)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_GET_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    setError(pParse, SQLITE_INTERNAL, "Not Implemented");
    logmsg(LOGMSG_INFO, "Getting alias %.*s", t1->n, t1->z);
}

/********************* GRANT AUTHORIZAZIONS ************************************/

static int is_system_table(Parse *pParse, Token *nm, char *dst)
{
    char tablename[MAXTABLELEN];
    sqlite3 *db;

    /* missing table name? */
    if (!nm)
        return 0;

    if (comdb2TokenToStr(nm, tablename, sizeof(tablename))) {
        return 0;
    }

    db = pParse->db;

    if ((sqlite3HashFind(&db->aModule, tablename))) {
        strcpy(dst, tablename);
        return 1;
    }

    return 0;
}

void comdb2grant(Parse *pParse, int revoke, int permission, Token *nm,
                 Token *lnm, Token *u)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, revoke ? SQLITE_REVOKE : SQLITE_GRANT, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));

    if (arg)
    {
        bpfunc_arg__init(arg);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }

    BpfuncGrant *grant = (BpfuncGrant*) malloc(sizeof(BpfuncGrant));

    if (grant)
    {
        bpfunc_grant__init(grant);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->grant = grant;
    arg->type = BPFUNC_GRANT;
    grant->yesno = revoke;
    grant->perm = permission;
    grant->table = (char*) malloc(MAXTABLELEN);
    grant->table[0] = '\0';

    if (permission == AUTH_USERSCHEMA) {
      if (create_string_from_token(v, pParse, &grant->userschema, nm))
        goto clean_arg;
    } else {
        /* Check for remote request only if both the tokens are set. */
        if (lnm && (isRemote(pParse, &nm, &lnm))) {
            goto clean_arg;
        }

        if ((is_system_table(pParse, nm, grant->table))) {
            if (permission != AUTH_READ) {
                setError(
                    pParse, SQLITE_ERROR,
                    "Can't GRANT/REVOKE non-READ permissions on system table");
                goto clean_arg;
            }
        } else if (chkAndCopyTableTokens(pParse, grant->table, nm, lnm,
                                         ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL)) {
            goto clean_arg;
        }
    }

    if (create_string_from_token(v, pParse, &grant->username, u))
        goto clean_arg;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);

}

/****************************** AUTHENTICATION ON/OFF *******************************/

void comdb2enableAuth(Parse* pParse, int on)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateOpPassword(pParse))
    {
        setError(pParse, SQLITE_AUTH, "User does not have OP credentials");
        return;
    }

    Vdbe *v  = sqlite3GetVdbe(pParse);
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
    {
        bpfunc_arg__init(arg);
    }else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }   


    BpfuncAuthentication *auth = (BpfuncAuthentication*) malloc(sizeof(BpfuncAuthentication));
    
    if (auth)
    {
        bpfunc_authentication__init(auth);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->auth = auth;
    arg->type = BPFUNC_AUTHENTICATION;
    auth->enabled = on;
    gbl_check_access_controls = 1;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);

}

/****************************** PASSWORD *******************************/

void comdb2setPassword(Parse* pParse, Token* pwd, Token* nm)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    char username[MAX_USERNAME_LEN];
    char passwd[MAX_PASSWORD_LEN];

    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2TokenToStr(pwd, passwd, sizeof(passwd))) {
        setError(pParse, SQLITE_MISUSE, "Password is too long");
        return;
    }

    if (comdb2TokenToStr(nm, username, sizeof(username))) {
        setError(pParse, SQLITE_MISUSE, "User name is too long");
        return;
    }

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    if (arg == NULL) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }
    bpfunc_arg__init(arg);

    BpfuncPassword *password = (BpfuncPassword*) malloc(sizeof(BpfuncPassword));
    if (password == NULL) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }
    bpfunc_password__init(password);

    arg->pwd = password;
    arg->type = BPFUNC_PASSWORD;
    password->disable = 0;

    password->user = strdup(username);
    if (password->user == NULL) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    password->password = strdup(passwd);
    if (password->password == NULL) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    if (comdb2AuthenticateUserDDL(""))
    {
        struct sqlclntstate *clnt = get_sql_clnt();
        /* Check if its password change request */
        if (!(clnt && strcmp(clnt->current_user.name, password->user) == 0 )) {
            setError(pParse, SQLITE_AUTH, "User does not have OP credentials");
            goto clean_arg;
        }
    }

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc,
        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);
}

void comdb2deletePassword(Parse* pParse, Token* nm)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);
    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
    {
        bpfunc_arg__init(arg);
    }else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        return;
    }  
  
    BpfuncPassword * pwd = (BpfuncPassword*) malloc(sizeof(BpfuncPassword));
    
    if (pwd)
    {
        bpfunc_password__init(pwd);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->pwd = pwd;
    arg->type = BPFUNC_PASSWORD;
    pwd->disable = 1;
  
    if (create_string_from_token(v, pParse, &pwd->user, nm))
        goto clean_arg;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
        (vdbeFuncArgFree) &free_bpfunc_arg);
    
    return;

clean_arg:
    free_bpfunc_arg(arg);  
}

int comdb2genidcontainstime(void)
{
     bdb_state_type *bdb_state = thedb->bdb_env;
     return genid_contains_time(bdb_state);
}

int producekw(OpFunc *f)
{
    int found = 0;

    for (int i=0; i < sqlite3_keyword_count(); i++)
    {
        const char *zName = 0;
        int nName = 0;
        if( sqlite3_keyword_name(i, &zName, &nName)==SQLITE_OK ){
            char kw[100];

            if (nName < sizeof(kw)-1 && (f->int_arg == KW_RES || f->int_arg == KW_FB)) {
                // See if reserved word
                int tok;

                strncpy(kw, zName, nName);
                kw[nName] = 0;

                int rc = sqlite3GetToken((unsigned char*) kw, &tok);
                if (rc > 0) {
                    int isfb = sqlite3ParserFallback(tok);
                    if ((isfb && f->int_arg == KW_FB) || (!isfb && f->int_arg == KW_RES)) {
                        opFuncPrintf(f, "%.*s", nName, zName);
                        found++;
                    }
                }
                continue;
            }
            else if (f->int_arg == KW_ALL)
                opFuncPrintf(f, "%.*s", nName, zName);
            found++;
        }
    }
    f->rc = found>0 ? SQLITE_OK : SQLITE_DONE;
    f->errorMsg = NULL;
    return SQLITE_OK;
}

void comdb2getkw(Parse* pParse, int arg)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_GET_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Keyword"};
    const int coltype = OPFUNC_STRING_TYPE;
    OpFuncSetup stp = { 1, colname, &coltype, sqlite3_keyword_count() };
    comdb2prepareOpFunc(v, pParse, arg, NULL, &producekw, (vdbeFuncArgFree)  &free, &stp);

}

static int produceAnalyzeCoverage(OpFunc *f)
{

    char  *tablename = (char*) f->arg;
    int rst;
    int bdberr; 
    tran_type *tran = curtran_gettran();
    int rc = bdb_get_analyzecoverage_table(tran, tablename, &rst, &bdberr);
    curtran_puttran(tran);
    
    if (!rc)
    {
        opFuncWriteInteger(f, (int) rst );
        f->rc = SQLITE_OK;
        f->errorMsg = NULL;
    } else 
    {
        f->rc = SQLITE_INTERNAL;
        f->errorMsg = "Could not read value";
    }
    return SQLITE_OK;
}

void comdb2getAnalyzeCoverage(Parse* pParse, Token *nm, Token *lnm)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_GET_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Coverage"};
    const int coltype = OPFUNC_INT_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 256};
    char *tablename = (char*) malloc (MAXTABLELEN);

    if (chkAndCopyTableTokens(pParse, tablename, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        free(tablename);
    else
        comdb2prepareOpFunc(v, pParse, 0, tablename, &produceAnalyzeCoverage,
                            (vdbeFuncArgFree)  &free, &stp);
}

void comdb2CreateRangePartition(Parse *pParse, Token *nm, Token *col,
        ExprList* limits)
{
    if (comdb2IsPrepareOnly(pParse))
        return;
    if(comdb2IsDryrun(pParse)) {
        setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
        return;
    }
#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_CREATE_PART, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    char tblname[MAXTABLELEN];

    if (chkAndCopyTableTokens(pParse, tblname, nm, NULL,
                              ERROR_ON_TBL_NOT_FOUND, 0, 0, NULL))
        return;

    shard_range_create(pParse, tblname, col, limits);
}

static int produceAnalyzeThreshold(OpFunc *f)
{

    char  *tablename = (char*) f->arg;
    long long int rst;
    int bdberr;
    tran_type *tran = curtran_gettran();
    int rc = bdb_get_analyzethreshold_table(tran, tablename, &rst, &bdberr);
    curtran_puttran(tran);

    if (!rc)
    {
        opFuncWriteInteger(f, (int) rst );
        f->rc = SQLITE_OK;
        f->errorMsg = NULL;
    } else 
    {
        f->rc = SQLITE_INTERNAL;
        f->errorMsg = "Could not read value";
    }
    return SQLITE_OK;
}

void comdb2getAnalyzeThreshold(Parse* pParse, Token *nm, Token *lnm)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_GET_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Threshold"};
    const int coltype = OPFUNC_INT_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 256};
    char *tablename = (char*) malloc (MAXTABLELEN);

    if (chkAndCopyTableTokens(pParse, tablename, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        free(tablename);
    else
        comdb2prepareOpFunc(v, pParse, 0, tablename, &produceAnalyzeThreshold,
                            (vdbeFuncArgFree)  &free, &stp);
}

int resolveTableName(sqlite3 *db, struct SrcList_item *p, const char *zDB,
                     char *tableName, size_t len)
{
   struct sqlclntstate *clnt = get_sql_clnt();
   if ((zDB && (!strcasecmp(zDB, "main") || !strcasecmp(zDB, "temp"))))
   {
       snprintf(tableName, len, "%s", p->zName);
   } else if (clnt &&
              (clnt->current_user.have_name) &&         /* authenticated */
              !strchr(p->zName, '@') &&                 /* mustn't have user
                                                           name */
              strncasecmp(p->zName, "sqlite_", 7) &&    /* sqlite table */
              strncasecmp(p->zName, "comdb2sys_", 10) &&/* old system table
                                                           name */
              !sqlite3HashFind(&db->aModule, p->zName)) /* sqlite module */
   {
       char userschema[MAXTABLELEN];
       int bdberr;
       int bytes_written;
       bdb_state_type *bdb_state = thedb->bdb_env;
       tran_type *tran = curtran_gettran();
       int rc = bdb_tbl_access_userschema_get(bdb_state, tran, clnt->current_user.name, userschema, &bdberr);
       curtran_puttran(tran);
       if (rc == 0) {
         if (userschema[0] == '\0') {
           bytes_written = snprintf(tableName, len, "%s", p->zName);
           if (bytes_written >= len) {
               return 1;
           }
         } else {
           bytes_written = snprintf(tableName, len, "%s@%s", p->zName,
                                    userschema);
           if (bytes_written >= len) {
               return 1;
           }
         }
       } else {
         bytes_written = snprintf(tableName, len, "%s@%s", p->zName,
                                  clnt->current_user.name);
         if (bytes_written >= len) {
             return 1;
         }
       }
   } else {
       snprintf(tableName, len, "%s", p->zName);
   }
   return 0;
}


void comdb2timepartRetention(Parse *pParse, Token *nm, Token *lnm, int retention)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    Vdbe *v  = sqlite3GetVdbe(pParse);
    BpfuncArg *arg = NULL;

    if (retention < 2)
    {
        setError(pParse, SQLITE_ERROR, "Retention must be 2 or higher");
        return;
    }

    arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err;
    BpfuncTimepartRetention *tp_retention = (BpfuncTimepartRetention*) 
        malloc(sizeof(BpfuncTimepartRetention));

    if (tp_retention)
        bpfunc_timepart_retention__init(tp_retention);
    else
        goto err;

    arg->tp_ret = tp_retention;
    arg->type = BPFUNC_TIMEPART_RETENTION;
    tp_retention->timepartname = (char*) malloc(MAXTABLELEN);

    if (!tp_retention->timepartname)
        goto err;

    if (chkAndCopyTableTokens(pParse, tp_retention->timepartname, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto clean_arg;

    tp_retention->newvalue = retention;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree)&free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);
}

static void comdb2CounterInt(Parse *pParse, Token *nm, Token *lnm,
        int isset, long long value)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    char name[MAXTABLELEN];
    char *query;

    if (chkAndCopyPartitionTokens(pParse, name, nm, lnm))
        goto err;

    if (isset == 0)
        value = logical_partition_next_rollout(name);

    query = logical_cron_update_sql(name, value, isset==0);

    sqlite3NestedParsePreserveFlags(pParse, query);

    sqlite3_free(query);
    return;
err:
    logmsg(LOGMSG_ERROR, "%s: failed to parse generated query!\n", __func__);
}

void comdb2CounterIncr(Parse *pParse, Token *nm, Token *lnm)
{
    comdb2CounterInt(pParse, nm, lnm, 0, 0);
}

void comdb2CounterSet(Parse *pParse, Token *nm, Token *lnm, long long value)
{
    comdb2CounterInt(pParse, nm, lnm, 1, value);
}

void sqlite3AlterRenameTable(Parse *pParse, Token *pSrcName, Token *pName)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_ALTER_TABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    char table[MAXTABLELEN];
    char newTable[MAXTABLELEN];
    struct schema_change_type *sc;
    struct dbtable *db;

    Vdbe *v = sqlite3GetVdbe(pParse);

    if (comdb2TokenToStr(pSrcName, table, sizeof(table))) {
        setError(pParse, SQLITE_MISUSE, "Table name is too long");
        return;
    }

    if (timepart_is_partition(table)) {
        setError(pParse, SQLITE_MISUSE, "Time partitions cannot be renamed");
        return;
    }

    if (comdb2TokenToStr(pName, newTable, sizeof(newTable))) {
        setError(pParse, SQLITE_MISUSE, "Table name is too long");
        return;
    }


    db = get_dbtable_by_name(newTable);
    /* cannot rename to an existing table, unless we are removing an alias
    */
    if (db && !(gbl_lightweight_rename && db->sqlaliasname && (strncmp(db->sqlaliasname, table, MAXTABLELEN) == 0))) {
        setError(pParse, SQLITE_ERROR, "New table name already exists");
        return;
    }

    sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(pParse, sc->tablename, pSrcName, NULL,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto out;

    comdb2WriteTransaction(pParse);
    sc->nothrevent = 1;
    sc->live = 1;
    sc->kind = gbl_lightweight_rename?SC_ALIASTABLE:SC_RENAMETABLE;
    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto out;
        }
    }
    strncpy0(sc->newtable, newTable, sizeof(sc->newtable));

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

void comdb2schemachangeCommitsleep(Parse* pParse, int num)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    gbl_commit_sleep = num;
}

void comdb2schemachangeConvertsleep(Parse* pParse, int num)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    gbl_convert_sleep = num;
}

void comdb2WriteTransaction(Parse *pParse)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct sqlclntstate *clnt = get_sql_clnt();
    if (clnt && clnt->is_readonly) {
      setError(pParse, SQLITE_READONLY, "connection/database in read-only mode");
      return;
    }

    pParse->write = 1;
}

/* Column flags */
enum {
    COLUMN_NO_NULL = 1 << 0,
    COLUMN_DELETED = 1 << 1,
};

typedef LISTC_T(struct comdb2_index_part) comdb2_index_part_lst;
typedef LISTC_T(struct comdb2_key) comdb2_key_lst;

struct comdb2_column {
    /* Name of the column or csc2 style expression (denoting index
     * on expression) */
    char *name;
    /* Default value */
    char *def;
    /* Column type */
    uint8_t type;
    /* Type length */
    uint32_t len;
    /* Column flags */
    uint8_t flags;
    /* More flags */
    struct field_conv_opts convopts;
    /* Link */
    LINKC_T(struct comdb2_column) lnk;
};

/* Index column flags */
enum {
    INDEX_ORDER_DESC = 1 << 0,
    INDEX_IS_EXPR = 1 << 2,
};

struct comdb2_index_part {
    char *name;
    /* Index column flags */
    uint8_t flags;
    /* Reference to the column. */
    struct comdb2_column *column;
    /* Link */
    LINKC_T(struct comdb2_index_part) lnk;
};

/* Key flags */
enum {
    KEY_DUP = 1 << 0,
    KEY_DATACOPY = 1 << 1,
    KEY_DELETED = 1 << 2,
    KEY_UNIQNULLS = 1 << 3,
    KEY_RECNUM = 1 << 4,
    KEY_PARTIALDATACOPY = 1 << 5,
};

struct comdb2_partial_datacopy_field{
    /* Field name */
    char *name;
    /* Link */
    LINKC_T(struct comdb2_partial_datacopy_field) lnk;
};

struct comdb2_key {
    /* Name of the index */
    char *name;
    /* Parent table name */
    char *table;
    /* Partial index expression */
    char *where;
    /* Key flags */
    uint8_t flags;
    /* List of fields in partial datacopy */
    LISTC_T(struct comdb2_partial_datacopy_field) partial_datacopy_list;
    /* List of columns */
    comdb2_index_part_lst idx_col_list;
    /* Link */
    LINKC_T(struct comdb2_key) lnk;
};

/* Supported constraint types */
enum {
    CONS_FKEY = 1 << 1,
    CONS_CHECK = 1 << 2,
};

#define CONS_ALL (CONS_FKEY | CONS_CHECK)

/* Constraint flags */
enum {
    CONS_UPD_CASCADE = 1 << 0,
    CONS_DEL_CASCADE = 1 << 1,
    CONS_DEL_SETNULL = 1 << 2,
    CONS_DELETED     = 1 << 3,
};

struct comdb2_constraint {
    /* Name of the constraint. */
    char *name;
    /* Constraint type */
    uint8_t type;

    /*
       The following are helper fields to hold the column names and respective
       sort orders as specified in the query, to be later used to find the
       matching keys.
     */

    /* List of index columns in the child table. */
    comdb2_index_part_lst child_idx_col_list;
    /* List of index columns in the parent table. */
    comdb2_index_part_lst parent_idx_col_list;

    /* A reference to the child key */
    struct comdb2_key *child;
    /* Parent table */
    char *parent_table;
    /* Parent key name */
    char *parent_key;
    /* Constraint flags */
    uint8_t flags;
    /* CHECK expr */
    char *check_expr;
    /* Link */
    LINKC_T(struct comdb2_constraint) lnk;
};

struct comdb2_schema {
    /* Name of the table */
    char *name;
    /* Table options */
    uint32_t table_options;
    /* Staging list of new/existing columns */
    LISTC_T(struct comdb2_column) column_list;
    /* Staging list of new/existing keys */
    comdb2_key_lst key_list;
    /* Staging list of new/existing constraints */
    LISTC_T(struct comdb2_constraint) constraint_list;

    /* Link */
    LINKC_T(struct comdb2_schema) lnk;
};

/* DDL context flags */
enum {
    DDL_NOOP = 1 << 0,
    DDL_PENDING = 1 << 2,
};

/* DDL context for CREATE/ALTER command */
struct comdb2_ddl_context {
    /* Table definition */
    struct comdb2_schema *schema;
    /* User tag definitions */
    LISTC_T(struct comdb2_schema) tag_list;
    /* Flags */
    int flags;
    /* Memory allocator */
    comdb2ma mem;
    /* Only used during ALTER TABLE */
    struct comdb2_column *alter_column;
    /* Table name */
    char tablename[MAXTABLELEN];
    /* Partitioning */
    struct comdb2_partition *partition;
    char *partition_first_shardname;
};

/* Type properties */
enum {
    FLAG_ALLOW_ARRAY = 1 << 0,
    FLAG_QUOTE_DEFAULT = 1 << 1,
    /* cstring types need an extra byte for NULL-terminator. */
    FLAG_EXTRA_BYTE = 1 << 2,
};

/* A mapping from SQL types to Comdb2 types. */
#define TYPE_MAPPING                                                           \
    XMACRO_TYPE(SQL_TYPE_USHORT, "u_short", "u_short", 0)                      \
    XMACRO_TYPE(SQL_TYPE_SHORT, "short", "short", 0)                           \
    XMACRO_TYPE(SQL_TYPE_UINT, "u_int", "u_int", 0)                            \
    XMACRO_TYPE(SQL_TYPE_INT, "int", "int", 0)                                 \
    XMACRO_TYPE(SQL_TYPE_LONGLONG, "longlong", "longlong", 0)                  \
    XMACRO_TYPE(SQL_TYPE_ULONGLONG, "u_longlong", "u_longlong", 0)             \
    XMACRO_TYPE(SQL_TYPE_CSTRING, "cstring", "cstring",                        \
                FLAG_ALLOW_ARRAY | FLAG_QUOTE_DEFAULT)                         \
    XMACRO_TYPE(SQL_TYPE_VUTF8, "vutf8", "vutf8",                              \
                FLAG_ALLOW_ARRAY | FLAG_QUOTE_DEFAULT)                         \
    XMACRO_TYPE(SQL_TYPE_BLOB, "blob", "blob",                                 \
                FLAG_ALLOW_ARRAY | FLAG_QUOTE_DEFAULT)                         \
    XMACRO_TYPE(SQL_TYPE_BYTE, "byte", "byte",                                 \
                FLAG_ALLOW_ARRAY | FLAG_QUOTE_DEFAULT)                         \
    XMACRO_TYPE(SQL_TYPE_DATETIME, "datetime", "datetime", FLAG_QUOTE_DEFAULT) \
    XMACRO_TYPE(SQL_TYPE_DATETIMEUS, "datetimeus", "datetimeus",               \
                FLAG_QUOTE_DEFAULT)                                            \
    XMACRO_TYPE(SQL_TYPE_INTERVALDS, "intervalds", "intervalds",               \
                FLAG_QUOTE_DEFAULT)                                            \
    XMACRO_TYPE(SQL_TYPE_INTERVALDSUS, "intervaldsus", "intervaldsus",         \
                FLAG_QUOTE_DEFAULT)                                            \
    XMACRO_TYPE(SQL_TYPE_INTERVALYM, "intervalym", "intervalym",               \
                FLAG_QUOTE_DEFAULT)                                            \
    XMACRO_TYPE(SQL_TYPE_DECIMAL32, "decimal32", "decimal32", 0)               \
    XMACRO_TYPE(SQL_TYPE_DECIMAL64, "decimal64", "decimal64", 0)               \
    XMACRO_TYPE(SQL_TYPE_DECIMAL128, "decimal128", "decimal128", 0)            \
    XMACRO_TYPE(SQL_TYPE_SMALLFLOAT, "smallfloat", "float", 0)                 \
    XMACRO_TYPE(SQL_TYPE_FLOAT, "float", "float", 0)                           \
    XMACRO_TYPE(SQL_TYPE_DOUBLE, "double", "double", 0)                        \
    /* Additional types mapped to a Comdb2 type. */                            \
    XMACRO_TYPE(SQL_TYPE_VARCHAR, "varchar", "cstring",                        \
                FLAG_ALLOW_ARRAY | FLAG_QUOTE_DEFAULT | FLAG_EXTRA_BYTE)       \
    XMACRO_TYPE(SQL_TYPE_CHAR, "char", "cstring",                              \
                FLAG_ALLOW_ARRAY | FLAG_QUOTE_DEFAULT | FLAG_EXTRA_BYTE)       \
    XMACRO_TYPE(SQL_TYPE_TEXT, "text", "vutf8",                                \
                FLAG_ALLOW_ARRAY | FLAG_QUOTE_DEFAULT)                         \
    XMACRO_TYPE(SQL_TYPE_INTEGER, "integer", "int", 0)                         \
    XMACRO_TYPE(SQL_TYPE_SMALLINT, "smallint", "short", 0)                     \
    XMACRO_TYPE(SQL_TYPE_BIGINT, "bigint", "longlong", 0)                      \
    XMACRO_TYPE(SQL_TYPE_LARGEINT, "largeint", "longlong", 0)                  \
    XMACRO_TYPE(SQL_TYPE_REAL, "real", "float", 0)                             \
    /* End marker */                                                           \
    XMACRO_TYPE(SQL_TYPE_LAST, 0, 0, 0)

#define XMACRO_TYPE(code, sql_str, comdb2_str, flags) code,
typedef enum { TYPE_MAPPING } type_codes;
#undef XMACRO_TYPE

#define XMACRO_TYPE(code, sql_str, comdb2_str, flags) sql_str,
static const char *type_sql_str[] = {TYPE_MAPPING};
#undef XMACRO_TYPE

#define XMACRO_TYPE(code, sql_str, comdb2_str, flags) sizeof(sql_str) - 1,
static size_t type_sql_str_len[] = {TYPE_MAPPING};
#undef XMACRO_TYPE

#define XMACRO_TYPE(code, sql_str, comdb2_str, flags) comdb2_str,
static const char *type_comdb2_str[] = {TYPE_MAPPING};
#undef XMACRO_TYPE

#define XMACRO_TYPE(code, sql_str, comdb2_str, flags) flags,
static int type_flags[] = {TYPE_MAPPING};
#undef XMACRO_TYPE

static inline int validAutoIncrementColumnCheck(struct comdb2_column *column)
{
    if ( column->type == SQL_TYPE_LONGLONG )
        return 0;
    if ( gbl_permit_small_sequences ) {
        switch ( column->type ) {
            case SQL_TYPE_SHORT:
            case SQL_TYPE_INT:
                return 0;
            break;
        }
    }
    return 1;
}

/*
  Allocate Comdb2 DDL context to be used during parsing.
*/
static struct comdb2_ddl_context *create_ddl_context(Parse *pParse)
{
    struct comdb2_ddl_context *ctx;

    /* allocated already ? */
    if (pParse->comdb2_ddl_ctx)
        return pParse->comdb2_ddl_ctx;

    /* Allocate struct comdb2_ddl_context. */
    ctx = calloc(1, sizeof(struct comdb2_ddl_context));
    if (ctx == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __FILE__, __LINE__);
        return NULL;
    }

    /* All memory allocations must happen using this allocator. */
    ctx->mem = comdb2ma_create(0, 0, "parser_ddl_context", COMDB2MA_MT_SAFE);
    if (ctx->mem == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d comdb2ma_create() failed\n", __FILE__,
               __LINE__);
        goto err;
    }

    /* Allocate struct comdb2_schema */
    ctx->schema = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_schema));
    if (ctx->schema == NULL) {
        logmsg(LOGMSG_ERROR, "%s:%d out of memory\n", __FILE__, __LINE__);
        goto err;
    }

    /* Initialize various lists */
    listc_init(&ctx->tag_list, offsetof(struct comdb2_schema, lnk));
    listc_init(&ctx->schema->column_list, offsetof(struct comdb2_column, lnk));
    listc_init(&ctx->schema->key_list, offsetof(struct comdb2_key, lnk));
    listc_init(&ctx->schema->constraint_list,
               offsetof(struct comdb2_constraint, lnk));

    pParse->comdb2_ddl_ctx = ctx;
    return ctx;

err:
    if (ctx->mem) {
        comdb2ma_destroy(ctx->mem);
    }
    free(ctx);
    return NULL;
}

/*
  Deallocate Comdb2 DDL context.
*/
static void free_ddl_context(Parse *pParse)
{
    struct comdb2_ddl_context *ctx;

    ctx = pParse->comdb2_ddl_ctx;
    if (ctx == 0)
        return;

    free(ctx->partition_first_shardname);
    free(ctx->partition);
    
    comdb2ma_destroy(ctx->mem);
    free(ctx);

    pParse->comdb2_ddl_ctx = 0;
    return;
}

/*
  Parse the specified type and return the position of the matching entry
  in type_mapping array. Also parse the size (if any) and store it into
  memory pointed by the size.

  @return
   -1    Error (invalid type/size)
   type  Position in type_mapping array
*/
static int comdb2_parse_sql_type(const char *type, int *size)
{
    char *endptr;
    size_t type_len;
    int accepts_size;

    type_len = strlen(type);

    for (int i = 0; i < SQL_TYPE_LAST; ++i) {

        /* Check if current type could accept size. */
        accepts_size = (type_flags[i] & FLAG_ALLOW_ARRAY);

        if ((accepts_size == 0) && (type_sql_str_len[i] != type_len)) {
            continue;
        }

        if (strncasecmp(type, type_sql_str[i], type_sql_str_len[i]) == 0) {
            /* No size specified. */
            if ((type_sql_str_len[i]) == type_len) {
                *size = 0;
                return i;
            }

            const char *ptr = type + type_sql_str_len[i];

            /* Move past whitespaces (if any). */
            while (isspace(*ptr))
                ptr++;

            if (*ptr != '(') {
                /* Malformed size. */
                return -1;
            }

            /* A size has been specified. */

            if (accepts_size == 0) {
                /* The type does not accept size. */
                return -1;
            }

            errno = 0;
            *size = strtol(ptr + 1, &endptr, 10);

            /* Correction: cstring types require an additional byte. */
            if ((type_flags[i] & FLAG_EXTRA_BYTE) != 0) {
                (*size)++;
            }

            if (errno == EINVAL || errno == ERANGE || *size < 0) {
                /* Malformed size. */
                return -1;
            }

            while (isspace(*endptr))
                endptr++;
            if (*endptr != ')') {
                /* Missing closing parenthesis */
                return -1;
            }
            return i;
        }
    }
    return -1;
}

/*
  Convert the type and length of the retrieved fields so they can be added
  to the new generated csc2.
*/
static int prepare_column_for_csc2(struct comdb2_column *column)
{
    uint8_t *type = &column->type;
    uint32_t *len = &column->len;
    int in_len;

    assert(type && len);

    in_len = *len;

    /* For most of the non-array types we do not need their size. */
    *len = 0;

    switch (*type) {
    case SERVER_UINT:
        switch (in_len) {
        case 3:
            *type = SQL_TYPE_USHORT;
            break;
        case 5:
            *type = SQL_TYPE_UINT;
            break;
        case 9:
            *type = SQL_TYPE_ULONGLONG;
            break;
        default:
            goto err;
        }
        break;
    case CLIENT_UINT:
        switch (in_len) {
        case 2:
            *type = SQL_TYPE_USHORT;
            break;
        case 4:
            *type = SQL_TYPE_UINT;
            break;
        case 8:
            *type = SQL_TYPE_ULONGLONG;
            break;
        default:
            goto err;
        }
        break;
    case SERVER_BINT:
        switch (in_len) {
        case 3:
            *type = SQL_TYPE_SHORT;
            break;
        case 5:
            *type = SQL_TYPE_INT;
            break;
        case 9:
            *type = SQL_TYPE_LONGLONG;
            break;
        default:
            goto err;
        }
        break;
    case CLIENT_INT:
        switch (in_len) {
        case 2:
            *type = SQL_TYPE_SHORT;
            break;
        case 4:
            *type = SQL_TYPE_INT;
            break;
        case 8:
            *type = SQL_TYPE_LONGLONG;
            break;
        default:
            goto err;
        }
        break;
    case SERVER_BREAL:
        in_len--;
        /* fallthrough */
    case CLIENT_REAL:
        switch (in_len) {
        case 4:
            *type = SQL_TYPE_FLOAT;
            break;
        case 8:
            *type = SQL_TYPE_DOUBLE;
            break;
        default:
            goto err;
        }
        break;
    case SERVER_BCSTR: /* fallthrough */
    case CLIENT_CSTR:
        *type = SQL_TYPE_CSTRING;
        *len = in_len;
        break;
    case SERVER_BYTEARRAY:
        *type = SQL_TYPE_BYTE;
        *len = in_len - 1;
        break;
    case CLIENT_BYTEARRAY:
        *type = SQL_TYPE_BYTE;
        *len = in_len;
        break;
    case SERVER_DATETIME: /* fallthrough */
    case CLIENT_DATETIME:
        *type = SQL_TYPE_DATETIME;
        break;
    case SERVER_INTVYM: /* fallthrough */
    case CLIENT_INTVYM:
        *type = SQL_TYPE_INTERVALYM;
        break;
    case SERVER_INTVDS: /* fallthrough */
    case CLIENT_INTVDS:
        *type = SQL_TYPE_INTERVALDS;
        break;
    case SERVER_VUTF8:
        *type = SQL_TYPE_VUTF8;
        *len = in_len - 5;
        break;
    case CLIENT_VUTF8:
        *type = SQL_TYPE_VUTF8;
        *len = in_len;
        break;
    case SERVER_DECIMAL:
        switch (in_len) {
        case 7:
            *type = SQL_TYPE_DECIMAL32;
            break;
        case 13:
            *type = SQL_TYPE_DECIMAL64;
            break;
        case 22:
            *type = SQL_TYPE_DECIMAL128;
            break;
        default:
            goto err;
        }
        break;
    case SERVER_BLOB: /* fallthrough */
    case SERVER_BLOB2:
        *type = SQL_TYPE_BLOB;
        *len = in_len - 5;
        break;
    case CLIENT_BLOB: /* fallthrough */
    case CLIENT_BLOB2:
        *type = SQL_TYPE_BLOB;
        break;
    case SERVER_DATETIMEUS: /* fallthrough */
    case CLIENT_DATETIMEUS:
        *type = SQL_TYPE_DATETIMEUS;
        break;
    case SERVER_INTVDSUS: /* fallthrough */
    case CLIENT_INTVDSUS:
        *type = SQL_TYPE_INTERVALDSUS;
        break;
    default:
        goto err;
    }
    return 0;
err:
    logmsg(LOGMSG_ERROR,
           "%s:%d Invalid/unhandled type encountered (type: %d, "
           "len: %d). Please report a bug.\n",
           __FILE__, __LINE__, *type, in_len);
    return 1;
}

static void csc2_append_fkey_cons(struct strbuf *csc2,
                                  struct comdb2_constraint *constraint)
{
    if (constraint->name)
        strbuf_appendf(csc2, "\"%s\" = ", constraint->name);

    strbuf_appendf(csc2, "\"%s\" -> ", constraint->child->name);
    strbuf_appendf(csc2, "<\"%s\":\"%s\"> ", constraint->parent_table,
                   constraint->parent_key);

    if ((constraint->flags & CONS_UPD_CASCADE) != 0) {
        strbuf_append(csc2, "on update cascade ");
    }
    if ((constraint->flags & CONS_DEL_CASCADE) != 0) {
        strbuf_append(csc2, "on delete cascade ");
    }
    if ((constraint->flags & CONS_DEL_SETNULL) != 0) {
        strbuf_append(csc2, "on delete set null ");
    }
}

static void csc2_append_check_cons(struct strbuf *csc2,
                                   struct comdb2_constraint *constraint)
{
    strbuf_append(csc2, "check ");
    if (constraint->name) {
        strbuf_appendf(csc2, "\"%s\" = ", constraint->name);
    }
    strbuf_appendf(csc2, "{where %s}", constraint->check_expr);
}

/*
  Format the table information into a CSC2 string.
*/
static char *format_csc2(struct comdb2_ddl_context *ctx)
{
    char *str;
    /* Buffer to store CSC2 representation */
    struct strbuf *csc2;
    struct comdb2_column *column;
    struct comdb2_key *key;
    struct comdb2_constraint *constraint;
    int nkeys = 0;
    int nconstraints = 0;

    csc2 = strbuf_new();

    /* Schema (columns) section */
    strbuf_append(csc2, "schema\n\t{");
    LISTC_FOR_EACH(&ctx->schema->column_list, column, lnk)
    {
        if (column->flags & COLUMN_DELETED)
            continue;

        /* Append type and name */
        strbuf_appendf(csc2, "\n\t\t%s ", type_comdb2_str[column->type]);
        strbuf_appendf(csc2, "%s", column->name);
        if (column->len > 0) {
            strbuf_appendf(csc2, "[%d] ", column->len);
        } else {
            strbuf_append(csc2, " ");
        }

        /* Append default. The default is always a null-terminated string. */
        if (column->def) {
            assert(column->type < SQL_TYPE_LAST);

            /*
              Check whether the default value needs to be quoted. Note: CSC2
              does not allow single quoted value.
            */
            if (*column->def == '(') {
                /* turn DEFAULT(funct()) into dbstore={func()} */
                int len = strlen(column->def);
                assert(column->def[len - 1] == ')');
                strbuf_appendf(csc2, "dbstore = {%.*s} ", len - 2, column->def + 1);
            } else if ((type_flags[column->type] & FLAG_QUOTE_DEFAULT) != 0) {
                strbuf_appendf(csc2, "dbstore = \"%s\" ", column->def);
            } else {
                strbuf_appendf(csc2, "dbstore = %s ", column->def);
            }
        }

        if (column->convopts.dbpad > 0) {
            strbuf_appendf(csc2, "dbpad = %d ", column->convopts.dbpad);
        }

        /* No need to print 'null = no'. That's implicit. */
        if ((column->flags & COLUMN_NO_NULL) == 0) {
            strbuf_append(csc2, "null = yes ");
        }
    }
    strbuf_append(csc2, "\n\t}\n");

    /* Keys section */
    LISTC_FOR_EACH(&ctx->schema->key_list, key, lnk)
    {
        if (key->flags & KEY_DELETED)
            continue;

        ++nkeys;

        /* Opening keys section. */
        if (nkeys == 1)
            strbuf_append(csc2, "keys\n\t{");

        strbuf_append(csc2, "\n\t\t");

        if ((key->flags & KEY_DUP) != 0) {
            strbuf_append(csc2, "dup ");
        }

        if ((key->flags & (KEY_DATACOPY | KEY_PARTIALDATACOPY)) != 0) {
            strbuf_append(csc2, "datacopy ");

            if (key->flags & KEY_PARTIALDATACOPY) {
                int added = 0;
                struct comdb2_partial_datacopy_field *partial_datacopy_field;

                strbuf_append(csc2, "(");
                LISTC_FOR_EACH(&key->partial_datacopy_list, partial_datacopy_field, lnk)
                {
                    if (added > 0) {
                        strbuf_append(csc2, ", ");
                    }

                    strbuf_append(csc2, partial_datacopy_field->name);

                    added++;
                }
                strbuf_append(csc2, ") ");
            }
        }

        if ((key->flags & KEY_UNIQNULLS) != 0) {
            strbuf_append(csc2, "uniqnulls ");
        }

        strbuf_appendf(csc2, "\"%s\" = ", key->name);

        int added = 0;
        struct comdb2_index_part *idx_part;
        LISTC_FOR_EACH(&key->idx_col_list, idx_part, lnk)
        {
            if (added > 0) {
                strbuf_append(csc2, "+ ");
            }

            /* Expression index parts do not have column reference. */
            assert(((idx_part->flags & INDEX_IS_EXPR) != 0) ||
                   ((idx_part->column->flags & COLUMN_DELETED) == 0));

            strbuf_appendf(csc2, "%s%s ",
                           (idx_part->flags & INDEX_ORDER_DESC) ? "<DESCEND> "
                                                                : "",
                           idx_part->name);

            added++;
        }

        if (key->where != 0) {
            strbuf_appendf(csc2, "{ %s } ", key->where);
        }
    }

    /* Closing keys section. */
    if (nkeys)
        strbuf_append(csc2, "\n\t}\n");

    /* Constraints section */
    LISTC_FOR_EACH(&ctx->schema->constraint_list, constraint, lnk)
    {
        if (constraint->flags & CONS_DELETED)
            continue;

        ++nconstraints;

        /* Opening constraints section. */
        if (nconstraints == 1)
            strbuf_append(csc2, "constraints\n\t{");

        strbuf_append(csc2, "\n\t\t");

        if (constraint->type == CONS_FKEY) {
            csc2_append_fkey_cons(csc2, constraint);
        } else if (constraint->type == CONS_CHECK) {
            csc2_append_check_cons(csc2, constraint);
        }
    }

    /* Closing constraints section. */
    if (nconstraints)
        strbuf_append(csc2, "\n\t}\n");

    /* Tags */
    struct comdb2_schema *tag;
    LISTC_FOR_EACH(&ctx->tag_list, tag, lnk)
    {
        strbuf_appendf(csc2, "tag \"%s\"\n\t{", tag->name);

        column = 0;
        LISTC_FOR_EACH(&tag->column_list, column, lnk)
        {
            /* Append type and name */
            strbuf_appendf(csc2, "\n\t\t%s ", type_comdb2_str[column->type]);
            strbuf_appendf(csc2, "%s", column->name);
            if (column->len > 0) {
                strbuf_appendf(csc2, "[%d] ", column->len);
            } else {
                strbuf_append(csc2, " ");
            }
        }
        strbuf_append(csc2, "\n\t}\n");
    }

    str = strdup((char *)strbuf_buf(csc2));
    strbuf_free(csc2);

    logmsg(LOGMSG_DEBUG, "CSC2: %s\n", str);

    return str;
}

#define COMDB2_PK "COMDB2_PK"
#define GEN_KEY_PREFIX "KEY"
#define GEN_CONS_PREFIX "CONSTRAINT"

/* Generate a key name for the specified key. */
static int gen_key_name(struct comdb2_key *key, const char *table, char *out,
                        size_t out_size)
{
    struct comdb2_index_part *idx_part;
    char buf[16 * 1024];
    int pos = 0;
    unsigned long crc;

    /* Table name */
    SNPRINTF(buf, sizeof(buf), pos, "%s", table)

    /* DATACOPY/PARTIALDATACOPY */
    if (key->flags & (KEY_DATACOPY | KEY_PARTIALDATACOPY)) {
        SNPRINTF(buf, sizeof(buf), pos, "%s", "DATACOPY")

        if (key->flags & KEY_PARTIALDATACOPY) {
            int added = 0;
            struct comdb2_partial_datacopy_field *partial_datacopy_field;

            SNPRINTF(buf, sizeof(buf), pos, "%s", "(")
            LISTC_FOR_EACH(&key->partial_datacopy_list, partial_datacopy_field, lnk)
            {
                if (added > 0) {
                    SNPRINTF(buf, sizeof(buf), pos, "%s", ", ")
                }

                SNPRINTF(buf, sizeof(buf), pos, "%s", partial_datacopy_field->name)

                added++;
            }
            SNPRINTF(buf, sizeof(buf), pos, "%s", ")")
        }
    }

    /* DUP */
    if (key->flags & KEY_DUP)
        SNPRINTF(buf, sizeof(buf), pos, "%s", "DUP")

    /* RECNUM */
    if (key->flags & KEY_RECNUM)
        SNPRINTF(buf, sizeof(buf), pos, "%s", "RECNUM")

    /* UNIQNULLS */
    if (key->flags & KEY_UNIQNULLS)
        SNPRINTF(buf, sizeof(buf), pos, "%s", "UNIQNULLS")

    LISTC_FOR_EACH(&key->idx_col_list, idx_part, lnk)
    {
        assert(((idx_part->flags & INDEX_IS_EXPR) != 0) ||
               ((idx_part->column->flags & COLUMN_DELETED) == 0));
        SNPRINTF(buf, sizeof(buf), pos, "%s", idx_part->name)

        if (idx_part->flags & INDEX_ORDER_DESC)
            SNPRINTF(buf, sizeof(buf), pos, "%s", "DESC")
    }

done:
    crc = crc32(0, (unsigned char *)buf, pos);

    snprintf(out, out_size, "$%s_%X", GEN_KEY_PREFIX, (unsigned int)crc);

    return 0;
}

/*
  Generate the constraint name using crc32.

  The 'in' buffer contains the following information to ensure the
  uniqueness of the constraint name, thus generated.
      - child key (columns and respective sort orders)
      - parent table name, and
      - parent key (columns and respective sort orders)
*/
static int gen_constraint_name_int(char *in, size_t in_size, char *out,
                                   size_t out_size)
{
    unsigned long crc;
    crc = crc32(0, (unsigned char *)in, in_size);

    snprintf(out, out_size, "$%s_%X", GEN_CONS_PREFIX, (unsigned int)crc);
    return 0;
}

static int serialize_check_attributes(const char *check_expr, char *buf,
                                      size_t buf_sz)
{
    int pos = 0;
    /* CHECK expression */
    SNPRINTF(buf, buf_sz, pos, "%s", check_expr);

done:
    return pos;
}

/* Serialize the details of the constraint into the specified buffer.
 * NOTE: There's a sister function below 'serialize_fk_attributes2' that does
 * the same this, but out of a different structure (struct comdb2_constraint).
 */
static int serialize_fk_attributes(constraint_t *pConstraint, int parent_idx,
                                   char *buf, size_t buf_sz)
{
    struct dbtable *table;
    struct schema *key;
    int pos = 0;
#ifndef NDEBUG
    int found = 0;
#endif

    /* Child key columns and sort orders */
    for (int i = 0; i < pConstraint->lcltable->schema->nix; i++) {
        if (strcasecmp(pConstraint->lclkeyname,
                       pConstraint->lcltable->schema->ix[i]->csctag) == 0) {
#ifndef NDEBUG
            found = 1;
#endif
            key = pConstraint->lcltable->schema->ix[i];

            for (int j = 0; j < key->nmembers; j++) {
                /* Column name */
                SNPRINTF(buf, buf_sz, pos, "%s", key->member[j].name)

                /* Sort order */
                if (key->member[j].flags & INDEX_DESCEND)
                    SNPRINTF(buf, buf_sz, pos, "%s", "DESC")
            }
            break;
        }
    }
#ifndef NDEBUG
    assert(found == 1);
#endif

    /* Parent table name */
    SNPRINTF(buf, buf_sz, pos, "%s", pConstraint->table[parent_idx])

    /* Get the parent table */
    table = get_dbtable_by_name(pConstraint->table[parent_idx]);

    /* There must be a valid referenced table. */
    assert(table);

    /* Parent key columns and sort orders */
#ifndef NDEBUG
    found = 0;
#endif
    for (int i = 0; i < table->schema->nix; i++) {
        if (strcasecmp(pConstraint->keynm[parent_idx],
                       table->schema->ix[i]->csctag) == 0) {
#ifndef NDEBUG
            found = 1;
#endif
            key = table->schema->ix[i];

            for (int j = 0; j < key->nmembers; j++) {
                /* Column name */
                SNPRINTF(buf, buf_sz, pos, "%s", key->member[j].name)

                /* Sort order */
                if (key->member[j].flags & INDEX_DESCEND)
                    SNPRINTF(buf, buf_sz, pos, "%s", "DESC")
            }
            break;
        }
    }
#ifndef NDEBUG
    assert(found == 1);
#endif

done:
    return pos;
}

static int serialize_fk_attributes2(struct comdb2_constraint *constraint,
                                    char *buf, size_t buf_sz)
{
    int pos = 0;
    struct comdb2_index_part *idx_part;

    /* Child key columns and sort orders */
    LISTC_FOR_EACH(&constraint->child_idx_col_list, idx_part, lnk)
    {
        /* Column name */
        SNPRINTF(buf, buf_sz, pos, "%s", idx_part->name)

        /* Sort order */
        if (idx_part->flags & INDEX_ORDER_DESC)
            SNPRINTF(buf, buf_sz, pos, "%s", "DESC")
    }

    /* Parent table name */
    SNPRINTF(buf, buf_sz, pos, "%s", constraint->parent_table)

    /* Parent key columns and sort orders */
    LISTC_FOR_EACH(&constraint->parent_idx_col_list, idx_part, lnk)
    {
        /* Column name */
        SNPRINTF(buf, buf_sz, pos, "%s", idx_part->name)

        /* Sort order */
        if (idx_part->flags & INDEX_ORDER_DESC)
            SNPRINTF(buf, buf_sz, pos, "%s", "DESC")
    }

done:
    return pos;
}

int gen_fk_constraint_name(constraint_t *pConstraint, int parent_idx, char *out,
                           size_t out_size)
{
    char buf[3 * 1024];
    char *ptr = (char *)buf;
    int end;

    end = serialize_fk_attributes(pConstraint, parent_idx, ptr, sizeof(buf));
    gen_constraint_name_int(buf, end, out, out_size);

    return 0;
}

int gen_check_constraint_name(check_constraint_t *pConstraint, char *out,
                              size_t out_size)
{
    char buf[3 * 1024];
    char *ptr = (char *)buf;
    int end;

    end = serialize_check_attributes(pConstraint->expr, ptr, sizeof(buf));
    gen_constraint_name_int(buf, end, out, out_size);

    return 0;
}

static int gen_constraint_name(struct comdb2_constraint *constraint, char *out,
                               size_t out_size)
{
    char buf[3 * 1024];
    char *ptr = (char *)buf;
    int end;

    if (constraint->type == CONS_CHECK) {
        end = serialize_check_attributes(constraint->check_expr, ptr,
                                         sizeof(buf));
    } else {
        end = serialize_fk_attributes2(constraint, ptr, sizeof(buf));
    }
    gen_constraint_name_int(buf, end, out, out_size);

    return 0;
}

static int is_pk(const char *key)
{
    return (((strncasecmp(key, COMDB2_PK, sizeof(COMDB2_PK) - 1)) == 0) ? 1
                                                                        : 0);
}

static struct comdb2_key *find_idx_by_name(struct comdb2_ddl_context *ctx,
                                           const char *key_name)
{
    struct comdb2_key *key;

    LISTC_FOR_EACH(&ctx->schema->key_list, key, lnk)
    {
        /* Ignore the dropped key(s). */
        if (key->flags & KEY_DELETED)
            continue;

        if ((strcasecmp(key->name, key_name) == 0)) {
            return key;
        }
    }
    return 0;
}

static struct comdb2_column *find_column_by_name(struct comdb2_ddl_context *ctx,
                                                 const char *column_name)
{
    struct comdb2_column *column;

    LISTC_FOR_EACH(&ctx->schema->column_list, column, lnk)
    {
        /* Ignore the dropped column(s). */
        if (column->flags & COLUMN_DELETED)
            continue;

        if ((strcasecmp(column->name, column_name) == 0)) {
            return column;
        }
    }
    return 0;
}

static struct comdb2_key *find_suitable_key(comdb2_index_part_lst *idx_col_list,
                                            comdb2_key_lst *key_list)
{
    struct comdb2_key *current_key;
    struct comdb2_key *matched_key = NULL;
    struct comdb2_index_part *current_idx_part;
    struct comdb2_index_part *idx_part;
    int matched;

    LISTC_FOR_EACH(key_list, current_key, lnk)
    {
        if ((current_key->flags & KEY_DELETED) ||
            (listc_size(idx_col_list) > listc_size(&current_key->idx_col_list)))
            continue;

        /* Let's start by assuming that we have found the matching key. */
        matched = 1;

        current_idx_part = LISTC_TOP(&current_key->idx_col_list);

        LISTC_FOR_EACH(idx_col_list, idx_part, lnk)
        {
            if (strcasecmp(idx_part->name, current_idx_part->name) != 0) {
                matched = 0;
            }
            /* Move to the next index column in the key. */
            current_idx_part = LISTC_NEXT(current_idx_part, lnk);
        }
        if (matched) {
            /* Prefer the smaller of the matched keys. */
            if (matched_key &&
                (listc_size(matched_key) > listc_size(current_key))) {
                matched_key = current_key;
            } else {
                matched_key = current_key;
            }
        }
    }
    return matched_key;
}

static char *prepare_csc2(Parse *pParse, struct comdb2_ddl_context *ctx)
{
    char *csc2;
    struct comdb2_column *column;
    struct comdb2_index_part *child_idx_part;
    struct comdb2_key *key;
    struct comdb2_constraint *constraint;

    int ncolumns = 0;
    LISTC_FOR_EACH(&ctx->schema->column_list, column, lnk)
    {
        if (column->flags & COLUMN_DELETED)
            continue;
        ncolumns++;
    }

    /* Check whether there is at least one column. */
    if (ncolumns == 0) {
        setError(pParse, SQLITE_ERROR, "Table must have at least one column.");
        goto cleanup;
    }

    /*
      Check the properties of PRIMARY KEYs:
      * Unique
      * Columns must not allow NULLs
      * Must be only one per table
      * Check that datacopy and partial datacopy are both not set
      * Check that partial datacopy columns are valid
    */
    int pk_count = 0;
    LISTC_FOR_EACH(&ctx->schema->key_list, key, lnk)
    {
        if (key->flags & KEY_DELETED)
            continue;

        if (is_pk(key->name)) {
            if (++pk_count > 1) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "Multiple primary key definitions.");
                goto cleanup;
            }

            /* Primary keys mustn't be dup. */
            if (key->flags & KEY_DUP) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "A primary key must be UNIQUE.");
                goto cleanup;
            }

            /* Also make sure none of its columns allow NULLs. (n^2) */
            struct comdb2_index_part *idx_part;
            LISTC_FOR_EACH(&key->idx_col_list, idx_part, lnk)
            {
                /* There must not be a dropped column in the key. */
                assert((idx_part->column->flags & COLUMN_DELETED) == 0);
                if ((idx_part->column->flags & COLUMN_NO_NULL) == 0) {
                    pParse->rc = SQLITE_ERROR;
                    sqlite3ErrorMsg(pParse, "A primary key column must be "
                                            "NOT NULL.");
                    goto cleanup;
                }
            }
        }

        if (key->flags & KEY_PARTIALDATACOPY) {
            /* Make sure datacopy and partial datacopy are not set */
            if (key->flags & KEY_DATACOPY) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "Cannot have datacopy and partial datacopy.");
                goto cleanup;
            }

            /* Make sure all partial datacopy fields are valid */
            struct comdb2_partial_datacopy_field *partial_datacopy_field;
            LISTC_FOR_EACH(&key->partial_datacopy_list, partial_datacopy_field, lnk)
            {
                if (find_column_by_name(ctx, partial_datacopy_field->name) == 0) {
                    pParse->rc = SQLITE_ERROR;
                    sqlite3ErrorMsg(pParse, "Invalid partial datacopy field \"%s\".", partial_datacopy_field->name);
                    goto cleanup;
                }
            }

        }
    }

    /*
      Choose/assign an appropriate (non-dropped) child key for each constraint
      that does not have one.
    */
    LISTC_FOR_EACH(&ctx->schema->constraint_list, constraint, lnk)
    {
        /* Check whether the constraint has been dropped. */
        if (constraint->flags & CONS_DELETED)
            continue;

        /* Skip for CHECK constraints. */
        if (constraint->type == CONS_CHECK)
            continue;

        /* Check if there's already a child key */
        if (constraint->child)
            continue;

        /* The parent table and key must have already been set by now. */
        assert(constraint->parent_table && constraint->parent_key);

        constraint->child = find_suitable_key(&constraint->child_idx_col_list,
                                              &ctx->schema->key_list);

        /*
          Implicitly add a new DUP key if a matching local index was not found.
        */
        if (constraint->child == 0) {
            ExprList *pList = 0;
            int i = 0;

            LISTC_FOR_EACH(&constraint->child_idx_col_list, child_idx_part,
                           lnk)
            {
                Token x;

                x.z = child_idx_part->name;
                x.n = strlen(child_idx_part->name);

                Expr *pExpr = sqlite3ExprAlloc(pParse->db, TK_ID, &x, 0);
                if (pExpr == 0) goto oom;

                pList = sqlite3ExprListAppend(pParse, pList, pExpr);
                if (pList == 0) goto oom;

                sqlite3ExprListSetName(pParse, pList, &x, 0);
                if( pParse->db->mallocFailed ) goto oom;

                pList->a[i].pExpr->op = TK_ID;
                pList->a[i].pExpr->u.zToken = child_idx_part->name;
                pList->a[i].zName = child_idx_part->name;
                if (child_idx_part->flags & INDEX_ORDER_DESC) {
                    pList->a[i].sortOrder = SQLITE_SO_DESC;
                } else {
                    pList->a[i].sortOrder = SQLITE_SO_ASC;
                }

                i++;
            }

            comdb2AddIndex(pParse, 0 /* Key name will be generated */,
                           pList, 0, 0, 0, 0, 0, SQLITE_IDXTYPE_DUPKEY, 0, NULL);
            if (pParse->rc)
                goto cleanup;

            constraint->child =
                (struct comdb2_key *)LISTC_BOT(&ctx->schema->key_list);
        }
    }

    /* Generate CSC2 for the new/existing table. */
    csc2 = format_csc2(ctx);

    /* save context to client */
    if (comdb2_save_ddl_context(ctx->schema->name, ctx, ctx->mem) != 0) {
        /* We get here if we are not in client transaction or it failed to save
         */
        /*
          Now that we have the generated csc2 in a separate buffer, it
          is safe to teardown the parser context. The csc2 buffer will
          be reclaimed later in free_schema_change_type().
        */
        free_ddl_context(pParse);
    }

    return csc2;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return 0;
}

static int retrieve_table_options(struct dbtable *table)
{
    int table_options = 0;
    int odh;
    int inplace_updates;
    int instant_schema_change;
    int compr;
    int compr_blobs;

    get_db_odh(table, &odh);
    get_db_inplace_updates(table, &inplace_updates);
    get_db_instant_schema_change(table, &instant_schema_change);
    get_db_compress(table, &compr);
    get_db_compress_blobs(table, &compr_blobs);

    switch (odh) {
    case 0: table_options |= ODH_OFF; break;
    case 1: table_options |= ODH_ON; break;
    default: assert(0);
    }

    switch (inplace_updates) {
    case 0: table_options |= IPU_OFF; break;
    case 1: table_options |= IPU_ON; break;
    default: assert(0);
    }

    switch (instant_schema_change) {
    case 0: table_options |= ISC_OFF; break;
    case 1: table_options |= ISC_ON; break;
    default: assert(0);
    }

    switch (compr) {
    case BDB_COMPRESS_RLE8: table_options |= REC_RLE; break;
    case BDB_COMPRESS_CRLE: table_options |= REC_CRLE; break;
    case BDB_COMPRESS_ZLIB: table_options |= REC_ZLIB; break;
    case BDB_COMPRESS_LZ4: table_options |= REC_LZ4; break;
    case BDB_COMPRESS_NONE: table_options |= REC_NONE; break;
    default: assert(0);
    }

    switch (compr_blobs) {
    case BDB_COMPRESS_RLE8: table_options |= BLOB_RLE; break;
    case BDB_COMPRESS_CRLE: table_options |= BLOB_CRLE; break;
    case BDB_COMPRESS_ZLIB: table_options |= BLOB_ZLIB; break;
    case BDB_COMPRESS_LZ4: table_options |= BLOB_LZ4; break;
    case BDB_COMPRESS_NONE: table_options |= BLOB_NONE; break;
    default: assert(0);
    }

    return table_options;
}

/* Retrieve table columns */
static int retrieve_columns(Parse *pParse, struct comdb2_ddl_context *ctx,
                            struct schema *src_schema,
                            struct comdb2_schema *dst_schema)
{
    struct comdb2_column *column;
    char *def_str;

    for (int i = 0; i < src_schema->nmembers; i++) {
        column = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_column));
        if (column == 0)
            goto oom;

        /* Name */
        column->name = comdb2_strdup(ctx->mem, src_schema->member[i].name);
        if (column->name == 0)
            goto oom;

        /* Convert the default value to string. */
        if (src_schema->member[i].in_default) {
            def_str = sql_field_default_trans(&src_schema->member[i], 0);
            /* Remove the quotes around the default value (if any). */
            sqlite3Dequote(def_str);

            column->def = comdb2_strdup(ctx->mem, def_str);
            sqlite3_free(def_str);
        }

        /* Type */
        column->type = src_schema->member[i].type;

        /* Length */
        column->len = src_schema->member[i].len;

        /* Flags */
        if (src_schema->member[i].flags & NO_NULL) {
            column->flags |= COLUMN_NO_NULL;
        }

        /* Copy column_conv_opts */
        column->convopts = src_schema->member[i].convopts;

        /* Convert type and length */
        prepare_column_for_csc2(column);

        /* Add it to the list */
        listc_abl(&dst_schema->column_list, column);
    }
    return 0;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");
    return 1;
}

static void escape_expr(struct strbuf *out, const char *expr)
{
    while (*expr) {
        if (*expr == '"') {
            strbuf_append(out, "\\\"");
        } else {
            strbuf_appendf(out, "%c", *expr);
        }
        ++expr;
    }
}

static int retrieve_check_constraint(Parse *pParse,
                                     struct comdb2_ddl_context *ctx,
                                     check_constraint_t *cons)
{
    struct comdb2_constraint *constraint;

    constraint = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_constraint));
    if (constraint == 0)
        goto oom;

    /* Type */
    constraint->type = CONS_CHECK;

    /* CHECK expression */
    constraint->check_expr = comdb2_strdup(ctx->mem, cons->expr);
    if (constraint->check_expr == 0)
        goto oom;

    /* TODO: (NC) escape quotes? */

    /* Name */
    assert(cons->consname);
    constraint->name = comdb2_strdup(ctx->mem, cons->consname);
    if (constraint->name == 0)
        goto oom;

    listc_abl(&ctx->schema->constraint_list, constraint);
    return 0;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");
    return 2;
}

static int retrieve_fk_constraint(Parse *pParse, struct comdb2_ddl_context *ctx,
                                  constraint_t *cons)
{
    struct comdb2_constraint *constraint;
    struct comdb2_index_part *idx_part;
    struct comdb2_key *child_key;
    struct dbtable *parent_table;
    struct schema *parent_schema;
    struct comdb2_key *current;
    int key_found = 0;

    /* Locate the child key. */
    LISTC_FOR_EACH(&ctx->schema->key_list, current, lnk)
    {
        if (strcasecmp(cons->lclkeyname, current->name) == 0) {
            child_key = current;
            key_found = 1;
            break;
        }
    }

    if (key_found == 0) {
        setError(pParse, SQLITE_ERROR,
                 "FK: Local key used in the foreign key constraint could not "
                 "be found.");
        goto err;
    }

    /* Locate the parent key. */
    for (int i = 0; i < cons->nrules; i++) {
        parent_schema = 0;
        parent_table = get_dbtable_by_name(cons->table[i]);
        if (parent_table == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "FK: Parent table '%s' not found.",
                            cons->table[i]);
            goto err;
        }

        for (int j = 0; j < parent_table->schema->nix; j++) {
            if (strcasecmp(parent_table->schema->ix[j]->csctag,
                           cons->keynm[i]) == 0) {
                parent_schema = parent_table->schema->ix[j];
            }
        }
        if (parent_schema == 0) {
            setError(pParse, SQLITE_ERROR,
                     "FK: Referenced key used in the foreign key constraint "
                     "could not be found.");
            goto err;
        }

        constraint =
            comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_constraint));
        if (constraint == 0)
            goto oom;

        /* Type */
        constraint->type = CONS_FKEY;

        /* Initialize the lists. */
        listc_init(&constraint->child_idx_col_list,
                   offsetof(struct comdb2_index_part, lnk));
        listc_init(&constraint->parent_idx_col_list,
                   offsetof(struct comdb2_index_part, lnk));

        /* Add child index columns. */
        struct comdb2_index_part *current;
        LISTC_FOR_EACH(&child_key->idx_col_list, current, lnk)
        {
            idx_part =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_part));
            if (idx_part == 0)
                goto oom;

            idx_part->name = current->name;
            idx_part->flags = current->flags;
            idx_part->column = current->column;

            listc_abl(&constraint->child_idx_col_list, idx_part);
        }

        /* Add parent index columns. */
        for (int j = 0; j < parent_schema->nmembers; j++) {
            idx_part =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_part));
            if (idx_part == 0)
                goto oom;

            idx_part->name =
                comdb2_strdup(ctx->mem, parent_schema->member[j].name);
            if (idx_part->name == 0)
                goto oom;

            if (parent_schema->member[j].flags & INDEX_DESCEND) {
                idx_part->flags |= INDEX_ORDER_DESC;
            }
            /* There's no comdb2_column for foreign columns. */
            // idx_part->column = 0;

            listc_abl(&constraint->parent_idx_col_list, idx_part);
        }

        /* Reference to the child key. */
        constraint->child = child_key;

        /* Parent table name. */
        constraint->parent_table =
            comdb2_strdup(ctx->mem, parent_table->tablename);
        if (constraint->parent_table == 0)
            goto oom;

        /* Parent key name */
        constraint->parent_key = comdb2_strdup(ctx->mem, parent_schema->csctag);
        if (constraint->parent_key == 0)
            goto oom;

        /* Flags */
        if (cons->flags & CT_UPD_CASCADE) {
            constraint->flags |= CONS_UPD_CASCADE;
        }
        if (cons->flags & CT_DEL_CASCADE) {
            constraint->flags |= CONS_DEL_CASCADE;
        }
        if (cons->flags & CT_DEL_SETNULL) {
            constraint->flags |= CONS_DEL_SETNULL;
        }

        if (cons->consname) {
            /*
              CSC2 does not allow named constraints to have multiple
              parent key references.
            */
            assert(i == 0);
            constraint->name = comdb2_strdup(ctx->mem, cons->consname);
            if (constraint->name == 0)
                goto oom;
        }
        listc_abl(&ctx->schema->constraint_list, constraint);
    }
    return 0;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

err:
    return 1;
}

/*
  Fetch the schema definition of the table being altered.
*/
static int retrieve_schema(Parse *pParse, struct comdb2_ddl_context *ctx)
{
    struct dbtable *table;
    struct schema *schema;
    struct dbtag *tag;

    assert(ctx != 0);

    if (ctx->partition_first_shardname) {
        table = get_dbtable_by_name(ctx->partition_first_shardname);
    } else {
        table = get_dbtable_by_name(ctx->schema->name);
    }

    if (table == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Table '%s' not found.", ctx->schema->name);
        goto err;
    }
    schema = table->schema;

    /* Retrieve the table options. */
    ctx->schema->table_options = retrieve_table_options(table);

    /* Retrieve table columns. */
    if (retrieve_columns(pParse, ctx, schema, ctx->schema)) {
        goto err;
    }

    /* Populate keys list */
    struct comdb2_key *key;
    for (int i = 0; i < schema->nix; i++) {
        key = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_key));
        if (key == 0)
            goto oom;

        /* Key name */
        key->name = comdb2_strdup(ctx->mem, schema->ix[i]->csctag);
        if (key->name == 0)
            goto oom;

        /* Parent table name */
        key->table = comdb2_strdup(ctx->mem, ctx->schema->name);
        if (key->table == 0)
            goto oom;

        /* Partial index expression */
        if (schema->ix[i]->where) {
            char *endptr;
            size_t where_sz;

            where_sz = strlen(schema->ix[i]->where);
            key->where =
                comdb2_strndup(ctx->mem, schema->ix[i]->where, where_sz);
            if (key->where == 0)
                goto oom;

            /* Remove trailing spaces. */
            endptr = key->where + where_sz;
            while (isspace(*(--endptr))) {
            }
            *(++endptr) = 0;
        }

        /* Key flags */
        if (schema->ix[i]->flags & SCHEMA_RECNUM) {
            key->flags |= KEY_RECNUM;
        }
        if (schema->ix[i]->flags & SCHEMA_DUP) {
            key->flags |= KEY_DUP;
        }
        if (schema->ix[i]->flags & SCHEMA_DATACOPY) {
            key->flags |= KEY_DATACOPY;
        }
        if (schema->ix[i]->flags & SCHEMA_UNIQNULLS) {
            key->flags |= KEY_UNIQNULLS;
        }

        listc_init(&key->partial_datacopy_list, offsetof(struct comdb2_partial_datacopy_field, lnk));

        if (schema->ix[i]->flags & SCHEMA_PARTIALDATACOPY) {
            key->flags |= KEY_PARTIALDATACOPY;

            struct comdb2_partial_datacopy_field *partial_datacopy_field;
            struct schema *partial_datacopy = schema->ix[i]->partial_datacopy;
            for (int j = 0; j < partial_datacopy->nmembers; j++) {
                partial_datacopy_field = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_partial_datacopy_field));
                if (partial_datacopy_field == 0) {
                    goto oom;
                }
                partial_datacopy_field->name = comdb2_strdup(ctx->mem, partial_datacopy->member[j].name);
                if (partial_datacopy_field->name == 0) {
                    goto oom;
                }
                listc_abl(&key->partial_datacopy_list, partial_datacopy_field);
            }
        }

        listc_init(&key->idx_col_list, offsetof(struct comdb2_index_part, lnk));

        struct comdb2_column *column;
        struct comdb2_index_part *idx_part;
        int idx;
        for (int j = 0; j < schema->ix[i]->nmembers; j++) {
            idx_part =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_part));
            if (idx_part == 0)
                goto oom;

            if (schema->ix[i]->member[j].isExpr) {
                struct strbuf *csc2_expr;
                struct comdb2_column expr_col;

                /* field.idx is the index of column in the table. It's -1
                 * for index on expression. */
                assert(schema->ix[i]->member[j].idx == -1);
                idx_part->flags |= INDEX_IS_EXPR;

                csc2_expr = strbuf_new();

                /* Type */
                expr_col.type = schema->ix[i]->member[j].type;
                expr_col.len =schema->ix[i]->member[j].len;

                /* Convert type and length */
                prepare_column_for_csc2(&expr_col);
                strbuf_appendf(csc2_expr, "(%s", type_comdb2_str[expr_col.type]);
                if (expr_col.len > 0) {
                    strbuf_appendf(csc2_expr, "[%d]", expr_col.len);
                }

                strbuf_append(csc2_expr, ")\"");

                /* Expression */
                escape_expr(csc2_expr, schema->ix[i]->member[j].name);

                strbuf_append(csc2_expr, "\"");

                idx_part->name =
                    comdb2_strdup(ctx->mem, (char *) strbuf_buf(csc2_expr));

                strbuf_free(csc2_expr);

                /* No need to refer to the column as we have got all required
                 * information */
                idx_part->column = 0;
            } else {
                /* Column name */
                idx_part->name = schema->ix[i]->member[j].name;

                idx = schema->ix[i]->member[j].idx;
                /* Retrieve the column at the given position. */
                LISTC_FOR_EACH(&ctx->schema->column_list, column, lnk)
                {
                    if (idx == 0)
                        break;
                    idx--;
                }

                /* Column name */
                idx_part->name = column->name;

                /* Column reference */
                idx_part->column = column;
            }

            /* Column flags */
            if (schema->ix[i]->member[j].flags & INDEX_DESCEND) {
                idx_part->flags |= INDEX_ORDER_DESC;
            }

            listc_abl(&key->idx_col_list, idx_part);
        }
        listc_abl(&ctx->schema->key_list, key);
    }

    /* Populate constraints list */
    for (int i = 0; i < table->n_constraints; i++) {
        if ((retrieve_fk_constraint(pParse, ctx, &table->constraints[i])))
            goto err;
    }
    for (int i = 0; i < table->n_check_constraints; i++) {
        if ((retrieve_check_constraint(pParse, ctx,
                                       &table->check_constraints[i])))
            goto err;
    }

    /* Fetch all user-defined tags. */
    lock_taglock();
    tag = hash_find_readonly(gbl_tag_hash, &ctx->schema->name);
    if (tag) {
        struct schema *old_tag;
        LISTC_FOR_EACH(&tag->taglist, old_tag, lnk)
        {
            /* Skip internal tags. */
            if (old_tag->tag[0] != '.' &&
                (old_tag->flags & SCHEMA_INDEX) == 0) {
                struct comdb2_schema *new_tag =
                    comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_schema));
                if (!new_tag) {
                    unlock_taglock();
                    goto oom;
                }

                new_tag->name = comdb2_strdup(ctx->mem, old_tag->tag);
                if (new_tag->name == 0) {
                    unlock_taglock();
                    goto oom;
                }

                /* Retrieve tag columns. */
                listc_init(&new_tag->column_list,
                           offsetof(struct comdb2_column, lnk));
                if (retrieve_columns(pParse, ctx, old_tag, new_tag)) {
                    unlock_taglock();
                    goto err;
                }

                /* Add it to the list */
                listc_abl(&ctx->tag_list, new_tag);
            }
        }
    }
    unlock_taglock();

    return 0;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

err:
    return 1;
}

#define use_sqlite_impl(parse) (parse->comdb2_ddl_ctx == NULL)

void comdb2AlterTableStart(
    Parse *pParse, /* Parser context */
    Token *pName1, /* First part of the name of the table. */
    Token *pName2  /* Second part of the name of the table. */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_ALTER_TABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    struct comdb2_ddl_context *ctx;

    assert(pParse->comdb2_ddl_ctx == 0);

    ctx = create_ddl_context(pParse);
    if (ctx == 0)
        goto oom;

    if ((chkAndCopyTableTokens(pParse, ctx->tablename, pName1, pName2,
                               ERROR_ON_TBL_NOT_FOUND, 1, 0,
                               &ctx->partition_first_shardname)))
        goto cleanup;

    ctx->schema->name = comdb2_strdup(ctx->mem, ctx->tablename);
    if (ctx->schema->name == 0)
        goto oom;

    /*
      Add all the columns, indexes and constraints in the table to the
      respective list.
     */
    if (retrieve_schema(pParse, ctx)) {
        goto cleanup;
    }

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

/*
  Finalize the ALTER TABLE command.
*/
void comdb2AlterTableEnd(Parse *pParse)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    Vdbe *v;
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    v = sqlite3GetVdbe(pParse);

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == 0)
        goto oom;

    memcpy(sc->tablename, ctx->tablename, MAXTABLELEN);

    sc->kind =
        ((ctx->flags & DDL_PENDING) != 0) ? SC_ALTERTABLE_PENDING : SC_ALTERTABLE;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto cleanup;
        }
    }

    fillTableOption(sc, ctx->schema->table_options);
    if(OPT_ON(ctx->schema->table_options, FORCE_SC)){
        sc->force = 1;
    }

    if (ctx->partition)
        sc->partition = *ctx->partition;

    /* prepare_csc2 can free the ctx, do not touch it afterwards ! */
    sc->newcsc2 = prepare_csc2(pParse, ctx);
    if (sc->newcsc2 == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        goto cleanup;
    }

    if (sc->dryrun)
        comdb2prepareSString(v, pParse, 0, sc, &comdb2SqlDryrunSchemaChange,
                             (vdbeFuncArgFree)&free_schema_change_type);
    else
        comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_schema_change_type(sc);
    free_ddl_context(pParse);
    return;
}

void comdb2AlterCommitPending(Parse *pParse /* Parsing context */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    ctx->flags |= DDL_PENDING;
}

void comdb2CreateTableStart(
    Parse *pParse, /* Parser context */
    Token *pName1, /* First part of the name of the table or view */
    Token *pName2, /* Second part of the name of the table or view */
    int isTemp,    /* True if this is a TEMP table */
    int isView,    /* True if this is a VIEW */
    int isVirtual, /* True if this is a VIRTUAL table */
    int noErr      /* Do nothing if table already exists */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        int code = isTemp ? SQLITE_CREATE_TEMP_TABLE : SQLITE_CREATE_TABLE;
        if( sqlite3AuthCheck(pParse, code, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    int table_exists = 0;

    if (isTemp || isView || isVirtual || pParse->db->init.busy ||
        pParse->db->isExpert || IN_SPECIAL_PARSE) {
        pParse->comdb2_ddl_ctx = 0;
        sqlite3StartTable(pParse, pName1, pName2, isTemp, isView, isVirtual,
                          noErr);
        return;
    }

    struct comdb2_ddl_context *ctx = create_ddl_context(pParse);
    if (ctx == 0)
        goto oom;

    if (chkAndCopyTableTokens(pParse, ctx->tablename, pName1, pName2,
                              (noErr) ? ERROR_IGNORE : ERROR_ON_TBL_FOUND, 1,
                              &table_exists, NULL))
        goto cleanup;

    ctx->schema->name = comdb2_strdup(ctx->mem, ctx->tablename);
    if (ctx->schema->name == 0)
        goto oom;

    if (noErr && table_exists) {
        ctx->flags |= DDL_NOOP;
        logmsg(LOGMSG_DEBUG, "Table '%s' already exists.", ctx->tablename);
        /* We'll not free the context here, as the flag's needed later. */
        goto out;
    }

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");
cleanup:
    free_ddl_context(pParse);
out:
    return;
}

void comdb2CreateTableEnd(
    Parse *pParse, /* Parse context */
    Token *pCons,  /* The ',' token after the last column defn. */
    Token *pEnd,   /* The ')' before options in the CREATE TABLE */
    u8 tabOpts,    /* Extra table options. Usually 0. */
    int comdb2Opts /* Comdb2 specific table options. */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct schema_change_type *sc = 0;
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    Vdbe *v;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3EndTable(pParse, pCons, pEnd, tabOpts, 0);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        goto cleanup;
    }
    v = sqlite3GetVdbe(pParse);

    sc = new_schemachange_type();
    if (sc == 0)
        goto oom;

    memcpy(sc->tablename, ctx->tablename, MAXTABLELEN);

    sc->kind = SC_ADDTABLE;
    sc->nothrevent = 1;
    sc->live = 1;

    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto cleanup;
        }
    }

    fillTableOption(sc, comdb2Opts);

    if (ctx->partition)
        sc->partition = *ctx->partition;

    /* prepare_csc2 can free the ctx, do not touch it afterwards ! */
    sc->newcsc2 = prepare_csc2(pParse, ctx);
    if (sc->newcsc2 == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        goto cleanup;
    }

    if (sc->dryrun)
        comdb2prepareSString(v, pParse, 0, sc, &comdb2SqlDryrunSchemaChange,
                             (vdbeFuncArgFree)&free_schema_change_type);
    else
        comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");
cleanup:
    free_schema_change_type(sc);
    free_ddl_context(pParse);
    return;
}

void comdb2CreateTableLikeEnd(
    Parse *pParse, /* Parse context */
    Token *pName1, /* First part of the name of the table */
    Token *pName2  /* Second part of the name of the table */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    char *newTab;
    char *otherTab;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    assert(!use_sqlite_impl(pParse));

    if (isRemote(pParse, &pName1, &pName2)) {
        return;
    }

    otherTab = comdb2_strndup(ctx->mem, pName1->z, pName1->n);
    if (otherTab == 0)
        goto oom;
    sqlite3Dequote(otherTab);

    /* Retrieve the schema of the existing table. */
    newTab = ctx->schema->name;
    ctx->schema->name = otherTab;
    if (retrieve_schema(pParse, ctx)) {
        goto cleanup;
    }
    ctx->schema->name = newTab;

    comdb2CreateTableEnd(pParse, 0, 0, 0, ctx->schema->table_options);

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

void comdb2AddColumn(Parse *pParse, /* Parser context */
                     Token *pName,  /* Name of the column */
                     Token *pType   /* Type of the column */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_column *column;
    struct comdb2_column *current;
    char type[pType->n + 1];
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    int rc;
    int column_exists;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        // TODO: BAD ASSERT: if ((pParse->pNewTable) == 0) assert(0);
        sqlite3AddColumn(pParse, pName, pType);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    /* Allocate a new column. */
    column = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_column));
    if (column == 0)
        goto oom;

    /* Column name */
    column->name = comdb2_strndup(ctx->mem, pName->z, pName->n);
    if (column->name == 0)
        goto oom;
    sqlite3Dequote(column->name);

    /* Column type */
    if (pType->n == 0) {
        setError(pParse, SQLITE_MISUSE, "No type specified.");
        goto cleanup;
    }
    strncpy0(type, pType->z, sizeof(type));
    sqlite3Dequote(type);

    if ((rc = comdb2_parse_sql_type(type, (int *)&column->len)) == -1) {
        setError(pParse, SQLITE_MISUSE, "Invalid type specified.");
        goto cleanup;
    }
    column->type = (uint8_t)rc;

    column_exists = 0;
    LISTC_FOR_EACH(&ctx->schema->column_list, current, lnk)
    {
        if ((strcasecmp(column->name, current->name) == 0) &&
            ((current->flags & COLUMN_DELETED) == 0)) {
            column_exists = 1;
            break;
        }
    }

    if (column_exists == 1) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Duplicate column name '%s'.", current->name);
        goto cleanup;
    } else {
        /* We cannot allow dropping and adding the same column in the same
         * ALTER TABLE command as that could potentially switch the position
         * of the column in the resulting csc2 schema. Which, if allowed,
         * would cause the csc2 system to simply "re-position the existing
         * column along with all its data" - not something user actually
         * asked for.
         */
        LISTC_FOR_EACH_REVERSE(&ctx->schema->column_list, current, lnk)
        {
            if ((strcasecmp(column->name, current->name) == 0) &&
                ((current->flags & COLUMN_DELETED) != 0)) {
                sqlite3ErrorMsg(
                    pParse,
                    "Cannot DROP and ADD same column in an ALTER command.");
                pParse->rc = SQLITE_MISUSE;
                goto cleanup;
            }
        }
    }

    listc_abl(&ctx->schema->column_list, column);

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

static void comdb2ColumnSetDefault(
    Parse *pParse,                /* Parsing context */
    struct comdb2_column *column, /* Set the default value of this column */
    Expr *pExpr,        /* The parsed expression of the default value */
    const char *zStart, /* Start of the default value text */
    const char *zEnd    /* First character past end of defaut value text */
)
{
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    char *def;
    int def_len;

    /* need to remove space at the end: zStart is beggining of next word */
    while (zEnd - 1 != zStart && *(zEnd - 1) == ' ')
        --zEnd;

    /* Add DEFAULT to the specified column. */
    def_len = zEnd - zStart;
    assert(def_len > 0);

    def = comdb2_strndup(ctx->mem, zStart, def_len);
    if (def == 0)
        goto oom;
    /* Remove the quotes around the default value (if any). */
    sqlite3Dequote(def);
    column->def = def;

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

    free_ddl_context(pParse);
    return;
}

void comdb2AddDefaultValue(
    Parse *pParse,      /* Parsing context */
    Expr *pExpr,        /* The parsed expression of the default value */
    const char *zStart, /* Start of the default value text */
    const char *zEnd    /* First character past end of defaut value text */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_column *column;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3AddDefaultValue(pParse, pExpr, zStart, zEnd);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    /* Add DEFAULT to the last added column. */
    column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);
    comdb2ColumnSetDefault(pParse, column, pExpr, zStart, zEnd);
    return;
}

static void comdb2ColumnSetNull(Parse *pParse, struct comdb2_column *column)
{
    /* Clear the COLUMN_NO_NULL bit. */
    column->flags &= ~COLUMN_NO_NULL;
    return;
}

/*
 * Set autoincrement flag 
 */
void comdb2SetAutoIncrement(Parse *pParse)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_column *column;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);

    if (( validAutoIncrementColumnCheck(column) )) {
        setError(pParse, SQLITE_MISUSE, COMDB2_INVALID_AUTOINCREMENT);
        return;
    }

    char *nextsequence="nextsequence";
    comdb2ColumnSetDefault(pParse, column, NULL, nextsequence, nextsequence + 12);
    return;
}

/*
  Allow NULLs for the last added column.
*/
void comdb2AddNull(Parse *pParse)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_column *column;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);
    comdb2ColumnSetNull(pParse, column);

    return;
}

static void comdb2ColumnSetNotNull(Parse *pParse, struct comdb2_column *column)
{
    /* Set the COLUMN_NO_NULL bit. */
    column->flags |= COLUMN_NO_NULL;
    return;
}

/*
  Disallow NULLs for the last added column.
*/
void comdb2AddNotNull(Parse *pParse, int onError)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_column *column;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3AddNotNull(pParse, onError);
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);
    comdb2ColumnSetNotNull(pParse, column);

    return;
}

void comdb2AddDbpad(Parse *pParse, int dbpad)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_column *column;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }
    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);
    column->convopts.dbpad = dbpad;

    return;
}

static struct comdb2_constraint *
find_cons_by_name(struct comdb2_ddl_context *ctx, const char *cons, int type)
{
    struct comdb2_constraint *constraint;
    char *constraint_name;
    char constraint_name_buf[MAXGENCONSLEN + 1];

    LISTC_FOR_EACH(&ctx->schema->constraint_list, constraint, lnk)
    {
        /* Ignore the dropped constraints. */
        if (constraint->flags & CONS_DELETED)
            continue;

        if ((constraint->type & type) == 0)
            continue;

        if (constraint->name == 0) {
            gen_constraint_name(constraint, constraint_name_buf,
                                sizeof(constraint_name_buf));
            constraint_name = constraint_name_buf;
        } else {
            constraint_name = constraint->name;
        }

        if ((strcasecmp(cons, constraint_name)) == 0) {
            return constraint;
        }
    }
    return 0;
}

static void comdb2AddIndexInt(
    Parse *pParse,      /* All information about this parse */
    char *keyname,      /* Index name (Optional) */
    ExprList *pList,    /* A list of columns to be indexed */
    int onError,        /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
    Expr *pPIWhere,     /* WHERE clause for partial indices */
    const char *zStart, /* Start of WHERE clause token text */
    const char *zEnd,   /* End of WHERE clause token text */
    int sortOrder,      /* Sort order of primary key when pList==NULL */
    u8 idxType,         /* The index type */
    int withOpts,       /* WITH options (DATACOPY) */
    ExprList *pdList    /* A list of columns in the partial datacopy */
)
{
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_key *key;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        assert(idxType != SQLITE_IDXTYPE_DUPKEY);
        sqlite3CreateIndex(pParse, 0, 0, 0, pList, onError, 0, 0, 0, 0,
                           idxType);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    key = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_key));
    if (key == 0)
        goto oom;

    if (keyname) {
        if (idxType != SQLITE_IDXTYPE_PRIMARYKEY && is_pk(keyname)) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Invalid key name '%s'.", keyname);
            goto cleanup;
        }
        key->name = keyname;
    } else {
        /* Key name not specified, will be generated later. */
    }

    switch (idxType) {
    case SQLITE_IDXTYPE_APPDEF:
        /* For CREATE INDEX, we need to check onError */
        if (onError != OE_Abort) {
            key->flags |= KEY_DUP;
        } else {
            key->flags |= KEY_UNIQNULLS;
        }
        break;
    case SQLITE_IDXTYPE_UNIQUE:
        /* fallthrough */
    case SQLITE_IDXTYPE_PRIMARYKEY:
        key->flags |= KEY_UNIQNULLS;
        break;
    case SQLITE_IDXTYPE_DUPKEY:
        key->flags |= KEY_DUP;
        break;
    default:
        assert(0);
    }

    if (withOpts == 1) {
        key->flags |= KEY_DATACOPY;
    }

    /* Initialize the index column list. */
    listc_init(&key->idx_col_list, offsetof(struct comdb2_index_part, lnk));

    /* Initialize the partial datacopy list. */
    listc_init(&key->partial_datacopy_list, offsetof(struct comdb2_partial_datacopy_field, lnk));

    /*
      pList == 0 imples that the PRIMARY/UNIQUE/DUP key was specified in the
      column definition.
    */
    if (pList == 0) {
        struct comdb2_column *column;
        struct comdb2_index_part *idx_part;
        idx_part =
            comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_part));
        if (idx_part == 0)
            goto oom;

        column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);

        idx_part->name = column->name;
        if (sortOrder == SQLITE_SO_DESC) {
            /* Only PKs accept sort order in the column definition. */
            assert(idxType == SQLITE_IDXTYPE_PRIMARYKEY);
            idx_part->flags |= INDEX_ORDER_DESC;
        }
        idx_part->column = column;

        /* Add the index column to the list. */
        listc_abl(&key->idx_col_list, idx_part);

        /* For a PRIMARY KEY, force its column to be NOT NULL. */
        if (idxType == SQLITE_IDXTYPE_PRIMARYKEY) {
            column->flags |= COLUMN_NO_NULL;
        }
    } else {
        struct comdb2_index_part *idx_part;
        struct ExprList_item *pListItem;
        int i;

        /* Validate the index column list. */
        sqlite3ExprListCheckLength(pParse, pList, "index");
        for (i = 0, pListItem = pList->a; i < pList->nExpr; i++, pListItem++) {

            idx_part =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_part));
            if (idx_part == 0)
                goto oom;

            switch (pListItem->pExpr->op) {
            case TK_ID: // fallthrough
            case TK_STRING: {
                struct comdb2_column *column;
                column = find_column_by_name(ctx, pListItem->pExpr->u.zToken);
                if (column == 0) {
                    pParse->rc = SQLITE_ERROR;
                    sqlite3ErrorMsg(pParse, "Unknown column '%s'.",
                                    pListItem->pExpr->u.zToken);
                    goto cleanup;
                }
                idx_part->name = column->name;

                /* For a PRIMARY KEY, force all its columns to be NOT NULL. */
                if (idxType == SQLITE_IDXTYPE_PRIMARYKEY) {
                    column->flags |= COLUMN_NO_NULL;
                }
                idx_part->column = column;
                break;
            }
            case TK_CAST: {
                Vdbe *v;
                char *type;
                char *ptr;
                char *expr;
                struct strbuf *csc2_expr;

                v = sqlite3GetVdbe(pParse);
                expr = sqlite3ExprDescribe(v, pListItem->pExpr->pLeft);
                if (!expr) {
                    pParse->rc = SQLITE_ERROR;
                    sqlite3ErrorMsg(pParse, "Invalid expression");
                    goto cleanup;
                }

                csc2_expr = strbuf_new();

                /* Type */
                type = comdb2_strndup(ctx->mem, pListItem->pExpr->u.zToken,
                                      strlen(pListItem->pExpr->u.zToken) + 1);
                /* Fix the type: convert '()' (sql-land) to '[]' (csc2-land) */
                ptr = type;
                while (*ptr) {
                    switch (*ptr) {
                    case '(':
                        *ptr = '[';
                        break;
                    case ')':
                        *ptr = ']';
                        break;
                    }
                    *ptr = tolower(*ptr);
                    ++ptr;
                }
                strbuf_appendf(csc2_expr, "(%s)", type);

                /* Expression */
                strbuf_append(csc2_expr, "\"");
                escape_expr(csc2_expr, expr);
                strbuf_append(csc2_expr, "\"");

                idx_part->name =
                    comdb2_strdup(ctx->mem, (char *) strbuf_buf(csc2_expr));

                strbuf_free(csc2_expr);

                /* No need to refer to the column as we have got all required
                 * information */
                idx_part->column = 0;
                idx_part->flags |= INDEX_IS_EXPR;
                break;
            }
            default:
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "Invalid index column list");
                goto cleanup;
            }

            if (pListItem->sortOrder == SQLITE_SO_DESC) {
                idx_part->flags |= INDEX_ORDER_DESC;
            }

            /* Add the index column to the list. */
            listc_abl(&key->idx_col_list, idx_part);
        }
    }

    if (pdList) { // partial datacopy
        if (key->flags & KEY_DATACOPY) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Cannot have datacopy and partial datacopy.");
            goto cleanup;
        }

        key->flags |= KEY_PARTIALDATACOPY;

        struct comdb2_partial_datacopy_field *partial_datacopy_field;
        struct ExprList_item *pListItem;
        struct comdb2_column *column;
        int i;

        /* Validate the partial datacopy list. */
        sqlite3ExprListCheckLength(pParse, pdList, "partial datacopy");
        for (i = 0, pListItem = pdList->a; i < pdList->nExpr; i++, pListItem++) {
            column = find_column_by_name(ctx, pListItem->pExpr->u.zToken);
            if (column == 0) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "Unknown column '%s'.",
                                pListItem->pExpr->u.zToken);
                goto cleanup;
            }

            LISTC_FOR_EACH(&key->partial_datacopy_list, partial_datacopy_field, lnk)
            {
                if (strcmp(column->name, partial_datacopy_field->name) == 0) {
                    pParse->rc = SQLITE_ERROR;
                    sqlite3ErrorMsg(pParse, "Duplicate field '%s'.",
                                    column->name);
                    goto cleanup;
                }
            }

            partial_datacopy_field = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_partial_datacopy_field));
            if (partial_datacopy_field == 0) {
                goto oom;
            }

            // partial_datacopy_field->name = column->name; // shouldn't we strdup?
            partial_datacopy_field->name = comdb2_strdup(ctx->mem, column->name);
            if (partial_datacopy_field->name == 0) {
                goto oom;
            }

            /* Add the partial datacopy column to the list. */
            listc_abl(&key->partial_datacopy_list, partial_datacopy_field);
        }

        // check for decimal columns (currently not supported)
        LISTC_FOR_EACH(&ctx->schema->column_list, column, lnk)
        {
            /* Ignore the dropped column(s). */
            if (column->flags & COLUMN_DELETED)
                continue;

            if (column->type == SQL_TYPE_DECIMAL32 || column->type == SQL_TYPE_DECIMAL64 || column->type == SQL_TYPE_DECIMAL128) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "Currently cannot have partial datacopy with decimal columns.");
                goto cleanup;
            }

        }
    } else if (withOpts == 2) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Expected partial datacopy columns or 'ALL' keyword after 'INCLUDE'.");
        goto cleanup;
    }

    if (pPIWhere && zStart && zEnd) {
        char *where_clause;
        size_t where_sz;

        if (gbl_noenv_messages == 0) {
            extern int gbl_noenv_messages;
            setError(pParse, SQLITE_ERROR, "Partial index not enabled");
            goto cleanup;
        }

        where_sz = zEnd - zStart;
        assert(where_sz > 0);
        where_clause = comdb2_strndup(ctx->mem, zStart, where_sz + 1);
        if (where_clause == 0)
            goto oom;

        where_sz += (sizeof("where") + 1);
        key->where = comdb2_malloc(ctx->mem, where_sz);
        if (key->where == 0)
            goto oom;

        snprintf(key->where, where_sz, "%s", where_clause);
    }

    /*
      Generate a key name if it has not been explicitly provided in
      the command.
    */
    if (key->name == 0) {
        char *loc_keyname = comdb2_malloc(ctx->mem, MAXGENKEYLEN);
        if (loc_keyname == 0) {
            goto oom;
        }

        gen_key_name(key, ctx->schema->name, loc_keyname, MAXGENKEYLEN);
        key->name = loc_keyname;
    }

    /*
      Check if a key already exists with the same name. For indices
      created via CREATE INDEX (SQLITE_IDXTYPE_APPDEF) this check has
      already been done to address IF NOT EXISTS.
    */
    if (idxType != SQLITE_IDXTYPE_APPDEF && find_idx_by_name(ctx, key->name)) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Index '%s' already exists.", key->name);
        goto cleanup;
    }

    /* Add the key to the list. */
    listc_abl(&ctx->schema->key_list, key);

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

void comdb2AddPrimaryKey(
    Parse *pParse,   /* Parsing context */
    ExprList *pList, /* List of column names to be indexed */
    int onError,     /* What to do with a uniqueness conflict */
    int autoInc,     /* True if the AUTOINCREMENT keyword is present */
    int sortOrder    /* SQLITE_SO_ASC or SQLITE_SO_DESC */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    char *keyname;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3AddPrimaryKey(pParse, pList, onError, autoInc, sortOrder);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    keyname = comdb2_strdup(ctx->mem, COMDB2_PK);
    if (keyname == 0)
        goto oom;

    comdb2AddIndexInt(pParse, keyname, pList, onError, 0, 0, 0, sortOrder,
                      SQLITE_IDXTYPE_PRIMARYKEY, 0, NULL);
    if (pParse->rc)
        goto cleanup;

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

void comdb2DropPrimaryKey(Parse *pParse /* Parsing context */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    Token t = {COMDB2_PK, sizeof(COMDB2_PK) - 1};
    comdb2AlterDropIndex(pParse, &t);
    return;
}

/* Implementation of index added via CREATE\ALTER TABLE commands. */
void comdb2AddIndex(
    Parse *pParse,      /* All information about this parse */
    Token *pName,       /* Index name (Optional) */
    ExprList *pList,    /* A list of columns to be indexed */
    int onError,        /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
    Expr *pPIWhere,     /* WHERE clause for partial indices */
    const char *zStart, /* Start of WHERE clause token text */
    const char *zEnd,   /* End of WHERE clause token text */
    int sortOrder,      /* Sort order of primary key when pList==NULL */
    u8 idxType,         /* The index type */
    int withOpts,       /* WITH options (DATACOPY) */
    ExprList *pdList    /* A list of columns in the partial datacopy */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    char *keyname;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        assert(idxType != SQLITE_IDXTYPE_DUPKEY);
        sqlite3CreateIndex(pParse, 0, 0, 0, pList, onError, 0, 0, 0, 0,
                           idxType);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    if (pName && pName->n > 0) {
        keyname = comdb2_strndup(ctx->mem, pName->z, pName->n);
        if (keyname == 0)
            goto oom;
        sqlite3Dequote(keyname);
    } else {
        /* Key name not specified, one will be generated later. */
        keyname = 0;
    }

    comdb2AddIndexInt(pParse, keyname, pList, onError, pPIWhere, zStart,
                      zEnd, sortOrder, idxType, withOpts, pdList);
    if (pParse->rc)
        goto cleanup;

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

/* Implementation of CREATE INDEX command. */
void comdb2CreateIndex(
    Parse *pParse,      /* All information about this parse */
    Token *pName1,      /* First part of index name (db or index name). */
    Token *pName2,      /* Second part of index name. May be NULL */
    SrcList *pTblName,  /* Table to index */
    ExprList *pList,    /* A list of columns to be indexed */
    int onError,        /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
    Token *pStart,      /* The CREATE token that begins this statement */
    Expr *pPIWhere,     /* WHERE clause for partial indices */
    const char *zStart, /* Start of WHERE clause token text */
    const char *zEnd,   /* End of WHERE clause token text */
    int sortOrder,      /* Sort order of primary key when pList==NULL */
    int ifNotExist,     /* Omit error if index already exists */
    u8 idxType,         /* The index type */
    int withOpts,       /* WITH options (DATACOPY) */
    ExprList *pdList,   /* A list of columns in the partial datacopy */
    int temp)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        int code = temp ? SQLITE_CREATE_TEMP_INDEX : SQLITE_CREATE_INDEX;
        if( sqlite3AuthCheck(pParse, code, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v;
    struct schema_change_type *sc;
    struct comdb2_ddl_context *ctx;
    char *keyname;

    if (temp || pParse->db->init.busy || pParse->db->isExpert ||
        IN_SPECIAL_PARSE) {
        sqlite3CreateIndex(pParse, pName1, pName2, pTblName, pList, onError,
                           pStart, pPIWhere, sortOrder, ifNotExist, idxType);
        return;
    }

    assert(pTblName->nSrc == 1);
    assert(pName1->n != 0);
    assert(pList != 0);
    assert(idxType == SQLITE_IDXTYPE_APPDEF);

    v = sqlite3GetVdbe(pParse);

    sc = new_schemachange_type();
    if (sc == 0)
        goto oom;

    ctx = create_ddl_context(pParse);
    if (ctx == 0)
        goto oom;

    ctx->schema->name = comdb2_strdup(ctx->mem, pTblName->a[0].zName);
    if (ctx->schema->name == 0)
        goto oom;

    if (isRemote(pParse, &pName1, &pName2)) {
        return;
    }

    if ((chkAndCopyTable(pParse, sc->tablename, ctx->schema->name,
                         strlen(ctx->schema->name), ERROR_ON_TBL_NOT_FOUND, 1, 0,
                         &ctx->partition_first_shardname)))
        goto cleanup;

    /*
       Add all the columns, indexes and constraints in the table to the
       respective list.
    */
    if (retrieve_schema(pParse, ctx)) {
        goto cleanup;
    }

    keyname = comdb2_strndup(ctx->mem, pName1->z, pName1->n);
    if (keyname == 0)
        goto oom;
    sqlite3Dequote(keyname);

    /* Check whether an index with the same name already exists. */
    if (find_idx_by_name(ctx, keyname)) {
        if (ifNotExist == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Index '%s' already exists.", keyname);
        } else {
            /* Do not report this as an error. */
            logmsg(LOGMSG_DEBUG, "Index '%s' already exists.", keyname);
        }
        goto cleanup;
    }

    comdb2AddIndexInt(pParse, keyname, pList, onError, pPIWhere, zStart,
                      zEnd, sortOrder, idxType, withOpts, pdList);
    if (pParse->rc)
        goto cleanup;

    sc->kind = SC_ALTERTABLE_INDEX;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto cleanup;
        }
    }

    fillTableOption(sc, ctx->schema->table_options);

    sc->newcsc2 = prepare_csc2(pParse, ctx);
    if (sc->newcsc2 == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        goto cleanup;
    }
    if(sc->dryrun) {
        comdb2prepareSString(v, pParse, 0, sc, &comdb2SqlDryrunSchemaChange,
                    (vdbeFuncArgFree)&free_schema_change_type);
    } else {
        comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    }
    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_schema_change_type(sc);
    free_ddl_context(pParse);
    return;
}

/*
  Check whether the requested action is supported by Comdb2.
*/
static int check_constraint_action(Parse *pParse, int *flags)
{
    int in_flags = *flags;

    u8 on_delete = (u8)(in_flags & 0xff);
    u8 on_update = (u8)((in_flags >> 8) & 0xff);

    *flags = 0;

    if ((on_delete != 0 && (on_delete != OE_Cascade &&
                            on_delete != OE_SetNull)) ||
        (on_update != 0 && on_update != OE_Cascade)) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "requested cascade action is not supported");
        return 1;
    } else {
        if (on_delete == OE_Cascade) {
            *flags |= CONS_DEL_CASCADE;
        }
        if (on_delete == OE_SetNull) {
            *flags |= CONS_DEL_SETNULL;
        }
        if (on_update == OE_Cascade) {
            *flags |= CONS_UPD_CASCADE;
        }
    }

    return 0;
}

/* search matching key in parent table using saved client context:
 * return 1 if found and no error, 0 otherwise
 */
static int
find_parent_key_in_client_context(Parse *pParse, struct comdb2_ddl_context *ctx,
                                  struct comdb2_constraint *constraint)
{
    struct comdb2_ddl_context *clnt_ctx = NULL;
    struct comdb2_key *key;

    clnt_ctx = comdb2_get_ddl_context(constraint->parent_table);
    if (clnt_ctx == NULL &&
        strcasecmp(ctx->schema->name, constraint->parent_table) == 0)
        clnt_ctx = ctx;
    if (clnt_ctx == NULL)
        return 0;

    key = find_suitable_key(&constraint->parent_idx_col_list,
                            &clnt_ctx->schema->key_list);

    if (key == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse,
                        "A matching key for the FOREIGN KEY was not "
                        "found in the parent (referenced) table '%s'.",
                        constraint->parent_table);
        return 0;
    } else {
        constraint->parent_key = comdb2_strdup(ctx->mem, key->name);
        if (constraint->parent_key == 0) {
            setError(pParse, SQLITE_NOMEM, "System out of memory");
            return 0;
        }
    }
    return 1;
}

/* Set the constraint name. Also check whether another
 * constraint exists with the same name.
 */
static int set_constraint_name(Parse *pParse,
                               struct comdb2_constraint *constraint)
{
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    char constraint_name_buf[MAXCONSLEN + 1];
    char *constraint_name;

    if (pParse->constraintName.n == 0) {
        /* Generate the constraint name. */
        gen_constraint_name(constraint, constraint_name_buf,
                            sizeof(constraint_name_buf));
        constraint_name = constraint_name_buf;
    } else {
        if (pParse->constraintName.n > MAXCONSLEN) {
            setError(pParse, SQLITE_MISUSE, "Constraint name is too long.");
            return 1;
        }
        memcpy(constraint_name_buf, pParse->constraintName.z,
               pParse->constraintName.n);
        constraint_name_buf[pParse->constraintName.n] = 0;
        constraint_name = constraint_name_buf;
        sqlite3Dequote(constraint_name);
    }

    /* Check whether a similar constraint already exists. */
    if ((find_cons_by_name(ctx, constraint_name, CONS_ALL))) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Constraint '%s' already exists.",
                        constraint_name);
        return 1;
    }

    /* Don't use auto-generated constraint name for foreign keys. */
    if (pParse->constraintName.n > 0 || constraint->type != CONS_FKEY) {
        constraint->name = comdb2_strdup(ctx->mem, constraint_name);
        if (constraint->name == 0) {
            setError(pParse, SQLITE_NOMEM, "System out of memory");
            return 1;
        }
    }

    return 0;
}

void comdb2CreateForeignKey(
    Parse *pParse,      /* Parsing context */
    ExprList *pFromCol, /* Columns in this table that point to other table */
    Token *pTo,         /* Name of the other table */
    ExprList *pToCol,   /* Columns in the other table */
    int flags           /* Conflict resolution algorithms. */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_constraint *constraint;
    struct comdb2_index_part *idx_part;
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct dbtable *parent_table;
    int key_found = 0;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3CreateForeignKey(pParse, pFromCol, pTo, pToCol, flags);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    constraint = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_constraint));
    if (constraint == 0)
        goto oom;

    /* Initialize the lists. */
    listc_init(&constraint->child_idx_col_list,
               offsetof(struct comdb2_index_part, lnk));
    listc_init(&constraint->parent_idx_col_list,
               offsetof(struct comdb2_index_part, lnk));

    assert(pToCol);

    /*
      FROM column(s) and sort order(s)

      pFromCol == 0 imples that the FOREIGN KEY was specified in the column
      definition.
    */
    if (pFromCol == 0) {
        struct comdb2_column *column;

        /* Child column is the last one added to the column list. */
        column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);

        idx_part =
            comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_part));
        if (idx_part == 0)
            goto oom;

        idx_part->name = comdb2_strdup(ctx->mem, column->name);
        if (idx_part->name == 0)
            goto oom;

        /* Note: In this case the sort order is always ASC. */
        // idx_part->flags = 0;

        /* Assign the reference. */
        idx_part->column = column;

        listc_abl(&constraint->child_idx_col_list, idx_part);
    } else {
        /*
          Though some RDBMSs allow this, the number of referenced columns in
          FK must not be zero. PG, for instance picks the primary key from the
          referenced table.
        */
        assert(pToCol->nExpr > 0);

        for (int i = 0; i < pFromCol->nExpr; i++) {
            idx_part =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_part));
            if (idx_part == 0)
                goto oom;

            idx_part->name = comdb2_strdup(ctx->mem, pFromCol->a[i].zName);
            if (idx_part->name == 0)
                goto oom;

            assert(pFromCol->a[i].sortOrder == SQLITE_SO_ASC);

            /* There's no comdb2_column for foreign columns. */
            // idx_part->column = 0;

            listc_abl(&constraint->child_idx_col_list, idx_part);
        }
    }

    /*
      TO (referenced) column(s) and sort order(s).
    */
    for (int i = 0; i < pToCol->nExpr; i++) {
        idx_part =
            comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_part));
        if (idx_part == 0)
            goto oom;

        idx_part->name = comdb2_strdup(ctx->mem, pToCol->a[i].zName);
        if (idx_part->name == 0)
            goto oom;

        assert(pToCol->a[i].sortOrder == SQLITE_SO_ASC);
        // idx_part->column = 0;

        listc_abl(&constraint->parent_idx_col_list, idx_part);
    }

    /* To be assigned later */
    // constraint->child = 0;

    /* Referenced table */
    constraint->parent_table = comdb2_strndup(ctx->mem, pTo->z, pTo->n);
    if (constraint->parent_table == 0)
        goto oom;
    sqlite3Dequote(constraint->parent_table);

    key_found = find_parent_key_in_client_context(pParse, ctx, constraint);
    if (pParse->rc)
        goto cleanup;
    else if (key_found)
        goto parent_key_found;

    /* Determine an appropriate key in the parent table. */
    parent_table = get_dbtable_by_name(constraint->parent_table);
    if (parent_table == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(
            pParse, "Parent table '%s' of the FOREIGN KEY could not be found.",
            constraint->parent_table);
        goto cleanup;
    }

    for (int i = 0; i < parent_table->schema->nix; i++) {
        if (parent_table->schema->ix[i]->nmembers <
            listc_size(&constraint->parent_idx_col_list))
            continue;

        /* Let's start by assuming that we have found the matching key. */
        key_found = 1;
        int j = 0;
        LISTC_FOR_EACH(&constraint->parent_idx_col_list, idx_part, lnk)
        {
            if (strcasecmp(idx_part->name,
                           parent_table->schema->ix[i]->member[j].name) !=
                0) {
                key_found = 0;
                break;
            }
            j++;
        }

        if (key_found == 1) {
            constraint->parent_key =
                comdb2_strdup(ctx->mem, parent_table->schema->ix[i]->csctag);
            if (constraint->parent_key == 0)
                goto oom;

            /* Matching key found */
            break;
        }
    }

    if (key_found == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse,
                        "A matching key for the FOREIGN KEY was not "
                        "found in the parent (referenced) table '%s'.",
                        constraint->parent_table);
        goto cleanup;
    }

parent_key_found:
    /* Verify the constraint action. */
    if ((check_constraint_action(pParse, &flags))) {
        goto cleanup;
    }
    constraint->flags = flags;
    constraint->type = CONS_FKEY;

    if ((set_constraint_name(pParse, constraint)) != 0) {
        goto cleanup;
    }

    /* Add this new constraint to the list. */
    listc_abl(&ctx->schema->constraint_list, constraint);

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

void comdb2DeferForeignKey(Parse *pParse, int isDeferred)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    if (use_sqlite_impl(pParse)) {
        assert(pParse->comdb2_ddl_ctx == 0);
        sqlite3DeferForeignKey(pParse, isDeferred);
        return;
    }
    return;
}

static void drop_constraint(Parse *pParse, Token *pName, int type)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    char *name;
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_constraint *cons;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    assert(pParse->db->init.busy == 0);

    name = comdb2_strndup(ctx->mem, pName->z, pName->n);
    if (name == 0)
        goto oom;
    sqlite3Dequote(name);

    /* Check whether the constraint exists. */
    cons = find_cons_by_name(ctx, name, type);
    if (cons) {
        /* Mark it as dropped. */
        cons->flags |= CONS_DELETED;
    } else {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Constraint '%s' not found.", name);
        goto cleanup;
    }

    /* Constraint marked for removal. */

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
}

void comdb2DropForeignKey(Parse *pParse, /* Parser context */
                          Token *pName   /* Foreign key name */
)
{
    drop_constraint(pParse, pName, CONS_FKEY);
    return;
}

void comdb2DropConstraint(Parse *pParse, /* Parser context */
                          Token *pName   /* Foreign key name */
)
{
    drop_constraint(pParse, pName, CONS_ALL);
    return;
}

/*
 * Check whether the specified key has any existing associated
 * constraint(s) and drop if asked.
 */
static int check_dependent_cons(struct comdb2_ddl_context *ctx,
                                struct comdb2_key *key, int drop)
{
    struct comdb2_constraint *constraint;

    LISTC_FOR_EACH(&ctx->schema->constraint_list, constraint, lnk)
    {
        /* Skip if the constraint has already been dropped. */
        if (constraint->flags & CONS_DELETED) {
            continue;
        }

        if (constraint->child == key) {
            if (drop) {
                constraint->flags |= CONS_DELETED;
            } else {
                return 1;
            }
        }
    }
    return 0;
}

/*
 * Check whether the specified column has any existing associated
 * key(s) and drop if asked.
 */
static int check_dependent_keys(struct comdb2_ddl_context *ctx,
                                const char *column, int drop)
{
    struct comdb2_key *key;
    struct comdb2_index_part *idx_col;

    /* Check if an index exists that has this column a member. */
    LISTC_FOR_EACH(&ctx->schema->key_list, key, lnk)
    {
        /* Skip if the key has already been dropped. */
        if (key->flags & KEY_DELETED)
            continue;

        LISTC_FOR_EACH(&key->idx_col_list, idx_col, lnk)
        {
            if (strcasecmp(idx_col->name, column) == 0) {
                if (drop) {
                    /* Drop the dependent constraints. */
                    check_dependent_cons(ctx, key, 1);

                    /* Mark the key as deleted. */
                    key->flags |= KEY_DELETED;
                } else {
                    return 1;
                }
            }
        }
    }
    return 0;
}

/*
  Drop the specified column.
*/
void comdb2DropColumn(Parse *pParse, /* Parser context */
                      Token *pName   /* Name of the column */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_column *column;
    char *name;
    int column_exists = 0;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    assert(pParse->db->init.busy == 0);

    name = comdb2_strndup(ctx->mem, pName->z, pName->n);
    if (name == 0)
        goto oom;
    sqlite3Dequote(name);

    LISTC_FOR_EACH(&ctx->schema->column_list, column, lnk)
    {
        if ((strcasecmp(name, column->name)) == 0) {
            /* Check whether an index is referring to this column. */
            if (check_dependent_keys(ctx, name, gbl_ddl_cascade_drop)) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse,
                                "Column '%s' cannot be dropped as it is part "
                                "of an existing key.",
                                name);
                goto cleanup;
            }

            /* Mark the column as deleted. */
            column->flags |= COLUMN_DELETED;
            column_exists = 1;
            break;
        }
    }

    /* Check whether the column exists. */
    if (column_exists == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Column '%s' not found.", name);
        goto cleanup;
    }

    /* Column marked for removal. */

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

void comdb2DropIndexInt(Parse *pParse, char *idx_name)
{
    struct comdb2_key *key;
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;

    assert(ctx);

    /* Find the index in the context. */
    key = find_idx_by_name(ctx, idx_name);

    if (!key) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Key '%s' not found.", idx_name);
        goto cleanup;
    } else {
        /* First, check whether a constraint is associated with this key. */
        if (check_dependent_cons(ctx, key, gbl_ddl_cascade_drop)) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse,
                            "Key '%s' cannot be dropped as it is being used "
                            "in a foreign key constraint.",
                            idx_name);
            goto cleanup;
        }

        /* Mark the key as deleted. */
        key->flags |= KEY_DELETED;
    }

    return;

cleanup:
    free_ddl_context(pParse);
    return;
}

/*
  Top-level implementation for DROP INDEX
*/
void comdb2DropIndex(Parse *pParse, Token *pName1, Token *pName2, int ifExists)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_DROP_INDEX, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v;
    struct dbtable *table = NULL;
    struct schema_change_type *sc;
    struct comdb2_ddl_context *ctx;
    char *idx_name;
    char *tbl_name = 0;
    int index_count = 0;

    assert(pParse->comdb2_ddl_ctx == 0);

    v = sqlite3GetVdbe(pParse);

    sc = new_schemachange_type();
    if (sc == 0)
        goto oom;

    ctx = create_ddl_context(pParse);
    if (ctx == 0)
        goto oom;

    /* Index name */
    idx_name = comdb2_strndup(ctx->mem, pName1->z, pName1->n);
    if (idx_name == 0)
        goto oom;
    sqlite3Dequote(idx_name);

    /* The table name has been specified. */
    if (pName2) {
        int found = 0;
        tbl_name = comdb2_strndup(ctx->mem, pName2->z, pName2->n);
        if (tbl_name == 0)
            goto oom;
        sqlite3Dequote(tbl_name);

        if ((chkAndCopyTable(pParse, sc->tablename, tbl_name, strlen(tbl_name),
                             ERROR_ON_TBL_NOT_FOUND, 1, 0, &ctx->partition_first_shardname)))
            goto cleanup;

        if (authenticateSC(sc->tablename, pParse))
            goto cleanup;

        if (ctx->partition_first_shardname) {
            table = get_dbtable_by_name(ctx->partition_first_shardname);
        } else {
            table = get_dbtable_by_name(tbl_name);
        }
        /* Already checked in chkAndCopyTable(). */
        assert(table);

        /* Check whether an index exist with the specified name. */
        for (int i = 0; i < table->schema->nix; i++) {
            if ((strcasecmp(table->schema->ix[i]->csctag, idx_name)) == 0) {
                found = 1;
                break;
            }
        }

        if (found == 0) {
            if (ifExists == 0) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "Index '%s' not found.", idx_name);
            }
            goto cleanup;
        }
    } else {
        /*
          No table name specified, the index name must be unqiue across
          the database or we fail.

          Unlike Sqlite, Comdb2 allows multiples keys with same name
          but in different tables. So, we first need to check if the
          specified index is unique across ALL the tables, and only
          drop the index if it is, or fail otherwise.
        */

        for (int i = 0; i < thedb->num_dbs; i++) {
            for (int j = 0; j < thedb->dbs[i]->schema->nix; j++) {
                if ((strcasecmp(thedb->dbs[i]->schema->ix[j]->csctag,
                                idx_name)) == 0) {
                    table = thedb->dbs[i];
                    index_count++;
                }
            }
        }

        if (index_count == 0) {
            if (ifExists == 0) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "Index '%s' not found.", idx_name);
            }
            goto cleanup;
        } else if (index_count > 1) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Multiple indexes with same name '%s'.",
                            idx_name);
            goto cleanup;
        }

        if ((chkAndCopyTable(pParse, sc->tablename, table->tablename,
                             strlen(table->tablename), ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL)))
            goto cleanup;

        if (authenticateSC(sc->tablename, pParse))
            goto cleanup;
    }

    ctx->schema->name = table->tablename;

    /*
      Add all the columns, indexes and constraints in the table to the
      respective list.
    */
    if (retrieve_schema(pParse, ctx)) {
        goto cleanup;
    }

    /* Mark the index for removal. */
    comdb2DropIndexInt(pParse, idx_name);
    if (pParse->rc)
        goto cleanup;

    sc->kind = SC_DROPTABLE_INDEX;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    if(comdb2IsDryrun(pParse)){
        if(comdb2SCIsDryRunnable(sc)){
            sc->dryrun = 1;
        } else {
            setError(pParse, SQLITE_MISUSE, "DRYRUN not supported for this operation");
            goto cleanup;
        }
    }

    fillTableOption(sc, ctx->schema->table_options);

    sc->newcsc2 = prepare_csc2(pParse, ctx);
    if (sc->newcsc2 == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        goto cleanup;
    }

    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_schema_change_type(sc);
    free_ddl_context(pParse);
    return;
}

/*
  Implementation for ALTER TABLE [..] DROP INDEX.
*/
void comdb2AlterDropIndex(Parse *pParse, Token *pName)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    char *keyname;

    assert(pName->n);

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    keyname = comdb2_strndup(ctx->mem, pName->z, pName->n);
    if (keyname == 0)
        goto oom;
    sqlite3Dequote(keyname);

    comdb2DropIndexInt(pParse, keyname);
    if (pParse->rc)
        goto cleanup;

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

#define ODH_FLAGS (ODH_OFF|ODH_ON)
#define IPU_FLAGS (IPU_OFF|IPU_ON)
#define ISC_FLAGS (ISC_OFF|ISC_ON)
#define BLOB_CMPR_FLAGS (BLOB_NONE|BLOB_RLE|BLOB_CRLE|BLOB_ZLIB|BLOB_LZ4)
#define REC_CMPR_FLAGS (REC_NONE|REC_RLE|REC_CRLE|REC_ZLIB|REC_LZ4)
#define REBUILD_FLAGS (REBUILD_ALL|REBUILD_DATA|REBUILD_BLOB)

static int bitSetCount(int num) {
    int count = 0;
    while (num) {
        ++count;
        num &= (num-1);
    }
    return count;
}

static int checkAndSetBits(Parse *pParse, uint32_t *dst, uint32_t src,
                           uint32_t propertyFlags, const char *propertyName) {
    uint32_t propertyBits = src & propertyFlags;
    int count = bitSetCount(propertyBits);
    if (count >= 1) {
        if (count > 1) {
            sqlite3ErrorMsg(pParse, "Conflicting '%s' options", propertyName);
            return 1;
        }

        *dst &= (~propertyFlags);
        *dst |= propertyBits;
    }
    return 0;
}

void comdb2AlterTableOptions(
    Parse *pParse,      /* Parse context */
    uint32_t comdb2Opts /* Comdb2 specific table properties */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    /* At this point, we have already retreived the existing table properties.
     * Here we update all the flags that was explicitly asked in the ALTER
     * command. */

    uint32_t *tableOpts = &ctx->schema->table_options;

    if ((checkAndSetBits(pParse, tableOpts, comdb2Opts, ODH_FLAGS, "ODH")) == 1) {
        return;
    }
    if ((checkAndSetBits(pParse, tableOpts, comdb2Opts, IPU_FLAGS, "IPU")) == 1) {
        return;
    }
    if ((checkAndSetBits(pParse, tableOpts, comdb2Opts, ISC_FLAGS, "ISC")) == 1) {
        return;
    }
    if ((checkAndSetBits(pParse, tableOpts, comdb2Opts, BLOB_CMPR_FLAGS, "BLOB COMPRESSION")) == 1) {
        return;
    }
    if ((checkAndSetBits(pParse, tableOpts, comdb2Opts, REC_CMPR_FLAGS, "RECORD COMPRESSION")) == 1) {
        return;
    }
    if ((checkAndSetBits(pParse, tableOpts, comdb2Opts, REBUILD_FLAGS, "REBUILD")) == 1) {
        return;
    }
    if (comdb2Opts & PAGE_ORDER) {
        *tableOpts |= PAGE_ORDER;
    }
    if (comdb2Opts & READ_ONLY) {
        *tableOpts |= READ_ONLY;
    }
    return;
}

/*
  Implementation of PUT TUNABLE
 */
void comdb2putTunable(Parse *pParse, Token *name1, Token *name2, Token *value)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_PUT_TUNABLE, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    if (comdb2AuthenticateUserOp(pParse))
        return;

    char t_name[160];
    char *t_name1;
    char *t_name2 = NULL;
    char *t_value = NULL;
    int rc;
    comdb2_tunable_err err;

    rc = create_string_from_token(NULL, pParse, &t_name1, name1);
    if (rc != SQLITE_OK)
        goto cleanup; /* Error has been set. */
    if (name2 && name2->n > 0) {
        rc = create_string_from_token(NULL, pParse, &t_name2, name2);
        if (rc != SQLITE_OK)
            goto cleanup; /* Error has been set. */
        snprintf(t_name, sizeof(t_name) - 1, "%s.%s", t_name1, t_name2);
    } else {
        snprintf(t_name, sizeof(t_name) - 1, "%s", t_name1);
    }
    rc = create_string_from_token(NULL, pParse, &t_value, value);
    if (rc != SQLITE_OK)
        goto cleanup; /* Error has been set. */

    if ((err = handle_runtime_tunable(t_name, t_value))) {
        setError(pParse, SQLITE_ERROR, tunable_error(err));
    }

cleanup:
    free(t_name1);
    free(t_name2);
    free(t_value);
    return;
}

// Use create_string_from_token() to store the string on heap.
int comdb2TokenToStr(Token *nm, char *out, size_t len)
{
    char *buf;
    int rc = 0;
    int malloced = 0;

    if (likely(nm->n < 100)) {
        buf = alloca(nm->n + 1);
    } else {
        buf = malloc(nm->n + 1);
        if (buf == 0) /* malloc failed */
            return -1;
        malloced = 1;
    }
    memcpy(buf, nm->z, nm->n);
    buf[nm->n] = '\0';

    sqlite3Dequote(buf);

    if (strlen(buf) >= len) {
        rc = 1;
        goto done;
    }

    strcpy(out, buf);

done:
    if (malloced)
        free(buf);

    return rc;
}

/* 
  Implementation of ALTER TABLE .. ALTER COLUMN .. 
 */
void comdb2AlterColumnStart(Parse *pParse /* Parser context */,
                            Token *pName /* Column name */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_column *current;
    char *column_name;
    int column_exists;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    column_name = comdb2_strndup(ctx->mem, pName->z, pName->n);
    if (column_name == 0)
        goto oom;
    sqlite3Dequote(column_name);

    column_exists = 0;
    LISTC_FOR_EACH(&ctx->schema->column_list, current, lnk)
    {
        if ((strcasecmp(column_name, current->name) == 0) &&
            ((current->flags & COLUMN_DELETED) == 0)) {
            column_exists = 1;
            break;
        }
    }

    if (column_exists == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Column '%s' not found.", column_name);
        goto cleanup;
    }

    ctx->alter_column = current;

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

void comdb2AlterColumnEnd(Parse *pParse /* Parser context */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    if (pParse->comdb2_ddl_ctx) {
        assert(pParse->comdb2_ddl_ctx->alter_column);
        pParse->comdb2_ddl_ctx->alter_column = 0;
    }
    return;
}

/* 
  Implementation of ALTER TABLE .. ALTER COLUMN .. TYPE .. 
 */
void comdb2AlterColumnType(Parse *pParse, /* Parser context */
                           Token *pType /* New type of the column */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    char type[pType->n + 1];
    int rc;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    /* Column type */
    if (pType->n == 0) {
        setError(pParse, SQLITE_MISUSE, "No type specified.");
        goto cleanup;
    }
    strncpy0(type, pType->z, sizeof(type));
    sqlite3Dequote(type);

    if ((rc = comdb2_parse_sql_type(type, (int *)&ctx->alter_column->len)) ==
        -1) {
        setError(pParse, SQLITE_MISUSE, "Invalid type specified.");
        goto cleanup;
    }
    ctx->alter_column->type = (uint8_t)rc;

    return;

cleanup:
    free_ddl_context(pParse);
    return;
}

/*
  Implementation of ALTER TABLE .. ALTER COLUMN .. SET DEFAULT .. 
 */
void comdb2AlterColumnSetDefault(
    Parse *pParse,      /* Parsing context */
    Expr *pExpr,        /* The parsed expression of the default value */
    const char *zStart, /* Start of the default value text */
    const char *zEnd /* First character past end of defaut value text */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    assert(ctx->alter_column);
    comdb2ColumnSetDefault(pParse, ctx->alter_column, pExpr, zStart, zEnd);
    return;
}

/*
  Implementation of ALTER TABLE .. ALTER COLUMN .. DROP DEFAULT .. 
 */
void comdb2AlterColumnDropDefault(Parse *pParse /* Parser context */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    assert(ctx->alter_column);
    ctx->alter_column->def = 0;
    return;
}

void comdb2AlterColumnDropAutoIncrement(Parse *pParse /* Parser context */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    assert(ctx->alter_column);
    ctx->alter_column->def = 0;
    return;
}

/*
  Implementation of ALTER TABLE .. ALTER COLUMN .. SET NOT NULL .. 
 */
void comdb2AlterColumnSetNotNull(Parse *pParse /* Parser context */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    assert(ctx->alter_column);
    comdb2ColumnSetNotNull(pParse, ctx->alter_column);
}

/*
  Implementation of ALTER TABLE .. ALTER COLUMN .. DROP NOT NULL .. 
 */
void comdb2AlterColumnDropNotNull(Parse *pParse /* Parser context */)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    assert(ctx->alter_column);
    comdb2ColumnSetNull(pParse, ctx->alter_column);
}

void comdb2SchemachangeControl(Parse *pParse, int action, Token *nm, Token *lnm)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    Vdbe *v = sqlite3GetVdbe(pParse);
    char *t_action = NULL;

    struct schema_change_type *sc = new_schemachange_type();

    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    sc->preempted = action;

    if (chkAndCopyTableTokens(pParse, sc->tablename, nm, lnm,
                              ERROR_ON_TBL_NOT_FOUND, 1, 0, NULL))
        goto out;

    if (authenticateSC(sc->tablename, pParse))
        goto out;

    tran_type *tran = curtran_gettran();
    int rc = get_csc2_file_tran(sc->tablename, -1, &sc->newcsc2, NULL, tran);
    curtran_puttran(tran);
    if (rc) {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: %s\n", __func__,
               sc->tablename);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }
    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    if (t_action)
        free(t_action);
    free_schema_change_type(sc);
}

void comdb2AddCheckConstraint(Parse *pParse,      /* Parsing context */
                              Expr *pCheckExpr,   /* The check expression */
                              const char *zStart, /* Start of CHECK expr text */
                              const char *zEnd    /* End of CHECK expr text */
)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

    if (gbl_legacy_schema) {
        setError(pParse, SQLITE_ERROR, "CHECK CONSTRAINT not enabled");
        return;
    }

    struct comdb2_constraint *constraint;
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    size_t check_expr_sz;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3AddCheckConstraint(pParse, pCheckExpr);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    constraint = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_constraint));
    if (constraint == 0)
        goto oom;

    constraint->type = CONS_CHECK;
    /* Get CHECK expression */
    assert(pCheckExpr && zStart && zEnd);
    check_expr_sz = zEnd - zStart;
    assert(check_expr_sz > 0);
    constraint->check_expr = comdb2_strndup(ctx->mem, zStart, check_expr_sz);
    if (constraint->check_expr == 0)
        goto oom;

    /* TODO: (NC) escape quotes? */

    if ((set_constraint_name(pParse, constraint)) != 0) {
        goto cleanup;
    }

    /* Add this new constraint to the list. */
    listc_abl(&ctx->schema->constraint_list, constraint);

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    sqlite3ExprDelete(pParse->db, pCheckExpr);
    free_ddl_context(pParse);
    return;
}

/*
  Implementation of CREATE VIEW
 */
void comdb2_create_view(Parse *pParse, const char *view_name, int view_name_len,
                        const char *zStmt, int temp)
{
    if (gbl_view_feature == 0) {
        setError(pParse, SQLITE_MISUSE, "VIEWs support not enabled");
        return;
    }

    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_CREATE_VIEW, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v = sqlite3GetVdbe(pParse);

    if (temp) {
        setError(pParse, SQLITE_MISUSE, "Can't create temporary views");
        return;
    }

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (view_name_len >= MAXTABLELEN) {
        setError(pParse, SQLITE_MISUSE, "View name is too long");
        goto out;
    } else {
        memcpy(sc->tablename, view_name, view_name_len);
    }

    sc->newcsc2 = strdup(zStmt); /* Freed by free_schema_change_type() */
    if (sc->newcsc2 == NULL) {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto out;
    }

    sc->kind = SC_ADD_VIEW;
    sc->nothrevent = 1;
    sc->live = 1;
    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
    return;
}

/*
  Implementation of DROP VIEW
 */
void comdb2_drop_view(Parse *pParse, SrcList *pName)
{
    if (comdb2IsPrepareOnly(pParse))
        return;

#ifndef SQLITE_OMIT_AUTHORIZATION
    {
        if( sqlite3AuthCheck(pParse, SQLITE_DROP_VIEW, 0, 0, 0) ){
            setError(pParse, SQLITE_AUTH, COMDB2_NOT_AUTHORIZED_ERRMSG);
            return;
        }
    }
#endif

    Vdbe *v = sqlite3GetVdbe(pParse);

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    sc->tablename_len = strlen(pName->a[0].zName);
    if (sc->tablename_len >= MAXTABLELEN) {
        setError(pParse, SQLITE_MISUSE, "View name is too long");
        goto out;
    }
    memcpy(sc->tablename, pName->a[0].zName, sc->tablename_len);

    sc->kind = SC_DROP_VIEW;
    sc->nothrevent = 1;
    sc->live = 1;
    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

/* delete entry by seed from comdb2_sc_history 
 * called from stored procedure function db_comdb_delete_sc_history()
 */
int comdb2DeleteFromScHistory(char *tablename, uint64_t seed)
{
    BpfuncArg arg = {{0}};
    bpfunc_arg__init(&arg);

    BpfuncDeleteFromScHistory tblseed = {{0}};
    bpfunc_delete_from_sc_history__init(&tblseed);

    arg.tblseed = &tblseed;
    arg.type = BPFUNC_DELETE_FROM_SC_HISTORY;
    tblseed.tablename = tablename;
    tblseed.seed = seed;
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = get_sql_clnt();
    int rc = 0;
    if(!clnt->intrans) {
        if ((rc = osql_sock_start(clnt, OSQL_SOCK_REQ, 0)) == 0)
            clnt->intrans = 1;
    }
    if (!rc)
        rc = osql_bpfunc_logic(thd, &arg);
    return rc;
}

static struct comdb2_partition *_get_partition(Parse* pParse, int remove)
{
    if (comdb2IsPrepareOnly(pParse))
        return NULL;
    
    struct comdb2_ddl_context *ctx;

    ctx = create_ddl_context(pParse);
    if (ctx == 0)
        goto oom;

    if (ctx->partition) {
        setError(pParse, SQLITE_ERROR, "Only one partitioning operation per txn supported");
        goto cleanup;
    }

    if (ctx->partition_first_shardname && !remove) {
        setError(pParse, SQLITE_ERROR, "Partition already exists");
        goto cleanup;
    }

    ctx->partition = calloc(1, sizeof(struct comdb2_partition));
    if (!ctx->partition) {
        goto oom;
    }

    return ctx->partition;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return NULL;
}

/**
 * Create Time Partition v2 (schema based)
 *
 */
void comdb2CreateTimePartition(Parse* pParse, Token* period, Token* retention,
                               Token* start)
{
    struct comdb2_partition *partition;

    if (!gbl_partitioned_table_enabled) {
        setError(pParse, SQLITE_ABORT, "Create partitioned table not enabled");
        return;
    }

    partition = _get_partition(pParse, 0);
    if (!partition)
        return;

    partition->type = PARTITION_ADD_TIMED;

    if (comdb2GetTimePartitionParams(pParse, period, retention, start,
                                     (int32_t*)&partition->u.tpt.period,
                                     (int32_t*)&partition->u.tpt.retention,
                                     (int64_t*)&partition->u.tpt.start)) {
        free_ddl_context(pParse);
    }
}

/**
 * Create Manual Partition
 *
 */
void comdb2CreateManualPartition(Parse *pParse, Token *retention, Token *start)
{
    struct comdb2_partition *partition;

    if (!gbl_partitioned_table_enabled) {
        setError(pParse, SQLITE_ABORT, "Create manual partitioned table not enabled");
        return;
    }

    partition = _get_partition(pParse, 0);
    if (!partition)
        return;

    partition->type = PARTITION_ADD_MANUAL;
    partition->u.tpt.period = VIEW_PARTITION_MANUAL;

    int32_t tmp = 0;
    if (comdb2GetManualPartitionParams(pParse, retention, start,
                                     (int32_t*)&partition->u.tpt.retention,
                                     &tmp)) {
        free_ddl_context(pParse);
    }
    partition->u.tpt.start = tmp;
}

/*
 * Mark the partition for merging, with or without a table to be merged in
 * If the table to be merged in is provided, its data will be moved to target
 * and the source will be dropped
 * If the table is not provided, if the target is a partition, merge it into a
 * standalone table; otherwise operation is NOP
 */
void comdb2SaveMergeTable(Parse *pParse, Token *name, Token *database, int alter)
{
    struct comdb2_partition *partition;

    if (!gbl_merge_table_enabled) {
        setError(pParse, SQLITE_ABORT, "Merge table not enabled");
        return;
    }

    partition = _get_partition(pParse, alter);
    if (!partition)
        return;

    partition->type = PARTITION_MERGE;

    if  (!name)
        return;

    char *partition_first_shardname = NULL;

    if (chkAndCopyTableTokens(pParse, partition->u.mergetable.tablename, name, database,
                              ERROR_ON_TBL_NOT_FOUND, 1, NULL, &partition_first_shardname)) {
        return;                             
    }

    if (partition_first_shardname) {
        setError(pParse, SQLITE_ABORT, "Merge partition not supported yet");
    }

    partition->u.mergetable.version = comdb2_table_version(partition->u.mergetable.tablename);

    free(partition_first_shardname);
}

#include <default_consumer_v1.h>

void create_default_consumer_sp(Parse *p, char *spname)
{
    Vdbe *v = sqlite3GetVdbe(p);
    struct schema_change_type *sc;
    const char *version = "comdb2 default consumer 1.0";

    sc = new_schemachange_type();
    sc->kind = SC_ADDSP;
    strcpy(sc->tablename, spname);
    strcpy(sc->fname, version);
    sc->newcsc2 = strdup(default_consumer_v1);
    comdb2PrepareSC(v, p, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)&free_schema_change_type);

    sc = new_schemachange_type();
    sc->kind = SC_DEFAULTSP;
    strcpy(sc->tablename, spname);
    strcpy(sc->fname, version);
    comdb2prepareNoRows(v, p, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)&free_schema_change_type);

}
