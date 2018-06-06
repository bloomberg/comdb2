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
#include "cdb2_constants.h"

#define INCLUDE_KEYWORDHASH_H
#define INCLUDE_FINALKEYWORD_H
#include <keywordhash.h>

extern pthread_key_t query_info_key;
extern int gbl_commit_sleep;
extern int gbl_convert_sleep;
extern int gbl_check_access_controls;
extern int gbl_allow_user_schema;
extern int gbl_ddl_cascade_drop;

/******************* Utility ****************************/

static inline int setError(Parse *pParse, int rc, const char *msg)
{
    pParse->rc = rc;
    sqlite3ErrorMsg(pParse, "%s", msg);
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

static inline int chkAndCopyTable(Parse *pParse, char *dst, const char *name,
                                  size_t max_length, int mustexist)
{
    char tmp_dst[MAXTABLELEN];
    struct sql_thread *thd =pthread_getspecific(query_info_key);
    /* Remove quotes if any. */
    if ((name[0] == '\'') && (name[max_length-2] == '\'')) {
      strncpy(tmp_dst, name+1, max_length-2);
      /* Guarantee null termination. */
      tmp_dst[max_length - 3] = '\0';
    } else {
      strncpy(tmp_dst, name, max_length);
      /* Guarantee null termination. */
      tmp_dst[max_length - 1] = '\0';
    }

    if(gbl_allow_user_schema && thd->clnt->user[0] != '\0' && strcasecmp(thd->clnt->user,DEFAULT_USER) != 0) {
        char* username = strstr(tmp_dst, "@");
        if (username) {
            /* Do nothing. */
            strncpy(dst, tmp_dst, MAXTABLELEN);
        } else { /* Add usernmame. */
            /* Make it part of user schema. */
            char userschema[MAXTABLELEN];
            int bdberr; 
            bdb_state_type *bdb_state = thedb->bdb_env;
            if (bdb_tbl_access_userschema_get(bdb_state, NULL, thd->clnt->user, userschema, &bdberr) == 0) {
              if (userschema[0] == '\0') {
                snprintf(dst, MAXTABLELEN, "%s", tmp_dst);
              } else {
                snprintf(dst, MAXTABLELEN, "%s@%s", tmp_dst, userschema);
              }
            } else {
              snprintf(dst, MAXTABLELEN, "%s@%s",tmp_dst, thd->clnt->user);
            }
        }
    } else {
       strncpy(dst, tmp_dst, MAXTABLELEN);
    }
    
    if(!timepart_is_timepart(dst, 1))
    {
        struct dbtable *db = get_dbtable_by_name(dst);

        if (db == NULL && mustexist)
        {
            setError(pParse, SQLITE_ERROR, "Table not found");
            return SQLITE_ERROR;
        }

        if (db != NULL && !mustexist)
        {
            setError(pParse, SQLITE_ERROR, "Table already exists");
            return SQLITE_ERROR;
        }

        if (timepart_is_shard(dst, 1))
        {
            setError(pParse, SQLITE_ERROR, "Shards cannot be schema changed independently");
            return SQLITE_ERROR;
        }

        if (db) {
            /* use original tablename */
            strncpy(dst, db->tablename, MAXTABLELEN);
        }
    }
    else
    {
        /* maybe mark it */
    }

    return SQLITE_OK;
}

static inline int create_string_from_token(Vdbe* v, Parse* pParse, char** dst, Token* t)
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

static inline int copyNosqlToken(Vdbe* v, Parse *pParse, char** buf,
    Token *t)
{
    if (*buf == NULL)    
        *buf = (char*) malloc((t->n));

    if (*buf == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return SQLITE_NOMEM;
    }

    if (t->n < 3)
    {
        **buf = '\0';
    } else
    {
        strncpy(*buf, t->z + 1,t->n - 2);
        (*buf)[t->n - 2] = '\0';
    }
    
    return SQLITE_OK;
}

static inline int chkAndCopyTableTokens(Vdbe *v, Parse *pParse, char *dst,
                                        Token *t1, Token *t2, int mustexist)
{
    int rc;
    int max_size;

    if (t1 == NULL)
    {
        return SQLITE_OK;
    }

    /* Check for remote request only if both the tokens are set. */
    if (t2 && (rc = isRemote(pParse, &t1, &t2))) {
        return rc;
    }

    if (t1->n + 1 <= MAXTABLELEN)
        max_size = t1->n + 1;
    else
        return setError(pParse, SQLITE_MISUSE, "Tablename is too long");

    if ((rc = chkAndCopyTable(pParse, dst, t1->z, max_size, mustexist)))
        return rc;

    return SQLITE_OK;
}

static void fillTableOption(struct schema_change_type* sc, int opt)
{
    if (OPT_ON(opt, ODH_OFF))
        sc->headers = 0;
    else 
        sc->headers = 1;

    if (OPT_ON(opt, IPU_OFF))
        sc->ip_updates = 0;
    else
        sc->ip_updates = 1;

    if (OPT_ON(opt, ISC_OFF))
        sc->instant_sc = 0;
    else
        sc->instant_sc = 1;

    sc->compress_blobs = -1;
    if (OPT_ON(opt, BLOB_NONE))
        sc->compress_blobs = BDB_COMPRESS_NONE;
    else if (OPT_ON(opt, BLOB_RLE))
        sc->compress_blobs = BDB_COMPRESS_RLE8;
    else if (OPT_ON(opt, BLOB_ZLIB))
        sc->compress_blobs = BDB_COMPRESS_ZLIB;
    else if (OPT_ON(opt, BLOB_LZ4))
        sc->compress_blobs = BDB_COMPRESS_LZ4;

    sc->compress = -1;
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

    if (OPT_ON(opt, FORCE_REBUILD))
        sc->force_rebuild = 1;
    else
        sc->force_rebuild = 0;

    if (OPT_ON(opt, PAGE_ORDER))
        sc->scanmode = SCAN_PAGEORDER;

    if (OPT_ON(opt, READ_ONLY))
        sc->live = 0;
    else
        sc->live = 1;

    sc->commit_sleep = gbl_commit_sleep;
    sc->convert_sleep = gbl_convert_sleep;
}

int comdb2PrepareSC(Vdbe *v, Parse *pParse, int int_arg,
                    struct schema_change_type *arg, vdbeFunc func,
                    vdbeFuncArgFree freeFunc)
{
    comdb2WriteTransaction(pParse);
    Table *t = sqlite3LocateTable(pParse, LOCATE_NOERR, arg->table, NULL);
    if (t) {
        sqlite3VdbeAddTable(v, t);
    }
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    thd->clnt->verifyretry_off = 1;
    return comdb2prepareNoRows(v, pParse, int_arg, arg, func, freeFunc);
}

int comdb2AuthenticateUserDDL(Vdbe* v, const char *tablename, Parse* pParse)
{
     struct sql_thread *thd = pthread_getspecific(query_info_key);
     bdb_state_type *bdb_state = thedb->bdb_env;
     int bdberr; 
     int authOn = bdb_authentication_get(bdb_state, NULL, &bdberr); 
    
     if (authOn != 0)
        return SQLITE_OK;

     if (thd->clnt && tablename)
     {
        if (bdb_tbl_op_access_get(bdb_state, NULL, 0, 
            tablename, thd->clnt->user, &bdberr))
          return SQLITE_AUTH;
        else
            return SQLITE_OK;
     }

     return SQLITE_AUTH;
}


int comdb2AuthenticateUserOp(Vdbe* v, Parse* pParse)
{
     char tablename[MAXTABLELEN] = {0};
     if (comdb2AuthenticateUserDDL(v, tablename, pParse))
         setError(pParse, SQLITE_AUTH, "User does not have OP credentials");
     else
         return SQLITE_OK;

     return SQLITE_AUTH;
}

/* Only an op user can turn authentication on. */
static int comdb2AuthenticateOpPassword(Vdbe* v, Parse* pParse)
{
     char tablename[MAXTABLELEN] = {0};

     struct sql_thread *thd = pthread_getspecific(query_info_key);
     bdb_state_type *bdb_state = thedb->bdb_env;
     int bdberr; 

     if (thd->clnt)
     {
         /* Authenticate the password first, as we haven't been doing it so far. */
         struct sqlclntstate *s = thd->clnt;
         if (bdb_user_password_check(s->user, s->password, NULL))
         {
            return SQLITE_AUTH;
         }
         
         /* Check if the user is OP user. */
         if (bdb_tbl_op_access_get(bdb_state, NULL, 0, 
             tablename, thd->clnt->user, &bdberr))
             return SQLITE_AUTH;
         else
             return SQLITE_OK;
     }

     return SQLITE_AUTH;
}

int comdb2SqlDryrunSchemaChange(OpFunc *f)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct schema_change_type *s = (struct schema_change_type*)f->arg;

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

    /*
    */
    osqlstate_t *osql = &thd->clnt->osql;
    osql->xerr.errval = 0;
    f->errorMsg = osql->xerr.errstr;
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

static int comdb2SqlSchemaChange(OpFunc *f)
{
    return comdb2SqlSchemaChange_int(f, 0);
}

int comdb2SqlSchemaChange_tran(OpFunc *f)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct sqlclntstate *clnt = thd->clnt;
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
    rc = osql_sock_commit(clnt, OSQL_SOCK_REQ);
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

int authenticateSC(const char * table,  Parse *pParse) 
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    char *username = strstr(table, "@");
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (username && strcmp(username+1, thd->clnt->user) == 0) {
        return 0;
    } else if (comdb2AuthenticateUserDDL(v, table, pParse) == 0) {
        return 0;
    } else if (comdb2AuthenticateUserOp(v, pParse) == 0) {
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
    Vdbe *v  = sqlite3GetVdbe(pParse);
    if (temp) {
        setError(pParse, SQLITE_MISUSE, "Can't create temporary csc2 table");
        return;
    }
    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if ((isRemote(pParse, &pName1, &pName2))) {
        return;
    }

    TokenStr(table, pName1);
    if (noErr && get_dbtable_by_name(table))
        goto out;

    if (chkAndCopyTableTokens(v, pParse, sc->table, pName1, pName2, 0))
        goto out;

    if (authenticateSC(sc->table, pParse))
        goto out;

    sc->addonly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    fillTableOption(sc, opt);
    copyNosqlToken(v, pParse, &sc->newcsc2, csc2);
    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

void comdb2AlterTableCSC2(
  Parse *pParse,   /* Parser context */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int opt,         /* Various options for alter (compress, etc) */
  Token *csc2,
  int dryrun
)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse,sc->table, pName1, pName2, 1))
        goto out;

    if (authenticateSC(sc->table, pParse))
        goto out;

    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    sc->dryrun = dryrun;
    fillTableOption(sc, opt);
    copyNosqlToken(v, pParse, &sc->newcsc2, csc2);
    if(dryrun)
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

    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);

    struct schema_change_type* sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTable(pParse, sc->table, pName->a[0].zName, MAXTABLELEN, 1))
        goto out;

    if (authenticateSC(sc->table, pParse))
        goto out;

    sc->same_schema = 1;
    sc->drop_table = 1;
    sc->fastinit = 1;
    sc->nothrevent = 1;
    
    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL )) {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: %s\n", __func__,
               sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }

    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

static inline void comdb2Rebuild(Parse *pParse, Token* nm, Token* lnm, int opt)
{
    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);

    struct schema_change_type* sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse,sc->table, nm, lnm, 1))
        goto out;

    if (authenticateSC(sc->table, pParse))
        goto out;

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

    if (OPT_ON(opt, PAGE_ORDER))
        sc->scanmode = SCAN_PAGEORDER;

    if (OPT_ON(opt, READ_ONLY))
        sc->live = 0;
    else
        sc->live = 1;

    sc->commit_sleep = gbl_commit_sleep;
    sc->convert_sleep = gbl_convert_sleep;

    sc->same_schema = 1;
    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL ))
    {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: %s\n", __func__,
               sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }
    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}


void comdb2RebuildFull(Parse* p, Token* nm,Token* lnm, int opt)
{
    comdb2Rebuild(p, nm,lnm, REBUILD_ALL + REBUILD_DATA + REBUILD_BLOB + opt);
}


void comdb2RebuildData(Parse* p, Token* nm, Token* lnm, int opt)
{
    comdb2Rebuild(p,nm,lnm,REBUILD_DATA + opt);
}

void comdb2RebuildDataBlob(Parse* p,Token* nm, Token* lnm, int opt)
{
    comdb2Rebuild(p, nm, lnm, REBUILD_BLOB + opt);
}

void comdb2Truncate(Parse* pParse, Token* nm, Token* lnm)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse,sc->table, nm, lnm, 1))
        goto out;

    if (authenticateSC(sc->table, pParse))
        goto out;

    sc->fastinit = 1;
    sc->nothrevent = 1;
    sc->same_schema = 1;

    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL ))
    {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: %s\n", __func__,
               sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }
    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}


void comdb2RebuildIndex(Parse* pParse, Token* nm, Token* lnm, Token* index, int opt)
{
    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);
    char* indexname;
    int index_num;

    struct schema_change_type* sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v,pParse,sc->table, nm, lnm, 1))
        goto out;

    if (authenticateSC(sc->table, pParse))
        goto out;

    sc->same_schema = 1;
    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL )) {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: %s\n", __func__,
               sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }

    if (create_string_from_token(v, pParse, &indexname, index))
        goto out; // TODO RETURN ERROR

    int rc = getidxnumbyname(sc->table, indexname, &index_num );
    if( rc ){
        logmsg(LOGMSG_ERROR, "!table:index '%s:%s' not found\n", sc->table, indexname);
        setError(pParse, SQLITE_ERROR, "Index not found");
        goto out;
    }

    free(indexname);

    sc->nothrevent = 1;
    sc->live = 1;
    sc->rebuild_index = 1;
    sc->index_to_rebuild = index_num;
    sc->scanmode = gbl_default_sc_scanmode;

    if (OPT_ON(opt, PAGE_ORDER))
        sc->scanmode = SCAN_PAGEORDER;

    if (OPT_ON(opt, READ_ONLY))
        sc->live = 0;
    else
        sc->live = 1;

    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

/********************** STORED PROCEDURES ****************************************/

void comdb2CreateProcedure(Parse* pParse, Token* nm, Token* ver, Token* proc)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    if (comdb2AuthenticateUserOp(v, pParse))
        return;     
    TokenStr(name, nm);
    if (strlen(name) >= MAX_SPNAME) {
        sqlite3ErrorMsg(pParse, "bad procedure name:%s", name);
        return;
    }
    struct schema_change_type* sc = new_schemachange_type();
    strcpy(sc->table, name);
    sc->newcsc2 = malloc(proc->n);
    sc->addsp = 1;
    if (ver) {
        TokenStr(version, ver);
        size_t len = strlen(version);
        if (len == 0 || len >= MAX_SPVERSION_LEN) {
            sqlite3ErrorMsg(pParse, "bad procedure version:%s", version);
            free_schema_change_type(sc);
            return;
        }
        strcpy(sc->fname, version);
    }
    copyNosqlToken(v, pParse, &sc->newcsc2, proc);
    const char* colname[] = {"version"};
    const int coltype = OPFUNC_STRING_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 256};
    comdb2prepareOpFunc(v, pParse, 1, sc, &comdb2ProcSchemaChange, (vdbeFuncArgFree)&free_schema_change_type, &stp);
}

void comdb2DefaultProcedure(Parse* pParse, Token* nm, Token* ver, int str)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    if (comdb2AuthenticateUserOp(v, pParse))
        return;     
    TokenStr(name, nm);
    if (strlen(name) >= MAX_SPNAME) {
        sqlite3ErrorMsg(pParse, "bad procedure name:%s", name);
        return;
    }
    struct schema_change_type* sc = new_schemachange_type();
    strcpy(sc->table, name);
    if (str) {
        TokenStr(version, ver);
        size_t len = strlen(version);
        if (len == 0 || len >= MAX_SPVERSION_LEN) {
            sqlite3ErrorMsg(pParse, "bad procedure version:%s", version);
            free_schema_change_type(sc);
            return;
        }
        strcpy(sc->fname, version);
    } else {
        sc->newcsc2 = malloc(ver->n + 1);
        strncpy(sc->newcsc2, ver->z, ver->n);
        sc->newcsc2[ver->n] = '\0';
    }
    sc->defaultsp = 1;

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, 
                        (vdbeFuncArgFree)  &free_schema_change_type);
}

void comdb2DropProcedure(Parse* pParse, Token* nm, Token* ver, int str)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    int rc;   
    struct schema_change_type* sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }
    int max_length = nm->n < MAXTABLELEN ? nm->n : MAXTABLELEN;

    if (comdb2AuthenticateUserOp(v, pParse)) {
        free_schema_change_type(sc);
        return;       
    }

    strncpy(sc->table, nm->z, max_length);
    
    
    if (str) {
        TokenStr(version, ver);
        strcpy(sc->fname, version);
    } else {
        sc->newcsc2 = malloc(ver->n + 1);
        strncpy(sc->newcsc2, ver->z, ver->n);
        sc->newcsc2[ver->n] = '\0';
    }
    sc->delsp = 1;
  
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange_tran, 
                        (vdbeFuncArgFree)  &free_schema_change_type);
}
/********************* PARTITIONS  **********************************************/


void comdb2CreateTimePartition(Parse* pParse, Token* table, Token* partition_name, Token* period, Token* retention, Token* start)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    int max_length;

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    if (!arg) goto err; 
    bpfunc_arg__init(arg);

    BpfuncCreateTimepart *tp = malloc(sizeof(BpfuncCreateTimepart));
    if (!tp) goto err;
    bpfunc_create_timepart__init(tp);
    
    arg->crt_tp = tp;
    arg->type = BPFUNC_CREATE_TIMEPART;
    tp->tablename = (char*) malloc(MAXTABLELEN);
    memset(tp->tablename, '\0', MAXTABLELEN);
    if (table && chkAndCopyTableTokens(v, pParse, tp->tablename, table, NULL, 1)) 
        goto err;


    max_length = partition_name->n < MAXTABLELEN ? partition_name->n : MAXTABLELEN;
    tp->partition_name = (char*) malloc(MAXTABLELEN);
    memset(tp->partition_name, '\0', MAXTABLELEN);
    strncpy(tp->partition_name, partition_name->z, max_length);

    char period_str[50];
    memset(period_str, '\0', sizeof(period_str));

    assert (*period->z == '\'' || *period->z == '\"');
    period->z++;
    period->n -= 2;
    
    max_length = period->n < 50 ? period->n : 50;
    strncpy(period_str, period->z, max_length);
    tp->period = name_to_period(period_str);
    
    if (tp->period == VIEW_TIMEPART_INVALID) {
        setError(pParse, SQLITE_ERROR, "Invalid period name");
        goto clean_arg;
    }

    char retention_str[10];
    memset(retention_str, '\0', sizeof(retention_str));
    max_length = retention->n < 10 ? retention->n : 10;
    strncpy(retention_str, retention->z, max_length);
    tp->retention = atoi(retention_str);

    char start_str[200];
    memset(start_str,0, sizeof(start_str));
    
    assert (*start->z == '\'' || *start->z == '\"');
    start->z++;
    start->n -= 2;

    max_length = start->n < 200 ? start->n : 200;
    strncpy(start_str, start->z, max_length);
    tp->start = convert_time_string_to_epoch(start_str);

    if (tp->start == -1 ) {
        setError(pParse, SQLITE_ERROR, "Invalid start date");
        goto clean_arg;
    }

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);
    return;

err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);   
}


void comdb2DropTimePartition(Parse* pParse, Token* partition_name)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    int max_length;

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    if (!arg) goto err; 
    bpfunc_arg__init(arg);
    
    BpfuncDropTimepart *tp = malloc(sizeof(BpfuncDropTimepart));
    if (!tp) goto err;
    bpfunc_drop_timepart__init(tp);
    
    arg->drop_tp = tp;
    arg->type = BPFUNC_DROP_TIMEPART;
    max_length = partition_name->n < MAXTABLELEN ? partition_name->n : MAXTABLELEN;
    tp->partition_name = (char*) malloc(MAXTABLELEN);
    memset(tp->partition_name, '\0', MAXTABLELEN);
    strncpy(tp->partition_name, partition_name->z, max_length);

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if(arg)
        free_bpfunc_arg(arg);
}


/********************* BULK IMPORT ***********************************************/

void comdb2bulkimport(Parse* pParse, Token* nm,Token* lnm, Token* nm2, Token* lnm2)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

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
    Vdbe *v  = sqlite3GetVdbe(pParse);
    int percentage = pc;
    int threads = GET_ANALYZE_THREAD(opt);
    int sum_threads = GET_ANALYZE_SUMTHREAD(opt);
  
    if (comdb2AuthenticateUserOp(v, pParse))
        return;       
  
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

        if (chkAndCopyTableTokens(v, pParse, tablename, nm, lnm, 1)) {
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

void comdb2analyzeCoverage(Parse* pParse, Token* nm, Token* lnm, int newscale)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    if (comdb2AuthenticateUserOp(v, pParse))
        return;

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
        
    if (chkAndCopyTableTokens(v, pParse, ancov_f->tablename, nm, lnm, 1)) 
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
    Vdbe *v  = sqlite3GetVdbe(pParse);
    if (comdb2AuthenticateUserOp(v, pParse))
        return;

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
        
    if (chkAndCopyTableTokens(v, pParse, ancov_f->tablename, nm, lnm, 1)) 
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
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);   
}

void comdb2enableRowlocks(Parse* pParse, int enable)
{
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
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);   
}

void comdb2analyzeThreshold(Parse* pParse, Token* nm, Token* lnm, int newthreshold)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    if (comdb2AuthenticateUserOp(v, pParse))
        return;

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
        
    if (chkAndCopyTableTokens(v, pParse, anthr_f->tablename, nm, lnm, 1)) 
        return;  
    
    anthr_f->newvalue = newthreshold;
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, 
                        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);
}

/********************* ALIAS **************************************************/

void comdb2setAlias(Parse* pParse, Token* name, Token* url)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        return;       

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

    if (chkAndCopyTableTokens(v, pParse, alias_f->name, name, NULL, 0))
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
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        return;       

    setError(pParse, SQLITE_INTERNAL, "Not Implemented");
    logmsg(LOGMSG_INFO, "Getting alias %.*s", t1->n, t1->z); 
}

/********************* GRANT AUTHORIZAZIONS ************************************/

void comdb2grant(Parse* pParse, int revoke, int permission, Token* nm,Token* lnm, Token* u)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
        return;  

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
    } else if (chkAndCopyTableTokens(v, pParse, grant->table, nm, lnm, 1)) {
        goto clean_arg;
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
    Vdbe *v  = sqlite3GetVdbe(pParse);

    int rc = SQLITE_OK;
 
    if (comdb2AuthenticateOpPassword(v, pParse)) 
    {
        setError(pParse, SQLITE_AUTH, "User does not have OP credentials");
        return;
    }

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
  
    BpfuncPassword * password = (BpfuncPassword*) malloc(sizeof(BpfuncPassword));
    
    if (password)
    {
        bpfunc_password__init(password);
    } else
    {
        setError(pParse, SQLITE_NOMEM, "Out of Memory");
        goto clean_arg;
    }

    arg->pwd = password;
    arg->type = BPFUNC_PASSWORD;
    password->disable = 0;
  
    if (create_string_from_token(v, pParse, &password->user, nm) ||
        create_string_from_token(v, pParse, &password->password, pwd))
            goto clean_arg;

    if (comdb2AuthenticateUserDDL(v, "", pParse))
    {
        struct sql_thread *thd = pthread_getspecific(query_info_key);
        /* Check if its password change request */
        if (!(thd && thd->clnt &&
                   strcmp(thd->clnt->user, password->user) == 0 )) {
            setError(pParse, SQLITE_AUTH, "User does not have OP credentials");
            return;
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
    Vdbe *v  = sqlite3GetVdbe(pParse);

    if (comdb2AuthenticateUserOp(v, pParse))
    {
        setError(pParse, SQLITE_AUTH, "User does not have OP credentials");
        return;
    }


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

    for (int i=0; i < SQLITE_N_KEYWORD; i++)
    {
        if ((f->int_arg == KW_ALL) ||
            (f->int_arg == KW_RES && f_keywords[i].reserved) ||
            (f->int_arg == KW_FB && !f_keywords[i].reserved))
                opFuncPrintf(f, "%s", f_keywords[i].name );
    }
    f->rc = SQLITE_OK;
    f->errorMsg = NULL;
    return SQLITE_OK;
}

void comdb2getkw(Parse* pParse, int arg)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Keyword"};
    const int coltype = OPFUNC_STRING_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, SQLITE_KEYWORD_LEN};
    comdb2prepareOpFunc(v, pParse, arg, NULL, &producekw, (vdbeFuncArgFree)  &free, &stp);

}

static int produceAnalyzeCoverage(OpFunc *f)
{

    char  *tablename = (char*) f->arg;
    int rst;
    int bdberr; 
    int rc = bdb_get_analyzecoverage_table(NULL, tablename, &rst, &bdberr);
    
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
    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Coverage"};
    const int coltype = OPFUNC_INT_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 256};
    char *tablename = (char*) malloc (MAXTABLELEN);

    if (chkAndCopyTableTokens(v, pParse, tablename, nm, lnm, 1)) 
        free(tablename);
    else
        comdb2prepareOpFunc(v, pParse, 0, tablename, &produceAnalyzeCoverage, 
                            (vdbeFuncArgFree)  &free, &stp);
}

static int produceAnalyzeThreshold(OpFunc *f)
{

    char  *tablename = (char*) f->arg;
    long long int rst;
    int bdberr; 
    int rc = bdb_get_analyzethreshold_table(NULL, tablename, &rst, &bdberr);
    
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
    Vdbe *v  = sqlite3GetVdbe(pParse);
    const char* colname[] = {"Threshold"};
    const int coltype = OPFUNC_INT_TYPE;
    OpFuncSetup stp = {1, colname, &coltype, 256};
    char *tablename = (char*) malloc (MAXTABLELEN);

    if (chkAndCopyTableTokens(v, pParse, tablename, nm, lnm, 1)) goto clean;
    
    comdb2prepareOpFunc(v, pParse, 0, tablename, &produceAnalyzeThreshold, (vdbeFuncArgFree)  &free, &stp);

    return;

clean:
    free(tablename);
}

//holymoly!!
void resolveTableName(struct SrcList_item *p, const char *zDB, char *tableName)
{
   struct sql_thread *thd = pthread_getspecific(query_info_key);
   if ((zDB && (!strcasecmp(zDB, "main") || !strcasecmp(zDB, "temp"))))
   {
       sprintf(tableName, "%s", p->zName);
   } else if (thd->clnt && (thd->clnt->user[0] != '\0') && !strstr(p->zName, "@")
          && strncasecmp(p->zName, "sqlite_", 7) && strncasecmp(p->zName, "comdb2", 6))
   {
       char userschema[MAXTABLELEN];
       int bdberr; 
       bdb_state_type *bdb_state = thedb->bdb_env;
       if (bdb_tbl_access_userschema_get(bdb_state, NULL, thd->clnt->user, userschema, &bdberr) == 0) {
         if (userschema[0] == '\0') {
           snprintf(tableName, MAXTABLELEN, "%s", p->zName);
         } else {
           snprintf(tableName, MAXTABLELEN, "%s@%s", p->zName, userschema);
         }
       } else {
         snprintf(tableName, MAXTABLELEN, "%s@%s", p->zName, thd->clnt->user);
       }
   } else {
       sprintf(tableName, "%s", p->zName);
   }
}


void comdb2timepartRetention(Parse *pParse, Token *nm, Token *lnm, int retention)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);


    if (comdb2AuthenticateUserOp(v, pParse))
        goto err;       

    if (retention < 2)
    {
        setError(pParse, SQLITE_ERROR, "Retention must be 2 or higher");
        goto clean_arg;
    }

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
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
        
    if (chkAndCopyTableTokens(v, pParse, tp_retention->timepartname, nm, lnm, 1)) 
        return;  
    
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


void sqlite3AlterRenameTable(Parse *pParse, Token *pSrcName, Token *pName,
        int dryrun)
{
    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);
    struct schema_change_type *sc;

    TokenStr(table, pSrcName);
    TokenStr(newtable, pName);

    if(get_dbtable_by_name(newtable)) {
        setError(pParse, SQLITE_ERROR, "New table name exists");
        return;
    }

    sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse, sc->table, pSrcName, NULL, 1))
        goto out;

    if (authenticateSC(sc->table, pParse))
        goto out;


    comdb2WriteTransaction(pParse);
    sc->nothrevent = 1;
    sc->live = 1;
    sc->rename = 1;
    strncpy(sc->newtable, newtable, sizeof(sc->newtable));

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb, (vdbeFuncArgFree) &free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

void comdb2schemachangeCommitsleep(Parse* pParse, int num)
{
    gbl_commit_sleep = num;
}

void comdb2schemachangeConvertsleep(Parse* pParse, int num)
{
    gbl_convert_sleep = num;
}

void comdb2WriteTransaction(Parse *pParse)
{
    pParse->write = 1;
}

/* Column flags */
enum {
    COLUMN_NO_NULL = 1 << 0,
    COLUMN_DELETED = 1 << 1,
};

struct comdb2_column {
    /* Name of the column */
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
};

struct comdb2_index_column {
    /* Column name */
    char *name;
    /* Index column flags */
    uint8_t flags;
    /* Reference to the column. */
    struct comdb2_column *column;
    /* Link */
    LINKC_T(struct comdb2_index_column) lnk;
};

/* Key flags */
enum {
    KEY_DUP = 1 << 0,
    KEY_DATACOPY = 1 << 1,
    KEY_DELETED = 1 << 2,
    KEY_UNIQNULLS = 1 << 3
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
    /* List of columns */
    LISTC_T(struct comdb2_index_column) idx_col_list;
    /* Link */
    LINKC_T(struct comdb2_key) lnk;
};

/* Constraint flags */
enum {
    CONS_UPD_CASCADE = 1 << 0,
    CONS_DEL_CASCADE = 1 << 1,
    CONS_DELETED = 1 << 2,
};

struct comdb2_constraint {
    /* Name of the constraint. */
    char *name;

    /*
       The following are helper fields to hold the column names and respective
       sort orders as specified in the query, to be later used to find the
       matching keys.
     */

    /* List of index columns in the child table. */
    LISTC_T(struct comdb2_index_column) child_idx_col_list;
    /* List of index columns in the parent table. */
    LISTC_T(struct comdb2_index_column) parent_idx_col_list;

    /* A reference to the child key */
    struct comdb2_key *child;
    /* Parent table */
    char *parent_table;
    /* Parent key name */
    char *parent_key;
    /* Constraint flags */
    uint8_t flags;

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
    LISTC_T(struct comdb2_key) key_list;
    /* Staging list of new/existing constraints */
    LISTC_T(struct comdb2_constraint) constraint_list;

    /* Link */
    LINKC_T(struct comdb2_schema) lnk;
};

/* DDL context flags */
enum { DDL_NOOP = 1 << 0, DDL_DRYRUN = 1 << 1 };

/* DDL context for CREATE/ALTER command */
struct comdb2_ddl_context {
    /* Table definition */
    struct comdb2_schema *schema;
    /* User tag definitions */
    LISTC_T(struct comdb2_schema) tag_list;
    /* Flags */
    int flags;
    /* Memory allocator. */
    comdb2ma mem;
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

/*
  Allocate Comdb2 DDL context to be used during parsing.
*/
static struct comdb2_ddl_context *create_ddl_context(Parse *pParse)
{
    struct comdb2_ddl_context *ctx;

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
        return NULL;
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
            if ((type_sql_str_len[i]) == strlen(type)) {
                *size = 0;
                return i;
            }

            if (type[type_sql_str_len[i]] != '(') {
                /* Malformed size. */
                return -1;
            }

            /* A size has been specified. */

            if (accepts_size == 0) {
                /* The type does not accept size. */
                return -1;
            }

            errno = 0;
            *size = strtol(type + type_sql_str_len[i] + 1, &endptr, 10);

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
  Convert the type and length into one that can be used directly
  in type_mapping array.
*/
static int fix_type_and_len(uint8_t *type, uint32_t *len)
{
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
    case SERVER_BYTEARRAY: /* fallthrough */
    case CLIENT_BYTEARRAY:
        *type = SQL_TYPE_BYTE;
        *len = in_len - 1;
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
    case SERVER_VUTF8: /* fallthrough */
    case CLIENT_VUTF8:
        *type = SQL_TYPE_VUTF8;
        *len = in_len - 5;
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
    case SERVER_BLOB:  /* fallthrough */
    case SERVER_BLOB2: /* fallthrough */
    case CLIENT_BLOB:  /* fallthrough */
    case CLIENT_BLOB2:
        *type = SQL_TYPE_BLOB;
        *len = in_len - 5;
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
            if ((type_flags[column->type] & FLAG_QUOTE_DEFAULT) != 0) {
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

        if ((key->flags & KEY_DATACOPY) != 0) {
            strbuf_append(csc2, "datacopy ");
        }

        if ((key->flags & KEY_UNIQNULLS) != 0) {
            strbuf_append(csc2, "uniqnulls ");
        }

        strbuf_appendf(csc2, "\"%s\" = ", key->name);

        int added = 0;
        struct comdb2_index_column *idx_column;
        LISTC_FOR_EACH(&key->idx_col_list, idx_column, lnk)
        {
            assert((idx_column->column->flags & COLUMN_DELETED) == 0);

            if (added > 0) {
                strbuf_append(csc2, "+ ");
            }
            strbuf_appendf(
                csc2, "%s%s ",
                (idx_column->flags & INDEX_ORDER_DESC) ? "<DESCEND> " : "",
                idx_column->column->name);
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
    struct comdb2_index_column *idx_column;
    char buf[16 * 1024];
    int pos = 0;
    unsigned long crc;

    /* Table name */
    SNPRINTF(buf, sizeof(buf), pos, "%s", table)

    /* DATACOPY */
    if (key->flags & KEY_DATACOPY)
        SNPRINTF(buf, sizeof(buf), pos, "%s", "DATACOPY")

    /* DUP */
    if (key->flags & KEY_DUP)
        SNPRINTF(buf, sizeof(buf), pos, "%s", "DUP")

    /* UNIQNULLS */
    if (key->flags & KEY_UNIQNULLS)
        SNPRINTF(buf, sizeof(buf), pos, "%s", "UNIQNULLS")

    LISTC_FOR_EACH(&key->idx_col_list, idx_column, lnk)
    {
        assert((idx_column->column->flags & COLUMN_DELETED) == 0);
        SNPRINTF(buf, sizeof(buf), pos, "%s", idx_column->name)
        if (idx_column->flags & INDEX_ORDER_DESC)
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

int gen_constraint_name(constraint_t *pConstraint, int parent_idx, char *out,
                        size_t out_size)
{
    char buf[3 * 1024];
    struct dbtable *table;
    struct schema *key;
    int pos = 0;
    int found = 0;

    /* Child key columns and sort orders */
    for (int i = 0; i < pConstraint->lcltable->schema->nix; i++) {
        if (strcasecmp(pConstraint->lclkeyname,
                       pConstraint->lcltable->schema->ix[i]->csctag) == 0) {
            found = 1;
            key = pConstraint->lcltable->schema->ix[i];

            for (int j = 0; j < key->nmembers; j++) {
                /* Column name */
                SNPRINTF(buf, sizeof(buf), pos, "%s", key->member[j].name)

                /* Sort order */
                if (key->member[j].flags & INDEX_DESCEND)
                    SNPRINTF(buf, sizeof(buf), pos, "%s", "DESC")
            }
            break;
        }
    }
    assert(found);

    /* Parent table name */
    SNPRINTF(buf, sizeof(buf), pos, "%s", pConstraint->table[parent_idx])

    /* Get the parent table */
    table = get_dbtable_by_name(pConstraint->table[parent_idx]);

    /* There must be a valid referenced table. */
    assert(table);

    /* Parent key columns and sort orders */
    found = 0;
    for (int i = 0; i < table->schema->nix; i++) {
        if (strcasecmp(pConstraint->keynm[parent_idx],
                       table->schema->ix[i]->csctag) == 0) {
            found = 1;
            key = table->schema->ix[i];

            for (int j = 0; j < key->nmembers; j++) {
                /* Column name */
                SNPRINTF(buf, sizeof(buf), pos, "%s", key->member[j].name)

                /* Sort order */
                if (key->member[j].flags & INDEX_DESCEND)
                    SNPRINTF(buf, sizeof(buf), pos, "%s", "DESC")
            }
            break;
        }
    }
    assert(found);

done:
    gen_constraint_name_int(buf, pos, out, out_size);

    return 0;
}

static int gen_constraint_name2(struct comdb2_constraint *constraint, char *out,
                                size_t out_size)
{
    char buf[3 * 1024];
    int pos = 0;
    struct comdb2_index_column *idx_column;

    /* Child key columns and sort orders */
    LISTC_FOR_EACH(&constraint->child_idx_col_list, idx_column, lnk)
    {
        /* Column name */
        SNPRINTF(buf, sizeof(buf), pos, "%s", idx_column->name)

        /* Sort order */
        if (idx_column->flags & INDEX_ORDER_DESC)
            SNPRINTF(buf, sizeof(buf), pos, "%s", "DESC")
    }

    /* Parent table name */
    SNPRINTF(buf, sizeof(buf), pos, "%s", constraint->parent_table)

    /* Parent key columns and sort orders */
    LISTC_FOR_EACH(&constraint->parent_idx_col_list, idx_column, lnk)
    {
        /* Column name */
        SNPRINTF(buf, sizeof(buf), pos, "%s", idx_column->name)

        /* Sort order */
        if (idx_column->flags & INDEX_ORDER_DESC)
            SNPRINTF(buf, sizeof(buf), pos, "%s", "DESC")
    }

done:
    gen_constraint_name_int(buf, pos, out, out_size);

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

static char *prepare_csc2(Parse *pParse, struct comdb2_ddl_context *ctx)
{
    char *csc2;
    struct comdb2_column *column;
    struct comdb2_index_column *child_idx_column;
    struct comdb2_index_column *key_idx_column;
    struct comdb2_key *key;
    struct comdb2_constraint *constraint;
    int key_found = 0;

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

    int pk_count = 0;
    LISTC_FOR_EACH(&ctx->schema->key_list, key, lnk)
    {
        if (key->flags & KEY_DELETED)
            continue;

        /*
          Check the properties of primary keys:
          * Unique
          * Columns must not allow NULLs
          * Must be only one per table
        */
        if (is_pk(key->name)) {
            if (++pk_count > 1) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "Multiple primary key definitions.");
                goto cleanup;
            }

            /* Primary keys mustn't be dup. */
            assert((key->flags & KEY_DUP) == 0);

            /* Also make sure none of its columns allow NULLs. (n^2) */
            struct comdb2_index_column *idx_column;
            LISTC_FOR_EACH(&key->idx_col_list, idx_column, lnk)
            {
                /* There must not be a dropped column in the key. */
                assert((idx_column->column->flags & COLUMN_DELETED) == 0);
                idx_column->column->flags |= COLUMN_NO_NULL;
            }
        }
    }

    /*
      Find the first *appropriate* non-dropped child key for this constraint.
    */
    LISTC_FOR_EACH(&ctx->schema->constraint_list, constraint, lnk)
    {
        /* Check whether the constraint has been dropped. */
        if (constraint->flags & CONS_DELETED)
            continue;

        /* Check if there's already a child key */
        if (constraint->child)
            continue;

        /* The parent table and key must have already been set by now. */
        assert(constraint->parent_table && constraint->parent_key);

        LISTC_FOR_EACH(&ctx->schema->key_list, key, lnk)
        {
            if (listc_size(&constraint->child_idx_col_list) >
                listc_size(&key->idx_col_list))
                continue;

            /* Lets start by assuming that we have found the matching key. */
            key_found = 1;

            key_idx_column = LISTC_TOP(&key->idx_col_list);

            LISTC_FOR_EACH(&constraint->child_idx_col_list, child_idx_column,
                           lnk)
            {
                if ((strcasecmp(child_idx_column->name, key_idx_column->name) !=
                     0) ||
                    (child_idx_column->flags != key_idx_column->flags)) {
                    key_found = 0;
                    break;
                }
                /* Move to the next index column in the key. */
                key_idx_column = LISTC_NEXT(key_idx_column, lnk);
            }

            if (key_found == 1) {
                constraint->child = key;
            }
        }

        /*
          Implicitly add a new DUP key if a matching local index was not found.
        */
        if (key_found == 0) {
            ExprList idx_cols;

            idx_cols.nExpr = listc_size(&constraint->child_idx_col_list);
            idx_cols.a = comdb2_calloc(
                ctx->mem, listc_size(&constraint->child_idx_col_list),
                sizeof(struct ExprList_item));
            if (idx_cols.a == 0)
                goto oom;

            int i = 0;
            LISTC_FOR_EACH(&constraint->child_idx_col_list, child_idx_column,
                           lnk)
            {
                idx_cols.a[i].pExpr = comdb2_calloc(ctx->mem, 1, sizeof(Expr));
                if (idx_cols.a[i].pExpr == 0)
                    goto oom;

                idx_cols.a[i].pExpr->u.zToken = child_idx_column->name;
                idx_cols.a[i].zName = child_idx_column->name;
                if (child_idx_column->flags & INDEX_ORDER_DESC) {
                    idx_cols.a[i].sortOrder = SQLITE_SO_DESC;
                } else {
                    idx_cols.a[i].sortOrder = SQLITE_SO_ASC;
                }
                i++;
            }

            comdb2AddIndex(pParse, 0 /* Key name will be generated */,
                           &idx_cols, 0, 0, 0, SQLITE_IDXTYPE_DUPKEY, 0);
            if (pParse->rc)
                goto cleanup;

            constraint->child =
                (struct comdb2_key *)LISTC_BOT(&ctx->schema->key_list);
        }
    }

    /* Generate CSC2 for the new/existing table. */
    csc2 = format_csc2(ctx);

    /*
      Now that we have the generated csc2 in a separate buffer, it
      is safe to teardown the parser context. The csc2 buffer will
      be reclaimed later in free_schema_change_type().
    */
    free_ddl_context(pParse);

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
    case 1: break;
    default: assert(0);
    }

    switch (inplace_updates) {
    case 0: table_options |= IPU_OFF; break;
    case 1: break;
    default: assert(0);
    }

    switch (instant_schema_change) {
    case 0: table_options |= ISC_OFF; break;
    case 1: break;
    default: assert(0);
    }

    switch (compr) {
    case BDB_COMPRESS_RLE8: table_options |= REC_RLE; break;
    case BDB_COMPRESS_CRLE: table_options |= REC_CRLE; break;
    case BDB_COMPRESS_ZLIB: table_options |= REC_ZLIB; break;
    case BDB_COMPRESS_LZ4: table_options |= REC_LZ4; break;
    case BDB_COMPRESS_NONE: break;
    default: assert(0);
    }

    switch (compr_blobs) {
    case BDB_COMPRESS_RLE8: table_options |= BLOB_RLE; break;
    case BDB_COMPRESS_CRLE: table_options |= BLOB_CRLE; break;
    case BDB_COMPRESS_ZLIB: table_options |= BLOB_ZLIB; break;
    case BDB_COMPRESS_LZ4: table_options |= BLOB_LZ4; break;
    case BDB_COMPRESS_NONE: break;
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
        fix_type_and_len(&column->type, (int *)&column->len);

        /* Add it to the list */
        listc_abl(&dst_schema->column_list, column);
    }
    return 0;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");
    return 1;
}

/*
  Fetch the schema definition of the table being altered.
*/
static int retrieve_schema(Parse *pParse, struct comdb2_ddl_context *ctx)
{
    struct dbtable *table;
    struct dbtable *parent_table;
    struct schema *schema;
    struct dbtag *tag;

    assert(ctx != 0);

    table = get_dbtable_by_name(ctx->schema->name);
    if (table == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Table '%s' not found.", ctx->schema->name);
        return 1;
    }
    schema = table->schema;

    /* Retrieve the table options. */
    ctx->schema->table_options = retrieve_table_options(table);

    /* Retrieve table columns. */
    if (retrieve_columns(pParse, ctx, schema, ctx->schema)) {
        return 1;
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
        if (schema->ix[i]->flags & SCHEMA_DUP) {
            key->flags |= KEY_DUP;
        }
        if (schema->ix[i]->flags & SCHEMA_DATACOPY) {
            key->flags |= KEY_DATACOPY;
        }
        if (schema->ix[i]->flags & SCHEMA_UNIQNULLS) {
            key->flags |= KEY_UNIQNULLS;
        }

        listc_init(&key->idx_col_list,
                   offsetof(struct comdb2_index_column, lnk));

        struct comdb2_column *column;
        struct comdb2_index_column *idx_column;
        int idx;
        for (int j = 0; j < schema->ix[i]->nmembers; j++) {
            idx_column =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_column));
            if (idx_column == 0)
                goto oom;

            idx = schema->ix[i]->member[j].idx;
            /* Retrieve the column at the given position. */
            LISTC_FOR_EACH(&ctx->schema->column_list, column, lnk)
            {
                if (idx == 0)
                    break;
                idx--;
            }

            /* Column name */
            idx_column->name = column->name;
            /* Column flags */
            if (schema->ix[i]->member[j].flags & INDEX_DESCEND) {
                idx_column->flags |= INDEX_ORDER_DESC;
            }
            /* Column reference */
            idx_column->column = column;

            listc_abl(&key->idx_col_list, idx_column);
        }
        listc_abl(&ctx->schema->key_list, key);
    }

    /* Populate constraints list */
    struct comdb2_constraint *constraint;
    struct comdb2_index_column *idx_column;
    struct comdb2_key *child_key;
    struct schema *parent_schema;
    for (int i = 0; i < table->n_constraints; i++) {
        struct comdb2_key *current;
        int key_found = 0;
        /* Locate the child key. */
        LISTC_FOR_EACH(&ctx->schema->key_list, current, lnk)
        {
            if (strcasecmp(table->constraints[i].lclkeyname, current->name) ==
                0) {
                child_key = current;
                key_found = 1;
                break;
            }
        }

        if (key_found == 0) {
            setError(pParse, SQLITE_ERROR,
                     "FK: Local key used in the foreign "
                     "key constraint could not be "
                     "found.");
            goto cleanup;
        }

        /* Locate the parent key. */
        for (int j = 0; j < table->constraints->nrules; j++) {
            parent_schema = 0;
            parent_table = get_dbtable_by_name(table->constraints[i].table[j]);
            if (parent_table == 0) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "FK: Parent table '%s' not found.",
                                table->constraints[i].table[j]);
                goto cleanup;
            }

            for (int k = 0; k < parent_table->schema->nix; k++) {
                if (strcasecmp(parent_table->schema->ix[k]->csctag,
                               table->constraints[i].keynm[j]) == 0) {
                    parent_schema = parent_table->schema->ix[k];
                }
            }
            if (parent_schema == 0) {
                setError(pParse, SQLITE_ERROR,
                         "FK: Referenced key used in the "
                         "foreign key constraint could "
                         "not be found.");
                goto cleanup;
            }

            constraint =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_constraint));
            if (constraint == 0)
                goto oom;

            /* Initialize the lists. */
            listc_init(&constraint->child_idx_col_list,
                       offsetof(struct comdb2_index_column, lnk));
            listc_init(&constraint->parent_idx_col_list,
                       offsetof(struct comdb2_index_column, lnk));

            /* Add child index columns. */
            struct comdb2_index_column *current;
            LISTC_FOR_EACH(&child_key->idx_col_list, current, lnk)
            {
                idx_column = comdb2_calloc(ctx->mem, 1,
                                           sizeof(struct comdb2_index_column));
                if (idx_column == 0)
                    goto oom;

                idx_column->name = current->name;
                idx_column->flags = current->flags;
                idx_column->column = current->column;

                listc_abl(&constraint->child_idx_col_list, idx_column);
            }

            /* Add parent index columns. */
            for (int i = 0; i < parent_schema->nmembers; i++) {
                idx_column = comdb2_calloc(ctx->mem, 1,
                                           sizeof(struct comdb2_index_column));
                if (idx_column == 0)
                    goto oom;

                idx_column->name =
                    comdb2_strdup(ctx->mem, parent_schema->member[i].name);
                if (idx_column->name == 0)
                    goto oom;

                if (parent_schema->member[i].flags & INDEX_DESCEND) {
                    idx_column->flags |= INDEX_ORDER_DESC;
                }
                /* There's no comdb2_column for foreign columns. */
                // idx_column->column = 0;

                listc_abl(&constraint->parent_idx_col_list, idx_column);
            }

            /* Reference to the child key. */
            constraint->child = child_key;

            /* Parent table name. */
            constraint->parent_table =
                comdb2_strdup(ctx->mem, parent_table->tablename);
            if (constraint->parent_table == 0)
                goto oom;

            /* Parent key name */
            constraint->parent_key =
                comdb2_strdup(ctx->mem, parent_schema->csctag);
            if (constraint->parent_key == 0)
                goto oom;

            /* Flags */
            if (table->constraints[i].flags & CT_UPD_CASCADE) {
                constraint->flags |= CONS_UPD_CASCADE;
            }
            if (table->constraints[i].flags & CT_DEL_CASCADE) {
                constraint->flags |= CONS_DEL_CASCADE;
            }

            if (table->constraints[i].consname) {
                /*
                  Csc2 does not allow named constraints to have multiple
                  parent key references.
                */
                assert(j == 0);
                constraint->name =
                    comdb2_strdup(ctx->mem, table->constraints[i].consname);
                if (constraint->name == 0)
                    goto oom;
            }

            listc_abl(&ctx->schema->constraint_list, constraint);
        }
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
                    return 1;
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

cleanup:
    return 1;
}

#define use_sqlite_impl(parse) (parse->comdb2_ddl_ctx == NULL)

void comdb2AlterTableStart(
    Parse *pParse, /* Parser context */
    Token *pName1, /* First part of the name of the table. */
    Token *pName2, /* Second part of the name of the table. */
    int dryrun     /* Whether its a dryrun? */
)
{
    struct comdb2_ddl_context *ctx;

    assert(pParse->comdb2_ddl_ctx == 0);

    if (isRemote(pParse, &pName1, &pName2)) {
        return;
    }

    ctx = create_ddl_context(pParse);
    if (ctx == 0) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    ctx->schema->name = comdb2_strndup(ctx->mem, pName1->z, pName1->n);
    if (ctx->schema->name == 0)
        goto oom;

    if (dryrun == 1)
        ctx->flags |= DDL_DRYRUN;

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
    Vdbe *v;
    int max_size;
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

    if (strlen(ctx->schema->name) + 1 <= MAXTABLELEN) {
        max_size = strlen(ctx->schema->name) + 1;
    } else {
        setError(pParse, SQLITE_MISUSE, "Tablename is too long.");
        goto cleanup;
    }

    if ((chkAndCopyTable(pParse, sc->table, ctx->schema->name, max_size, 1)))
        goto cleanup;

    if (authenticateSC(sc->table, pParse))
        goto cleanup;

    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    sc->dryrun = ((ctx->flags & DDL_DRYRUN) != 0) ? 1 : 0;

    fillTableOption(sc, ctx->schema->table_options);

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
    if (isTemp || isView || isVirtual || pParse->db->init.busy ||
        pParse->db->isExpert || IN_DECLARE_VTAB) {
        pParse->comdb2_ddl_ctx = 0;
        sqlite3StartTable(pParse, pName1, pName2, isTemp, isView, isVirtual,
                          noErr);
        return;
    }

    if (isRemote(pParse, &pName1, &pName2)) {
        return;
    }

    struct comdb2_ddl_context *ctx = create_ddl_context(pParse);
    if (ctx == 0)
        goto oom;

    ctx->schema->name = comdb2_strndup(ctx->mem, pName1->z, pName1->n);
    if (ctx->schema->name == 0)
        goto oom;
    sqlite3Dequote(ctx->schema->name);

    if (noErr && get_dbtable_by_name(ctx->schema->name)) {
        ctx->flags |= DDL_NOOP;
        logmsg(LOGMSG_DEBUG, "Table '%s' already exists.", ctx->schema->name);
        /* We'll not free the context here, as the flag's needed later. */
    }

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
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
    struct schema_change_type *sc = 0;
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    Vdbe *v;
    int max_size;
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

    if (strlen(ctx->schema->name) + 1 <= MAXTABLELEN) {
        max_size = strlen(ctx->schema->name) + 1;
    } else {
        setError(pParse, SQLITE_MISUSE, "Tablename is too long.");
        goto cleanup;
    }

    if ((chkAndCopyTable(pParse, sc->table, ctx->schema->name, max_size, 0)))
        goto cleanup;

    if (authenticateSC(sc->table, pParse))
        goto cleanup;
    sc->addonly = 1;
    sc->nothrevent = 1;
    sc->live = 1;

    fillTableOption(sc, comdb2Opts);

    sc->newcsc2 = prepare_csc2(pParse, ctx);
    if (sc->newcsc2 == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        goto cleanup;
    }

    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange,
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
    struct comdb2_column *column;
    char type[pType->n + 1];
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    int rc;

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
    strncpy0(type, pType->z, sizeof(type));
    sqlite3Dequote(type);

    if ((rc = comdb2_parse_sql_type(type, (int *)&column->len)) == -1) {
        setError(pParse, SQLITE_MISUSE, "Invalid type specified.");
        goto cleanup;
    }
    column->type = (uint8_t)rc;

    struct comdb2_column *current;
    LISTC_FOR_EACH(&ctx->schema->column_list, current, lnk)
    {
        if (strcasecmp(column->name, current->name) == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Duplicate column name '%s'.",
                            current->name);
            goto cleanup;
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

void comdb2AddDefaultValue(Parse *pParse, ExprSpan *pSpan)
{
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_column *column;
    char *def;
    int def_len;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3AddDefaultValue(pParse, pSpan);
        return;
    }

    if ((ctx->flags & DDL_NOOP) != 0) {
        return;
    }

    /* Add DEFAULT to the last add column. */
    def_len = pSpan->zEnd - pSpan->zStart;
    def = comdb2_strndup(ctx->mem, pSpan->zStart, def_len);
    if (def == 0)
        goto oom;
    /* Remove the quotes around the default value (if any). */
    sqlite3Dequote(def);

    column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);
    column->def = def;

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

/*
  Allow NULLs for the last added column.
*/
void comdb2AddNull(Parse *pParse)
{
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

    /* Clear the COLUMN_NO_NULL bit. */
    column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);
    column->flags &= ~COLUMN_NO_NULL;

    return;
}

/*
  Disallow NULLs for the last added column.
*/
void comdb2AddNotNull(Parse *pParse, int onError)
{
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

    /* Set the COLUMN_NO_NULL bit. */
    column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);
    column->flags |= COLUMN_NO_NULL;

    return;
}

void comdb2AddDbpad(Parse *pParse, int dbpad)
{
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
find_cons_by_name(struct comdb2_ddl_context *ctx, const char *cons)
{
    struct comdb2_constraint *constraint;
    char *constraint_name;
    char constraint_name_buf[MAXGENCONSLEN + 1];

    LISTC_FOR_EACH(&ctx->schema->constraint_list, constraint, lnk)
    {
        /* Ignore the dropped constraints. */
        if (constraint->flags & CONS_DELETED)
            continue;

        if (constraint->name == 0) {
            gen_constraint_name2(constraint, constraint_name_buf,
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
    ExprSpan *pPIWhere, /* WHERE clause for partial indices */
    int sortOrder,      /* Sort order of primary key when pList==NULL */
    u8 idxType,         /* The index type */
    int withOpts        /* WITH options (DATACOPY) */
)
{
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct comdb2_key *key;
    struct comdb2_column *column;

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

    if (keyname) {
        if (idxType != SQLITE_IDXTYPE_PRIMARYKEY && is_pk(keyname)) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Invalid key name '%s'.", keyname);
            goto cleanup;
        }

        /*
          Also check if a key already exists with the same name. For indices
          created via CREATE INDEX (SQLITE_IDXTYPE_APPDEF) this check has
          already been done to address IF NOT EXISTS.
        */
        if (idxType != SQLITE_IDXTYPE_APPDEF &&
            find_idx_by_name(ctx, keyname)) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Index '%s' already exists.", keyname);
            goto cleanup;
        }
    } else {
        /* Key name not specified, will be generated later. */
    }

    key = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_key));
    if (key == 0)
        goto oom;

    key->name = keyname;

    if (idxType == SQLITE_IDXTYPE_DUPKEY) {
        key->flags |= KEY_DUP;
    } else if (idxType == SQLITE_IDXTYPE_APPDEF) {
        /* For CREATE INDEX, we need to check onError */
        if (onError != OE_Abort) {
            key->flags |= KEY_DUP;
        } else {
            key->flags |= KEY_UNIQNULLS;
        }
    }

    if (withOpts == 1) {
        key->flags |= KEY_DATACOPY;
    }

    /* Initialize the index column list. */
    listc_init(&key->idx_col_list, offsetof(struct comdb2_index_column, lnk));

    /*
      pList == 0 imples that the PRIMARY/UNIQUE/DUP key was specified in the
      column definition.
    */
    struct comdb2_index_column *idx_column;
    if (pList == 0) {
        idx_column =
            comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_column));
        if (idx_column == 0)
            goto oom;

        column = (struct comdb2_column *)LISTC_BOT(&ctx->schema->column_list);

        idx_column->name = column->name;
        if (sortOrder == SQLITE_SO_DESC) {
            /* Only PKs accept sort order in the column definition. */
            assert(idxType == SQLITE_IDXTYPE_PRIMARYKEY);
            idx_column->flags |= INDEX_ORDER_DESC;
        }
        idx_column->column = column;

        /* Add the index column to the list. */
        listc_abl(&key->idx_col_list, idx_column);
    } else {
        for (int i = 0; i < pList->nExpr; i++) {
            idx_column =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_column));
            if (idx_column == 0)
                goto oom;

            column = find_column_by_name(ctx, pList->a[i].pExpr->u.zToken);
            if (column == 0) {
                pParse->rc = SQLITE_ERROR;
                sqlite3ErrorMsg(pParse, "Unknown column '%s'.",
                                pList->a[i].pExpr->u.zToken);
                goto cleanup;
            }

            idx_column->name = column->name;
            if (pList->a[i].sortOrder == SQLITE_SO_DESC) {
                idx_column->flags |= INDEX_ORDER_DESC;
            }
            idx_column->column = column;

            /* Add the index column to the list. */
            listc_abl(&key->idx_col_list, idx_column);
        }
    }

    if (pPIWhere && pPIWhere->pExpr != 0) {
        char *where_clause;
        size_t where_sz;

        where_sz = pPIWhere->zEnd - pPIWhere->zStart;
        assert(where_sz > 0);
        where_clause = comdb2_strndup(ctx->mem, pPIWhere->zStart, where_sz + 1);
        if (where_clause == 0)
            goto oom;

        where_sz += (sizeof("where") + 1);
        key->where = comdb2_malloc(ctx->mem, where_sz);
        if (key->where == 0)
            goto oom;

        snprintf(key->where, where_sz, "%s %s", "where", where_clause);
    }

    /*
      Generate a key name if it has not been explicitly provided in
      the command.
    */
    if (key->name == 0) {
        char *keyname = comdb2_malloc(ctx->mem, MAXGENKEYLEN);

        if (keyname == 0) {
            goto oom;
        }

        gen_key_name(key, ctx->schema->name, keyname, MAXGENKEYLEN);

        /* Check if a key already exists with the same name. */
        if (find_idx_by_name(ctx, keyname)) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Index '%s' already exists.", keyname);
            goto cleanup;
        }
        key->name = keyname;
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

    comdb2AddIndexInt(pParse, keyname, pList, onError, 0, sortOrder,
                      SQLITE_IDXTYPE_PRIMARYKEY, 0);
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
    ExprSpan *pPIWhere, /* WHERE clause for partial indices */
    int sortOrder,      /* Sort order of primary key when pList==NULL */
    u8 idxType,         /* The index type */
    int withOpts        /* WITH options (DATACOPY) */
)
{
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

    comdb2AddIndexInt(pParse, keyname, pList, onError, pPIWhere, sortOrder,
                      idxType, withOpts);
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
    ExprSpan *pPIWhere, /* WHERE clause for partial indices */
    int sortOrder,      /* Sort order of primary key when pList==NULL */
    int ifNotExist,     /* Omit error if index already exists */
    u8 idxType,         /* The index type */
    int withOpts,       /* WITH options (DATACOPY) */
    int temp)
{
    Vdbe *v;
    struct schema_change_type *sc;
    struct comdb2_ddl_context *ctx;
    int max_size;
    char *keyname;

    if (temp || pParse->db->init.busy || pParse->db->isExpert ||
        IN_DECLARE_VTAB) {
        sqlite3CreateIndex(pParse, pName1, pName2, pTblName, pList, onError,
                           pStart, pPIWhere->pExpr, sortOrder, ifNotExist,
                           idxType);
        return;
    }

    assert(pTblName->nSrc == 1);
    assert(pName1->n != 0);
    assert(pList != 0);

    if (isRemote(pParse, &pName1, &pName2)) {
        return;
    }

    v = sqlite3GetVdbe(pParse);

    sc = new_schemachange_type();
    if (sc == 0) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }
    assert(idxType == SQLITE_IDXTYPE_APPDEF);

    /* Its a CREATE INDEX command. */
    ctx = create_ddl_context(pParse);
    if (ctx == 0)
        goto oom;

    ctx->schema->name = comdb2_strdup(ctx->mem, pTblName->a[0].zName);
    if (ctx->schema->name == 0)
        goto oom;

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

    comdb2AddIndexInt(pParse, keyname, pList, onError, pPIWhere, sortOrder,
                      idxType, withOpts);
    if (pParse->rc)
        goto cleanup;

    if (strlen(ctx->schema->name) + 1 <= MAXTABLELEN) {
        max_size = strlen(ctx->schema->name) + 1;
    } else {
        setError(pParse, SQLITE_MISUSE, "Tablename is too long.");
        goto cleanup;
    }

    if ((chkAndCopyTable(pParse, sc->table, ctx->schema->name, max_size, 1)))
        goto cleanup;

    if (authenticateSC(sc->table, pParse))
        goto cleanup;

    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    sc->dryrun = 0;

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
  Check whether the requested action is supported by Comdb2.
*/
static int check_constraint_action(Parse *pParse, int *flags)
{
    int in_flags = *flags;

    u8 on_delete = (u8)(in_flags & 0xff);
    u8 on_update = (u8)((in_flags >> 8) & 0xff);

    *flags = 0;

    if ((on_delete != 0 && on_delete != OE_Cascade) ||
        (on_update != 0 && on_update != OE_Cascade)) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Actions other than cascading delete and "
                                "update are currently not supported in "
                                "Comdb2.");
        return 1;
    } else {
        if (on_delete == OE_Cascade) {
            *flags |= CT_DEL_CASCADE;
        }
        if (on_update == OE_Cascade) {
            *flags |= CT_UPD_CASCADE;
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
    struct comdb2_constraint *constraint;
    struct comdb2_index_column *idx_column;
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    struct dbtable *parent_table;
    char *constraint_name;
    char constraint_name_buf[MAXCONSLEN + 1];
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
               offsetof(struct comdb2_index_column, lnk));
    listc_init(&constraint->parent_idx_col_list,
               offsetof(struct comdb2_index_column, lnk));

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

        idx_column =
            comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_column));
        if (idx_column == 0)
            goto oom;

        idx_column->name = comdb2_strdup(ctx->mem, column->name);
        if (idx_column->name == 0)
            goto oom;

        /* Note: In this case the sort order is always ASC. */
        // idx_column->flags = 0;

        /* Assign the reference. */
        idx_column->column = column;

        listc_abl(&constraint->child_idx_col_list, idx_column);
    } else {
        /*
          Though some RDBMSs allow this, the number of referenced columns in
          FK must not be zero. PG, for instance picks the primary key from th
          referenced table.
        */
        assert(pToCol->nExpr > 0);

        for (int i = 0; i < pFromCol->nExpr; i++) {
            idx_column =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_column));
            if (idx_column == 0)
                goto oom;

            idx_column->name = comdb2_strdup(ctx->mem, pFromCol->a[i].zName);
            if (idx_column->name == 0)
                goto oom;

            if (pFromCol->a[i].sortOrder == SQLITE_SO_DESC) {
                idx_column->flags |= INDEX_ORDER_DESC;
            }

            /* There's no comdb2_column for foreign columns. */
            // idx_column->column = 0;

            listc_abl(&constraint->child_idx_col_list, idx_column);
        }
    }

    /*
      TO (referenced) column(s) and sort order(s).
    */
    for (int i = 0; i < pToCol->nExpr; i++) {
        idx_column =
            comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_index_column));
        if (idx_column == 0)
            goto oom;

        idx_column->name = comdb2_strdup(ctx->mem, pToCol->a[i].zName);
        if (idx_column->name == 0)
            goto oom;

        if (pToCol->a[i].sortOrder == SQLITE_SO_DESC) {
            idx_column->flags |= INDEX_ORDER_DESC;
        }
        // idx_column->column = 0;

        listc_abl(&constraint->parent_idx_col_list, idx_column);
    }

    /* To be assigned later */
    // constraint->child = 0;

    /* Referenced table */
    constraint->parent_table = comdb2_strndup(ctx->mem, pTo->z, pTo->n);
    if (constraint->parent_table == 0)
        goto oom;
    sqlite3Dequote(constraint->parent_table);

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

        /* Lets start by assuming that we have found the matching key. */
        key_found = 1;
        int j = 0;
        LISTC_FOR_EACH(&constraint->parent_idx_col_list, idx_column, lnk)
        {
            int sort_order =
                (parent_table->schema->ix[i]->member[j].flags & INDEX_DESCEND)
                    ? INDEX_ORDER_DESC
                    : 0;
            if ((strcasecmp(idx_column->name,
                            parent_table->schema->ix[i]->member[j].name) !=
                 0) ||
                idx_column->flags != sort_order) {
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

    /* Verify the conststraint action. */
    if ((check_constraint_action(pParse, &flags))) {
        goto cleanup;
    }
    constraint->flags = flags;

    if (pParse->constraintName.n == 0) {
        /*
          Check whether a similar constraint already exists.

          Generate the constraint name.
        */
        gen_constraint_name2(constraint, constraint_name_buf,
                             sizeof(constraint_name_buf));
        constraint_name = constraint_name_buf;
    } else {
        if (pParse->constraintName.n > MAXCONSLEN) {
            setError(pParse, SQLITE_MISUSE, "Constraint name is too long.");
            goto cleanup;
        }
        constraint->name = comdb2_strndup(ctx->mem, pParse->constraintName.z,
                                          pParse->constraintName.n);
        if (constraint->name == 0)
            goto oom;
        sqlite3Dequote(constraint->name);
        constraint_name = constraint->name;
    }
    if ((find_cons_by_name(ctx, constraint_name))) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Constraint '%s' already exists.",
                        constraint_name);
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
    struct comdb2_ddl_context *ctx = pParse->comdb2_ddl_ctx;
    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3DeferForeignKey(pParse, isDeferred);
        return;
    }
    return;
}

void comdb2DropForeignKey(Parse *pParse, /* Parser context */
                          Token *pName   /* Foreign key name */
)
{
    char *name;
    int fk_found = 0;
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

    /* Check whether the FK exists. */
    cons = find_cons_by_name(ctx, name);
    if (cons) {
        /* Mark FK as dropped. */
        cons->flags |= CONS_DELETED;
    } else {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Foreign key '%s' not found.", name);
        goto cleanup;
    }

    /* Foreign key marked for removal. */

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
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
    struct comdb2_index_column *idx_col;

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

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

/*
  Top-level implementation for DROP INDEX.
*/
void comdb2DropIndex(Parse *pParse, Token *pName1, Token *pName2, int ifExists)
{
    Vdbe *v;
    struct dbtable *table;
    struct schema_change_type *sc;
    struct comdb2_ddl_context *ctx;
    struct comdb2_schema *key;
    char *idx_name;
    char *tbl_name = 0;
    int max_size;
    int index_count = 0;

    assert(pParse->comdb2_ddl_ctx == 0);

    v = sqlite3GetVdbe(pParse);

    sc = new_schemachange_type();
    if (sc == 0) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

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

        table = get_dbtable_by_name(tbl_name);
        if (table == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Table '%s' not found.", tbl_name);
            goto cleanup;
        }

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

    if (strlen(ctx->schema->name) + 1 <= MAXTABLELEN) {
        max_size = strlen(ctx->schema->name) + 1;
    } else {
        setError(pParse, SQLITE_MISUSE, "Table name is too long.");
        goto cleanup;
    }

    if ((chkAndCopyTable(pParse, sc->table, ctx->schema->name, max_size, 1)))
        goto cleanup;

    if (authenticateSC(sc->table, pParse))
        goto cleanup;

    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    sc->dryrun = 0;

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
    struct comdb2_schema *key;
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

void comdb2putTunable(Parse *pParse, Token *name, Token *value)
{
    char *t_name;
    char *t_value;
    int rc;
    comdb2_tunable_err err;

    rc = create_string_from_token(NULL, pParse, &t_name, name);
    if (rc != SQLITE_OK)
        goto cleanup; /* Error has been set. */
    rc = create_string_from_token(NULL, pParse, &t_value, value);
    if (rc != SQLITE_OK)
        goto cleanup; /* Error has been set. */

    if ((err = handle_runtime_tunable(t_name, t_value))) {
        setError(pParse, SQLITE_ERROR, tunable_error(err));
    }

cleanup:
    free(t_name);
    free(t_value);
    return;
}
