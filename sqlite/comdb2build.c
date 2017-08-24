#include <stdio.h>
#include "sqliteInt.h"
#include <vdbeInt.h>
#include "comdb2build.h"
#include "comdb2vdbe.h"
#include <stdlib.h>
#include <stdarg.h>
#include <stdbool.h>
#include <string.h>
#include <xstring.h>
#include <schemachange.h>
#include <sc_lua.h>
#include <comdb2.h>
#include <sequences.h>
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

#define INCLUDE_KEYWORDHASH_H
#define INCLUDE_FINALKEYWORD_H
#include <keywordhash.h>
extern pthread_key_t query_info_key;
extern int gbl_commit_sleep;
extern int gbl_convert_sleep;
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
                    "DDL commands operate only on local schema only.");
}

extern int gbl_allow_user_schema;

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

    if(gbl_allow_user_schema && thd->sqlclntstate->user[0] != '\0' && strcasecmp(thd->sqlclntstate->user,DEFAULT_USER) != 0) {
        char* username = strstr(tmp_dst, "@");
        if (username) {
            /* Do nothing. */
            strncpy(dst, tmp_dst, MAXTABLELEN);
        } else { /* Add usernmame. */
            /* Make it part of user schema. */
            char userschema[MAXTABLELEN];
            int bdberr; 
            bdb_state_type *bdb_state = thedb->bdb_env;
            if (bdb_tbl_access_userschema_get(bdb_state, NULL, thd->sqlclntstate->user, userschema, &bdberr) == 0) {
              if (userschema[0] == '\0') {
                snprintf(dst, MAXTABLELEN, "%s", tmp_dst);
              } else {
                snprintf(dst, MAXTABLELEN, "%s@%s", tmp_dst, userschema);
              }
            } else {
              snprintf(dst, MAXTABLELEN, "%s@%s",tmp_dst, thd->sqlclntstate->user);
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
    }
    else
    {
        /* maybe mark it */
    }

    return SQLITE_OK;
}

static inline int chkAndCopySequence(Vdbe* v, Parse* pParse, char *dst,
    const char* name, size_t max_length, int mustexist)
{
    char tmp_dst[MAXTABLELEN];
    struct sql_thread *thd = pthread_getspecific(query_info_key);

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
    strncpy(dst, tmp_dst, MAXTABLELEN);
    strlower(dst);

    sequence_t *seq = getsequencebyname(dst);

    if (seq == NULL && mustexist)
    {
        setError(pParse, SQLITE_ERROR, "Sequence not found");
        return SQLITE_ERROR;
    }

    if (seq != NULL && !mustexist)
    {
        setError(pParse, SQLITE_ERROR, "Sequence already exists");
        return SQLITE_ERROR;
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

static inline int chkAndCopySequenceNames(Vdbe *v, Parse *pParse, char *dst,
   char *name, int mustexist)
{
 
    int rc;
    int max_size = strlen(name) + 1;

    if (max_size > MAXTABLELEN)
        return setError(pParse, SQLITE_MISUSE, "Sequence Name is too long");

    if ((rc = chkAndCopySequence(v, pParse, dst, name, max_size, mustexist)))
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
    thd->sqlclntstate->verifyretry_off = 1;
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

     if (thd->sqlclntstate && tablename && thd->sqlclntstate->user)
     {
        if (bdb_tbl_op_access_get(bdb_state, NULL, 0, 
            tablename, thd->sqlclntstate->user, &bdberr))
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

     if (thd->sqlclntstate && tablename && thd->sqlclntstate->user)
     {
         /* Authenticate the password first, as we haven't been doing it so far. */
         struct sqlclntstate *s = thd->sqlclntstate;
         if (bdb_user_password_check(s->user, s->password, NULL))
         {
            return SQLITE_AUTH;
         }
         
         /* Check if the user is OP user. */
         if (bdb_tbl_op_access_get(bdb_state, NULL, 0, 
             tablename, thd->sqlclntstate->user, &bdberr))
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
    osqlstate_t *osql = &thd->sqlclntstate->osql;
    osql->xerr.errval = 0;
    f->errorMsg = osql->xerr.errstr;
    return f->rc;
}

static int comdb2SqlSchemaChange_int(OpFunc *f, int usedb)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct schema_change_type *s = (struct schema_change_type*)f->arg;
    thd->sqlclntstate->osql.long_request = 1;
    if (usedb && getdbidxbyname(s->table) < 0) { // view
        usedb = 0;
    }
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
    osql_sock_start(thd->sqlclntstate, OSQL_SOCK_REQ ,0);
    comdb2SqlSchemaChange(f);
    int rst = osql_sock_commit(thd->sqlclntstate, OSQL_SOCK_REQ);
    osqlstate_t *osql = &thd->sqlclntstate->osql;
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
   struct sql_thread    *thd = pthread_getspecific(query_info_key);
   struct sqlclntstate  *clnt = thd->sqlclntstate;
   osqlstate_t          *osql = &clnt->osql;
   char                 *node = osql->host;
   int rc = 0;
   
   BpfuncArg *arg = (BpfuncArg*)f->arg;

   //osql_sock_start(clnt, OSQL_SOCK_REQ ,0);

   rc = osql_send_bpfunc(node, osql->rqid, osql->uuid, arg, NET_OSQL_SOCK_RPL,osql->logsb);
   
   //rc = osql_sock_commit(clnt, OSQL_SOCK_REQ);

   rc = osql->xerr.errval;
   osql->xerr.errval = 0;
   osql->xerr.errstr[0] = '\0';
    
    
    if (rc)
    {
        f->rc = rc;
        f->errorMsg = "FAIL"; // TODO This must be translated to a description
    } else
    {
        f->rc = SQLITE_OK;
        f->errorMsg = "";
    } 
   return rc;
}




/* ######################### Parser Reductions ############################ */

/**************************** Function prototypes ***************************/

static void comdb2Rebuild(Parse *p, Token* nm, Token* lnm, uint8_t opt);

/************************** Function definitions ****************************/

int authenticateSC(const char * table,  Parse *pParse) 
{
    if(gbl_schema_change_in_progress) return -1;

    Vdbe *v  = sqlite3GetVdbe(pParse);
    char *username = strstr(table, "@");
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (username && strcmp(username+1, thd->sqlclntstate->user) == 0) {
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
        logmsg(LOGMSG_ERROR, "%s: table schema not found: \n",  sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }

    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

static inline void comdb2Rebuild(Parse *pParse, Token* nm, Token* lnm, uint8_t opt)
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

    sc->commit_sleep = gbl_commit_sleep;
    sc->convert_sleep = gbl_convert_sleep;

    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL ))
    {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: \n",  sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }
    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}


void comdb2RebuildFull(Parse* p, Token* nm,Token* lnm)
{
    comdb2Rebuild(p, nm,lnm, REBUILD_ALL + REBUILD_DATA + REBUILD_BLOB);
}


void comdb2RebuildData(Parse* p, Token* nm, Token* lnm)
{
    comdb2Rebuild(p,nm,lnm,REBUILD_DATA);
}

void comdb2RebuildDataBlob(Parse* p,Token* nm, Token* lnm)
{
    comdb2Rebuild(p, nm, lnm, REBUILD_BLOB);
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
        logmsg(LOGMSG_ERROR, "%s: table schema not found: \n",  sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }
    comdb2PrepareSC(v, pParse, 0, sc, &comdb2SqlSchemaChange_usedb,
                    (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}


void comdb2RebuildIndex(Parse* pParse, Token* nm, Token* lnm, Token* index)
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

    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL )) {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: \n",  sc->table);
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
    logmsg(LOGMSG_DEBUG, "Bulk import from %.*s to", nm->n + lnm->n, nm->z, nm2->n +lnm2->n, nm2->z);
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
        if (!(thd && thd->sqlclntstate && thd->sqlclntstate->user &&
                   strcmp(thd->sqlclntstate->user, password->user) == 0 )) {
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

/****************************** SEQUENCES *******************************/
void comdb2CreateSequence(
    Parse *pParse, /* Parser context */
    char *name, /* Name of sequence */
    long long min_val,
    long long max_val,
    long long inc,
    bool cycle,
    long long start_val,
    long long chunk_size,
    bool noErr
)
{
    sqlite3 *db = pParse->db;
    Vdbe *v = sqlite3GetVdbe(pParse);

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    // Do not error if IF NOT EXISTS was supplied
    if (noErr && getsequencebyname(name) != NULL) goto out;

    if (chkAndCopySequenceNames(v, pParse, sc->table, name, 0))
        goto out;

    comdb2WriteTransaction(pParse);

    v->readOnly = 0;
    sc->type = DBTYPE_SEQUENCE;
    sc->addseq = 1;

    sc->seq_min_val = min_val;
    sc->seq_max_val = max_val;
    sc->seq_increment = inc;
    sc->seq_cycle = cycle;
    sc->seq_chunk_size = chunk_size;
    sc->seq_start_val = start_val;

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

void comdb2AlterSequence(
    Parse *pParse, /* Parser context */
    char *name, /* Name of sequence */
    long long min_val,
    long long max_val,
    long long inc,
    bool cycle,
    long long start_val,
    long long chunk_size,
    long long restart_val,
    int modified
)
{
    sqlite3 *db = pParse->db;
    Vdbe *v = sqlite3GetVdbe(pParse);

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopySequenceNames(v, pParse, sc->table, name, 1))
        goto out;

    comdb2WriteTransaction(pParse);

    v->readOnly = 0;
    sc->type = DBTYPE_SEQUENCE;
    sc->alterseq = 1;

    sc->seq_min_val = min_val;
    sc->seq_max_val = max_val;
    sc->seq_increment = inc;
    sc->seq_cycle = cycle;
    sc->seq_chunk_size = chunk_size;
    sc->seq_start_val = start_val;
    sc->seq_restart_val = restart_val;
    sc->seq_modified = modified;

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

void comdb2DropSequence(Parse *pParse, char *name)
{
    sqlite3 *db = pParse->db;
    Vdbe *v = sqlite3GetVdbe(pParse);

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopySequenceNames(v, pParse, sc->table, name, 1))
        goto out;

    comdb2WriteTransaction(pParse);

    v->readOnly = 0;
    sc->type = DBTYPE_SEQUENCE;
    sc->dropseq = 1;

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange,
                        (vdbeFuncArgFree)&free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
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
   } else if (thd->sqlclntstate && (thd->sqlclntstate->user[0] != '\0') && !strstr(p->zName, "@")
          && strncasecmp(p->zName, "sqlite_", 7) && strncasecmp(p->zName, "comdb2", 6))
   {
       char userschema[MAXTABLELEN];
       int bdberr; 
       bdb_state_type *bdb_state = thedb->bdb_env;
       if (bdb_tbl_access_userschema_get(bdb_state, NULL, thd->sqlclntstate->user, userschema, &bdberr) == 0) {
         if (userschema[0] == '\0') {
           snprintf(tableName, MAXTABLELEN, "%s", p->zName);
         } else {
           snprintf(tableName, MAXTABLELEN, "%s@%s", p->zName, userschema);
         }
       } else {
         snprintf(tableName, MAXTABLELEN, "%s@%s", p->zName, thd->sqlclntstate->user);
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

struct comdb2_field {
    char *name;
    struct field *field;
    LINKC_T(struct comdb2_field) lnk;
};

struct comdb2_schema {
    struct schema *schema;
    LINKC_T(struct comdb2_schema) lnk;
};

struct constraint {
    char *column[MAXCOLUMNS];
    char *referenced_table;
    char *referenced_column[MAXCOLUMNS];
    int ncols;
    int flags;
};

struct comdb2_constraint {
    struct constraint *constraint;
    LINKC_T(struct comdb2_constraint) lnk;
};

enum { COMDB2_DDL_CTX_FLAG_NOOP = 1 << 0, COMDB2_DDL_CTX_FLAG_DRYRUN = 1 << 1 };

struct comdb2_ddl_context {
    /* Name of the table/index */
    char *name;
    /* Flags */
    int flags;
    /* Copy of schema of the table being altered. */
    struct schema *schema;
    /* Table options */
    int table_options;
    /* Staging list of new/existing columns */
    LISTC_T(struct comdb2_field) column_list;
    /* Staging list of new/existing keys */
    LISTC_T(struct comdb2_schema) key_list;
    /* Staging list of new/existing constraints */
    LISTC_T(struct comdb2_constraint) constraint_list;
    /* Memory allocator. */
    comdb2ma mem;
};

/* Type properties */
enum { COMDB2_TYPE_FLAG_ALLOW_ARRAY = 1 };

#define COMDB2_TYPE(A, B, C)                                                   \
    {                                                                          \
        A, sizeof(A) - 1, B, C                                                 \
    }

struct comdb2_type_mapping {
    /* SQL type */
    char *sql_type;
    int sql_type_len;

    /* Comdb2 type */
    char *comdb2_type;

    /* Type properties */
    int flag;
} type_mapping[] = {
    /*
      Comdb2 types.
      WARNING: DO NOT CHANGE THE ORDER!
    */
    COMDB2_TYPE("u_short", "u_short", 0),
    COMDB2_TYPE("short", "short", 0),
    COMDB2_TYPE("u_int", "u_int", 0),
    COMDB2_TYPE("int", "int", 0),
    COMDB2_TYPE("longlong", "longlong", 0),
    COMDB2_TYPE("cstring", "cstring", COMDB2_TYPE_FLAG_ALLOW_ARRAY),
    COMDB2_TYPE("vutf8", "vutf8", COMDB2_TYPE_FLAG_ALLOW_ARRAY),
    COMDB2_TYPE("blob", "blob", COMDB2_TYPE_FLAG_ALLOW_ARRAY),
    COMDB2_TYPE("byte", "byte", COMDB2_TYPE_FLAG_ALLOW_ARRAY),
    COMDB2_TYPE("datetime", "datetime", 0),
    COMDB2_TYPE("datetimeus", "datetimeus", 0),
    COMDB2_TYPE("intervalds", "intervalds", 0),
    COMDB2_TYPE("intervaldsus", "intervaldsus", 0),
    COMDB2_TYPE("intervalym", "intervalym", 0),
    COMDB2_TYPE("decimal32", "decimal32", 0),
    COMDB2_TYPE("decimal64", "decimal64", 0),
    COMDB2_TYPE("decimal128", "decimal128", 0),
    COMDB2_TYPE("float", "float", 0),
    COMDB2_TYPE("double", "double", 0),

    /* Additional types mapped to a Comdb2 type. */
    COMDB2_TYPE("varchar", "cstring", COMDB2_TYPE_FLAG_ALLOW_ARRAY),
    COMDB2_TYPE("char", "cstring", COMDB2_TYPE_FLAG_ALLOW_ARRAY),
    COMDB2_TYPE("text", "vutf8", COMDB2_TYPE_FLAG_ALLOW_ARRAY),
    COMDB2_TYPE("integer", "int", 0),
    COMDB2_TYPE("smallint", "short", 0),
    COMDB2_TYPE("bigint", "longlong", 0),
    COMDB2_TYPE("real", "float", 0),
};

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

    listc_init(&ctx->column_list, offsetof(struct comdb2_field, lnk));
    listc_init(&ctx->key_list, offsetof(struct comdb2_schema, lnk));
    listc_init(&ctx->constraint_list, offsetof(struct comdb2_constraint, lnk));

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
    if (ctx == 0) return;

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

    for (int i = 0;
         i < sizeof(type_mapping) / sizeof(struct comdb2_type_mapping); ++i) {

        /* Check if current type could accept size. */
        accepts_size = (type_mapping[i].flag & COMDB2_TYPE_FLAG_ALLOW_ARRAY);

        if ((accepts_size == 0) && (type_mapping[i].sql_type_len != type_len)) {
            continue;
        }

        if (strncasecmp(type, type_mapping[i].sql_type,
                        type_mapping[i].sql_type_len) == 0) {

            /* No size specified. */
            if (type_mapping[i].sql_type_len == strlen(type)) {
                *size = 0;
                return i;
            }

            if (type[type_mapping[i].sql_type_len] != '(') {
                /* Malformed size. */
                return -1;
            }

            /* A size has been specified. */

            if (accepts_size == 0) {
                /* The type does not accept size. */
                return -1;
            }

            errno = 0;
            *size =
                strtol(type + type_mapping[i].sql_type_len + 1, &endptr, 10);

            /* Correction: cstring requires an additional byte. */
            if ((strncasecmp(type_mapping[i].sql_type, "varchar",
                             type_mapping[i].sql_type_len)) == 0) {
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
static int fix_type_and_len(int *type, int *len)
{
    int in_len;

    assert(type && len);

    in_len = *len;

    /* For most of the non-array types we do not need their size. */
    *len = 0;

    switch (*type) {
    case SERVER_UINT:
        switch (in_len) {
        case 3: *type = 0; break;
        case 5: *type = 2; break;
        default: goto err;
        }
        break;
    case SERVER_BINT:
        switch (in_len) {
        case 3: *type = 1; break;
        case 5: *type = 3; break;
        case 9: *type = 4; break;
        default: goto err;
        }
        break;
    case SERVER_BREAL:
        in_len--;
        switch (in_len) {
        case 4: *type = 17; break;
        case 8: *type = 18; break;
        default: goto err;
        }
        break;
    case SERVER_BCSTR:
        *type = 5;
        *len = in_len;
        break;
    case SERVER_BYTEARRAY:
        *type = 8;
        *len = in_len - 1;
        break;
    case SERVER_DATETIME: *type = 9; break;
    case SERVER_INTVYM: *type = 13; break;
    case SERVER_INTVDS: *type = 11; break;
    case SERVER_VUTF8:
        *type = 6;
        *len = in_len - 5;
        break;
    case SERVER_DECIMAL:
        switch (in_len) {
        case 7: *type = 14; break;
        case 13: *type = 15; break;
        case 22: *type = 16; break;
        default: goto err;
        }
        break;
    case SERVER_BLOB: /* fallthrough */
    case SERVER_BLOB2:
        *type = 7;
        *len = in_len - 5;
        break;
    case SERVER_DATETIMEUS: *type = 10; break;
    case SERVER_INTVDSUS: *type = 12; break;
    default: goto err;
    }
    return 0;
err:
    logmsg(LOGMSG_ERROR, "%s:%d Invalid type encountered\n", __FILE__,
           __LINE__);
    return 1;
}

/*
  Return the null-terminated string representation of the specified
  value.

  TODO(Nirbhay): How to detect/represent NULL values?

  @return
    Success    NULL-terminated string
    Error      NULL
*/
static char *format_val(comdb2ma ma, char *val, int type, int len)
{
    char *buf;
    int buf_len;

    switch (type) {
    case SERVER_UINT:
        switch (len) {
        case 3: {
            unsigned short v;
            memcpy(&v, val + 1, len - 1);
            v = ntohs(v);
            buf_len = snprintf(NULL, 0, "%hu", v);
            buf_len++;
            buf = comdb2_malloc(ma, buf_len);
            snprintf(buf, buf_len, "%hu", v);
            return buf;
        }
        case 5: {
            unsigned int v;
            memcpy(&v, val + 1, len - 1);
            v = ntohs(v);
            buf_len = snprintf(NULL, 0, "%u", v);
            buf_len++;
            buf = comdb2_malloc(ma, buf_len);
            snprintf(buf, buf_len, "%hu", v);
            return buf;
        }
        default: goto err;
        }
    case SERVER_BINT:
        switch (len) {
        case 3: {
            int2b v;
            short sval;
            memcpy(&v, val + 1, len - 1);
            v = ntohs(v);
            int2b_to_int2(v, &sval);
            buf_len = snprintf(NULL, 0, "%hd", sval);
            buf_len++;
            buf = comdb2_malloc(ma, buf_len);
            snprintf(buf, buf_len, "%hd", sval);
            return buf;
        }
        case 5: {
            int4b v;
            int ival;
            memcpy(&v, val + 1, len - 1);
            v = ntohl(v);
            int4b_to_int4(v, &ival);
            buf_len = snprintf(NULL, 0, "%d", ival);
            buf_len++;
            buf = comdb2_malloc(ma, buf_len);
            snprintf(buf, buf_len, "%d", ival);
            return buf;
        }
        case 9: {
            int8b v;
            long long lval;
            memcpy(&v, val + 1, len - 1);
            v = flibc_ntohll(v);
            int8b_to_int8(v, &lval);
            buf_len = snprintf(NULL, 0, "%lld", lval);
            buf_len++;
            buf = comdb2_malloc(ma, buf_len);
            snprintf(buf, buf_len, "%lld", lval);
            return buf;
        }
        default: goto err;
        }
    case SERVER_BREAL:
        switch (len) {
        case 5: {
            ieee4b v;
            float fval;
            memcpy(&v, val + 1, len - 1);
            v = ntohl(v);
            ieee4b_to_ieee4(v, &fval);
            buf_len = snprintf(NULL, 0, "%f", (double)fval);
            buf_len++;
            buf = comdb2_malloc(ma, buf_len);
            snprintf(buf, buf_len, "%f", (double)fval);
            return buf;
        }
        case 9: {
            ieee8b v;
            double dval;
            memcpy(&v, val + 1, len - 1);
            v = flibc_ntohll(v);
            ieee8b_to_ieee8(v, &dval);
            buf_len = snprintf(NULL, 0, "%f", dval);
            buf_len++;
            buf = comdb2_malloc(ma, buf_len);
            snprintf(buf, buf_len, "%f", dval);
            return buf;
        }
        default: goto err;
        }
    case SERVER_BCSTR:
        buf_len = strlen(val + 1) + 3;
        buf = comdb2_malloc(ma, buf_len);
        snprintf(buf, buf_len, "\"%s\"", val + 1);
        return buf;
        break;
    case SERVER_BYTEARRAY:  /* fallthrough */
    case SERVER_BLOB:       /* fallthrough */
    case SERVER_DATETIME:   /* fallthrough */
    case SERVER_INTVYM:     /* fallthrough */
    case SERVER_INTVDS:     /* fallthrough */
    case SERVER_VUTF8:      /* fallthrough */
    case SERVER_DECIMAL:    /* fallthrough */
    case SERVER_BLOB2:      /* fallthrough */
    case SERVER_DATETIMEUS: /* fallthrough */
    case SERVER_INTVDSUS:   /* fallthrough */
    default: goto err;
    }

err:
    logmsg(LOGMSG_ERROR, "%s:%d Invalid value encountered\n", __FILE__,
           __LINE__);
    return 0;
}

struct csc2_constraint {
    char *lclkey;
    int ncnstrts;
    int flags;
    char *table[MAXCONSTRAINTS];
    char *keynm[MAXCONSTRAINTS];
};

/*
  Format the table information into a CSC2 string.
*/
static char *format_csc2(struct comdb2_ddl_context *ctx, int nconstraints,
                         struct csc2_constraint *constraint)
{
    char *str;
    /* Buffer to store CSC2 representation */
    struct strbuf *csc2;

    csc2 = strbuf_new();

    /* Schema (columns) section */
    strbuf_append(csc2, "schema\n\t{");
    struct comdb2_field *current_column;
    LISTC_FOR_EACH(&ctx->column_list, current_column, lnk)
    {
        if (current_column->field == 0) continue;

        struct field *field = current_column->field;
        /* Append type and name */
        strbuf_appendf(csc2, "\n\t\t%s ",
                       type_mapping[field->type].comdb2_type);
        strbuf_appendf(csc2, "%s", field->name);
        if (field->len > 0) {
            strbuf_appendf(csc2, "[%d] ", field->len);
        } else {
            strbuf_append(csc2, " ");
        }

        /* Append default. The default is always a null-terminated string. */
        if (field->in_default) {
            strbuf_appendf(csc2, "dbstore = %s ", field->in_default);
        }

        if (field->convopts.dbpad > 0) {
            strbuf_appendf(csc2, "dbpad = %d ", field->convopts.dbpad);
        }

        /* No need to print 'null = no'. That's implicit. */
        if ((field->flags & NO_NULL) == 0) {
            strbuf_append(csc2, "null = yes ");
        }
    }
    strbuf_append(csc2, "\n\t}\n");

    /* Keys section */
    struct comdb2_schema *current_key;
    int nkeys = 0;
    LISTC_FOR_EACH(&ctx->key_list, current_key, lnk)
    {
        if (current_key->schema != 0) ++nkeys;
    }

    if (nkeys > 0) {
        strbuf_append(csc2, "keys\n\t{");
        LISTC_FOR_EACH(&ctx->key_list, current_key, lnk)
        {
            struct schema *key = current_key->schema;
            if (key == 0) continue;

            strbuf_append(csc2, "\n\t\t");

            if ((key->flags & SCHEMA_DUP) != 0) {
                strbuf_append(csc2, "dup ");
            }

            if ((key->flags & SCHEMA_DATACOPY) != 0) {
                strbuf_append(csc2, "datacopy ");
            }

            strbuf_appendf(csc2, "\"%s\" = ", key->csctag);

            int added = 0;
            for (int j = 0; j < key->nmembers; j++) {
                if (key->member[j].name == 0) continue;

                if (added > 0) {
                    strbuf_append(csc2, "+ ");
                }
                strbuf_appendf(
                    csc2, "%s%s ",
                    (key->member[j].flags & INDEX_DESCEND) ? "<DESCEND> " : "",
                    key->member[j].name);
                added++;
            }

            if (key->where != 0) {
                strbuf_appendf(csc2, "{ where %s } ", key->where);
            }
        }
        strbuf_append(csc2, "\n\t}\n");
    }

    /* Constraints section */
    if (nconstraints > 0) {
        strbuf_append(csc2, "constraints\n\t{");
        for (int i = 0; i < nconstraints; i++) {
            strbuf_appendf(csc2, "\n\t\t\"%s\" -> ", constraint[i].lclkey);
            for (int j = 0; j < constraint[i].ncnstrts; j++) {
                strbuf_appendf(csc2, "<\"%s\":\"%s\"> ", constraint[i].table[j],
                               constraint[i].keynm[j]);
            }

            if (constraint[i].flags != 0) {
                if ((constraint[i].flags & CT_UPD_CASCADE) != 0) {
                    strbuf_append(csc2, "on update cascade ");
                }
                if ((constraint[i].flags & CT_DEL_CASCADE) != 0) {
                    strbuf_append(csc2, "on delete cascade ");
                }
            }
        }
        strbuf_append(csc2, "\n\t}\n");
    }

    str = strdup((char *)strbuf_buf(csc2));
    strbuf_free(csc2);

    logmsg(LOGMSG_DEBUG, "CSC2: %s\n", str);

    return str;
}

/* Generate a key name for the specified key. */
static char *gen_key_name(struct comdb2_ddl_context *ctx, const char *tabname,
                          struct schema *key)
{
    char buf[53];
    char *keyname;

    form_new_style_name(buf, sizeof(buf), key, "KEY", tabname);

    keyname = comdb2_strdup(ctx->mem, buf);

    return keyname;
}

static char *prepare_csc2(Parse *pParse, struct comdb2_ddl_context *ctx)
{
    char *csc2;
    struct comdb2_field *current_column;
    struct csc2_constraint constraint[MAXCONSTRAINTS];
    int ncolumns = 0;
    int nconstraints = 0;

    LISTC_FOR_EACH(&ctx->column_list, current_column, lnk)
    {
        /*
          During ALTER TABLE, the fields are deleted by simply setting
          them to 0.
        */
        if (current_column->field != 0) ncolumns++;
    }

    /* Check whether there is at least one column. */
    if (ncolumns == 0) {
        setError(pParse, SQLITE_ERROR, "Table must have at least one column.");
        goto cleanup;
    }

    int unnamed_keys = 0;
    char keyname[100];
    struct comdb2_schema *current_key;
    LISTC_FOR_EACH(&ctx->key_list, current_key, lnk)
    {
        /*
          During ALTER TABLE/DROP INDEX, the fields are deleted by
          simply setting them to 0.
        */
        if (current_key->schema == 0) continue;

        /*
          Generate a key name if its has not been explicitly provided in
          the command.
        */
        if (current_key->schema->csctag == 0) {
            current_key->schema->csctag =
                gen_key_name(ctx, ctx->name, current_key->schema);
            if (current_key->schema->csctag == 0) goto oom;
        }
    }

    struct comdb2_constraint *current_constraint;
    LISTC_FOR_EACH(&ctx->constraint_list, current_constraint, lnk)
    {
        int key_found = 0;
        struct dbtable *parent_table;
        struct schema *parent_key;
        struct schema *child_key;
        struct csc2_constraint csc2_constraint;
        int ncols;

        /*
          During ALTER TABLE/DROP INDEX, the constraints are deleted by
          simply setting them to 0.
        */
        if (current_constraint->constraint == 0) continue;

        /* Find the "non-dropped" child key for current constraint. */
        LISTC_FOR_EACH(&ctx->key_list, current_key, lnk)
        {
            if (current_key->schema != 0 &&
                current_constraint->constraint->ncols ==
                    current_key->schema->nmembers) {
                /*
                  Let's start by assuming that we have found a matching key.
                */
                key_found = 1;
                ncols = current_key->schema->nmembers;
                for (int i = 0; i < ncols; ++i) {
                    if (strcasecmp(current_constraint->constraint->column[i],
                                   current_key->schema->member[i].name)) {
                        /* Mismatch */
                        key_found = 0;
                        break;
                    }
                }
                if (key_found == 1) {
                    child_key = current_key->schema;
                    break;
                }
            }
        }
        if (key_found == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "A matching key for the FOREIGN KEY was "
                                    "not found in the child table '%s'",
                            ctx->name);
            goto cleanup;
        }

        /*
          A matching local key has been found. On to find the parent key
          for current constraint.
        */
        parent_table = get_dbtable_by_name(
            current_constraint->constraint->referenced_table);
        if (parent_table == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(
                pParse,
                "Parent table '%s' of the FOREIGN KEY could not be found.",
                current_constraint->constraint->referenced_table);
            goto cleanup;
        }

        key_found = 0;
        for (int i = 0; i < parent_table->schema->nix; i++) {
            if (parent_table->schema->ix[i]->nmembers ==
                current_constraint->constraint->ncols) {
                /*
                  Let's start by assuming that we have found a matching key.
                */
                key_found = 1;
                ncols = current_constraint->constraint->ncols;
                for (int j = 0; j < ncols; j++) {
                    if (strcasecmp(parent_table->schema->ix[i]->member[j].name,
                                   current_constraint->constraint
                                       ->referenced_column[j])) {
                        /* Mismatch */
                        key_found = 0;
                        break;
                    }
                }
                if (key_found == 1) {
                    /* Found a matching key. */
                    parent_key = parent_table->schema->ix[i];
                    break;
                }
            }
        }
        if (key_found == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "A matching key for the FOREIGN KEY was "
                                    "not found in the parent (referenced) "
                                    "table '%s'.",
                            current_constraint->constraint->referenced_table);
            goto cleanup;
        }

        /*
          A matching parent key has been found. Let's now populated
          csc2_constraint and copy it to the constraints array.
        */
        csc2_constraint.lclkey = comdb2_strdup(ctx->mem, child_key->csctag);
        if (csc2_constraint.lclkey == 0) goto oom;
        csc2_constraint.ncnstrts = 1;
        csc2_constraint.flags = current_constraint->constraint->flags;
        csc2_constraint.table[0] =
            comdb2_strdup(ctx->mem, parent_table->dbname);
        if (csc2_constraint.table[0] == 0) goto oom;
        csc2_constraint.keynm[0] = comdb2_strdup(ctx->mem, parent_key->csctag);
        if (csc2_constraint.keynm[0] == 0) goto oom;
        constraint[nconstraints] = csc2_constraint;
        nconstraints++;
    }

    /* Generate CSC2 for the new/existing table. */
    csc2 = format_csc2(ctx, nconstraints, constraint);

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

static struct schema *clone_schema_ma(comdb2ma ma, struct schema *from)
{
    int i;

    struct schema *sc = comdb2_calloc(ma, 1, sizeof(struct schema));
    sc->tag = comdb2_strdup(ma, from->tag);
    sc->nmembers = from->nmembers;
    sc->member = comdb2_malloc(ma, from->nmembers * sizeof(struct field));
    sc->flags = from->flags;
    sc->nix = from->nix;
    if (sc->nix) sc->ix = comdb2_calloc(ma, from->nix, sizeof(struct schema *));

    for (i = 0; i < from->nmembers; i++) {
        sc->member[i] = from->member[i];
        sc->member[i].name = comdb2_strdup(ma, from->member[i].name);
        if (from->member[i].in_default) {
            sc->member[i].in_default =
                comdb2_malloc(ma, sc->member[i].in_default_len);
            memcpy(sc->member[i].in_default, from->member[i].in_default,
                   from->member[i].in_default_len);
        }
        if (from->member[i].out_default) {
            sc->member[i].out_default =
                comdb2_malloc(ma, sc->member[i].out_default_len);
            memcpy(sc->member[i].out_default, from->member[i].out_default,
                   from->member[i].out_default_len);
        }
    }

    for (i = 0; i < from->nix; i++) {
        if (from->ix && from->ix[i])
            sc->ix[i] = clone_schema_ma(ma, from->ix[i]);
    }

    sc->ixnum = from->ixnum;
    sc->recsize = from->recsize;
    sc->numblobs = from->numblobs;

    if (from->csctag) sc->csctag = comdb2_strdup(ma, from->csctag);

    if (from->datacopy) {
        sc->datacopy = comdb2_malloc(ma, from->nmembers * sizeof(int));
        memcpy(sc->datacopy, from->datacopy, from->nmembers * sizeof(int));
    }
    return sc;
}

/*
  Fetch the schema definition of the table being altered.
*/
static int retrieve_schema(Parse *pParse, struct comdb2_ddl_context *ctx)
{
    struct dbtable *table;
    struct dbtable *parent_table;
    struct schema *schema = 0;
    struct schema *key;
    struct schema *parent_key;
    struct constraint *constraint;
    struct comdb2_field *fentry;
    struct comdb2_schema *sentry;
    struct comdb2_constraint *centry;

    assert(ctx != 0);

    table = get_dbtable_by_name(ctx->name);
    if (table == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Table '%s' not found.", ctx->name);
        return 1;
    }

    ctx->table_options = retrieve_table_options(table);

    schema = clone_schema_ma(ctx->mem, table->schema);
    ctx->schema = schema;

    /* Change the type and length of fields in the cloned schema. */
    for (int i = 0; i < schema->nmembers; i++) {
        fix_type_and_len(&schema->member[i].type, &schema->member[i].len);
    }

    /* Populate columns list */
    for (int i = 0; i < schema->nmembers; i++) {
        fentry = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_field));
        if (fentry == 0) goto oom;

        fentry->name = schema->member[i].name;
        fentry->field = &schema->member[i];
        /* Convert the default value to string. */
        if (fentry->field->in_default) {
            fentry->field->in_default = format_val(
                ctx->mem, fentry->field->in_default,
                fentry->field->in_default_type, fentry->field->in_default_len);
        }
        listc_abl(&ctx->column_list, fentry);
    }

    /* Populate keys list */
    for (int i = 0; i < schema->nix; i++) {
        sentry = comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_schema));
        if (sentry == 0) goto oom;

        sentry->schema = schema->ix[i];
        listc_abl(&ctx->key_list, sentry);
    }

    /* Populate constraints list */
    for (int i = 0; i < table->n_constraints; i++) {
        key = 0;
        /* Locate the child key. */
        for (int j = 0; j < table->nix; j++) {
            if (strcasecmp(table->schema->ix[j]->csctag,
                           table->constraints[i].lclkeyname) == 0) {
                key = table->schema->ix[j];
            }
        }
        if (key == 0) {
            setError(pParse, SQLITE_ERROR, "FK: Local key used in the foreign "
                                           "key constraint could not be "
                                           "found.");
            goto cleanup;
        }

        /* Locate the parent key. */
        for (int j = 0; j < table->constraints->nrules; j++) {
            parent_key = 0;
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
                    parent_key = parent_table->schema->ix[k];
                }
            }
            if (parent_key == 0) {
                setError(pParse, SQLITE_ERROR, "FK: Referenced key used in the "
                                               "foreign key constraint could "
                                               "not be found.");
                goto cleanup;
            }

            assert(key->nmembers == parent_key->nmembers);

            constraint = comdb2_calloc(ctx->mem, 1, sizeof(struct constraint));
            if (constraint == 0) goto oom;
            centry =
                comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_constraint));
            if (centry == 0) goto oom;

            constraint->referenced_table =
                comdb2_strdup(ctx->mem, parent_table->dbname);
            if (constraint->referenced_table == 0) goto oom;
            constraint->ncols = parent_key->nmembers;
            constraint->flags = table->constraints[i].flags;

            for (int p = 0; p < constraint->ncols; p++) {
                constraint->column[p] =
                    comdb2_strdup(ctx->mem, key->member[p].name);
                if (constraint->column[p] == 0) goto oom;
                constraint->referenced_column[p] =
                    comdb2_strdup(ctx->mem, parent_key->member[p].name);
                if (constraint->referenced_column[p] == 0) goto oom;
            }

            centry->constraint = constraint;
            listc_abl(&ctx->constraint_list, centry);
        }
    }
    return 0;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    return 1;
}

static inline int use_sqlite_impl(Parse *pParse)
{
    if (pParse->db->init.busy || IN_DECLARE_VTAB ||
        pParse->db->force_sqlite_impl) {
        return 1;
    }
    return 0;
}

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

    ctx->name = comdb2_strndup(ctx->mem, pName1->z, pName1->n);
    if (ctx->name == 0) goto oom;

    if (dryrun == 1) ctx->flags |= COMDB2_DDL_CTX_FLAG_DRYRUN;

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
    struct comdb2_ddl_context *ctx;
    Vdbe *v;
    int max_size;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    v = sqlite3GetVdbe(pParse);

    struct schema_change_type *sc = new_schemachange_type();
    if (sc == 0) goto oom;

    if (strlen(ctx->name) + 1 <= MAXTABLELEN) {
        max_size = strlen(ctx->name) + 1;
    } else {
        setError(pParse, SQLITE_MISUSE, "Tablename is too long.");
        goto cleanup;
    }

    if ((chkAndCopyTable(pParse, sc->table, ctx->name, max_size, 1)))
        goto cleanup;

    if (authenticateSC(sc->table, pParse)) goto cleanup;

    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    sc->dryrun = ((ctx->flags & COMDB2_DDL_CTX_FLAG_DRYRUN) != 0) ? 1 : 0;

    fillTableOption(sc, ctx->table_options);

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
    struct comdb2_ddl_context *ctx;

    assert(pParse->comdb2_ddl_ctx == 0);

    if (isTemp) pParse->db->force_sqlite_impl = 1;

    if (use_sqlite_impl(pParse)) {
        sqlite3StartTable(pParse, pName1, pName2, isTemp, isView, isVirtual,
                          noErr);
        return;
    }

    if (isRemote(pParse, &pName1, &pName2)) {
        return;
    }

    ctx = create_ddl_context(pParse);
    if (ctx == 0) goto oom;

    ctx->name = comdb2_strndup(ctx->mem, pName1->z, pName1->n);
    if (ctx->name == 0) goto oom;
    sqlite3Dequote(ctx->name);

    if (noErr && get_dbtable_by_name(ctx->name)) {
        ctx->flags |= COMDB2_DDL_CTX_FLAG_NOOP;
        logmsg(LOGMSG_DEBUG, "Table '%s' already exists.", ctx->name);
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
    struct comdb2_ddl_context *ctx;
    struct schema_change_type *sc = 0;
    Vdbe *v;
    int max_size;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3EndTable(pParse, pCons, pEnd, tabOpts, 0);
        return;
    } else {
        /* Comdb2 table; check if Sqlite specific WITHOUT ROWID is used. */
        if (tabOpts != 0) {
            setError(pParse, SQLITE_ERROR,
                     "WITHOUT ROWID is not supported in Comdb2.");
            goto cleanup;
        }
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & COMDB2_DDL_CTX_FLAG_NOOP) != 0) {
        goto cleanup;
    }

    v = sqlite3GetVdbe(pParse);

    sc = new_schemachange_type();
    if (sc == 0) goto oom;

    if (strlen(ctx->name) + 1 <= MAXTABLELEN) {
        max_size = strlen(ctx->name) + 1;
    } else {
        setError(pParse, SQLITE_MISUSE, "Tablename is too long.");
        goto cleanup;
    }

    if ((chkAndCopyTable(pParse, sc->table, ctx->name, max_size, 0)))
        goto cleanup;

    if (authenticateSC(sc->table, pParse)) goto cleanup;
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

void comdb2AddColumn(Parse *pParse, /* Parser context */
                     Token *pName,  /* Name of the column */
                     Token *pType   /* Type of the column */
                     )
{
    struct comdb2_ddl_context *ctx;
    char type[pType->n + 1];
    struct field *field;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

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

    if ((ctx->flags & COMDB2_DDL_CTX_FLAG_NOOP) != 0) {
        return;
    }

    /* Allocate a new field. */
    field = comdb2_calloc(ctx->mem, 1, sizeof(struct field));
    if (field == 0) goto oom;

    /* Field name */
    field->name = comdb2_strndup(ctx->mem, pName->z, pName->n);
    if (field->name == 0) goto oom;
    sqlite3Dequote(field->name);

    /* Field type */
    strncpy0(type, pType->z, sizeof(type));
    sqlite3Dequote(type);

    if ((field->type = comdb2_parse_sql_type(type, &field->len)) == -1) {
        setError(pParse, SQLITE_MISUSE, "Invalid type specified.");
        goto cleanup;
    }

    struct comdb2_field *entry =
        comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_field));
    if (entry == 0) goto oom;

    entry->name = field->name;
    entry->field = field;

    struct comdb2_field *current;
    LISTC_FOR_EACH(&ctx->column_list, current, lnk)
    {
        if (strcasecmp(field->name, current->name) == 0) {
            setError(pParse, SQLITE_ERROR, "Duplicate column name.");
            goto cleanup;
        }
    }

    listc_abl(&ctx->column_list, entry);

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

void comdb2AddDefaultValue(Parse *pParse, ExprSpan *pSpan)
{
    struct comdb2_ddl_context *ctx;
    char *def;
    int def_len;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3AddDefaultValue(pParse, pSpan);
        return;
    }

    if ((ctx->flags & COMDB2_DDL_CTX_FLAG_NOOP) != 0) {
        return;
    }

    /* Add DEFAULT to the last add column. */
    def_len = pSpan->zEnd - pSpan->zStart;
    def = comdb2_strndup(ctx->mem, pSpan->zStart, def_len);
    if (def == 0) goto oom;

    ((struct comdb2_field *)LISTC_BOT(&ctx->column_list))->field->in_default =
        def;

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
    struct comdb2_ddl_context *ctx;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & COMDB2_DDL_CTX_FLAG_NOOP) != 0) {
        return;
    }

    /* Clear the NO_NULL bit. */
    ((struct comdb2_field *)LISTC_BOT(&ctx->column_list))->field->flags &=
        ~NO_NULL;

    return;
}

/*
  Disallow NULLs for the last added column.
*/
void comdb2AddNotNull(Parse *pParse, int onError)
{
    struct comdb2_ddl_context *ctx;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3AddNotNull(pParse, onError);
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & COMDB2_DDL_CTX_FLAG_NOOP) != 0) {
        return;
    }

    /* Set the NO_NULL bit. */
    ((struct comdb2_field *)LISTC_BOT(&ctx->column_list))->field->flags |=
        NO_NULL;

    return;
}

void comdb2AddDbpad(Parse *pParse, int dbpad)
{
    struct comdb2_ddl_context *ctx;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }
    if ((ctx->flags & COMDB2_DDL_CTX_FLAG_NOOP) != 0) {
        return;
    }

    ((struct comdb2_field *)LISTC_BOT(&ctx->column_list))
        ->field->convopts.dbpad = dbpad;
    return;
}

void comdb2AddPrimaryKey(
    Parse *pParse,   /* Parsing context */
    ExprList *pList, /* List of field names to be indexed */
    int onError,     /* What to do with a uniqueness conflict */
    int autoInc,     /* True if the AUTOINCREMENT keyword is present */
    int sortOrder    /* SQLITE_SO_ASC or SQLITE_SO_DESC */
    )
{
    struct comdb2_ddl_context *ctx;
    struct schema *key;
    struct field *member;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

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

    if ((ctx->flags & COMDB2_DDL_CTX_FLAG_NOOP) != 0) {
        return;
    }

    key = comdb2_calloc(ctx->mem, 1, sizeof(struct schema));
    if (key == 0) goto oom;

    if (pParse->constraintName.n != 0) {
        key->csctag = comdb2_strndup(ctx->mem, pParse->constraintName.z,
                                     pParse->constraintName.n);
        if (key->csctag == 0) goto oom;
    } else {
        key->csctag = comdb2_strdup(ctx->mem, "PRIMARY_KEY");
        if (key->csctag == 0) goto oom;
    }
    key->flags = SCHEMA_INDEX;

    /*
      pList == 0 imples that the PRIMARY KEY was specified in the column
      definition.
    */
    if (pList == 0) {
        key->nmembers = 1;
        key->member = comdb2_calloc(ctx->mem, 1, sizeof(struct field));
        if (key->member == 0) goto oom;

        member = &key->member[0];
        member->name = comdb2_strdup(
            ctx->mem,
            ((struct comdb2_field *)LISTC_BOT(&ctx->column_list))->field->name);
        if (member->name == 0) goto oom;
        if (sortOrder == SQLITE_SO_DESC) {
            member->flags |= INDEX_DESCEND;
        }
    } else {
        key->nmembers = pList->nExpr;
        key->member =
            comdb2_calloc(ctx->mem, pList->nExpr, sizeof(struct field));
        if (key->member == 0) goto oom;

        for (int i = 0; i < pList->nExpr; i++) {
            member = &key->member[i];
            member->name = comdb2_strdup(
                ctx->mem,
                pList->a[i].pExpr->u.zToken); /* zToken is 0-terminated */
            if (member->name == 0) goto oom;

            if (pList->a[i].sortOrder == SQLITE_SO_DESC) {
                member->flags |= INDEX_DESCEND;
            }
        }
    }

    struct comdb2_schema *entry =
        comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_schema));
    if (entry == 0) goto oom;

    entry->schema = key;

    listc_abl(&ctx->key_list, entry);

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

void comdb2AddIndex(
    Parse *pParse,   /* All information about this parse */
    ExprList *pList, /* A list of columns to be indexed */
    int onError,     /* OE_Abort, OE_Ignore, OE_Replace, or OE_None */
    u8 idxType       /* The index type */
    )
{
    struct comdb2_ddl_context *ctx;
    struct schema *key;
    struct field *member;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3CreateIndex(pParse, 0, 0, 0, pList, onError, 0, 0, 0, 0,
                           idxType);
        return;
    }

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    if ((ctx->flags & COMDB2_DDL_CTX_FLAG_NOOP) != 0) {
        return;
    }

    key = comdb2_calloc(ctx->mem, 1, sizeof(struct schema));
    if (key == 0) goto oom;

    if (pParse->constraintName.n != 0) {
        key->csctag = comdb2_strndup(ctx->mem, pParse->constraintName.z,
                                     pParse->constraintName.n);
        if (key->csctag == 0) goto oom;
    }
    key->flags = SCHEMA_INDEX;

    /*
      pList == 0 imples that the UNIQUE key was specified in the column
      definition.
    */
    if (pList == 0) {
        key->nmembers = 1;
        key->member = comdb2_calloc(ctx->mem, 1, sizeof(struct field));
        if (key->member == 0) goto oom;

        member = &key->member[0];
        member->name = comdb2_strdup(
            ctx->mem,
            ((struct comdb2_field *)LISTC_BOT(&ctx->column_list))->field->name);
        if (member->name == 0) goto oom;
    } else {
        key->nmembers = pList->nExpr;
        key->member =
            comdb2_calloc(ctx->mem, pList->nExpr, sizeof(struct field));
        if (key->member == 0) goto oom;

        for (int i = 0; i < pList->nExpr; i++) {
            member = &key->member[i];
            member->name = comdb2_strdup(
                ctx->mem,
                pList->a[i].pExpr->u.zToken); /* zToken is 0-terminated */
            if (member->name == 0) goto oom;
            if (pList->a[i].sortOrder == SQLITE_SO_DESC) {
                member->flags |= INDEX_DESCEND;
            }
        }
    }

    struct comdb2_schema *entry =
        comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_schema));
    if (entry == 0) goto oom;
    entry->schema = key;

    listc_abl(&ctx->key_list, entry);

    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

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
    int withOpts        /* WITH options (DATACOPY) */
    )
{
    Vdbe *v;
    struct schema_change_type *sc;
    struct comdb2_ddl_context *ctx;
    struct schema *key;
    struct field *member;
    struct dbtable *table;
    int max_size;
    int found;
    char *keyname;

    assert(pParse->comdb2_ddl_ctx == 0);

    if (use_sqlite_impl(pParse)) {
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
    if (ctx == 0) goto oom;

    ctx->name = comdb2_strdup(ctx->mem, pTblName->a[0].zName);
    if (ctx->name == 0) goto oom;

    /* Check if an index already exists with the requested name. */
    found = 0;
    table = get_dbtable_by_name(ctx->name);
    if (table == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Table '%s' not found.", ctx->name);
        goto cleanup;
    }

    keyname = comdb2_strndup(ctx->mem, pName1->z, pName1->n);
    if (keyname == 0) goto oom;
    sqlite3Dequote(keyname);

    for (int i = 0; i < table->schema->nix; i++) {
        if ((strcasecmp(table->schema->ix[i]->csctag, keyname)) == 0) {
            found = 1;
            break;
        }
    }

    if (found == 1) {
        if (ifNotExist == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Index '%s' already exists.", keyname);
        } else {
            logmsg(LOGMSG_DEBUG, "Index '%s' already exists.", keyname);
        }
        goto cleanup;
    }

    /*
       Add all the columns, indexes and constraints in the table to the
       respective list.
    */
    if (retrieve_schema(pParse, ctx)) {
        goto cleanup;
    }

    key = comdb2_calloc(ctx->mem, 1, sizeof(struct schema));
    if (key == 0) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        goto cleanup;
    }

    key->csctag = keyname;
    key->flags = SCHEMA_INDEX;

    if (onError == OE_None) {
        key->flags |= SCHEMA_DUP;
    } else {
        assert(onError == OE_Abort);
    }

    if (withOpts == 1) {
        key->flags |= SCHEMA_DATACOPY;
    }

    key->nmembers = pList->nExpr;
    key->member = comdb2_calloc(ctx->mem, pList->nExpr, sizeof(struct field));
    if (key->member == 0) goto oom;

    for (int i = 0; i < pList->nExpr; i++) {
        member = &key->member[i];
        member->name = comdb2_strdup(
            ctx->mem, pList->a[i].pExpr->u.zToken); /* zToken is 0-terminated */
        if (ctx->name == 0) goto oom;

        if (pList->a[i].sortOrder == SQLITE_SO_DESC) {
            member->flags |= INDEX_DESCEND;
        }
    }

    if (pPIWhere->pExpr != 0) {
        assert((pPIWhere->zEnd - pPIWhere->zStart - 2) > 0);
        key->where = comdb2_strndup(ctx->mem, pPIWhere->zStart + 1,
                                    pPIWhere->zEnd - pPIWhere->zStart - 2);
        if (key->where == 0) goto oom;
    }

    struct comdb2_schema *entry =
        comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_schema));
    if (entry == 0) goto oom;
    entry->schema = key;

    listc_abl(&ctx->key_list, entry);

    if (strlen(ctx->name) + 1 <= MAXTABLELEN) {
        max_size = strlen(ctx->name) + 1;
    } else {
        setError(pParse, SQLITE_MISUSE, "Tablename is too long.");
        goto cleanup;
    }

    if ((chkAndCopyTable(pParse, sc->table, ctx->name, max_size, 1)))
        goto cleanup;

    if (authenticateSC(sc->table, pParse)) goto cleanup;

    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    sc->dryrun = 0;

    fillTableOption(sc, ctx->table_options);

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
    struct comdb2_ddl_context *ctx;
    struct constraint *constraint;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

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

    if ((ctx->flags & COMDB2_DDL_CTX_FLAG_NOOP) != 0) {
        return;
    }

    /*
      pFromCol == 0 imples that the FOREIGN KEY was specified in the column
      definition.
    */
    if (pFromCol == 0) {
        if (pToCol && pToCol->nExpr != 1) {
            TokenStr(table, pTo);
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(
                pParse, "Foreign key on %s should reference only one column of "
                        "table %s.",
                ((struct comdb2_field *)LISTC_BOT(&ctx->column_list))
                    ->field->name,
                table);
            goto cleanup;
        }
        constraint = comdb2_calloc(ctx->mem, 1, sizeof(struct constraint));
        if (constraint == 0) goto oom;

        constraint->ncols = 1;
        constraint->column[0] = comdb2_strdup(
            ctx->mem,
            ((struct comdb2_field *)LISTC_BOT(&ctx->column_list))->field->name);
        if (constraint->column[0] == 0) goto oom;
        constraint->referenced_table = comdb2_strndup(ctx->mem, pTo->z, pTo->n);
        if (constraint->referenced_table == 0) goto oom;
        sqlite3Dequote(constraint->referenced_table);
        int n = sqlite3Strlen30(pToCol->a[0].zName);
        constraint->referenced_column[0] =
            comdb2_strndup(ctx->mem, pToCol->a[0].zName, n);
        if (constraint->referenced_column[0] == 0) goto oom;
        if ((check_constraint_action(pParse, &flags))) {
            goto cleanup;
        }
        constraint->flags = flags;

        struct comdb2_constraint *entry =
            comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_constraint));
        if (entry == 0) goto oom;

        entry->constraint = constraint;
        listc_abl(&ctx->constraint_list, entry);
    } else if (pToCol && pToCol->nExpr != pFromCol->nExpr) {
        setError(pParse, SQLITE_ERROR,
                 "Number of columns in foreign "
                 "key does not match the number of columns in the "
                 "referenced table.");
        goto cleanup;
    } else {
        constraint = comdb2_calloc(ctx->mem, 1, sizeof(struct constraint));
        if (constraint == 0) goto oom;

        constraint->ncols = pToCol->nExpr;
        constraint->referenced_table = comdb2_strndup(ctx->mem, pTo->z, pTo->n);
        if (constraint->referenced_table == 0) goto oom;
        sqlite3Dequote(constraint->referenced_table);

        int n;
        for (int i = 0; i < pToCol->nExpr; i++) {
            n = sqlite3Strlen30(pFromCol->a[i].zName);
            constraint->column[i] =
                comdb2_strndup(ctx->mem, pFromCol->a[i].zName, n);
            if (constraint->column[i] == 0) goto oom;

            n = sqlite3Strlen30(pToCol->a[i].zName);
            constraint->referenced_column[i] =
                comdb2_strndup(ctx->mem, pToCol->a[i].zName, n);
            if (constraint->referenced_column[i] == 0) goto oom;
        }
        if ((check_constraint_action(pParse, &flags))) {
            goto cleanup;
        }
        constraint->flags = flags;

        struct comdb2_constraint *entry =
            comdb2_calloc(ctx->mem, 1, sizeof(struct comdb2_constraint));
        if (entry == 0) goto oom;

        entry->constraint = constraint;
        listc_abl(&ctx->constraint_list, entry);
    }
    return;

oom:
    setError(pParse, SQLITE_NOMEM, "System out of memory");

cleanup:
    free_ddl_context(pParse);
    return;
}

void comdb2DeferForeignKey(Parse *pParse, int isDeferred)
{
    struct comdb2_ddl_context *ctx;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

    if (use_sqlite_impl(pParse)) {
        assert(ctx == 0);
        sqlite3DeferForeignKey(pParse, isDeferred);
        return;
    }
    return;
}

/*
  Iterate through the list of constraints and drop ones associated with
  this key.
*/
static void drop_dependent_cons(struct comdb2_ddl_context *ctx,
                                struct schema *key)
{
    int ncols;
    int match_found;
    struct comdb2_constraint *current_constraint;
    LISTC_FOR_EACH(&ctx->constraint_list, current_constraint, lnk)
    {
        match_found = 0;
        if (current_constraint->constraint->ncols == key->nmembers) {
            ncols = current_constraint->constraint->ncols;
            match_found = 1;

            for (int i = 0; i < ncols; i++) {
                if ((strcasecmp(current_constraint->constraint->column[i],
                                key->member[i].name))) {
                    match_found = 0;
                    break;
                }
            }

            /*
              Check whether this constraints uses the key to be dropped.
            */
            if (match_found) {
                current_constraint->constraint = 0;
            }
        }
    }
    return;
}

/*
  Remove the specified column from the current keys.
*/
static void drop_dependent_keys(struct comdb2_ddl_context *ctx,
                                const char *column)
{
    struct comdb2_schema *current_key;

    /* Check if an index exists that has this column a member. */
    LISTC_FOR_EACH(&ctx->key_list, current_key, lnk)
    {
        /* Skip if the key has already been dropped. */
        if (current_key->schema == 0) continue;

        for (int i = 0; i < current_key->schema->nmembers; i++) {
            if (strcasecmp(current_key->schema->member[i].name, column) == 0) {

                /*
                  If the index comprises only of this single column, we
                  should as well drop the index entry. And before doing
                  so, we must also drop it from the constraints list.
                */
                if (current_key->schema->nmembers == 1) {
                    drop_dependent_cons(ctx, current_key->schema);
                    /* Mark the key as dropped. */
                    current_key->schema = 0;
                    break;
                } else {
                    /*
                      Mark the member as removed by setting its name to 0.
                    */
                    current_key->schema->member[i].name = 0;
                }
            }
        }
    }
    return;
}

/*
  Drop the specified column.
*/
void comdb2DropColumn(Parse *pParse, /* Parser context */
                      Token *pName   /* Name of the column */
                      )
{
    struct comdb2_ddl_context *ctx;
    char *name;
    int column_exists = 0;

    ctx = (struct comdb2_ddl_context *)pParse->comdb2_ddl_ctx;

    if (ctx == 0) {
        /* An error must have been set. */
        assert(pParse->rc != 0);
        return;
    }

    assert(pParse->db->init.busy == 0);

    name = comdb2_strndup(ctx->mem, pName->z, pName->n);
    if (name == 0) goto oom;
    sqlite3Dequote(name);

    struct comdb2_field *current;
    LISTC_FOR_EACH(&ctx->column_list, current, lnk)
    {
        if ((strcasecmp(name, current->name)) == 0) {
            /* Modify/drop the index referring to this column. */
            drop_dependent_keys(ctx, name);

            /* Mark the column as deleted. */
            current->field = 0;
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

/*
  Internal implementation of DROP INDEX.
*/
static void comdb2DropIndexInt(Parse *pParse, struct dbtable *table,
                               const char *idx_name)
{
    Vdbe *v;
    struct schema_change_type *sc;
    struct comdb2_ddl_context *ctx;
    int max_size;

    assert(use_sqlite_impl(pParse) == 0);

    v = sqlite3GetVdbe(pParse);

    sc = new_schemachange_type();
    if (sc == 0) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    ctx = create_ddl_context(pParse);
    if (ctx == 0) goto oom;

    ctx->name = table->dbname;

    /*
      Add all the columns, indexes and constraints in the table to the
      respective list.
     */
    if (retrieve_schema(pParse, ctx)) {
        goto cleanup;
    }

    /* Mark the index as dropped. */
    struct comdb2_schema *current_key;
    int found = 0;
    LISTC_FOR_EACH(&ctx->key_list, current_key, lnk)
    {
        if ((strcasecmp(current_key->schema->csctag, idx_name)) == 0) {
            found = 1;

            /* Also drop the constraints associated with this key.  */
            drop_dependent_cons(ctx, current_key->schema);

            /* Mark the key as deleted. */
            current_key->schema = 0;
            break;
        }
    }
    /* At this point the index must have been found. */
    assert(found == 1);

    if (strlen(ctx->name) + 1 <= MAXTABLELEN) {
        max_size = strlen(ctx->name) + 1;
    } else {
        setError(pParse, SQLITE_MISUSE, "Table name is too long.");
        goto cleanup;
    }

    if ((chkAndCopyTable(pParse, sc->table, ctx->name, max_size, 1)))
        goto cleanup;

    if (authenticateSC(sc->table, pParse)) goto cleanup;

    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    sc->dryrun = 0;

    fillTableOption(sc, ctx->table_options);

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
  Top-level implementation for DROP INDEX.
  All the cleanups take place in comdb2DropIndexInt().
*/
void comdb2DropIndex(Parse *pParse, SrcList *pName, int ifExists)
{
    struct dbtable *table;
    struct dbtable *parent_table;
    int index_count = 0;

    assert(pParse->comdb2_ddl_ctx == 0);

    if (use_sqlite_impl(pParse)) {
        sqlite3DropIndex(pParse, pName, ifExists);
        return;
    }

    assert(pName->nSrc == 1);

    if (pName->a[0].zDatabase != 0) {
        setError(pParse, SQLITE_MISUSE,
                 "DDL commands operate only on local dbs");
        return;
    }

    /*
      Unlike Sqlite, Comdb2 allows multiples keys with same name
      but in different tables. So, we first need to check if the
      specified index is unique across ALL the tables, and only
      drop the index if it is, or fail otherwise.
    */

    for (int i = 0; i < thedb->num_dbs; i++) {
        table = thedb->dbs[i];
        for (int j = 0; j < table->schema->nix; j++) {
            if ((strcasecmp(table->schema->ix[j]->csctag, pName->a[0].zName)) ==
                0) {
                parent_table = table;
                index_count++;
            }
        }
    }

    if (index_count == 0) {
        if (ifExists == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Index '%s' not found.", pName->a[0].zName);
        }
        return;
    }

    if (index_count > 1) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Multiple indexes with same name '%s'.",
                        pName->a[0].zName);
        return;
    }

    comdb2DropIndexInt(pParse, parent_table, pName->a[0].zName);

    return;
}

/*
  Top-level implementation for DROP INDEX .. ON .. command.
  All the cleanups take place in comdb2DropIndexInt().
*/
void comdb2DropIndexExtn(Parse *pParse, Token *idxName, Token *tabName,
                         int ifExists)
{
    struct dbtable *table;
    int found = 0;

    assert(pParse->comdb2_ddl_ctx == 0);

    TokenStr(table_name, tabName);
    sqlite3Dequote(table_name);
    table = get_dbtable_by_name(table_name);
    if (table == 0) {
        pParse->rc = SQLITE_ERROR;
        sqlite3ErrorMsg(pParse, "Table '%s' not found.", table_name);
        return;
    }

    /* Check whether an index exist with the specified name. */
    TokenStr(index_name, idxName);
    sqlite3Dequote(index_name);
    for (int i = 0; i < table->schema->nix; i++) {
        if ((strcasecmp(table->schema->ix[i]->csctag, index_name)) == 0) {
            found = 1;
            break;
        }
    }

    if (found == 0) {
        if (ifExists == 0) {
            pParse->rc = SQLITE_ERROR;
            sqlite3ErrorMsg(pParse, "Index '%s' not found.", index_name);
        }
        return;
    }

    comdb2DropIndexInt(pParse, table, index_name);

    return;
}

void comdb2putTunable(Parse *pParse, Token *name, Token *value)
{
    char *t_name;
    char *t_value;
    int rc;
    comdb2_tunable_err err;

    rc = create_string_from_token(NULL, pParse, &t_name, name);
    if (rc != SQLITE_OK) goto cleanup; /* Error has been set. */
    rc = create_string_from_token(NULL, pParse, &t_value, value);
    if (rc != SQLITE_OK) goto cleanup; /* Error has been set. */

    if ((err = handle_runtime_tunable(t_name, t_value))) {
        setError(pParse, SQLITE_ERROR, tunable_error(err));
    }

cleanup:
    free(t_name);
    free(t_value);
    return;
}
