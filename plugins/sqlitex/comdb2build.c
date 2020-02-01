#include <stdio.h>
#include "sqliteInt.h"
#include <vdbeInt.h>
#include "comdb2build.h"
#include "comdb2vdbe.h"
#include <stdlib.h>
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
#include <str0.h>

#define INCLUDE_KEYWORDHASH_H 
#define INCLUDE_FINALKEYWORD_H
#include <keywordhash.h>
extern pthread_key_t query_info_key;
/******************* Utility ****************************/

static inline int setError(Parse *pParse, int rc, char *msg)
{
    pParse->rc = rc;
    sqlitexErrorMsg(pParse, "%s", msg);
    return rc;
}

// return 0 on failure to parse
int readIntFromTokenX(Token* t, int *rst)
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

static inline int isRemote(Token *t, Token *t2)
{
    return t->n > 0 && (t2 != NULL && t2->n > 0) ;
}

extern int gbl_allow_user_schema;

static inline int chkAndCopyTable(Vdbe* v, Parse* pParse, char *dst,
    const char* name, size_t max_length, int mustexist)
{
    char tmp_dst[MAXTABLELEN];
    struct sql_thread *thd =pthread_getspecific(query_info_key);
    /* Remove quotes if any. */
    if (((name[0] == '\'') && (name[max_length-2] == '\'')) || ((name[0] == '"') && (name[max_length-2] == '"'))) {
      strncpy(tmp_dst, name+1, max_length-2);
      /* Guarantee null termination. */
      tmp_dst[max_length - 3] = '\0';
    } else {
      strncpy(tmp_dst, name, max_length);
      /* Guarantee null termination. */
      tmp_dst[max_length - 1] = '\0';
    }

    if(gbl_allow_user_schema && thd->clnt->user[0] != '\0' ) {
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

        if (timepart_is_shard(dst, 1, NULL))
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
    
    if (t1->n + 1 <= MAXTABLELEN)
        max_size = t1->n + 1;
    else
        return setError(pParse, SQLITE_MISUSE, "Tablename is too long");

    if (isRemote(t1, t2))
       return setError(pParse, SQLITE_MISUSE, "DDL commands operate only on local dbs");

    if ((rc = chkAndCopyTable(v, pParse, dst, t1->z, max_size, mustexist)))
        return rc;

    return SQLITE_OK;
}

void fillTableOption(struct schema_change_type* sc, int opt)
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
    {
        sc->instant_sc = 1;
    }

	sc->compress_blobs = -1;
    if (OPT_ON(opt, BLOB_RLE))
        sc->compress_blobs = BDB_COMPRESS_RLE8;
    else if (OPT_ON(opt, BLOB_ZLIB))
        sc->compress_blobs = BDB_COMPRESS_ZLIB;
    else if (OPT_ON(opt, BLOB_LZ4))
        sc->compress_blobs = BDB_COMPRESS_LZ4;

    sc->compress = -1;
    if (OPT_ON(opt, REC_RLE))
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
}

int comdb2AuthenticateUserDDL(Vdbe* v, char *tablename, Parse* pParse)
{
     struct sql_thread *thd = pthread_getspecific(query_info_key);
     bdb_state_type *bdb_state = thedb->bdb_env;
     int bdberr; 
     int authOn = bdb_authentication_get(bdb_state, NULL, &bdberr); 
    
     if (authOn != 0)
        return SQLITE_OK;

     if (thd->clnt && tablename && thd->clnt->user)
     {
        if (bdb_tbl_op_access_get(bdb_state, NULL, 0, 
            tablename, thd->clnt->user, &bdberr))
          return SQLITE_AUTH;
        else
            return SQLITE_OK;
     }

     return SQLITE_AUTH;
}


int comdb2AuthenticateUserOpX(Vdbe* v, Parse* pParse)
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

     if (thd->clnt && thd->clnt->user)
     {
         /* Authenticate the password first, as we haven't been doing it so far. */
         if (bdb_user_password_check(thd->clnt->user, thd->clnt->password, NULL) == 0) {
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

int comdb2SqlSchemaChange(OpFunc *f)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct schema_change_type *s = (struct schema_change_type*)f->arg;
    osql_sock_start(thd->clnt, OSQL_SOCK_REQ ,0);
    osql_schemachange_logic(s, thd, 0);
    int rst = osql_sock_commit(thd->clnt, OSQL_SOCK_REQ);
    osqlstate_t *osql = &thd->clnt->osql;
    if (osql->xerr.errval == COMDB2_SCHEMACHANGE_OK) {
        osql->xerr.errval = 0;
    }
    f->rc = osql->xerr.errval;
    f->errorMsg = osql->xerr.errstr;
    return f->rc;
}

static int comdb2ProcSchemaChange(OpFunc *f)
{
	int rc = comdb2SqlSchemaChange(f);
	if (rc == 0) {
		opFuncPrintfX(f, "%s", f->errorMsg);
	}
	return rc;
}

void free_rstMsgX(struct rstMsg* rec)
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
int comdb2SendBpfuncX(OpFunc *f)
{
   struct sql_thread    *thd = pthread_getspecific(query_info_key);
   struct sqlclntstate  *clnt = thd->clnt;
   osqlstate_t          *osql = &clnt->osql;
   int rc = 0;

   BpfuncArg *arg = (BpfuncArg*)f->arg;

   osql_sock_start(clnt, OSQL_SOCK_REQ ,0);

   rc = osql_send_bpfunc(osql->host, osql->rqid, thd->clnt->osql.uuid, arg,
                         NET_OSQL_SOCK_RPL, osql->logsb);

   rc = osql_sock_commit(clnt, OSQL_SOCK_REQ);

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

static void comdb2rebuild(Parse *p, Token* nm, Token* lnm, int opt);

/************************** Function definitions ****************************/

int authenticateSC(struct schema_change_type* sc,  Parse *pParse) {
    Vdbe *v  = sqlitexGetVdbe(pParse);
    char *username = strstr(sc->tablename, "@");
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    if (username && strcmp(username+1, thd->clnt->user) == 0) {
        return 0;
    } else if (comdb2AuthenticateUserDDL(v, sc->tablename, pParse) == 0) {
        return 0;
    } else if (comdb2AuthenticateUserOpX(v, pParse) == 0) {
        return 0;
    }
    return -1;
}

void comdb2CreateTable(
  Parse *pParse,   /* Parser context */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int opt,         /* True if this is a TEMP table */
  Token *csc2,
  int temp,
  int noErr
)
{
    struct schema_change_type* sc = NULL;
    sqlitex *db = pParse->db;
    Vdbe *v  = sqlitexGetVdbe(pParse);
    if (temp) {
        setError(pParse, SQLITE_MISUSE, "can't create temp csc2 table");
	return;
    }
    sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }
    TokenStr(table, pName1);
    if (noErr && get_dbtable_by_name(table))
            goto out;
    if (chkAndCopyTableTokens(v, pParse, sc->tablename, pName1, pName2, 0))
        goto out;

    if (authenticateSC(sc, pParse))
        return;

    v->readOnly = 0;
    sc->addonly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    fillTableOption(sc, opt);
    copyNosqlToken(v, pParse, &sc->newcsc2, csc2);
    comdb2prepareNoRowsX(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree) &free_schema_change_type);
    return;
out:
    free_schema_change_type(sc);

}

void comdb2AlterTable(
  Parse *pParse,   /* Parser context */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int opt,      /* True if this is a TEMP table */
  Token *csc2
)
{
    sqlitex *db = pParse->db;
    Vdbe *v  = sqlitexGetVdbe(pParse);
    
    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse,sc->tablename, pName1, pName2, 1))
        return;

    if (authenticateSC(sc, pParse))
        return;

    v->readOnly = 0;
    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->use_plan = 1;
    sc->live = 1;
    sc->scanmode = SCAN_PARALLEL;
    fillTableOption(sc, opt);

    copyNosqlToken(v, pParse, &sc->newcsc2, csc2);
    comdb2prepareNoRowsX(v, pParse, 0,  sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);

}

void comdb2DropTableX(Parse *pParse, SrcList *pName)
{

    sqlitex *db = pParse->db;
    Vdbe *v  = sqlitexGetVdbe(pParse);

    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        free_schema_change_type(sc);
        return;
    }

    if (chkAndCopyTable(v, pParse, sc->tablename, pName->a[0].zName, MAXTABLELEN, 1))
        return;

    if (authenticateSC(sc, pParse))
        return;


    v->readOnly = 0;
    sc->same_schema = 1;
    sc->drop_table = 1;
    sc->fastinit = 1;
    sc->nothrevent = 1;
    
    if(get_csc2_file(sc->tablename, -1 , &sc->newcsc2, NULL ))
    {
        fprintf(stderr, "%s: table schema not found: \n",  sc->tablename);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        free_schema_change_type(sc); 
        return;
    }

    comdb2prepareNoRowsX(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
}

static inline void comdb2rebuild(Parse *pParse, Token* nm, Token* lnm, int opt)
{
    sqlitex *db = pParse->db;
    Vdbe *v  = sqlitexGetVdbe(pParse);

    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse,sc->tablename, nm, lnm, 1))
        return;

    if (authenticateSC(sc, pParse))
        return;


    v->readOnly = 0;
    sc->nothrevent = 1;
    sc->scanmode = gbl_default_sc_scanmode;
    sc->same_schema = 1;
    
    if (OPT_ON(opt, REBUILD_ALL))
        sc->force_rebuild = 1;

    if (OPT_ON(opt, REBUILD_DATA))
        sc->force_dta_rebuild = 1;
    
    if (OPT_ON(opt, REBUILD_BLOB))
        sc->force_blob_rebuild = 1;

    if (OPT_ON(opt, PAGE_ORDER))
        sc->scanmode = SCAN_PAGEORDER;

    if (OPT_ON(opt, READ_ONLY))
        sc->live = 0;
    else
        sc->live = 1;


    if(get_csc2_file(sc->tablename, -1 , &sc->newcsc2, NULL ))
    {
        fprintf(stderr, "%s: table schema not found: \n",  sc->tablename);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        free_schema_change_type(sc); 
        return;
    }
    comdb2prepareNoRowsX(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);

}


void comdb2rebuildFull(Parse* p, Token* nm,Token* lnm, int opt)
{
    comdb2rebuild(p, nm,lnm, REBUILD_ALL + REBUILD_DATA + REBUILD_BLOB + opt);
}


void comdb2rebuildData(Parse* p, Token* nm, Token* lnm, int opt)
{
    comdb2rebuild(p,nm,lnm,REBUILD_DATA + opt);
}

void comdb2rebuildDataBlob(Parse* p,Token* nm, Token* lnm, int opt)
{
    comdb2rebuild(p, nm, lnm, REBUILD_BLOB + opt);
}

void comdb2truncate(Parse* pParse, Token* nm, Token* lnm)
{
    sqlitex *db = pParse->db;
    Vdbe *v  = sqlitexGetVdbe(pParse);

    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v, pParse,sc->tablename, nm, lnm, 1))
        return;

    if (authenticateSC(sc, pParse))
        return;

    v->readOnly = 0;
    sc->fastinit = 1;
    sc->nothrevent = 1;
    sc->same_schema = 1;

    if(get_csc2_file(sc->tablename, -1 , &sc->newcsc2, NULL ))
    {
        fprintf(stderr, "%s: table schema not found: \n",  sc->tablename);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        free_schema_change_type(sc); 
        return;
    }
    comdb2prepareNoRowsX(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
}


void comdb2rebuildIndex(Parse* pParse, Token* nm, Token* lnm, Token* index, int opt)
{
    sqlitex *db = pParse->db;
    Vdbe *v  = sqlitexGetVdbe(pParse);
    char* indexname;
    int index_num;

    struct schema_change_type* sc = new_schemachange_type();

    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }

    if (chkAndCopyTableTokens(v,pParse,sc->tablename, nm, lnm, 1))
        return;

    if (authenticateSC(sc, pParse))
        return;

    if(get_csc2_file(sc->tablename, -1 , &sc->newcsc2, NULL ))
    {
        fprintf(stderr, "%s: table schema not found: \n",  sc->tablename);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        free_schema_change_type(sc); 
        return;
    }

    if (create_string_from_token(v, pParse, &indexname, index))
        return; // TODO RETURN ERROR

    int rc = getidxnumbyname(sc->tablename, indexname, &index_num );
    if(rc) {
        fprintf(stderr, "!table:index '%s:%s' not found\n", sc->tablename, indexname);
        free_schema_change_type(sc);
        setError(pParse, SQLITE_ERROR, "Index not found");
        return;
    }
    
    free(indexname);

    v->readOnly = 0;
    sc->nothrevent = 1;
    sc->rebuild_index = 1;
    sc->index_to_rebuild = index_num;
    sc->scanmode = gbl_default_sc_scanmode;
    sc->same_schema = 1;

    if (OPT_ON(opt, PAGE_ORDER))
        sc->scanmode = SCAN_PAGEORDER;

    if (OPT_ON(opt, READ_ONLY))
        sc->live = 0;
    else
        sc->live = 1;

    comdb2prepareNoRowsX(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
}

/********************** STORED PROCEDURES ****************************************/

void comdb2CreateProcedureX(Parse* pParse, Token* nm, Token* ver, Token* proc)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);
    if (comdb2AuthenticateUserOpX(v, pParse))
        return;     
    TokenStr(name, nm);
    if (strlen(name) >= MAX_SPNAME) {
        sqlitexErrorMsg(pParse, "bad procedure name:%s", name);
        return;
    }
    struct schema_change_type* sc = new_schemachange_type();
    strcpy(sc->tablename, name);
    sc->newcsc2 = malloc(proc->n);
    sc->addsp = 1;
    if (ver) {
        TokenStr(version, ver);
        size_t len = strlen(version);
        if (len == 0 || len >= MAX_SPVERSION_LEN) {
            sqlitexErrorMsg(pParse, "bad procedure version:%s", version);
            return;
        }
        strcpy(sc->fname, version);
    }
    copyNosqlToken(v, pParse, &sc->newcsc2, proc);
    v->readOnly = 0;
    char* colname[] = {"version"};
    int coltype = OPFUNC_STRING_TYPE;
    OpFuncSetup stp = {1, (char **)colname, (int *)&coltype, 256};
    comdb2prepareOpFuncX(v, pParse, 1, sc, &comdb2ProcSchemaChange, (vdbeFuncArgFree)&free_schema_change_type, &stp);
}

void comdb2DefaultProcedureX(Parse* pParse, Token* nm, Token* ver, int str)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);
    if (comdb2AuthenticateUserOpX(v, pParse))
        return;     
    TokenStr(name, nm);
    if (strlen(name) >= MAX_SPNAME) {
        sqlitexErrorMsg(pParse, "bad procedure name:%s", name);
        return;
    }
    struct schema_change_type* sc = new_schemachange_type();
    strcpy(sc->tablename, name);
    if (str) {
        TokenStr(version, ver);
        size_t len = strlen(version);
        if (len == 0 || len >= MAX_SPVERSION_LEN) {
            sqlitexErrorMsg(pParse, "bad procedure version:%s", version);
            return;
        }
        strcpy(sc->fname, version);
    } else {
        sc->newcsc2 = malloc(ver->n + 1);
        strncpy(sc->newcsc2, ver->z, ver->n);
    }
    v->readOnly = 0;
    sc->defaultsp = 1;

    comdb2prepareNoRowsX(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
}

void comdb2DropProcedureX(Parse* pParse, Token* nm, Token* ver, int str)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);
    int rc;   
    struct schema_change_type* sc = new_schemachange_type();
    int max_length = nm->n < MAXTABLELEN ? nm->n : MAXTABLELEN;

    if (comdb2AuthenticateUserOpX(v, pParse))
        return;       

    strncpy(sc->tablename, nm->z, max_length);
    
    if (sc == NULL)
    {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }
    if (str) {
        TokenStr(version, ver);
        strcpy(sc->fname, version);
    } else {
        sc->newcsc2 = malloc(ver->n + 1);
        strncpy(sc->newcsc2, ver->z, ver->n);
    }
    v->readOnly = 0;
    sc->delsp = 1;
  
    comdb2prepareNoRowsX(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);    
}
/********************* PARTITIONS  **********************************************/


void comdb2CreateTimePartition(Parse* pParse, Token* table, Token* partition_name, Token* period, Token* retention, Token* start)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    int max_length;

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err; 

    BpfuncCreateTimepart *tp = malloc(sizeof(BpfuncCreateTimepart));
    
    if (tp)
        bpfunc_create_timepart__init(tp);
    else
        goto err;
    
    arg->crt_tp = tp;
    arg->type = BPFUNC_CREATE_TIMEPART;
    tp->tablename = (char*) malloc(MAXTABLELEN);
    memset(tp->tablename, '\0', MAXTABLELEN);
    if (table && chkAndCopyTableTokens(v, pParse, tp->tablename, table, NULL, 1)) 
        goto err;

    if (partition_name->n >= MAXTABLELEN)
    {
        setError(pParse, SQLITE_ERROR, "Partition name too long");
        goto clean_arg;
    }
    tp->partition_name = (char*) malloc(MAXTABLELEN);
    memset(tp->partition_name, '\0', MAXTABLELEN);
    strncpy(tp->partition_name, partition_name->z, partition_name->n);

    char period_str[50];

    assert (*period->z == '\'' || *period->z == '\"');
    period->z++;
    period->n -= 2;

    if (period->n >= sizeof(period_str)) {
        setError(pParse, SQLITE_MISUSE, "Invalid period name");
        goto clean_arg;
    }
    strncpy0(period_str, period->z, period->n + 1);
    tp->period = name_to_period(period_str);

    if (tp->period == VIEW_PARTITION_INVALID)
    {
        setError(pParse, SQLITE_ERROR, "Invalid period name");
        goto clean_arg;
    }

    char retention_str[10];
    if (retention->n >= sizeof(retention_str)) {
        setError(pParse, SQLITE_MISUSE, "Invalid retention");
        goto clean_arg;
    }
    strncpy0(retention_str, retention->z, retention->n + 1);
    tp->retention = atoi(retention_str);

    char start_str[200];

    assert (*start->z == '\'' || *start->z == '\"');
    start->z++;
    start->n -= 2;

    if (start->n >= sizeof(start_str)) {
        setError(pParse, SQLITE_MISUSE, "Invalid start date");
        goto clean_arg;
    }
    strncpy0(start_str, start->z, start->n + 1);
    tp->start = convert_from_start_string(tp->period, start_str);

    if (tp->start == -1 )
    {
        setError(pParse, SQLITE_ERROR, "Invalid start date");
        goto clean_arg;
    }

    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, (vdbeFuncArgFree) &free_bpfunc_arg);
    return;

err:
        setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);   
    
}


void comdb2DropTimePartition(Parse* pParse, Token* partition_name)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);
    int max_length;

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));
    
    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err; 
    
    BpfuncDropTimepart *tp = malloc(sizeof(BpfuncDropTimepart));
    

    if (tp)
        bpfunc_drop_timepart__init(tp);
    else
        goto err;
    
    arg->drop_tp = tp;
    arg->type = BPFUNC_DROP_TIMEPART;
    max_length = partition_name->n < MAXTABLELEN ? partition_name->n : MAXTABLELEN;
    tp->partition_name = (char*) malloc(MAXTABLELEN);
    memset(tp->partition_name, '\0', MAXTABLELEN);
    strncpy(tp->partition_name, partition_name->z, max_length);

    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
        setError(pParse, SQLITE_INTERNAL, "Internal Error");
//clean_arg:
    free_bpfunc_arg(arg);

}


/********************* BULK IMPORT ***********************************************/

void comdb2bulkimportX(Parse* pParse, Token* nm,Token* lnm, Token* nm2, Token* lnm2)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    setError(pParse, SQLITE_INTERNAL, "Not Implemented");
    //fprintf(stderr, "Bulk import from %.*s to", nm->n + lnm->n, nm->z, nm2->n +lnm2->n, nm2->z);
}

/********************* ANALYZE ***************************************************/

int comdb2vdbeAnalyzeX(OpFunc *f)
{
    char *tablename = f->arg;
    int percentage = f->int_arg;
    int rc;

    if (tablename == NULL)
        rc = analyze_database(NULL, percentage, 1);
    else
        rc = analyze_table(tablename, NULL, percentage,1);

    if (rc)
    {
        f->rc = rc;
        f->errorMsg = "Analyze could not run because of internal problems";
    } else
    {
        f->rc = SQLITE_OK;
        f->errorMsg = "";
    } 

    return rc;
}


void comdb2analyzeX(Parse* pParse, int opt, Token* nm, Token* lnm, int pc)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    int percentage = pc;
    int threads = GET_ANALYZE_THREAD(opt);
    int sum_threads = GET_ANALYZE_SUMTHREAD(opt);

    if (comdb2AuthenticateUserOpX(v, pParse))
        return;

    if (threads > 0)
        analyze_set_max_table_threads(NULL, &threads);
    if (sum_threads)
        analyze_set_max_sampling_threads(NULL, &sum_threads);

    if (nm == NULL)
    {
        comdb2prepareNoRowsX(v, pParse, pc, NULL, &comdb2vdbeAnalyzeX, (vdbeFuncArgFree) &free);
    } else
    {
        char *tablename = (char*) malloc(MAXTABLELEN);
        if (!tablename)
            goto err;

        if (nm && lnm && chkAndCopyTableTokens(v, pParse, tablename, nm, lnm, 1)) 
        {
            free(tablename);
            goto err;
        }
        else
           comdb2prepareNoRowsX(v, pParse, pc, tablename, &comdb2vdbeAnalyzeX, (vdbeFuncArgFree) &free); 
    }

    return;

err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");

}

void comdb2analyzeCoverageX(Parse* pParse, Token* nm, Token* lnm, int newscale)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    if (comdb2AuthenticateUserOpX(v, pParse))
        return;

    if (newscale < -1 || newscale > 100)
    {
        setError(pParse, SQLITE_ERROR, "Coverage must be between -1 and 100");
        return;
    }

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));

    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err;
    BpfuncAnalyzeCoverage *ancov_f = (BpfuncAnalyzeCoverage*) malloc(sizeof(BpfuncAnalyzeCoverage));

    if (ancov_f)
        bpfunc_analyze_coverage__init(ancov_f);
    else
        goto err;

    arg->an_cov = ancov_f;
    arg->type = BPFUNC_ANALYZE_COVERAGE;
    ancov_f->tablename = (char*) malloc(MAXTABLELEN);

    if (!ancov_f->tablename)
        goto err;

    if (chkAndCopyTableTokens(v, pParse, ancov_f->tablename, nm, lnm, 1)) 
        goto clean_arg;

    ancov_f->newvalue = newscale;

    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);
}

void comdb2analyzeThresholdX(Parse* pParse, Token* nm, Token* lnm, int newthreshold)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    if (comdb2AuthenticateUserOpX(v, pParse))
        return;

    if (newthreshold < -1 || newthreshold > 100)
    {
        setError(pParse, SQLITE_ERROR, "Threshold must be between -1 and 100");
        return;
    }

    BpfuncArg *arg = (BpfuncArg*) malloc(sizeof(BpfuncArg));

    if (arg)
        bpfunc_arg__init(arg);
    else
        goto err;

    BpfuncAnalyzeThreshold *anthr_f = (BpfuncAnalyzeThreshold*) malloc(sizeof(BpfuncAnalyzeThreshold));

    if (anthr_f)
        bpfunc_analyze_threshold__init(anthr_f);
    else
        goto err;

    arg->an_thr = anthr_f;
    arg->type = BPFUNC_ANALYZE_THRESHOLD;
    anthr_f->tablename = (char*) malloc(MAXTABLELEN);

    if (!anthr_f->tablename)
        goto err;

    if (chkAndCopyTableTokens(v, pParse, anthr_f->tablename, nm, lnm, 1)) 
        goto clean_arg;

    anthr_f->newvalue = newthreshold;
    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);
}

/********************* ALIAS **************************************************/

void comdb2setAliasX(Parse* pParse, Token* name, Token* url)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    if (comdb2AuthenticateUserOpX(v, pParse))
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

    if (name && chkAndCopyTableTokens(v,pParse, alias_f->name, name, NULL, 0)) 
        goto clean_arg;

    assert (*url->z == '\'' || *url->z == '\"');
    url->z++;
    url->n -= 2;

    if (create_string_from_token(v, pParse, &alias_f->remote, url))
        goto clean_arg;

    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);
}

void comdb2getAliasX(Parse* pParse, Token* t1)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    if (comdb2AuthenticateUserOpX(v, pParse))
        return;       

    setError(pParse, SQLITE_INTERNAL, "Not Implemented");
    fprintf(stderr, "Getting alias %.*s", t1->n, t1->z); 
}

/********************* GRANT AUTHORIZAZIONS ************************************/

void comdb2grantX(Parse* pParse, int revoke, int permission, Token* nm,Token* lnm, Token* u)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    if (comdb2AuthenticateUserOpX(v, pParse))
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
    } else {
      if (nm && lnm && chkAndCopyTableTokens(v,pParse, grant->table, nm, lnm, 1))
          goto clean_arg;
    }

    if (create_string_from_token(v, pParse, &grant->username, u))
        goto clean_arg;

    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);

}

/****************************** AUTHENTICATION ON/OFF *******************************/

void comdb2enableAuthX(Parse* pParse, int on)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    int rc = SQLITE_OK;
 
    if (comdb2AuthenticateOpPassword(v, pParse)) 
    {
        setError(pParse, SQLITE_AUTH, "User does not have OP credentials");
        return;
    }

    if(!on)
    {
        setError(pParse, SQLITE_INTERNAL, "SET AUTHENTICATION OFF is not allowed");
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

    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, 
        (vdbeFuncArgFree) &free_bpfunc_arg);

    return;

clean_arg:
    free_bpfunc_arg(arg);

}

/****************************** PASSWORD *******************************/

void comdb2setPasswordX(Parse* pParse, Token* pwd, Token* nm)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);
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
        
    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, 
        (vdbeFuncArgFree) &free_bpfunc_arg);
    
    return;

clean_arg:
    free_bpfunc_arg(arg);  
}

void comdb2deletePasswordX(Parse* pParse, Token* nm)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);
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

    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, 
        (vdbeFuncArgFree) &free_bpfunc_arg);
    
    return;

clean_arg:
    free_bpfunc_arg(arg);  
}

int comdb2genidcontainstimeX(void)
{
     bdb_state_type *bdb_state = thedb->bdb_env;
     return genid_contains_time(bdb_state);
}

int producekwX(OpFunc *f)
{

    for (int i=0; i < SQLITE_N_KEYWORD; i++)
    {
        if ((f->int_arg == KW_ALL) ||
            (f->int_arg == KW_RES && f_keywords[i].reserved) ||
            (f->int_arg == KW_FB && !f_keywords[i].reserved))
                opFuncPrintfX(f, "%s", f_keywords[i].name );
    }
    f->rc = SQLITE_OK;
    f->errorMsg = NULL;
    return SQLITE_OK;
}

void comdb2getkwX(Parse* pParse, int arg)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);
    const char* colname[] = {"Keyword"};
    const int coltype = OPFUNC_STRING_TYPE;
    OpFuncSetup stp = {1, (char **)colname, (int *)&coltype, SQLITE_KEYWORD_LEN};
    comdb2prepareOpFuncX(v, pParse, arg, NULL, &producekwX, (vdbeFuncArgFree)  &free, &stp);

}

static int produceAnalyzeCoverage(OpFunc *f)
{

    char  *tablename = (char*) f->arg;
    int rst;
    int bdberr; 
    int rc = bdb_get_analyzecoverage_table(NULL, tablename, &rst, &bdberr);
    
    if (!rc)
    {
        opFuncWriteIntegerX(f, (int) rst );
        f->rc = SQLITE_OK;
        f->errorMsg = NULL;
    } else 
    {
        f->rc = SQLITE_INTERNAL;
        f->errorMsg = "Could not read value";
    }
    return SQLITE_OK;
}

void comdb2getAnalyzeCoverageX(Parse* pParse, Token *nm, Token *lnm)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);
    const char* colname[] = {"Coverage"};
    const int coltype = OPFUNC_INT_TYPE;
    OpFuncSetup stp = {1, (char **)colname, (int *)&coltype, 256};
    char *tablename = (char*) malloc (MAXTABLELEN);

    if (nm && lnm && chkAndCopyTableTokens(v,pParse, tablename, nm, lnm, 1)) 
        goto clean;
    
    comdb2prepareOpFuncX(v, pParse, 0, tablename, &produceAnalyzeCoverage, (vdbeFuncArgFree)  &free, &stp);

    return;

clean:
    free(tablename);
}

static int produceAnalyzeThreshold(OpFunc *f)
{

    char  *tablename = (char*) f->arg;
    long long int rst;
    int bdberr; 
    int rc = bdb_get_analyzethreshold_table(NULL, tablename, &rst, &bdberr);
    
    if (!rc)
    {
        opFuncWriteIntegerX(f, (int) rst );
        f->rc = SQLITE_OK;
        f->errorMsg = NULL;
    } else 
    {
        f->rc = SQLITE_INTERNAL;
        f->errorMsg = "Could not read value";
    }
    return SQLITE_OK;
}

void comdb2getAnalyzeThresholdX(Parse* pParse, Token *nm, Token *lnm)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);
    const char* colname[] = {"Threshold"};
    const int coltype = OPFUNC_INT_TYPE;
    OpFuncSetup stp = {1, (char **)colname, (int *)&coltype, 256};
    char *tablename = (char*) malloc (MAXTABLELEN);

    if (nm && lnm && chkAndCopyTableTokens(v,pParse, tablename, nm, lnm, 1)) 
        goto clean;
    
    comdb2prepareOpFuncX(v, pParse, 0, tablename, &produceAnalyzeThreshold, (vdbeFuncArgFree)  &free, &stp);

    return;

clean:
    free(tablename);
}

void resolveTableNameX(struct SrcList_item *p, char *zDB, char *tableName)
{
   struct sql_thread *thd = pthread_getspecific(query_info_key);
   if ((zDB && (!strcasecmp(zDB, "main") || !strcasecmp(zDB, "temp"))))
   {
       sprintf(tableName, "%s", p->zName);
   } else if (thd->clnt && (thd->clnt->user[0] != '\0') && !strstr(p->zName, "@")
          && strncasecmp(p->zName, "sqlite_", 7) && strncasecmp(p->zName, "comdb2sys_", 10))
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


void comdb2timepartRetentionX(Parse *pParse, Token *nm, Token *lnm, int retention)
{
    Vdbe *v  = sqlitexGetVdbe(pParse);

    if (comdb2AuthenticateUserOpX(v, pParse))
        return;

    if (retention < 2)
    {
        setError(pParse, SQLITE_ERROR, "Retention must be 2 or higher");
        return;
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
        goto clean_arg;

    tp_retention->newvalue = retention;

    comdb2prepareNoRowsX(v, pParse, 0, arg, &comdb2SendBpfuncX, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);
}

/* vim: set ts=4 sw=4 et: */
