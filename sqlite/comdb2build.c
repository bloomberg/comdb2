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
#include <logmsg.h>

#define INCLUDE_KEYWORDHASH_H 
#define INCLUDE_FINALKEYWORD_H
#include <keywordhash.h>
extern pthread_key_t query_info_key;
extern int gbl_commit_sleep;
extern int gbl_convert_sleep;
/******************* Utility ****************************/

static inline int setError(Parse *pParse, int rc, char *msg)
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
        struct db *db = getdbbyname(dst);

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
        sc->instant_sc = 1;

    if (OPT_ON(opt, BLOB_RLE))
        sc->compress_blobs = BDB_COMPRESS_RLE8;
    if (OPT_ON(opt, BLOB_CRLE))
        sc->compress_blobs = BDB_COMPRESS_NONE; // FIX this it should not exist
    if (OPT_ON(opt, BLOB_ZLIB))
        sc->compress_blobs = BDB_COMPRESS_ZLIB;
    if (OPT_ON(opt, BLOB_LZ4))
        sc->compress_blobs = BDB_COMPRESS_LZ4;

    if (sc->compress_blobs != BDB_COMPRESS_RLE8 &&
        sc->compress_blobs != BDB_COMPRESS_ZLIB && 
        sc->compress_blobs != BDB_COMPRESS_LZ4)
                sc->compress_blobs = BDB_COMPRESS_NONE;

    if (OPT_ON(opt, REC_RLE))
        sc->compress = BDB_COMPRESS_RLE8;
    if (OPT_ON(opt, REC_CRLE))
        sc->compress = BDB_COMPRESS_CRLE;
    if (OPT_ON(opt, REC_ZLIB))
        sc->compress = BDB_COMPRESS_ZLIB;
    if (OPT_ON(opt, REC_LZ4))
        sc->compress = BDB_COMPRESS_LZ4;

    if (sc->compress != BDB_COMPRESS_RLE8 &&
        sc->compress != BDB_COMPRESS_ZLIB && 
        sc->compress != BDB_COMPRESS_LZ4)
                sc->compress = BDB_COMPRESS_NONE;

    if (OPT_ON(opt, FORCE_REBUILD))
        sc->force_rebuild = 1;
    else
        sc->force_rebuild = 0;

    sc->commit_sleep = gbl_commit_sleep;
    sc->convert_sleep = gbl_convert_sleep;
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
    int do_dryrun(struct schema_change_type *sc);
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


int comdb2SqlSchemaChange(OpFunc *f)
{
    struct sql_thread *thd = pthread_getspecific(query_info_key);
    struct schema_change_type *s = (struct schema_change_type*)f->arg;
    thd->sqlclntstate->osql.long_request = 1;
    osql_sock_start(thd->sqlclntstate, OSQL_SOCK_REQ ,0);
    osql_schemachange_logic(s, thd);
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
	int rc = comdb2SqlSchemaChange(f);
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

   osql_sock_start(clnt, OSQL_SOCK_REQ ,0);

   rc = osql_send_bpfunc(node, osql->rqid, osql->uuid, arg, NET_OSQL_SOCK_RPL,osql->logsb);
   
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

static void comdb2rebuild(Parse *p, Token* nm, Token* lnm, uint8_t opt);

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

void comdb2CreateTable(
  Parse *pParse,   /* Parser context */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int opt,         /* Various options for create (compress, etc) */
  Token *csc2,
  int temp,
  int noErr
)
{
    sqlite3 *db = pParse->db;
    Vdbe *v  = sqlite3GetVdbe(pParse);
    if (temp) {
        setError(pParse, SQLITE_MISUSE, "can't create temp csc2 table");
        return;
    }
    struct schema_change_type *sc = new_schemachange_type();
    if (sc == NULL) {
        setError(pParse, SQLITE_NOMEM, "System out of memory");
        return;
    }
    TokenStr(table, pName1);
    if (noErr && getdbbyname(table))
        goto out;

    if (chkAndCopyTableTokens(v, pParse, sc->table, pName1, pName2, 0))
        goto out;

    if (authenticateSC(sc->table, pParse))
        goto out;

    v->readOnly = 0;
    sc->addonly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    fillTableOption(sc, opt);
    copyNosqlToken(v, pParse, &sc->newcsc2, csc2);
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree) &free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

void comdb2AlterTable(
  Parse *pParse,   /* Parser context */
  Token *pName1,   /* First part of the name of the table or view */
  Token *pName2,   /* Second part of the name of the table or view */
  int opt,         /* Various options for alter (compress, etc) */
  Token *csc2,
  int dryrun
)
{
    sqlite3 *db = pParse->db;
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

    v->readOnly = 0;
    sc->alteronly = 1;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->use_plan = 1;
    sc->scanmode = SCAN_PARALLEL;
    sc->dryrun = dryrun;
    fillTableOption(sc, opt);

    copyNosqlToken(v, pParse, &sc->newcsc2, csc2);
    if(dryrun)
        comdb2prepareSString(v, pParse, 0,  sc, &comdb2SqlDryrunSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
    else
        comdb2prepareNoRows(v, pParse, 0,  sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
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

    if (chkAndCopyTable(v, pParse, sc->table, pName->a[0].zName, MAXTABLELEN, 1))
        goto out;

    if (authenticateSC(sc->table, pParse))
        goto out;

    v->readOnly = 0;
    sc->same_schema = 1;
    sc->drop_table = 1;
    sc->fastinit = 1;
    sc->nothrevent = 1;
    
    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL )) {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: \n",  sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}

static inline void comdb2rebuild(Parse *pParse, Token* nm, Token* lnm, uint8_t opt)
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

    v->readOnly = 0;
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
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}


void comdb2rebuildFull(Parse* p, Token* nm,Token* lnm)
{
    comdb2rebuild(p, nm,lnm, REBUILD_ALL + REBUILD_DATA + REBUILD_BLOB); 
}


void comdb2rebuildData(Parse* p, Token* nm, Token* lnm)
{
    comdb2rebuild(p,nm,lnm,REBUILD_DATA);
}

void comdb2rebuildDataBlob(Parse* p,Token* nm, Token* lnm)
{
    comdb2rebuild(p, nm, lnm, REBUILD_BLOB);
}

void comdb2truncate(Parse* pParse, Token* nm, Token* lnm)
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

    v->readOnly = 0;
    sc->fastinit = 1;
    sc->nothrevent = 1;
    sc->same_schema = 1;

    if(get_csc2_file(sc->table, -1 , &sc->newcsc2, NULL ))
    {
        logmsg(LOGMSG_ERROR, "%s: table schema not found: \n",  sc->table);
        setError(pParse, SQLITE_ERROR, "Table schema cannot be found");
        goto out;
    }
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
    return;

out:
    free_schema_change_type(sc);
}


void comdb2rebuildIndex(Parse* pParse, Token* nm, Token* lnm, Token* index)
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

    v->readOnly = 0;
    sc->nothrevent = 1;
    sc->live = 1;
    sc->rebuild_index = 1;
    sc->index_to_rebuild = index_num;
    sc->scanmode = gbl_default_sc_scanmode;
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
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
    v->readOnly = 0;
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
    v->readOnly = 0;
    sc->defaultsp = 1;

    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);
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
    v->readOnly = 0;
    sc->delsp = 1;
  
    comdb2prepareNoRows(v, pParse, 0, sc, &comdb2SqlSchemaChange, (vdbeFuncArgFree)  &free_schema_change_type);    
}
/********************* PARTITIONS  **********************************************/


void comdb2CreateTimePartition(Parse* pParse, Token* table, Token* partition_name, Token* period, Token* retention, Token* start)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);

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
    
    if (tp->period == VIEW_TIMEPART_INVALID)
    {
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

    if (tp->start == -1 )
    {
        setError(pParse, SQLITE_ERROR, "Invalid start date");
        goto clean_arg;
    }

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);
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

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
        setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
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
        analyze_set_max_table_threads(threads);
    if (sum_threads)
        analyze_set_max_sampling_threads(sum_threads);

    if (nm == NULL)
    {
        comdb2prepareNoRows(v, pParse, pc, NULL, &comdb2vdbeAnalyze, (vdbeFuncArgFree) &free);
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
           comdb2prepareNoRows(v, pParse, pc, tablename, &comdb2vdbeAnalyze, (vdbeFuncArgFree) &free); 
    }

    return;

err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");

}

void comdb2analyzeCoverage(Parse* pParse, Token* nm, Token* lnm, int newscale)
{
    Vdbe *v  = sqlite3GetVdbe(pParse);


    if (comdb2AuthenticateUserOp(v, pParse))
        goto err;       

    if (newscale < -1 || newscale > 100)
    {
        setError(pParse, SQLITE_ERROR, "Coverage must be between -1 and 100");
        goto clean_arg;
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
        return;  
    
    ancov_f->newvalue = newscale;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

    return;
err:
    setError(pParse, SQLITE_INTERNAL, "Internal Error");
clean_arg:
    if (arg)
        free_bpfunc_arg(arg);

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
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);
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
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);
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
        goto clean_arg;       

    if (newthreshold < -1 || newthreshold > 100)
    {
        setError(pParse, SQLITE_ERROR, "Threshold must be between -1 and 100");
        goto clean_arg;
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
        return;  
    
    anthr_f->newvalue = newthreshold;
    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

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

    if (name && chkAndCopyTableTokens(v,pParse, alias_f->name, name, NULL, 0)) 
        goto clean_arg;

    assert (*url->z == '\'' || *url->z == '\"');
    url->z++;
    url->n -= 2;

    if (create_string_from_token(v, pParse, &alias_f->remote, url))
        goto clean_arg;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

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
    } else {
      if (nm && lnm && chkAndCopyTableTokens(v,pParse, grant->table, nm, lnm, 1))
          goto clean_arg;
    }

    if (create_string_from_token(v, pParse, &grant->username, u))
        goto clean_arg;

    comdb2prepareNoRows(v, pParse, 0, arg, &comdb2SendBpfunc, (vdbeFuncArgFree) &free_bpfunc_arg);

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

    if (nm && lnm && chkAndCopyTableTokens(v,pParse, tablename, nm, lnm, 1)) 
        goto clean;
    
    comdb2prepareOpFunc(v, pParse, 0, tablename, &produceAnalyzeCoverage, (vdbeFuncArgFree)  &free, &stp);

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

    if (nm && lnm && chkAndCopyTableTokens(v,pParse, tablename, nm, lnm, 1)) 
        goto clean;
    
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

void comdb2schemachangeCommitsleep(Parse* pParse, int num)
{
    gbl_commit_sleep = num;
}

void comdb2schemachangeConvertsleep(Parse* pParse, int num)
{
    gbl_convert_sleep = num;
}
