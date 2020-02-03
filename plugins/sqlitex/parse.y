/*
** 2001 September 15
**
** The author disclaims copyright to this source code.  In place of
** a legal notice, here is a blessing:
**
**    May you do good and not evil.
**    May you find forgiveness for yourself and forgive others.
**    May you share freely, never taking more than you give.
**
*************************************************************************
** This file contains SQLite's grammar for SQL.  Process this file
** using the lemon parser generator to generate C code that runs
** the parser.  Lemon will also generate a header file containing
** numeric codes for all of the tokens.
*/

// All token codes are small integers with #defines that begin with "TK_"
%token_prefix TK_

// The type of the data attached to each token is Token.  This is also the
// default type for non-terminals.
//
%token_type {Token}
%default_type {Token}

// The generated parser function takes a 4th argument as follows:
%extra_argument {Parse *pParse}

// This code runs whenever there is a syntax error
//
%syntax_error {
  UNUSED_PARAMETER(yymajor);  /* Silence some compiler warnings */
  assert( TOKEN.z[0] );  /* The tokenizer always gives us a token */
  sqlitexErrorMsg(pParse, "near \"%T\": syntax error", &TOKEN);
}
%stack_overflow {
  UNUSED_PARAMETER(yypMinor); /* Silence some compiler warnings */
  sqlitexErrorMsg(pParse, "parser stack overflow");
}

// The name of the generated procedure that implements the parser
// is as follows:
%name sqlitexParser

// The following text is included near the beginning of the C source
// code file that implements the parser.
//
%include {
#include "sqliteInt.h"
#include "comdb2Int.h"  // COMDB2 CUSTOM FUNCTION (ALL CUSTOM HEADERS ARE INCLUDED FROM HERE)
/*
** Disable all error recovery processing in the parser push-down
** automaton.
*/
#define YYNOERRORRECOVERY 1

/*
** Make yytestcase() the same as testcase()
*/
#define yytestcase(X) testcase(X)

/*
** An instance of this structure holds information about the
** LIMIT clause of a SELECT statement.
*/
struct LimitVal {
  Expr *pLimit;    /* The LIMIT expression.  NULL if there is no limit */
  Expr *pOffset;   /* The OFFSET expression.  NULL if there is none */
};

/*
** An instance of this structure is used to store the LIKE,
** GLOB, NOT LIKE, and NOT GLOB operators.
*/
struct LikeOp {
  Token eOperator;  /* "like" or "glob" or "regexp" */
  int bNot;         /* True if the NOT keyword is present */
};

/*
** An instance of the following structure describes the event of a
** TRIGGER.  "a" is the event type, one of TK_UPDATE, TK_INSERT,
** TK_DELETE, or TK_INSTEAD.  If the event is of the form
**
**      UPDATE ON (a,b,c)
**
** Then the "b" IdList records the list "a,b,c".
*/
struct TrigEvent { int a; IdList * b; };

/*
** An instance of this structure holds the ATTACH key and the key type.
*/
struct AttachKey { int type;  Token key; };

} // end %include

// Input is a single SQL command
input ::= cmdlist.
cmdlist ::= cmdlist ecmd.
cmdlist ::= ecmd.
ecmd ::= SEMI.
ecmd ::= explain cmdx SEMI.
explain ::= .           { sqlitexBeginParse(pParse, 0); }
%ifndef SQLITE_OMIT_EXPLAIN
explain ::= EXPLAIN.              { sqlitexBeginParse(pParse, 1); }
explain ::= EXPLAIN QUERY PLAN.   { sqlitexBeginParse(pParse, 2); }
%endif  SQLITE_OMIT_EXPLAIN
cmdx ::= cmd.           { sqlitexFinishCoding(pParse); }

///////////////////// Begin and end transactions. ////////////////////////////
//

cmd ::= BEGIN transtype(Y) trans_opt.  {sqlitexBeginTransaction(pParse, Y);}
trans_opt ::= .
trans_opt ::= TRANSACTION.
trans_opt ::= TRANSACTION nm.
%type transtype {int}
transtype(A) ::= .             {A = TK_DEFERRED;}
transtype(A) ::= DEFERRED(X).  {A = @X;}
transtype(A) ::= IMMEDIATE(X). {A = @X;}
transtype(A) ::= EXCLUSIVE(X). {A = @X;}
cmd ::= COMMIT trans_opt.      {sqlitexCommitTransaction(pParse);}
cmd ::= END trans_opt.         {sqlitexCommitTransaction(pParse);}
cmd ::= ROLLBACK trans_opt.    {sqlitexRollbackTransaction(pParse);}

savepoint_opt ::= SAVEPOINT.
savepoint_opt ::= .
cmd ::= SAVEPOINT nm(X). {
  sqlitexSavepoint(pParse, SAVEPOINT_BEGIN, &X);
}
cmd ::= RELEASE savepoint_opt nm(X). {
  sqlitexSavepoint(pParse, SAVEPOINT_RELEASE, &X);
}
cmd ::= ROLLBACK trans_opt TO savepoint_opt nm(X). {
  sqlitexSavepoint(pParse, SAVEPOINT_ROLLBACK, &X);
}

///////////////////// COMDB2 GET statements //////////////////////////////////

cmd ::= GET getcmd.

getcmd ::= ALIAS nm(U). {
    comdb2getAliasX(pParse, &U);
}

getcmd ::= KW. {
    comdb2getkwX(pParse, KW_ALL);
}

getcmd ::= RESERVED KW. {
    comdb2getkwX(pParse, KW_RES);
}

getcmd ::= NOT RESERVED KW. {
    comdb2getkwX(pParse, KW_FB);
}

///////////////////// COMDB2 PUT statements //////////////////////////////////

cmd ::= PUT putcmd.

putcmd ::= ANALYZE COVERAGE nm(Y) dbnm(Z) INTEGER(F). {
    int tmp;
    if (!readIntFromTokenX(&F, &tmp))
        tmp = 0;

    comdb2analyzeCoverageX(pParse,&Y,&Z,tmp);
}

putcmd ::= ANALYZE THRESHOLD nm(Y) dbnm(Z) INTEGER(F). {
    int tmp;
    if (!readIntFromTokenX(&F, &tmp))
        tmp = 0;
    comdb2analyzeThresholdX(pParse,&Y,&Z, tmp);
}

putcmd ::= DEFAULT PROCEDURE nm(N) INTEGER(V). {
    comdb2DefaultProcedureX(pParse,&N,&V,0);
}

putcmd ::= DEFAULT PROCEDURE nm(N) STRING(V). {
    comdb2DefaultProcedureX(pParse,&N,&V,1);
}

putcmd ::= ALIAS nm(Y) nm(U). {
    comdb2setAliasX(pParse,&Y,&U);
}

putcmd ::= PASSWORD OFF FOR nm(N). {
    comdb2deletePasswordX(pParse, &N);
}

putcmd ::= PASSWORD STRING(P) FOR nm(N). {
    comdb2setPasswordX(pParse, &P, &N);
}

putcmd ::= AUTHENTICATION ON. {
    comdb2enableAuthX(pParse, 1);
}

putcmd ::= AUTHENTICATION OFF. {
    comdb2enableAuthX(pParse, 0);
}

putcmd ::= TIME PARTITION nm(Y) dbnm(Z) RETENTION  INTEGER(R). {
    int tmp;
    if (!readIntFromTokenX(&R, &tmp))
        tmp = 0;
    comdb2timepartRetentionX(pParse, &Y, &Z, tmp);
}

///////////////////// COMDB2 REBUILD STATEMENTS //////////////////////////////

cmd ::= rebuild.

rebuild ::= REBUILD nm(T) dbnm(X) comdb2opt(O). { // REBUILD FULL CANNOT BE USED BECAUSE OF SQLITE SYNTAX
    comdb2rebuildFull(pParse,&T,&X,O);
}

rebuild ::= REBUILD INDEX nm(T) dbnm(Y) nm(X) comdb2opt(O). {
    comdb2rebuildIndex(pParse, &T,&Y, &X, O);
}

rebuild ::= REBUILD DATA nm(T) dbnm(X) comdb2opt(O). {
    comdb2rebuildData(pParse, &T, &X, O);
}

rebuild ::= REBUILD DATABLOB nm(N) dbnm(X) comdb2opt(O). {
    comdb2rebuildDataBlob(pParse,&N, &X, O);
}
/////////////////////COMDB2 GRANT STATEMENT //////////////////////////////////

%type sql_permission {int}
sql_permission(A) ::= READ. { A = AUTH_READ; }
sql_permission(A) ::= WRITE.{ A = AUTH_WRITE;}
sql_permission(A) ::= DDL.  { A = AUTH_OP;}

%type op_permission{int}
op_permission(A) ::= OP.   { A = AUTH_OP;   }

%type userschema{int}
userschema(A) ::= USERSCHEMA. {A = AUTH_USERSCHEMA;}

cmd ::= GRANT sql_permission(P) ON nm(T) dbnm(Y) TO nm(U). {
    comdb2grantX(pParse, 0, P, &T,&Y,&U);
}

cmd ::= GRANT op_permission(P) TO nm(U). {
    comdb2grantX(pParse, 0, P, NULL,NULL,&U);
}

cmd ::= GRANT userschema(P) nm(U1) TO nm(U2). {
    comdb2grantX(pParse, 0, P, &U1,NULL,&U2);
}

cmd ::= REVOKE sql_permission(P) ON nm(T) dbnm(Y) TO nm(U). {
    comdb2grantX(pParse, 1, P, &T,&Y,&U);
}

cmd ::= REVOKE op_permission(P) TO nm(U). {
    comdb2grantX(pParse, 1, P, NULL,NULL,&U);
}

cmd ::= REVOKE userschema(P) nm(U1) TO nm(U2). {
    comdb2grantX(pParse, 1, P, &U1,NULL,&U2);
}


//////////////////// COMDB2 TRUNCATE TABLE statement /////////////////////////

cmd ::= truncate.
truncate ::= TRUNCATE nm(T) dbnm(Y).
{
    comdb2truncate(pParse, &T, &Y);
}

cmd ::= BULKIMPORT nm(A) DOT nm(B) nm(C) DOT nm(D). {
    comdb2bulkimportX(pParse, &A, &B, &C, &D);
}

//////////////////// COMDB2 PARTITION //////////////////////////////////

cmd ::= createkw TIME PARTITION ON nm(A) AS nm(P) PERIOD STRING(D) RETENTION INTEGER(R) START STRING(S). {
    comdb2CreateTimePartition(pParse, &A, &P, &D, &R, &S);
}

cmd ::= DROP TIME PARTITION nm(N). {
    comdb2DropTimePartition(pParse, &N);
}

//////////////////// COMDB2 ANALYZE //////////////////////////////////////////
cmd ::= ANALYZE nm(N) dbnm(Y) analyzepercentage(P) analyzeopt(X). {
    comdb2analyzeX(pParse, X, &N, &Y, P);
}

cmd ::= ANALYZE ALL analyzepercentage(P) analyzeopt(X). {
    comdb2analyzeX(pParse, X, NULL, NULL, P);
}

%type analyzepercentage {int}
analyzepercentage(A) ::= . {A = 0;}
analyzepercentage(A) ::= INTEGER(X). {
    if (!readIntFromTokenX(&X, &(A)))
        A = 0;

}


%type analyzeopt {int}
analyzeopt(A) ::= . { A = 0;}
analyzeopt(A) ::= OPTIONS analyzeoptlst(X). {A=X;}

%type analyzeoptlst {int}
analyzeoptlst(A) ::= analyze_thds(L) COMMA analyze_sumthds(R). {A = L + R;}
analyzeoptlst(A) ::= analyze_sumthds(L) COMMA analyze_thds(R). {A = L + R;}
analyzeoptlst(A) ::= analyze_thds(X). {A = X;}
analyzeoptlst(A) ::= analyze_sumthds(X). {A = X;}

%type analyze_thds {int}
analyze_thds(A) ::= THREADS INTEGER(X). {
    int tmp;
    A = 0;
    if (!readIntFromTokenX(&X, &(tmp)))
        SET_ANALYZE_THREAD(A,0);

    SET_ANALYZE_THREAD(A,tmp);
}

%type analyze_sumthds {int}
analyze_sumthds(A) ::= SUMMARIZE INTEGER(X). {
    int tmp;
    A = 0;
    if (!readIntFromTokenX(&X, &(tmp)))
        SET_ANALYZE_SUMTHREAD(A, 0);
     SET_ANALYZE_SUMTHREAD(A,tmp);
}

///////////////////// COMDB2 CREATE TABLE statement //////////////////////////
%type comdb2opt {int}
comdb2opt(A) ::= . {A = 0;}
comdb2opt(A) ::= OPTIONS comdb2optlist(X). {A = X;}
%type comdb2optlist {int}
comdb2optlist(A) ::= comdb2optfield(O). {
    A = O;
}
comdb2optlist(A) ::= comdb2optlist(X) COMMA comdb2optfield(O). {
    A = X | O;
}
%type comdb2optfield {int}
comdb2optfield(A) ::= odh(O). {A = O;}
comdb2optfield(A) ::= ipu(I). {A = I;}
comdb2optfield(A) ::= isc(S). {A = S;}
comdb2optfield(A) ::= REBUILD. {A = FORCE_REBUILD;}
comdb2optfield(A) ::= PAGEORDER. {A = PAGE_ORDER;}
comdb2optfield(A) ::= READONLY. {A = READ_ONLY;}
comdb2optfield(A) ::= compress_blob(C). {A = C;}
comdb2optfield(A) ::= compress_rec(C). {A = C;}

%type odh {int}
odh(A) ::= ODH OFF. {A = ODH_OFF;}

%type ipu {int}
ipu(A) ::= IPU OFF. {A = IPU_OFF;}

%type isc {int}
isc(A) ::= ISC OFF. {A = ISC_OFF;}

%type compress_blob {int}
compress_blob(A) ::= BLOBFIELD blob_compress_type(T). { A = T;}

%type blob_compress_type {int}
blob_compress_type(A) ::= RLE. {A = BLOB_RLE;}
//blob_compress_type(A) ::= CRLE. {A = BLOB_CRLE;}
blob_compress_type(A) ::= ZLIB. {A = BLOB_ZLIB;}
blob_compress_type(A) ::= LZ4. {A = BLOB_LZ4;}

%type compress_rec {int}
compress_rec(A) ::= REC rle_compress_type(T). {A = T;}

%type rle_compress_type {int}
rle_compress_type(A) ::= RLE. {A = REC_RLE;}
rle_compress_type(A) ::= CRLE. {A = REC_CRLE;}
rle_compress_type(A) ::= ZLIB. {A = REC_ZLIB;}
rle_compress_type(A) ::= LZ4. {A = REC_LZ4;}

/////////////////// COMDB2 ALTER TABLE STATEMENT  //////////////////////////////


cmd ::= alter_comdb2table.
alter_comdb2table ::= ALTER TABLE nm(Y) dbnm(Z) comdb2opt(O) NOSQL(C). {
        comdb2AlterTable(pParse, &Y, &Z,O,&C);
}

///////////////////// The CREATE TABLE statement ////////////////////////////

cmd ::= create_table create_table_args.
create_table ::= createkw temp(T) TABLE ifnotexists(E) nm(Y) dbnm(Z) . {
        sqlitexStartTable(pParse,&Y,&Z,T,0,0,E);
}

cmd ::= create_comdb2table.
create_comdb2table ::= createkw temp(T) TABLE ifnotexists(E) nm(Y) dbnm(Z) comdb2opt(O) NOSQL(C). {
        comdb2CreateTable(pParse,&Y,&Z,O,&C,T,E);
}

%type temp {int}
temp(A) ::= TEMP.  { A = 1; }
temp(A) ::= . {A = 0;}

createkw(A) ::= CREATE(X).  {
  pParse->db->lookaside.bEnabled = 0;
  A = X;
}


%type ifnotexists {int}
ifnotexists(A) ::= .              {A = 0;}
ifnotexists(A) ::= IF NOT EXISTS. {A = 1;}

create_table_args ::= LP columnlist conslist_opt(X) RP(E) table_options(F). {
  sqlitexEndTable(pParse,&X,&E,F,0);
}
create_table_args ::= AS select(S). {
  sqlitexEndTable(pParse,0,0,0,S);
  sqlitexSelectDelete(pParse->db, S);
}
%type table_options {u8}
table_options(A) ::= .    {A = 0;}
table_options(A) ::= WITHOUT nm(X). {
  if( X.n==5 && sqlitex_strnicmp(X.z,"rowid",5)==0 ){
    A = TF_WithoutRowid;
  }else{
    A = 0;
    sqlitexErrorMsg(pParse, "unknown table option: %.*s", X.n, X.z);
  }
}
columnlist ::= columnlist COMMA column.
columnlist ::= column.

// A "column" is a complete description of a single column in a
// CREATE TABLE statement.  This includes the column name, its
// datatype, and other keywords such as PRIMARY KEY, UNIQUE, REFERENCES,
// NOT NULL and so forth.
//
column(A) ::= columnid(X) type carglist. {
  A.z = X.z;
  A.n = (int)(pParse->sLastToken.z-X.z) + pParse->sLastToken.n;
}
columnid(A) ::= nm(X). {
  sqlitexAddColumn(pParse,&X);
  A = X;
  pParse->constraintName.n = 0;
}


// An IDENTIFIER can be a generic identifier, or one of several
// keywords.  Any non-standard keyword can also be an identifier.
//
%token_class id  ID|INDEXED.

// The following directive causes tokens ABORT, AFTER, ASC, etc. to
// fallback to ID if they will not parse as their original value.
// This obviates the need for the "id" nonterminal.
//
%fallback ID
  ABORT ACTION AFTER ANALYZE ASC ATTACH BEFORE BEGIN BY CASCADE CAST COLUMNKW
  CONFLICT DATABASE DEFERRED DESC DETACH EACH END EXCLUSIVE EXPLAIN FAIL 
  IGNORE IMMEDIATE INITIALLY INSTEAD LIKE_KW MATCH NO PLAN
  QUERY KEY OF OFFSET PRAGMA RAISE RECURSIVE RELEASE REPLACE RESTRICT ROW
  ROLLBACK SAVEPOINT TEMP TRIGGER VACUUM VIEW VIRTUAL WITH WITHOUT
%ifdef SQLITE_OMIT_COMPOUND_SELECT
  EXCEPT INTERSECT UNION
%endif SQLITE_OMIT_COMPOUND_SELECT
  REINDEX RENAME CTIME_KW IF
// COMDB2 KEYWORDS  
  AGGREGATE
  ALIAS
  AUTHENTICATION
  BLOBFIELD
  BULKIMPORT
  CONSUMER
  COVERAGE
  CRLE
  DATA
  DATABLOB
  FOR
  FUNCTION
  GET
  GRANT
  IPU
  ISC
  KW
  LUA
  LZ4
  ODH
  OFF
  OP
  OPTIONS
  PAGEORDER
  PARTITION
  PASSWORD
  PERIOD
  PROCEDURE
  PUT
  REBUILD
  READ
  READONLY
  REC
  RESERVED
  RETENTION
  REVOKE
  RLE
  SCALAR
  START
  SUMMARIZE
  TIME
  THREADS
  THRESHOLD
  TRUNCATE
  VERSION
  WRITE
  DDL
  USERSCHEMA
  ZLIB
  .
%wildcard ANY.

// Define operator precedence early so that this is the first occurrence
// of the operator tokens in the grammer.  Keeping the operators together
// causes them to be assigned integer values that are close together,
// which keeps parser tables smaller.
//
// The token values assigned to these symbols is determined by the order
// in which lemon first sees them.  It must be the case that ISNULL/NOTNULL,
// NE/EQ, GT/LE, and GE/LT are separated by only a single value.  See
// the sqlitexExprIfFalse() routine for additional information on this
// constraint.
//
%left OR.
%left AND.
%right NOT.
%left IS MATCH LIKE_KW BETWEEN IN ISNULL NOTNULL NE EQ.
%left GT LE LT GE.
%right ESCAPE.
%left BITAND BITOR LSHIFT RSHIFT.
%left PLUS MINUS.
%left STAR SLASH REM.
%left CONCAT.
%left COLLATE.
%right BITNOT.

// And "ids" is an identifer-or-string.
//
%token_class ids  ID|STRING.

// The name of a column or table can be any of the following:
//
%type nm {Token}
nm(A) ::= id(X).         {A = X;}
nm(A) ::= STRING(X).     {A = X;}
nm(A) ::= JOIN_KW(X).    {A = X;}

// A typetoken is really one or more tokens that form a type name such
// as can be found after the column name in a CREATE TABLE statement.
// Multiple tokens are concatenated to form the value of the typetoken.
//
%type typetoken {Token}
type ::= .
type ::= typetoken(X).                   {sqlitexAddColumnType(pParse,&X);}
typetoken(A) ::= typename(X).   {A = X;}
typetoken(A) ::= typename(X) LP signed RP(Y). {
  A.z = X.z;
  A.n = (int)(&Y.z[Y.n] - X.z);
}
typetoken(A) ::= typename(X) LP signed COMMA signed RP(Y). {
  A.z = X.z;
  A.n = (int)(&Y.z[Y.n] - X.z);
}
%type typename {Token}
typename(A) ::= ids(X).             {A = X;}
typename(A) ::= typename(X) ids(Y). {A.z=X.z; A.n=Y.n+(int)(Y.z-X.z);}
signed ::= plus_num.
signed ::= minus_num.

// "carglist" is a list of additional constraints that come after the
// column name and column type in a CREATE TABLE statement.
//
carglist ::= carglist ccons.
carglist ::= .
ccons ::= CONSTRAINT nm(X).           {pParse->constraintName = X;}
ccons ::= DEFAULT term(X).            {sqlitexAddDefaultValue(pParse,&X);}
ccons ::= DEFAULT LP expr(X) RP.      {sqlitexAddDefaultValue(pParse,&X);}
ccons ::= DEFAULT PLUS term(X).       {sqlitexAddDefaultValue(pParse,&X);}
ccons ::= DEFAULT MINUS(A) term(X).      {
  ExprSpan v;
  v.pExpr = sqlitexPExpr(pParse, TK_UMINUS, X.pExpr, 0, 0);
  v.zStart = A.z;
  v.zEnd = X.zEnd;
  sqlitexAddDefaultValue(pParse,&v);
}
ccons ::= DEFAULT id(X).              {
  ExprSpan v;
  spanExpr(&v, pParse, TK_STRING, &X);
  sqlitexAddDefaultValue(pParse,&v);
}

// In addition to the type name, we also care about the primary key and
// UNIQUE constraints.
//
ccons ::= NULL onconf.
ccons ::= NOT NULL onconf(R).    {sqlitexAddNotNull(pParse, R);}
ccons ::= PRIMARY KEY sortorder(Z) onconf(R) autoinc(I).
                                 {sqlitexAddPrimaryKey(pParse,0,R,I,Z);}
ccons ::= UNIQUE onconf(R).      {sqlitexCreateIndex(pParse,0,0,0,0,R,0,0,0,0);}
ccons ::= CHECK LP expr(X) RP.   {sqlitexAddCheckConstraint(pParse,X.pExpr);}
ccons ::= REFERENCES nm(T) idxlist_opt(TA) refargs(R).
                                 {sqlitexCreateForeignKey(pParse,0,&T,TA,R);}
ccons ::= defer_subclause(D).    {sqlitexDeferForeignKey(pParse,D);}
ccons ::= COLLATE ids(C).        {sqlitexAddCollateType(pParse, &C);}

// The optional AUTOINCREMENT keyword
%type autoinc {int}
autoinc(X) ::= .          {X = 0;}
autoinc(X) ::= AUTOINCR.  {X = 1;}

// The next group of rules parses the arguments to a REFERENCES clause
// that determine if the referential integrity checking is deferred or
// or immediate and which determine what action to take if a ref-integ
// check fails.
//
%type refargs {int}
refargs(A) ::= .                  { A = OE_None*0x0101; /* EV: R-19803-45884 */}
refargs(A) ::= refargs(X) refarg(Y). { A = (X & ~Y.mask) | Y.value; }
%type refarg {struct {int value; int mask;}}
refarg(A) ::= MATCH nm.              { A.value = 0;     A.mask = 0x000000; }
refarg(A) ::= ON INSERT refact.      { A.value = 0;     A.mask = 0x000000; }
refarg(A) ::= ON DELETE refact(X).   { A.value = X;     A.mask = 0x0000ff; }
refarg(A) ::= ON UPDATE refact(X).   { A.value = X<<8;  A.mask = 0x00ff00; }
%type refact {int}
refact(A) ::= SET NULL.              { A = OE_SetNull;  /* EV: R-33326-45252 */}
refact(A) ::= SET DEFAULT.           { A = OE_SetDflt;  /* EV: R-33326-45252 */}
refact(A) ::= CASCADE.               { A = OE_Cascade;  /* EV: R-33326-45252 */}
refact(A) ::= RESTRICT.              { A = OE_Restrict; /* EV: R-33326-45252 */}
refact(A) ::= NO ACTION.             { A = OE_None;     /* EV: R-33326-45252 */}
%type defer_subclause {int}
defer_subclause(A) ::= NOT DEFERRABLE init_deferred_pred_opt.     {A = 0;}
defer_subclause(A) ::= DEFERRABLE init_deferred_pred_opt(X).      {A = X;}
%type init_deferred_pred_opt {int}
init_deferred_pred_opt(A) ::= .                       {A = 0;}
init_deferred_pred_opt(A) ::= INITIALLY DEFERRED.     {A = 1;}
init_deferred_pred_opt(A) ::= INITIALLY IMMEDIATE.    {A = 0;}

conslist_opt(A) ::= .                         {A.n = 0; A.z = 0;}
conslist_opt(A) ::= COMMA(X) conslist.        {A = X;}
conslist ::= conslist tconscomma tcons.
conslist ::= tcons.
tconscomma ::= COMMA.            {pParse->constraintName.n = 0;}
tconscomma ::= .
tcons ::= CONSTRAINT nm(X).      {pParse->constraintName = X;}
tcons ::= PRIMARY KEY LP idxlist(X) autoinc(I) RP onconf(R).
                                 {sqlitexAddPrimaryKey(pParse,X,R,I,0);}
tcons ::= UNIQUE LP idxlist(X) RP onconf(R).
                                 {sqlitexCreateIndex(pParse,0,0,0,X,R,0,0,0,0);}
tcons ::= CHECK LP expr(E) RP onconf.
                                 {sqlitexAddCheckConstraint(pParse,E.pExpr);}
tcons ::= FOREIGN KEY LP idxlist(FA) RP
          REFERENCES nm(T) idxlist_opt(TA) refargs(R) defer_subclause_opt(D). {
    sqlitexCreateForeignKey(pParse, FA, &T, TA, R);
    sqlitexDeferForeignKey(pParse, D);
}
%type defer_subclause_opt {int}
defer_subclause_opt(A) ::= .                    {A = 0;}
defer_subclause_opt(A) ::= defer_subclause(X).  {A = X;}

// The following is a non-standard extension that allows us to declare the
// default behavior when there is a constraint conflict.
//
%type onconf {int}
%type orconf {u8}
%type resolvetype {int}
onconf(A) ::= .                              {A = OE_Default;}
onconf(A) ::= ON CONFLICT resolvetype(X).    {A = X;}
orconf(A) ::= .                              {A = OE_Default;}
orconf(A) ::= OR resolvetype(X).             {A = (u8)X;}
resolvetype(A) ::= raisetype(X).             {A = X;}
resolvetype(A) ::= IGNORE.                   {A = OE_Ignore;}
/* COMDB2 MODIFICATION 
 * insert or replace logic not supported
 * resolvetype(A) ::= REPLACE.                  {A = OE_Replace;} 
 */

//////////////////// COMDB2 STORED PROCEDURES /////////////////////////////////

cmd ::= createkw PROCEDURE nm(N) NOSQL(X). {
    comdb2CreateProcedureX(pParse, &N, NULL, &X);
}
cmd ::= createkw PROCEDURE nm(N) VERSION STRING(V) NOSQL(X). {
    comdb2CreateProcedureX(pParse, &N, &V, &X);
}
cmd ::= DROP PROCEDURE nm(N) INTEGER(V). {
    comdb2DropProcedureX(pParse, &N, &V, 0);
}
cmd ::= DROP PROCEDURE nm(N) STRING(V). {
    comdb2DropProcedureX(pParse, &N, &V, 1);
}


////////////////////////// The DROP TABLE /////////////////////////////////////
//
cmd ::= DROP TABLE ifexists(E) fullname(X). {
  sqlitexDropTable(pParse, X, 0, E);
}
%type ifexists {int}
ifexists(A) ::= IF EXISTS.   {A = 1;}
ifexists(A) ::= .            {A = 0;}

///////////////////// The CREATE VIEW statement /////////////////////////////
//
%ifndef SQLITE_OMIT_VIEW
cmd ::= createkw(X) temp(T) VIEW ifnotexists(E) nm(Y) dbnm(Z) AS select(S). {
  sqlitexCreateView(pParse, &X, &Y, &Z, S, T, E);
}
cmd ::= DROP VIEW ifexists(E) fullname(X). {
  sqlitexDropTable(pParse, X, 1, E);
}
%endif  SQLITE_OMIT_VIEW

//////////////////////// The SELECT statement /////////////////////////////////
//
cmd ::= select(X).  {
  SelectDest dest = {SRT_Output, 0, 0, 0, 0, 0};
  sqlitexSelect(pParse, X, &dest);
  sqlitexFingerprintSelect((sqlitex_stmt*) pParse->pVdbe, X);
  sqlitexSelectDelete(pParse->db, X);
}

%type select {Select*}
%destructor select {sqlitexSelectDelete(pParse->db, $$);}
%type selectnowith {Select*}
%destructor selectnowith {sqlitexSelectDelete(pParse->db, $$);}
%type oneselect {Select*}
%destructor oneselect {sqlitexSelectDelete(pParse->db, $$);}

select(A) ::= with(W) selectnowith(X). {
  Select *p = X, *pNext, *pLoop;
  if( p ){
    int cnt = 0, mxSelect;
    p->pWith = W;
    if( p->pPrior ){
      u16 allValues = SF_Values;
      pNext = 0;
      for(pLoop=p; pLoop; pNext=pLoop, pLoop=pLoop->pPrior, cnt++){
        pLoop->pNext = pNext;
        pLoop->selFlags |= SF_Compound;
        allValues &= pLoop->selFlags;
      }
      if( allValues ){
        p->selFlags |= SF_AllValues;
      }else if(
        (mxSelect = pParse->db->aLimit[SQLITE_LIMIT_COMPOUND_SELECT])>0
        && cnt>mxSelect
      ){
        sqlitexErrorMsg(pParse, "too many terms in compound SELECT");
      }
    }
  }else{
    sqlitexWithDelete(pParse->db, W);
  }
  A = p;
}

selectnowith(A) ::= oneselect(X).                      {A = X;}
%ifndef SQLITE_OMIT_COMPOUND_SELECT
selectnowith(A) ::= selectnowith(X) multiselect_op(Y) oneselect(Z).  {
  Select *pRhs = Z;
  if( pRhs && pRhs->pPrior ){
    SrcList *pFrom;
    Token x;
    x.n = 0;
    pFrom = sqlitexSrcListAppendFromTerm(pParse,0,0,0,&x,pRhs,0,0);
    pRhs = sqlitexSelectNew(pParse,0,pFrom,0,0,0,0,0,0,0);
  }
  if( pRhs ){
    pRhs->op = (u8)Y;
    pRhs->pPrior = X;
    if( Y!=TK_ALL ) pParse->hasCompound = 1;
  }else{
    sqlitexSelectDelete(pParse->db, X);
  }
  A = pRhs;
}
%type multiselect_op {int}
multiselect_op(A) ::= UNION(OP).             {A = @OP;}
multiselect_op(A) ::= UNION ALL.             {A = TK_ALL;}
multiselect_op(A) ::= EXCEPT|INTERSECT(OP).  {A = @OP;}
%endif SQLITE_OMIT_COMPOUND_SELECT
oneselect(A) ::= SELECT(S) distinct(D) selcollist(W) from(X) where_opt(Y)
                 groupby_opt(P) having_opt(Q) orderby_opt(Z) limit_opt(L). {
  A = sqlitexSelectNew(pParse,W,X,Y,P,Q,Z,D,L.pLimit,L.pOffset);
#if SELECTTRACE_ENABLED
  /* Populate the Select.zSelName[] string that is used to help with
  ** query planner debugging, to differentiate between multiple Select
  ** objects in a complex query.
  **
  ** If the SELECT keyword is immediately followed by a C-style comment
  ** then extract the first few alphanumeric characters from within that
  ** comment to be the zSelName value.  Otherwise, the label is #N where
  ** is an integer that is incremented with each SELECT statement seen.
  */
  if( A!=0 ){
    const char *z = S.z+6;
    int i;
    sqlitex_snprintf(sizeof(A->zSelName), A->zSelName, "#%d",
                     ++pParse->nSelect);
    while( z[0]==' ' ) z++;
    if( z[0]=='/' && z[1]=='*' ){
      z += 2;
      while( z[0]==' ' ) z++;
      for(i=0; sqlitexIsalnum(z[i]); i++){}
      sqlitex_snprintf(sizeof(A->zSelName), A->zSelName, "%.*s", i, z);
    }
  }
#endif /* SELECTRACE_ENABLED */
}
oneselect(A) ::= values(X).    {A = X;}

%type values {Select*}
%destructor values {sqlitexSelectDelete(pParse->db, $$);}
values(A) ::= VALUES LP nexprlist(X) RP. {
  A = sqlitexSelectNew(pParse,X,0,0,0,0,0,SF_Values,0,0);
}
values(A) ::= values(X) COMMA LP exprlist(Y) RP. {
  Select *pRight = sqlitexSelectNew(pParse,Y,0,0,0,0,0,SF_Values,0,0);
  if( pRight ){
    pRight->op = TK_ALL;
    pRight->pPrior = X;
    A = pRight;
  }else{
    A = X;
  }
}

/* COMDB2 MODIFICATION 
 * add the SELECTV instruction */
oneselect(A) ::= SELECTV distinct(D) selcollist(W) from(X) where_opt(Y)
                 groupby_opt(P) having_opt(Q) orderby_opt(Z) limit_opt(L). {
  A = sqlitexSelectNew(pParse,W,X,Y,P,Q,Z,D,L.pLimit,L.pOffset);
  A->op = TK_SELECTV;
  A->recording = 1;
}


// The "distinct" nonterminal is true (1) if the DISTINCT keyword is
// present and false (0) if it is not.
//
%type distinct {u16}
distinct(A) ::= DISTINCT.   {A = SF_Distinct;}
distinct(A) ::= ALL.        {A = 0;}
distinct(A) ::= .           {A = 0;}

// selcollist is a list of expressions that are to become the return
// values of the SELECT statement.  The "*" in statements like
// "SELECT * FROM ..." is encoded as a special expression with an
// opcode of TK_ASTERISK.
//
%type selcollist {ExprList*}
%destructor selcollist {sqlitexExprListDelete(pParse->db, $$);}
%type sclp {ExprList*}
%destructor sclp {sqlitexExprListDelete(pParse->db, $$);}
sclp(A) ::= selcollist(X) COMMA.             {A = X;}
sclp(A) ::= .                                {A = 0;}
selcollist(A) ::= sclp(P) expr(X) as(Y).     {
   A = sqlitexExprListAppend(pParse, P, X.pExpr);
   if( Y.n>0 ) sqlitexExprListSetName(pParse, A, &Y, 1);
   sqlitexExprListSetSpan(pParse,A,&X);
}
selcollist(A) ::= sclp(P) STAR. {
  Expr *p = sqlitexExpr(pParse->db, TK_ASTERISK, 0);
  A = sqlitexExprListAppend(pParse, P, p);
}
selcollist(A) ::= sclp(P) nm(X) DOT STAR(Y). {
  Expr *pRight = sqlitexPExpr(pParse, TK_ASTERISK, 0, 0, &Y);
  Expr *pLeft = sqlitexPExpr(pParse, TK_ID, 0, 0, &X);
  Expr *pDot = sqlitexPExpr(pParse, TK_DOT, pLeft, pRight, 0);
  A = sqlitexExprListAppend(pParse,P, pDot);
}

// An option "AS <id>" phrase that can follow one of the expressions that
// define the result set, or one of the tables in the FROM clause.
//
%type as {Token}
as(X) ::= AS nm(Y).    {X = Y;}
as(X) ::= ids(Y).      {X = Y;}
as(X) ::= .            {X.n = 0;}


%type seltablist {SrcList*}
%destructor seltablist {sqlitexSrcListDelete(pParse->db, $$);}
%type stl_prefix {SrcList*}
%destructor stl_prefix {sqlitexSrcListDelete(pParse->db, $$);}
%type from {SrcList*}
%destructor from {sqlitexSrcListDelete(pParse->db, $$);}

// A complete FROM clause.
//
from(A) ::= .                {A = sqlitexDbMallocZero(pParse->db, sizeof(*A));}
from(A) ::= FROM seltablist(X). {
  A = X;
  sqlitexSrcListShiftJoinType(A);
}

// "seltablist" is a "Select Table List" - the content of the FROM clause
// in a SELECT statement.  "stl_prefix" is a prefix of this list.
//
stl_prefix(A) ::= seltablist(X) joinop(Y).    {
   A = X;
   if( ALWAYS(A && A->nSrc>0) ) A->a[A->nSrc-1].fg.jointype = (u8)Y;
}
stl_prefix(A) ::= .                           {A = 0;}
seltablist(A) ::= stl_prefix(X) nm(Y) dbnm(D) as(Z) indexed_opt(I)
                  on_opt(N) using_opt(U). {
  A = sqlitexSrcListAppendFromTerm(pParse,X,&Y,&D,&Z,0,N,U);
  sqlitexSrcListIndexedBy(pParse, A, &I);
}
seltablist(A) ::= stl_prefix(X) nm(Y) dbnm(D) LP exprlist(E) RP as(Z)
                  on_opt(N) using_opt(U). {
  A = sqlitexSrcListAppendFromTerm(pParse,X,&Y,&D,&Z,0,N,U);
  sqlitexSrcListFuncArgs(pParse, A, E);
}
%ifndef SQLITE_OMIT_SUBQUERY
  seltablist(A) ::= stl_prefix(X) LP select(S) RP
                    as(Z) on_opt(N) using_opt(U). {
    A = sqlitexSrcListAppendFromTerm(pParse,X,0,0,&Z,S,N,U);
  }
  seltablist(A) ::= stl_prefix(X) LP seltablist(F) RP
                    as(Z) on_opt(N) using_opt(U). {
    if( X==0 && Z.n==0 && N==0 && U==0 ){
      A = F;
    }else if( F->nSrc==1 ){
      A = sqlitexSrcListAppendFromTerm(pParse,X,0,0,&Z,0,N,U);
      if( A ){
        struct SrcList_item *pNew = &A->a[A->nSrc-1];
        struct SrcList_item *pOld = F->a;
        pNew->zName = pOld->zName;
        pNew->zDatabase = pOld->zDatabase;
        pNew->pSelect = pOld->pSelect;
        pOld->zName = pOld->zDatabase = 0;
        pOld->pSelect = 0;
      }
      sqlitexSrcListDelete(pParse->db, F);
    }else{
      Select *pSubquery;
      sqlitexSrcListShiftJoinType(F);
      pSubquery = sqlitexSelectNew(pParse,0,F,0,0,0,0,SF_NestedFrom,0,0);
      A = sqlitexSrcListAppendFromTerm(pParse,X,0,0,&Z,pSubquery,N,U);
    }
  }
%endif  SQLITE_OMIT_SUBQUERY

%type dbnm {Token}
dbnm(A) ::= .          {A.z=0; A.n=0;}
dbnm(A) ::= DOT nm(X). {A = X;}

%type fullname {SrcList*}
%destructor fullname {sqlitexSrcListDelete(pParse->db, $$);}
fullname(A) ::= nm(X) dbnm(Y).  {A = sqlitexSrcListAppend(pParse->db,0,&X,&Y);}

%type joinop {int}
%type joinop2 {int}
joinop(X) ::= COMMA|JOIN.              { X = JT_INNER; }
joinop(X) ::= JOIN_KW(A) JOIN.         { X = sqlitexJoinType(pParse,&A,0,0); }
joinop(X) ::= JOIN_KW(A) nm(B) JOIN.   { X = sqlitexJoinType(pParse,&A,&B,0); }
joinop(X) ::= JOIN_KW(A) nm(B) nm(C) JOIN.
                                       { X = sqlitexJoinType(pParse,&A,&B,&C); }

%type on_opt {Expr*}
%destructor on_opt {sqlitexExprDelete(pParse->db, $$);}
on_opt(N) ::= ON expr(E).   {N = E.pExpr;}
on_opt(N) ::= .             {N = 0;}

// Note that this block abuses the Token type just a little. If there is
// no "INDEXED BY" clause, the returned token is empty (z==0 && n==0). If
// there is an INDEXED BY clause, then the token is populated as per normal,
// with z pointing to the token data and n containing the number of bytes
// in the token.
//
// If there is a "NOT INDEXED" clause, then (z==0 && n==1), which is 
// normally illegal. The sqlitexSrcListIndexedBy() function 
// recognizes and interprets this as a special case.
//
%type indexed_opt {Token}
indexed_opt(A) ::= .                 {A.z=0; A.n=0;}

/* COMDB2 MODIFICATION
 * sqlite itself discourages use of these clauses and we want
 * to avoid having developers write queries that mess with the 
 * planner
 * indexed_opt(A) ::= INDEXED BY nm(X). {A = X;}
 * indexed_opt(A) ::= NOT INDEXED.      {A.z=0; A.n=1;}
*/

%type using_opt {IdList*}
%destructor using_opt {sqlitexIdListDelete(pParse->db, $$);}
using_opt(U) ::= USING LP idlist(L) RP.  {U = L;}
using_opt(U) ::= .                        {U = 0;}


%type orderby_opt {ExprList*}
%destructor orderby_opt {sqlitexExprListDelete(pParse->db, $$);}
%type sortlist {ExprList*}
%destructor sortlist {sqlitexExprListDelete(pParse->db, $$);}

orderby_opt(A) ::= .                          {A = 0;}
orderby_opt(A) ::= ORDER BY sortlist(X).      {A = X;}
sortlist(A) ::= sortlist(X) COMMA expr(Y) sortorder(Z). {
  A = sqlitexExprListAppend(pParse,X,Y.pExpr);
  if( A ) A->a[A->nExpr-1].sortOrder = (u8)Z;
}
sortlist(A) ::= expr(Y) sortorder(Z). {
  A = sqlitexExprListAppend(pParse,0,Y.pExpr);
  if( A && ALWAYS(A->a) ) A->a[0].sortOrder = (u8)Z;
}

%type sortorder {int}

sortorder(A) ::= ASC.           {A = SQLITE_SO_ASC;}
sortorder(A) ::= DESC.          {A = SQLITE_SO_DESC;}
sortorder(A) ::= .              {A = SQLITE_SO_ASC;}

%type groupby_opt {ExprList*}
%destructor groupby_opt {sqlitexExprListDelete(pParse->db, $$);}
groupby_opt(A) ::= .                      {A = 0;}
groupby_opt(A) ::= GROUP BY nexprlist(X). {A = X;}

%type having_opt {Expr*}
%destructor having_opt {sqlitexExprDelete(pParse->db, $$);}
having_opt(A) ::= .                {A = 0;}
having_opt(A) ::= HAVING expr(X).  {A = X.pExpr;}

%type limit_opt {struct LimitVal}

// The destructor for limit_opt will never fire in the current grammar.
// The limit_opt non-terminal only occurs at the end of a single production
// rule for SELECT statements.  As soon as the rule that create the 
// limit_opt non-terminal reduces, the SELECT statement rule will also
// reduce.  So there is never a limit_opt non-terminal on the stack 
// except as a transient.  So there is never anything to destroy.
//
//%destructor limit_opt {
//  sqlitexExprDelete(pParse->db, $$.pLimit);
//  sqlitexExprDelete(pParse->db, $$.pOffset);
//}
limit_opt(A) ::= .                    {A.pLimit = 0; A.pOffset = 0;}
limit_opt(A) ::= LIMIT expr(X).       {A.pLimit = X.pExpr; A.pOffset = 0;}
limit_opt(A) ::= LIMIT expr(X) OFFSET expr(Y). 
                                      {A.pLimit = X.pExpr; A.pOffset = Y.pExpr;}
limit_opt(A) ::= LIMIT expr(X) COMMA expr(Y). 
                                      {A.pOffset = X.pExpr; A.pLimit = Y.pExpr;}

/////////////////////////// The DELETE statement /////////////////////////////
//
%ifdef SQLITE_ENABLE_UPDATE_DELETE_LIMIT
cmd ::= with(C) DELETE FROM fullname(X) indexed_opt(I) where_opt(W) 
        orderby_opt(O) limit_opt(L). {
  sqlitexWithPush(pParse, C, 1);
  sqlitexSrcListIndexedBy(pParse, X, &I);
  sqlitexDeleteFrom(pParse,X,W,O,L.pLimit,L.pOffset);
}
%endif
%ifndef SQLITE_ENABLE_UPDATE_DELETE_LIMIT
cmd ::= with(C) DELETE FROM fullname(X) indexed_opt(I) where_opt(W). {
  sqlitexWithPush(pParse, C, 1);
  sqlitexSrcListIndexedBy(pParse, X, &I);
  sqlitexDeleteFrom(pParse,X,W,0,0,0);
}
%endif

%type where_opt {Expr*}
%destructor where_opt {sqlitexExprDelete(pParse->db, $$);}

where_opt(A) ::= .                    {A = 0;}
where_opt(A) ::= WHERE expr(X).       {A = X.pExpr;}

////////////////////////// The UPDATE command ////////////////////////////////
//
%ifdef SQLITE_ENABLE_UPDATE_DELETE_LIMIT
cmd ::= with(C) UPDATE orconf(R) fullname(X) indexed_opt(I) SET setlist(Y)
        where_opt(W) orderby_opt(O) limit_opt(L).  {
  sqlitexWithPush(pParse, C, 1);
  sqlitexSrcListIndexedBy(pParse, X, &I);
  sqlitexExprListCheckLength(pParse,Y,"set list"); 
  sqlitexUpdate(pParse,X,Y,W,R,O,L.pLimit,L.pOffset);
}
%endif
%ifndef SQLITE_ENABLE_UPDATE_DELETE_LIMIT
cmd ::= with(C) UPDATE orconf(R) fullname(X) indexed_opt(I) SET setlist(Y)
        where_opt(W).  {
  sqlitexWithPush(pParse, C, 1);
  sqlitexSrcListIndexedBy(pParse, X, &I);
  sqlitexExprListCheckLength(pParse,Y,"set list"); 
  sqlitexUpdate(pParse,X,Y,W,R,0,0,0);
}
%endif

%type setlist {ExprList*}
%destructor setlist {sqlitexExprListDelete(pParse->db, $$);}

setlist(A) ::= setlist(Z) COMMA nm(X) EQ expr(Y). {
  A = sqlitexExprListAppend(pParse, Z, Y.pExpr);
  sqlitexExprListSetName(pParse, A, &X, 1);
}
setlist(A) ::= nm(X) EQ expr(Y). {
  A = sqlitexExprListAppend(pParse, 0, Y.pExpr);
  sqlitexExprListSetName(pParse, A, &X, 1);
}

////////////////////////// The INSERT command /////////////////////////////////
//
cmd ::= with(W) insert_cmd(R) INTO fullname(X) inscollist_opt(F) select(S). {
  sqlitexWithPush(pParse, W, 1);
  sqlitexInsert(pParse, X, S, F, R);
}
cmd ::= with(W) insert_cmd(R) INTO fullname(X) inscollist_opt(F) DEFAULT VALUES.
{
  sqlitexWithPush(pParse, W, 1);
  sqlitexInsert(pParse, X, 0, F, R);
}

%type insert_cmd {u8}
insert_cmd(A) ::= INSERT orconf(R).   {A = R;}
/* COMDB2 MODIFICATION 
 * insert or replace logic not supported
 * insert_cmd(A) ::= REPLACE.            {A = OE_Replace;} 
 */

%type inscollist_opt {IdList*}
%destructor inscollist_opt {sqlitexIdListDelete(pParse->db, $$);}
%type idlist {IdList*}
%destructor idlist {sqlitexIdListDelete(pParse->db, $$);}

inscollist_opt(A) ::= .                       {A = 0;}
inscollist_opt(A) ::= LP idlist(X) RP.    {A = X;}
idlist(A) ::= idlist(X) COMMA nm(Y).
    {A = sqlitexIdListAppend(pParse->db,X,&Y);}
idlist(A) ::= nm(Y).
    {A = sqlitexIdListAppend(pParse->db,0,&Y);}

/////////////////////////// Expression Processing /////////////////////////////
//

%type expr {ExprSpan}
%destructor expr {sqlitexExprDelete(pParse->db, $$.pExpr);}
%type term {ExprSpan}
%destructor term {sqlitexExprDelete(pParse->db, $$.pExpr);}

%include {
  /* This is a utility routine used to set the ExprSpan.zStart and
  ** ExprSpan.zEnd values of pOut so that the span covers the complete
  ** range of text beginning with pStart and going to the end of pEnd.
  */
  static void spanSet(ExprSpan *pOut, Token *pStart, Token *pEnd){
    pOut->zStart = pStart->z;
    pOut->zEnd = &pEnd->z[pEnd->n];
  }

  /* Construct a new Expr object from a single identifier.  Use the
  ** new Expr to populate pOut.  Set the span of pOut to be the identifier
  ** that created the expression.
  */
  static void spanExpr(ExprSpan *pOut, Parse *pParse, int op, Token *pValue){
    pOut->pExpr = sqlitexPExpr(pParse, op, 0, 0, pValue);
    pOut->zStart = pValue->z;
    pOut->zEnd = &pValue->z[pValue->n];
  }
}

expr(A) ::= term(X).             {A = X;}
expr(A) ::= LP(B) expr(X) RP(E). {A.pExpr = X.pExpr; spanSet(&A,&B,&E);}
term(A) ::= NULL(X).             {spanExpr(&A, pParse, @X, &X);}
expr(A) ::= id(X).               {spanExpr(&A, pParse, TK_ID, &X);}
expr(A) ::= JOIN_KW(X).          {spanExpr(&A, pParse, TK_ID, &X);}
expr(A) ::= nm(X) DOT nm(Y). {
  Expr *temp1 = sqlitexPExpr(pParse, TK_ID, 0, 0, &X);
  Expr *temp2 = sqlitexPExpr(pParse, TK_ID, 0, 0, &Y);
  A.pExpr = sqlitexPExpr(pParse, TK_DOT, temp1, temp2, 0);
  spanSet(&A,&X,&Y);
}
expr(A) ::= nm(X) DOT nm(Y) DOT nm(Z). {
  Expr *temp1 = sqlitexPExpr(pParse, TK_ID, 0, 0, &X);
  Expr *temp2 = sqlitexPExpr(pParse, TK_ID, 0, 0, &Y);
  Expr *temp3 = sqlitexPExpr(pParse, TK_ID, 0, 0, &Z);
  Expr *temp4 = sqlitexPExpr(pParse, TK_DOT, temp2, temp3, 0);
  A.pExpr = sqlitexPExpr(pParse, TK_DOT, temp1, temp4, 0);
  spanSet(&A,&X,&Z);
}
term(A) ::= INTEGER|FLOAT|BLOB(X).  {spanExpr(&A, pParse, @X, &X);}
term(A) ::= STRING(X).              {spanExpr(&A, pParse, @X, &X);}
expr(A) ::= VARIABLE(X).     {
  if( X.n>=2 && X.z[0]=='#' && sqlitexIsdigit(X.z[1]) ){
    /* When doing a nested parse, one can include terms in an expression
    ** that look like this:   #1 #2 ...  These terms refer to registers
    ** in the virtual machine.  #N is the N-th register. */
    if( pParse->nested==0 ){
      sqlitexErrorMsg(pParse, "near \"%T\": syntax error", &X);
      A.pExpr = 0;
    }else{
      A.pExpr = sqlitexPExpr(pParse, TK_REGISTER, 0, 0, &X);
      if( A.pExpr ) sqlitexGetInt32(&X.z[1], &A.pExpr->iTable);
    }
  }else{
    spanExpr(&A, pParse, TK_VARIABLE, &X);
    sqlitexExprAssignVarNumber(pParse, A.pExpr);
  }
  spanSet(&A, &X, &X);
}
expr(A) ::= expr(E) COLLATE ids(C). {
  A.pExpr = sqlitexExprAddCollateToken(pParse, E.pExpr, &C, 1);
  A.zStart = E.zStart;
  A.zEnd = &C.z[C.n];
}
%ifndef SQLITE_OMIT_CAST
expr(A) ::= CAST(X) LP expr(E) AS typetoken(T) RP(Y). {
  A.pExpr = sqlitexPExpr(pParse, TK_CAST, E.pExpr, 0, &T);
  spanSet(&A,&X,&Y);
}
%endif  SQLITE_OMIT_CAST
expr(A) ::= id(X) LP distinct(D) exprlist(Y) RP(E). {
  if( Y && Y->nExpr>pParse->db->aLimit[SQLITE_LIMIT_FUNCTION_ARG] ){
    sqlitexErrorMsg(pParse, "too many arguments on function %T", &X);
  }
  A.pExpr = sqlitexExprFunction(pParse, Y, &X);
  spanSet(&A,&X,&E);
  if( D && A.pExpr ){
    A.pExpr->flags |= EP_Distinct;
  }
}
expr(A) ::= id(X) LP STAR RP(E). {
  A.pExpr = sqlitexExprFunction(pParse, 0, &X);
  spanSet(&A,&X,&E);
}
term(A) ::= CTIME_KW(OP). {
  A.pExpr = sqlitexExprFunction(pParse, 0, &OP);
  spanSet(&A, &OP, &OP);
}

%include {
  /* This routine constructs a binary expression node out of two ExprSpan
  ** objects and uses the result to populate a new ExprSpan object.
  */
  static void spanBinaryExpr(
    ExprSpan *pOut,     /* Write the result here */
    Parse *pParse,      /* The parsing context.  Errors accumulate here */
    int op,             /* The binary operation */
    ExprSpan *pLeft,    /* The left operand */
    ExprSpan *pRight    /* The right operand */
  ){
    pOut->pExpr = sqlitexPExpr(pParse, op, pLeft->pExpr, pRight->pExpr, 0);
    pOut->zStart = pLeft->zStart;
    pOut->zEnd = pRight->zEnd;
  }
}

expr(A) ::= expr(X) AND(OP) expr(Y).    {spanBinaryExpr(&A,pParse,@OP,&X,&Y);}
expr(A) ::= expr(X) OR(OP) expr(Y).     {spanBinaryExpr(&A,pParse,@OP,&X,&Y);}
expr(A) ::= expr(X) LT|GT|GE|LE(OP) expr(Y).
                                        {spanBinaryExpr(&A,pParse,@OP,&X,&Y);}
expr(A) ::= expr(X) EQ|NE(OP) expr(Y).  {spanBinaryExpr(&A,pParse,@OP,&X,&Y);}
expr(A) ::= expr(X) BITAND|BITOR|LSHIFT|RSHIFT(OP) expr(Y).
                                        {spanBinaryExpr(&A,pParse,@OP,&X,&Y);}
expr(A) ::= expr(X) PLUS|MINUS(OP) expr(Y).
                                        {spanBinaryExpr(&A,pParse,@OP,&X,&Y);}
expr(A) ::= expr(X) STAR|SLASH|REM(OP) expr(Y).
                                        {spanBinaryExpr(&A,pParse,@OP,&X,&Y);}
expr(A) ::= expr(X) CONCAT(OP) expr(Y). {spanBinaryExpr(&A,pParse,@OP,&X,&Y);}
%type likeop {struct LikeOp}
likeop(A) ::= LIKE_KW|MATCH(X).     {A.eOperator = X; A.bNot = 0;}
likeop(A) ::= NOT LIKE_KW|MATCH(X). {A.eOperator = X; A.bNot = 1;}
expr(A) ::= expr(X) likeop(OP) expr(Y).  [LIKE_KW]  {
  ExprList *pList;
  pList = sqlitexExprListAppend(pParse,0, Y.pExpr);
  pList = sqlitexExprListAppend(pParse,pList, X.pExpr);
  A.pExpr = sqlitexExprFunction(pParse, pList, &OP.eOperator);
  if( OP.bNot ) A.pExpr = sqlitexPExpr(pParse, TK_NOT, A.pExpr, 0, 0);
  A.zStart = X.zStart;
  A.zEnd = Y.zEnd;
  if( A.pExpr ) A.pExpr->flags |= EP_InfixFunc;
}
expr(A) ::= expr(X) likeop(OP) expr(Y) ESCAPE expr(E).  [LIKE_KW]  {
  ExprList *pList;
  pList = sqlitexExprListAppend(pParse,0, Y.pExpr);
  pList = sqlitexExprListAppend(pParse,pList, X.pExpr);
  pList = sqlitexExprListAppend(pParse,pList, E.pExpr);
  A.pExpr = sqlitexExprFunction(pParse, pList, &OP.eOperator);
  if( OP.bNot ) A.pExpr = sqlitexPExpr(pParse, TK_NOT, A.pExpr, 0, 0);
  A.zStart = X.zStart;
  A.zEnd = E.zEnd;
  if( A.pExpr ) A.pExpr->flags |= EP_InfixFunc;
}

%include {
  /* Construct an expression node for a unary postfix operator
  */
  static void spanUnaryPostfix(
    ExprSpan *pOut,        /* Write the new expression node here */
    Parse *pParse,         /* Parsing context to record errors */
    int op,                /* The operator */
    ExprSpan *pOperand,    /* The operand */
    Token *pPostOp         /* The operand token for setting the span */
  ){
    pOut->pExpr = sqlitexPExpr(pParse, op, pOperand->pExpr, 0, 0);
    pOut->zStart = pOperand->zStart;
    pOut->zEnd = &pPostOp->z[pPostOp->n];
  }                           
}

expr(A) ::= expr(X) ISNULL|NOTNULL(E).   {spanUnaryPostfix(&A,pParse,@E,&X,&E);}
expr(A) ::= expr(X) NOT NULL(E). {spanUnaryPostfix(&A,pParse,TK_NOTNULL,&X,&E);}

%include {
  /* A routine to convert a binary TK_IS or TK_ISNOT expression into a
  ** unary TK_ISNULL or TK_NOTNULL expression. */
  static void binaryToUnaryIfNull(Parse *pParse, Expr *pY, Expr *pA, int op){
    sqlitex *db = pParse->db;
    if( pY && pA && pY->op==TK_NULL ){
      pA->op = (u8)op;
      sqlitexExprDelete(db, pA->pRight);
      pA->pRight = 0;
    }
  }
}

//    expr1 IS expr2
//    expr1 IS NOT expr2
//
// If expr2 is NULL then code as TK_ISNULL or TK_NOTNULL.  If expr2
// is any other expression, code as TK_IS or TK_ISNOT.
// 
expr(A) ::= expr(X) IS expr(Y).     {
  spanBinaryExpr(&A,pParse,TK_IS,&X,&Y);
  binaryToUnaryIfNull(pParse, Y.pExpr, A.pExpr, TK_ISNULL);
}
expr(A) ::= expr(X) IS NOT expr(Y). {
  spanBinaryExpr(&A,pParse,TK_ISNOT,&X,&Y);
  binaryToUnaryIfNull(pParse, Y.pExpr, A.pExpr, TK_NOTNULL);
}

%include {
  /* Construct an expression node for a unary prefix operator
  */
  static void spanUnaryPrefix(
    ExprSpan *pOut,        /* Write the new expression node here */
    Parse *pParse,         /* Parsing context to record errors */
    int op,                /* The operator */
    ExprSpan *pOperand,    /* The operand */
    Token *pPreOp         /* The operand token for setting the span */
  ){
    pOut->pExpr = sqlitexPExpr(pParse, op, pOperand->pExpr, 0, 0);
    pOut->zStart = pPreOp->z;
    pOut->zEnd = pOperand->zEnd;
  }
}



expr(A) ::= NOT(B) expr(X).    {spanUnaryPrefix(&A,pParse,@B,&X,&B);}
expr(A) ::= BITNOT(B) expr(X). {spanUnaryPrefix(&A,pParse,@B,&X,&B);}
expr(A) ::= MINUS(B) expr(X). [BITNOT]
                               {spanUnaryPrefix(&A,pParse,TK_UMINUS,&X,&B);}
expr(A) ::= PLUS(B) expr(X). [BITNOT]
                               {spanUnaryPrefix(&A,pParse,TK_UPLUS,&X,&B);}

%type between_op {int}
between_op(A) ::= BETWEEN.     {A = 0;}
between_op(A) ::= NOT BETWEEN. {A = 1;}
expr(A) ::= expr(W) between_op(N) expr(X) AND expr(Y). [BETWEEN] {
  ExprList *pList = sqlitexExprListAppend(pParse,0, X.pExpr);
  pList = sqlitexExprListAppend(pParse,pList, Y.pExpr);
  A.pExpr = sqlitexPExpr(pParse, TK_BETWEEN, W.pExpr, 0, 0);
  if( A.pExpr ){
    A.pExpr->x.pList = pList;
  }else{
    sqlitexExprListDelete(pParse->db, pList);
  } 
  if( N ) A.pExpr = sqlitexPExpr(pParse, TK_NOT, A.pExpr, 0, 0);
  A.zStart = W.zStart;
  A.zEnd = Y.zEnd;
}
%ifndef SQLITE_OMIT_SUBQUERY
  %type in_op {int}
  in_op(A) ::= IN.      {A = 0;}
  in_op(A) ::= NOT IN.  {A = 1;}
  expr(A) ::= expr(X) in_op(N) LP exprlist(Y) RP(E). [IN] {
    if( Y==0 ){
      /* Expressions of the form
      **
      **      expr1 IN ()
      **      expr1 NOT IN ()
      **
      ** simplify to constants 0 (false) and 1 (true), respectively,
      ** regardless of the value of expr1.
      */
      A.pExpr = sqlitexPExpr(pParse, TK_INTEGER, 0, 0, &sqlitexIntTokens[N]);
      sqlitexExprDelete(pParse->db, X.pExpr);
    }else if( Y->nExpr==1 ){
      /* Expressions of the form:
      **
      **      expr1 IN (?1)
      **      expr1 NOT IN (?2)
      **
      ** with exactly one value on the RHS can be simplified to something
      ** like this:
      **
      **      expr1 == ?1
      **      expr1 <> ?2
      **
      ** But, the RHS of the == or <> is marked with the EP_Generic flag
      ** so that it may not contribute to the computation of comparison
      ** affinity or the collating sequence to use for comparison.  Otherwise,
      ** the semantics would be subtly different from IN or NOT IN.
      */
      Expr *pRHS = Y->a[0].pExpr;
      Y->a[0].pExpr = 0;
      sqlitexExprListDelete(pParse->db, Y);
      /* pRHS cannot be NULL because a malloc error would have been detected
      ** before now and control would have never reached this point */
      if( ALWAYS(pRHS) ){
        pRHS->flags &= ~EP_Collate;
        pRHS->flags |= EP_Generic;
      }
      A.pExpr = sqlitexPExpr(pParse, N ? TK_NE : TK_EQ, X.pExpr, pRHS, 0);
    }else{
      A.pExpr = sqlitexPExpr(pParse, TK_IN, X.pExpr, 0, 0);
      if( A.pExpr ){
        A.pExpr->x.pList = Y;
        sqlitexExprSetHeightAndFlags(pParse, A.pExpr);
      }else{
        sqlitexExprListDelete(pParse->db, Y);
      }
      if( N ) A.pExpr = sqlitexPExpr(pParse, TK_NOT, A.pExpr, 0, 0);
    }
    A.zStart = X.zStart;
    A.zEnd = &E.z[E.n];
  }
  expr(A) ::= LP(B) select(X) RP(E). {
    A.pExpr = sqlitexPExpr(pParse, TK_SELECT, 0, 0, 0);
    if( A.pExpr ){
      A.pExpr->x.pSelect = X;
      ExprSetProperty(A.pExpr, EP_xIsSelect|EP_Subquery);
      sqlitexExprSetHeightAndFlags(pParse, A.pExpr);
    }else{
      sqlitexSelectDelete(pParse->db, X);
    }
    A.zStart = B.z;
    A.zEnd = &E.z[E.n];
  }
  expr(A) ::= expr(X) in_op(N) LP select(Y) RP(E).  [IN] {
    A.pExpr = sqlitexPExpr(pParse, TK_IN, X.pExpr, 0, 0);
    if( A.pExpr ){
      A.pExpr->x.pSelect = Y;
      ExprSetProperty(A.pExpr, EP_xIsSelect|EP_Subquery);
      sqlitexExprSetHeightAndFlags(pParse, A.pExpr);
    }else{
      sqlitexSelectDelete(pParse->db, Y);
    }
    if( N ) A.pExpr = sqlitexPExpr(pParse, TK_NOT, A.pExpr, 0, 0);
    A.zStart = X.zStart;
    A.zEnd = &E.z[E.n];
  }
  expr(A) ::= expr(X) in_op(N) nm(Y) dbnm(Z). [IN] {
    SrcList *pSrc = sqlitexSrcListAppend(pParse->db, 0,&Y,&Z);
    A.pExpr = sqlitexPExpr(pParse, TK_IN, X.pExpr, 0, 0);
    if( A.pExpr ){
      A.pExpr->x.pSelect = sqlitexSelectNew(pParse, 0,pSrc,0,0,0,0,0,0,0);
      ExprSetProperty(A.pExpr, EP_xIsSelect|EP_Subquery);
      sqlitexExprSetHeightAndFlags(pParse, A.pExpr);
    }else{
      sqlitexSrcListDelete(pParse->db, pSrc);
    }
    if( N ) A.pExpr = sqlitexPExpr(pParse, TK_NOT, A.pExpr, 0, 0);
    A.zStart = X.zStart;
    A.zEnd = Z.z ? &Z.z[Z.n] : &Y.z[Y.n];
  }
  expr(A) ::= EXISTS(B) LP select(Y) RP(E). {
    Expr *p = A.pExpr = sqlitexPExpr(pParse, TK_EXISTS, 0, 0, 0);
    if( p ){
      p->x.pSelect = Y;
      ExprSetProperty(p, EP_xIsSelect|EP_Subquery);
      sqlitexExprSetHeightAndFlags(pParse, p);
    }else{
      sqlitexSelectDelete(pParse->db, Y);
    }
    A.zStart = B.z;
    A.zEnd = &E.z[E.n];
  }
%endif SQLITE_OMIT_SUBQUERY

/* CASE expressions */
expr(A) ::= CASE(C) case_operand(X) case_exprlist(Y) case_else(Z) END(E). {
  A.pExpr = sqlitexPExpr(pParse, TK_CASE, X, 0, 0);
  if( A.pExpr ){
    A.pExpr->x.pList = Z ? sqlitexExprListAppend(pParse,Y,Z) : Y;
    sqlitexExprSetHeightAndFlags(pParse, A.pExpr);
  }else{
    sqlitexExprListDelete(pParse->db, Y);
    sqlitexExprDelete(pParse->db, Z);
  }
  A.zStart = C.z;
  A.zEnd = &E.z[E.n];
}
%type case_exprlist {ExprList*}
%destructor case_exprlist {sqlitexExprListDelete(pParse->db, $$);}
case_exprlist(A) ::= case_exprlist(X) WHEN expr(Y) THEN expr(Z). {
  A = sqlitexExprListAppend(pParse,X, Y.pExpr);
  A = sqlitexExprListAppend(pParse,A, Z.pExpr);
}
case_exprlist(A) ::= WHEN expr(Y) THEN expr(Z). {
  A = sqlitexExprListAppend(pParse,0, Y.pExpr);
  A = sqlitexExprListAppend(pParse,A, Z.pExpr);
}
%type case_else {Expr*}
%destructor case_else {sqlitexExprDelete(pParse->db, $$);}
case_else(A) ::=  ELSE expr(X).         {A = X.pExpr;}
case_else(A) ::=  .                     {A = 0;} 
%type case_operand {Expr*}
%destructor case_operand {sqlitexExprDelete(pParse->db, $$);}
case_operand(A) ::= expr(X).            {A = X.pExpr;} 
case_operand(A) ::= .                   {A = 0;} 

%type exprlist {ExprList*}
%destructor exprlist {sqlitexExprListDelete(pParse->db, $$);}
%type nexprlist {ExprList*}
%destructor nexprlist {sqlitexExprListDelete(pParse->db, $$);}

exprlist(A) ::= nexprlist(X).                {A = X;}
exprlist(A) ::= .                            {A = 0;}
nexprlist(A) ::= nexprlist(X) COMMA expr(Y).
    {A = sqlitexExprListAppend(pParse,X,Y.pExpr);}
nexprlist(A) ::= expr(Y).
    {A = sqlitexExprListAppend(pParse,0,Y.pExpr);}


///////////////////////////// The CREATE INDEX command ///////////////////////
//
cmd ::= createkw(S) uniqueflag(U) INDEX ifnotexists(NE) nm(X) dbnm(D)
        ON nm(Y) LP idxlist(Z) RP where_opt(W). {
  sqlitexCreateIndex(pParse, &X, &D, 
                     sqlitexSrcListAppend(pParse->db,0,&Y,0), Z, U,
                      &S, W, SQLITE_SO_ASC, NE);
}

%type uniqueflag {int}
uniqueflag(A) ::= UNIQUE.  {A = OE_Abort;}
uniqueflag(A) ::= .        {A = OE_None;}

%type idxlist {ExprList*}
%destructor idxlist {sqlitexExprListDelete(pParse->db, $$);}
%type idxlist_opt {ExprList*}
%destructor idxlist_opt {sqlitexExprListDelete(pParse->db, $$);}

idxlist_opt(A) ::= .                         {A = 0;}
idxlist_opt(A) ::= LP idxlist(X) RP.         {A = X;}
idxlist(A) ::= idxlist(X) COMMA nm(Y) collate(C) sortorder(Z).  {
  Expr *p = sqlitexExprAddCollateToken(pParse, 0, &C, 1);
  A = sqlitexExprListAppend(pParse,X, p);
  sqlitexExprListSetName(pParse,A,&Y,1);
  sqlitexExprListCheckLength(pParse, A, "index");
  if( A ) A->a[A->nExpr-1].sortOrder = (u8)Z;
}
idxlist(A) ::= nm(Y) collate(C) sortorder(Z). {
  Expr *p = sqlitexExprAddCollateToken(pParse, 0, &C, 1);
  A = sqlitexExprListAppend(pParse,0, p);
  sqlitexExprListSetName(pParse, A, &Y, 1);
  sqlitexExprListCheckLength(pParse, A, "index");
  if( A ) A->a[A->nExpr-1].sortOrder = (u8)Z;
}

%type collate {Token}
collate(C) ::= .                 {C.z = 0; C.n = 0;}
collate(C) ::= COLLATE ids(X).   {C = X;}


///////////////////////////// The DROP INDEX command /////////////////////////
//
cmd ::= DROP INDEX ifexists(E) fullname(X).   {sqlitexDropIndex(pParse, X, E);}

///////////////////////////// The VACUUM command /////////////////////////////
//
%ifndef SQLITE_OMIT_VACUUM
%ifndef SQLITE_OMIT_ATTACH
cmd ::= VACUUM.                {sqlitexVacuum(pParse);}
cmd ::= VACUUM nm.             {sqlitexVacuum(pParse);}
%endif  SQLITE_OMIT_ATTACH
%endif  SQLITE_OMIT_VACUUM

///////////////////////////// The PRAGMA command /////////////////////////////
//
%ifndef SQLITE_OMIT_PRAGMA
cmd ::= PRAGMA nm(X) dbnm(Z).                {sqlitexPragma(pParse,&X,&Z,0,0);}
cmd ::= PRAGMA nm(X) dbnm(Z) EQ nmnum(Y).    {sqlitexPragma(pParse,&X,&Z,&Y,0);}
cmd ::= PRAGMA nm(X) dbnm(Z) LP nmnum(Y) RP. {sqlitexPragma(pParse,&X,&Z,&Y,0);}
cmd ::= PRAGMA nm(X) dbnm(Z) EQ minus_num(Y). 
                                             {sqlitexPragma(pParse,&X,&Z,&Y,1);}
cmd ::= PRAGMA nm(X) dbnm(Z) LP minus_num(Y) RP.
                                             {sqlitexPragma(pParse,&X,&Z,&Y,1);}

nmnum(A) ::= plus_num(X).             {A = X;}
nmnum(A) ::= nm(X).                   {A = X;}
nmnum(A) ::= ON(X).                   {A = X;}
nmnum(A) ::= DELETE(X).               {A = X;}
nmnum(A) ::= DEFAULT(X).              {A = X;}
%endif SQLITE_OMIT_PRAGMA
%token_class number INTEGER|FLOAT.
plus_num(A) ::= PLUS number(X).       {A = X;}
plus_num(A) ::= number(X).            {A = X;}
minus_num(A) ::= MINUS number(X).     {A = X;}
//////////////////////////// The CREATE TRIGGER command /////////////////////

%ifndef SQLITE_OMIT_TRIGGER

cmd ::= createkw trigger_decl(A) BEGIN trigger_cmd_list(S) END(Z). {
  Token all;
  all.z = A.z;
  all.n = (int)(Z.z - A.z) + Z.n;
  sqlitexFinishTrigger(pParse, S, &all);
}

trigger_decl(A) ::= temp(T) TRIGGER ifnotexists(NOERR) nm(B) dbnm(Z) 
                    trigger_time(C) trigger_event(D)
                    ON fullname(E) foreach_clause when_clause(G). {
  sqlitexBeginTrigger(pParse, &B, &Z, C, D.a, D.b, E, G, T, NOERR);
  A = (Z.n==0?B:Z);
}

%type trigger_time {int}
trigger_time(A) ::= BEFORE.      { A = TK_BEFORE; }
trigger_time(A) ::= AFTER.       { A = TK_AFTER;  }
trigger_time(A) ::= INSTEAD OF.  { A = TK_INSTEAD;}
trigger_time(A) ::= .            { A = TK_BEFORE; }

%type trigger_event {struct TrigEvent}
%destructor trigger_event {sqlitexIdListDelete(pParse->db, $$.b);}
trigger_event(A) ::= DELETE|INSERT(OP).       {A.a = @OP; A.b = 0;}
trigger_event(A) ::= UPDATE(OP).              {A.a = @OP; A.b = 0;}
trigger_event(A) ::= UPDATE OF idlist(X). {A.a = TK_UPDATE; A.b = X;}

foreach_clause ::= .
foreach_clause ::= FOR EACH ROW.

%type when_clause {Expr*}
%destructor when_clause {sqlitexExprDelete(pParse->db, $$);}
when_clause(A) ::= .             { A = 0; }
when_clause(A) ::= WHEN expr(X). { A = X.pExpr; }

%type trigger_cmd_list {TriggerStep*}
%destructor trigger_cmd_list {sqlitexDeleteTriggerStep(pParse->db, $$);}
trigger_cmd_list(A) ::= trigger_cmd_list(Y) trigger_cmd(X) SEMI. {
  assert( Y!=0 );
  Y->pLast->pNext = X;
  Y->pLast = X;
  A = Y;
}
trigger_cmd_list(A) ::= trigger_cmd(X) SEMI. { 
  assert( X!=0 );
  X->pLast = X;
  A = X;
}

// Disallow qualified table names on INSERT, UPDATE, and DELETE statements
// within a trigger.  The table to INSERT, UPDATE, or DELETE is always in 
// the same database as the table that the trigger fires on.
//
%type trnm {Token}
trnm(A) ::= nm(X).   {A = X;}
trnm(A) ::= nm DOT nm(X). {
  A = X;
  sqlitexErrorMsg(pParse, 
        "qualified table names are not allowed on INSERT, UPDATE, and DELETE "
        "statements within triggers");
}

// Disallow the INDEX BY and NOT INDEXED clauses on UPDATE and DELETE
// statements within triggers.  We make a specific error message for this
// since it is an exception to the default grammar rules.
//
tridxby ::= .
tridxby ::= INDEXED BY nm. {
  sqlitexErrorMsg(pParse,
        "the INDEXED BY clause is not allowed on UPDATE or DELETE statements "
        "within triggers");
}
tridxby ::= NOT INDEXED. {
  sqlitexErrorMsg(pParse,
        "the NOT INDEXED clause is not allowed on UPDATE or DELETE statements "
        "within triggers");
}



%type trigger_cmd {TriggerStep*}
%destructor trigger_cmd {sqlitexDeleteTriggerStep(pParse->db, $$);}
// UPDATE 
trigger_cmd(A) ::=
   UPDATE orconf(R) trnm(X) tridxby SET setlist(Y) where_opt(Z).  
   { A = sqlitexTriggerUpdateStep(pParse->db, &X, Y, Z, R); }

// INSERT
trigger_cmd(A) ::= insert_cmd(R) INTO trnm(X) inscollist_opt(F) select(S).
               {A = sqlitexTriggerInsertStep(pParse->db, &X, F, S, R);}

// DELETE
trigger_cmd(A) ::= DELETE FROM trnm(X) tridxby where_opt(Y).
               {A = sqlitexTriggerDeleteStep(pParse->db, &X, Y);}

// SELECT
trigger_cmd(A) ::= select(X).  {A = sqlitexTriggerSelectStep(pParse->db, X); }

// The special RAISE expression that may occur in trigger programs
expr(A) ::= RAISE(X) LP IGNORE RP(Y).  {
  A.pExpr = sqlitexPExpr(pParse, TK_RAISE, 0, 0, 0); 
  if( A.pExpr ){
    A.pExpr->affinity = OE_Ignore;
  }
  A.zStart = X.z;
  A.zEnd = &Y.z[Y.n];
}
expr(A) ::= RAISE(X) LP raisetype(T) COMMA nm(Z) RP(Y).  {
  A.pExpr = sqlitexPExpr(pParse, TK_RAISE, 0, 0, &Z); 
  if( A.pExpr ) {
    A.pExpr->affinity = (char)T;
  }
  A.zStart = X.z;
  A.zEnd = &Y.z[Y.n];
}
%endif  !SQLITE_OMIT_TRIGGER

%type raisetype {int}
raisetype(A) ::= ROLLBACK.  {A = OE_Rollback;}
raisetype(A) ::= ABORT.     {A = OE_Abort;}
raisetype(A) ::= FAIL.      {A = OE_Fail;}


////////////////////////  DROP TRIGGER statement //////////////////////////////
%ifndef SQLITE_OMIT_TRIGGER
cmd ::= DROP TRIGGER ifexists(NOERR) fullname(X). {
  sqlitexDropTrigger(pParse,X,NOERR);
}
%endif  !SQLITE_OMIT_TRIGGER

////////////////////// CREATE LUA commands ////////////////////
cmd ::= createkw LUA SCALAR FUNCTION nm(Q). {
	comdb2CreateScalarFuncX(pParse, &Q);
}

cmd ::= createkw LUA AGGREGATE FUNCTION nm(Q). {
	comdb2CreateAggFuncX(pParse, &Q);
}

cmd ::= createkw LUA TRIGGER nm(Q) ON table_trigger_event(T). {
  comdb2CreateTriggerX(pParse,0,&Q,T);
}

cmd ::= createkw LUA CONSUMER nm(Q) ON table_trigger_event(T). {
  comdb2CreateTriggerX(pParse,1,&Q,T);
}

table_trigger_event(A) ::= table_trigger_event(B) COMMA LP TABLE fullname(T) FOR trigger_events(C) RP. {
  A = comdb2AddTriggerTableX(pParse,B,T,C);
}

table_trigger_event(A) ::= LP TABLE fullname(T) FOR trigger_events(B) RP. {
  A = comdb2AddTriggerTableX(pParse,0,T,B);
}

%type table_trigger_event {Cdb2TrigTables*}
%destructor table_trigger_event {sqlitexDbFree(pParse->db, $$);}

%type cdb2_trigger_event {Cdb2TrigEvent}
%destructor cdb2_trigger_event {sqlitexIdListDelete(pParse->db, $$.cols);}

%type trigger_events {Cdb2TrigEvents*}
%destructor trigger_events {sqlitexDbFree(pParse->db, $$);}

trigger_events(A) ::= trigger_events(B) AND cdb2_trigger_event(C). {
  A = comdb2AddTriggerEventX(pParse,B,&C);
}
trigger_events(A) ::= cdb2_trigger_event(B). {
  A = comdb2AddTriggerEventX(pParse,0,&B);
}
cdb2_trigger_event(A) ::= DELETE. {
  A.op = TK_DELETE;
  A.cols = 0;
}
cdb2_trigger_event(A) ::= INSERT. {
  A.op = TK_INSERT;
  A.cols = 0;
}
cdb2_trigger_event(A) ::= UPDATE. {
  A.op = TK_UPDATE;
  A.cols = 0;
}
cdb2_trigger_event(A) ::= DELETE OF idlist(X). {
  A.op = TK_DELETE;
  A.cols = X;
}
cdb2_trigger_event(A) ::= INSERT OF idlist(X). {
  A.op = TK_INSERT;
  A.cols = X;
}
cdb2_trigger_event(A) ::= UPDATE OF idlist(X). {
  A.op = TK_UPDATE;
  A.cols = X;
}

///////////////////////// DROP LUA commands /////////////////////////
cmd ::= DROP LUA SCALAR FUNCTION nm(A). {
  comdb2DropScalarFuncX(pParse,&A);
}
cmd ::= DROP LUA AGGREGATE FUNCTION nm(A). {
  comdb2DropAggFuncX(pParse,&A);
}
cmd ::= DROP LUA TRIGGER nm(A). {
  comdb2DropTriggerX(pParse,&A);
}
cmd ::= DROP LUA CONSUMER nm(A). {
  comdb2DropTriggerX(pParse,&A);
}

//////////////////////// ATTACH DATABASE file AS name /////////////////////////
%ifndef SQLITE_OMIT_ATTACH
cmd ::= ATTACH database_kw_opt expr(F) AS expr(D) key_opt(K). {
  sqlitexAttach(pParse, F.pExpr, D.pExpr, K);
}
cmd ::= DETACH database_kw_opt expr(D). {
  sqlitexDetach(pParse, D.pExpr);
}

%type key_opt {Expr*}
%destructor key_opt {sqlitexExprDelete(pParse->db, $$);}
key_opt(A) ::= .                     { A = 0; }
key_opt(A) ::= KEY expr(X).          { A = X.pExpr; }

database_kw_opt ::= DATABASE.
database_kw_opt ::= .
%endif SQLITE_OMIT_ATTACH

////////////////////////// REINDEX collation //////////////////////////////////
%ifndef SQLITE_OMIT_REINDEX
cmd ::= REINDEX.                {sqlitexReindex(pParse, 0, 0);}
cmd ::= REINDEX nm(X) dbnm(Y).  {sqlitexReindex(pParse, &X, &Y);}
%endif  SQLITE_OMIT_REINDEX

/////////////////////////////////// ANALYZE ///////////////////////////////////
%ifndef SQLITE_OMIT_ANALYZE
cmd ::= ANALYZESQLITE.                {sqlitexAnalyze(pParse, 0, 0);}
cmd ::= ANALYZESQLITE nm(X) dbnm(Y).  {sqlitexAnalyze(pParse, &X, &Y);}
%endif

//////////////////////// ALTER TABLE table ... ////////////////////////////////
%ifndef SQLITE_OMIT_ALTERTABLE
cmd ::= ALTER TABLE fullname(X) RENAME TO nm(Z). {
  sqlitexAlterRenameTable(pParse,X,&Z);
}
cmd ::= ALTER TABLE add_column_fullname ADD kwcolumn_opt column(Y). {
  sqlitexAlterFinishAddColumn(pParse, &Y);
}
add_column_fullname ::= fullname(X). {
  pParse->db->lookaside.bEnabled = 0;
  sqlitexAlterBeginAddColumn(pParse, X);
}
kwcolumn_opt ::= .
kwcolumn_opt ::= COLUMNKW.
%endif  SQLITE_OMIT_ALTERTABLE

//////////////////////// CREATE VIRTUAL TABLE ... /////////////////////////////
%ifndef SQLITE_OMIT_VIRTUALTABLE
cmd ::= create_vtab.                       {sqlitexVtabFinishParse(pParse,0);}
cmd ::= create_vtab LP vtabarglist RP(X).  {sqlitexVtabFinishParse(pParse,&X);}
create_vtab ::= createkw VIRTUAL TABLE ifnotexists(E)
                nm(X) dbnm(Y) USING nm(Z). {
    sqlitexVtabBeginParse(pParse, &X, &Y, &Z, E);
}
vtabarglist ::= vtabarg.
vtabarglist ::= vtabarglist COMMA vtabarg.
vtabarg ::= .                       {sqlitexVtabArgInit(pParse);}
vtabarg ::= vtabarg vtabargtoken.
vtabargtoken ::= ANY(X).            {sqlitexVtabArgExtend(pParse,&X);}
vtabargtoken ::= lp anylist RP(X).  {sqlitexVtabArgExtend(pParse,&X);}
lp ::= LP(X).                       {sqlitexVtabArgExtend(pParse,&X);}
anylist ::= .
anylist ::= anylist LP anylist RP.
anylist ::= anylist ANY.
%endif  SQLITE_OMIT_VIRTUALTABLE


//////////////////////// COMMON TABLE EXPRESSIONS ////////////////////////////
%type with {With*}
%type wqlist {With*}
%destructor with {sqlitexWithDelete(pParse->db, $$);}
%destructor wqlist {sqlitexWithDelete(pParse->db, $$);}

with(A) ::= . {A = 0;}
%ifndef SQLITE_OMIT_CTE
with(A) ::= WITH wqlist(W).              { A = W; }
with(A) ::= WITH RECURSIVE wqlist(W).    { A = W; }

wqlist(A) ::= nm(X) idxlist_opt(Y) AS LP select(Z) RP. {
  A = sqlitexWithAdd(pParse, 0, &X, Y, Z);
}
wqlist(A) ::= wqlist(W) COMMA nm(X) idxlist_opt(Y) AS LP select(Z) RP. {
  A = sqlitexWithAdd(pParse, W, &X, Y, Z);
}
%endif  SQLITE_OMIT_CTE
/* vim: set ft=lemon: */
