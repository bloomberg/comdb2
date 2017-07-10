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
  sqlite3ErrorMsg(pParse, "near \"%T\": syntax error", &TOKEN);
}
%stack_overflow {
  sqlite3ErrorMsg(pParse, "parser stack overflow");
}

// The name of the generated procedure that implements the parser
// is as follows:
%name sqlite3Parser

// The following text is included near the beginning of the C source
// code file that implements the parser.
//
%include {
#include "sqliteInt.h"
#include "comdb2Int.h"  /* COMDB2 CUSTOM FUNCTION (ALL CUSTOM HEADERS
                           ARE INCLUDED FROM HERE) */
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
** Indicate that sqlite3ParserFree() will never be called with a null
** pointer.
*/
#define YYPARSEFREENEVERNULL 1

/*
** Alternative datatype for the argument to the malloc() routine passed
** into sqlite3ParserAlloc().  The default is size_t.
*/
#define YYMALLOCARGTYPE  u64

/*
** An instance of this structure holds information about the
** LIMIT clause of a SELECT statement.
*/
struct LimitVal {
  Expr *pLimit;    /* The LIMIT expression.  NULL if there is no limit */
  Expr *pOffset;   /* The OFFSET expression.  NULL if there is none */
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
** Disable lookaside memory allocation for objects that might be
** shared across database connections.
*/
static void disableLookaside(Parse *pParse){
  pParse->disableLookaside++;
  pParse->db->lookaside.bDisable++;
}

} // end %include

// Input is a single SQL command
input ::= cmdlist.
cmdlist ::= cmdlist ecmd.
cmdlist ::= ecmd.
ecmd ::= SEMI.
ecmd ::= explain cmdx SEMI.
explain ::= .
%ifndef SQLITE_OMIT_EXPLAIN
explain ::= EXPLAIN.              { pParse->explain = 1; }
explain ::= EXPLAIN QUERY PLAN.   { pParse->explain = 2; }
%endif  SQLITE_OMIT_EXPLAIN
cmdx ::= cmd.           { sqlite3FinishCoding(pParse); }

///////////////////// Begin and end transactions. ////////////////////////////
//

cmd ::= BEGIN transtype(Y) trans_opt.  {sqlite3BeginTransaction(pParse, Y);}
trans_opt ::= .
trans_opt ::= TRANSACTION.
trans_opt ::= TRANSACTION nm.
%type transtype {int}
transtype(A) ::= .             {A = TK_DEFERRED;}
transtype(A) ::= DEFERRED(X).  {A = @X; /*A-overwrites-X*/}
transtype(A) ::= IMMEDIATE(X). {A = @X; /*A-overwrites-X*/}
transtype(A) ::= EXCLUSIVE(X). {A = @X; /*A-overwrites-X*/}
cmd ::= COMMIT trans_opt.      {sqlite3CommitTransaction(pParse);}
cmd ::= END trans_opt.         {sqlite3CommitTransaction(pParse);}
cmd ::= ROLLBACK trans_opt.    {sqlite3RollbackTransaction(pParse);}

savepoint_opt ::= SAVEPOINT.
savepoint_opt ::= .
cmd ::= SAVEPOINT nm(X). {
  sqlite3Savepoint(pParse, SAVEPOINT_BEGIN, &X);
}
cmd ::= RELEASE savepoint_opt nm(X). {
  sqlite3Savepoint(pParse, SAVEPOINT_RELEASE, &X);
}
cmd ::= ROLLBACK trans_opt TO savepoint_opt nm(X). {
  sqlite3Savepoint(pParse, SAVEPOINT_ROLLBACK, &X);
}

///////////////////// The CREATE TABLE statement ////////////////////////////

/* CREATE TABLE */
cmd ::= create_table create_table_args.
create_table ::= createkw temp(T) TABLE ifnotexists(E) nm(Y) dbnm(Z) . {
  comdb2CreateTableStart(pParse,&Y,&Z,T,0,0,E);
}

/* CREATE TABLE (Comdb2 - CSC2) */
cmd ::= comdb2_create_table_csc2.
comdb2_create_table_csc2 ::= createkw temp(T) TABLE ifnotexists(E) nm(Y) dbnm(Z) comdb2opt(O) NOSQL(C). {
  comdb2CreateTableCSC2(pParse,&Y,&Z,O,&C,T,E);
}

%type temp {int}
temp(A) ::= TEMP.  { A = 1; }
temp(A) ::= . {A = 0;}

createkw(A) ::= CREATE(A).  {disableLookaside(pParse);}

%type ifnotexists {int}
ifnotexists(A) ::= .              {A = 0;}
ifnotexists(A) ::= IF NOT EXISTS. {A = 1;}

create_table_args ::= LP columnlist conslist_opt(X) RP(E) comdb2opt(O) table_options(F). {
  comdb2CreateTableEnd(pParse,&X,&E,F,O);
}
create_table_args ::= AS select(S). {
  sqlite3EndTable(pParse,0,0,0,S);
  sqlite3SelectDelete(pParse->db, S);
}

%type table_options {int}
table_options(A) ::= .    {A = 0;}
table_options(A) ::= WITHOUT nm(X). {
  if( X.n==5 && sqlite3_strnicmp(X.z,"rowid",5)==0 ){
    A = TF_WithoutRowid | TF_NoVisibleRowid;
  }else{
    A = 0;
    sqlite3ErrorMsg(pParse, "unknown table option: %.*s", X.n, X.z);
  }
}

columnlist ::= columnlist COMMA columnname carglist.
columnlist ::= columnname carglist.
columnname(A) ::= nm(A) typetoken(Y). {comdb2AddColumn(pParse,&A,&Y);}

///////////////////// COMDB2 SEQUENCE arguments ////////////////////////////

%include {
  #include <limits.h>

  typedef struct {
    int type;
    long long data;
  } seq_arg;

  typedef struct {
    /* Basic Attributes */
    long long min_val; /* Values dispensed must be greater than or equal to min_val */
    long long max_val; /* Values dispensed must be less than or equal to max_val */
    long long increment; /* Normal difference between two consecutively dispensed values */
    long long cycle; /* If cycling values is permitted */
    long long start_val; /* Value from START WITH from CREATE*/
    long long restart_val; /* Value from RESTART WITH from ALTER  */
    long long chunk_size; /* Number of values to allocate from llmeta */
    int modified; /* Flags indicating which fields were modified */
  } seq_args;
}

// Rule for long long ints
%type longlong {long long}
longlong(A) ::= ulonglong(A).
longlong(A) ::= MINUS INTEGER(B). {
  // Null terminate string
  char out[B.n + 2];
  memcpy(out+1, B.z, B.n+1);
  out[B.n+1] = '\0';
  out[0] = '-';

  A = strtoll(out, NULL, 10);
}

// Rule for non-negative long long
%type ulonglong {long long}
ulonglong(A) ::= INTEGER(B). {
  // Null terminate string
  char out[B.n + 1];
  memcpy(out, B.z, B.n);
  out[B.n] = '\0';

  A = strtoll(out, NULL, 10);
}

%type sequence_args {seq_args}
sequence_args(A) ::= sequence_arg(B) sequence_args(C). {
  // Copy previous set of options
  A = C;

  if (B.type < 0){
    return;
  }

  switch(B.type) {
    case SEQ_MIN_VAL:
      A.min_val = B.data;
      break;
    case SEQ_MAX_VAL:
      A.max_val = B.data;
      break;
    case SEQ_INC:
      if (B.data < 0){
        // Change default min_val and max_val
        if (!(A.modified & SEQ_MIN_VAL))
          A.min_val = LLONG_MIN;
        if (!(A.modified & SEQ_MAX_VAL))
          A.max_val = -1;
        if (!(A.modified & SEQ_START_VAL))
          A.start_val = A.max_val;
      }
      A.increment = B.data;
      break;
    case SEQ_CYCLE:
      A.cycle = B.data;
      break;
    case SEQ_START_VAL:
      A.start_val = B.data;
      break;
    case SEQ_CHUNK_SIZE:
      A.chunk_size = B.data;
      break;
  }

  // Mark field as modified from default
  A.modified |= B.type;
}
sequence_args(A) ::= . {
  // Defaults
  A.min_val = 1;
  A.max_val = LLONG_MAX;
  A.increment = 1;
  A.cycle = 0;
  A.start_val = A.min_val;
  A.chunk_size = 1;

  A.modified = 0;
}

%type sequence_arg {seq_arg}
sequence_arg(A) ::= sequence_start_with(A).
sequence_arg(A) ::= sequence_increment_by(A).
sequence_arg(A) ::= sequence_min_value(A).
sequence_arg(A) ::= sequence_max_value(A).
sequence_arg(A) ::= sequence_cycle(A).
sequence_arg(A) ::= sequence_chunk(A).

%type sequence_start_with {seq_arg}
sequence_start_with(A) ::= START WITH longlong(B). {
  A.type = SEQ_START_VAL;
  A.data = B;
}
sequence_start_with(A) ::= START longlong(B). {
  A.type = SEQ_START_VAL;
  A.data = B;
}

%type sequence_increment_by {seq_arg}
sequence_increment_by(A) ::= INCREMENT BY longlong(B). {
  A.type = SEQ_INC;
  A.data = B;
}

%type sequence_min_value {seq_arg}
sequence_min_value(A) ::= MINVALUE longlong(B). {
  A.type = SEQ_MIN_VAL;
  A.data = B;
}
sequence_min_value(A) ::= NO MINVALUE. {
  A.type = -1;
}

%type sequence_max_value {seq_arg}
sequence_max_value(A) ::= MAXVALUE longlong(B). {
  A.type = SEQ_MAX_VAL;
  A.data = B;
}
sequence_max_value(A) ::= NO MAXVALUE. {
  A.type = -1;
}

%type sequence_cycle {seq_arg}
sequence_cycle(A) ::= CYCLE. {
  A.type = SEQ_CYCLE;
  A.data = 1;
}
sequence_cycle(A) ::= NO CYCLE. {
  A.type = SEQ_CYCLE;
  A.data = 0;
}

%type sequence_chunk {seq_arg}
sequence_chunk(A) ::= CHUNK ulonglong(B). {
  A.type = SEQ_CHUNK_SIZE ;
  A.data = B;
}
sequence_chunk(A) ::= NO CHUNK. {
  A.type = -1;
}

///////////////////// COMDB2 CREATE SEQUENCE statement ////////////////////////////

cmd ::= createkw SEQUENCE ifnotexists(E) nm(N) sequence_args(A). {
  // Null terminate string
  char name[N.n + 1];
  memcpy(name, N.z, N.n);
  name[N.n] = '\0';

  comdb2CreateSequence(pParse,name,A.min_val,A.max_val,A.increment,A.cycle,A.start_val,A.chunk_size,E);
}

///////////////////// COMDB2 DROP SEQUENCE statement ////////////////////////////

cmd ::= DROP SEQUENCE nm(N). {
  // Null terminate string
  char name[N.n + 1];
  memcpy(name, N.z, N.n);
  name[N.n] = '\0';

  comdb2DropSequence(pParse,name);
}

///////////////////// COMDB2 ALTER SEQUENCE statement ////////////////////////////

cmd ::= dryrun(D) ALTER SEQUENCE nm(N) alter_sequence_args(A). {
  // Null terminate string
  char name[N.n + 1];
  memcpy(name, N.z, N.n);
  name[N.n] = '\0';

  // TODO: Figure out everything with restart value
  // comdb2AlterSequence(pParse,name,A.min_val,A.max_val,A.increment,A.cycle,A.start_val,A.chunk_size,A.modified,D);
}

%type sequence_restart_with {seq_arg}
sequence_restart_with(A) ::= RESTART WITH longlong(B). {
  A.type = SEQ_RESTART_VAL;
  A.data = B;
}
sequence_restart_with(A) ::= RESTART longlong(B). {
  A.type = SEQ_RESTART_VAL;
  A.data = B;
}
sequence_restart_with(A) ::= RESTART. {
  A.type = SEQ_RESTART_TO_START_VAL;
}

%type alter_sequence_arg {seq_arg}
alter_sequence_arg(A) ::= sequence_arg(A). // All other sequence options
alter_sequence_arg(A) ::= sequence_restart_with(A).


// Alter sequence adds RESTART option
%type alter_sequence_args {seq_args}
alter_sequence_args(A) ::= alter_sequence_arg(B) alter_sequence_args(C). {
  // Copy previous set of options
  A = C;

  if (B.type < 0){
    return;
  }

  switch(B.type) {
    case SEQ_MIN_VAL:
      A.min_val = B.data;
      break;
    case SEQ_MAX_VAL:
      A.max_val = B.data;
      break;
    case SEQ_INC:
      A.increment = B.data;
      break;
    case SEQ_CYCLE:
      A.cycle = B.data;
      break;
    case SEQ_START_VAL:
      A.start_val = B.data;
      break;
    case SEQ_CHUNK_SIZE:
      A.chunk_size = B.data;
      break;
    case SEQ_RESTART_VAL:
      A.restart_val = B.data;
  }

  // Mark field as modified from default
  A.modified |= B.type;
}
alter_sequence_args(A) ::= . {
  A.modified = 0;
}

// Define operator precedence early so that this is the first occurrence
// of the operator tokens in the grammer.  Keeping the operators together
// causes them to be assigned integer values that are close together,
// which keeps parser tables smaller.
//
// The token values assigned to these symbols is determined by the order
// in which lemon first sees them.  It must be the case that ISNULL/NOTNULL,
// NE/EQ, GT/LE, and GE/LT are separated by only a single value.  See
// the sqlite3ExprIfFalse() routine for additional information on this
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
//
// COMDB2 KEYWORDS  
//
  ADD AGGREGATE ALIAS AUTHENTICATION BLOBFIELD BULKIMPORT CHUNK
  COMMITSLEEP CONSUMER CONVERTSLEEP COVERAGE CRLE CYCLE DATA DATABLOB
  DATACOPY DBPAD DDL DEFERRABLE DISABLE DRYRUN ENABLE FOR FOREIGN FUNCTION
  GENID48 GET GRANT INCREMENT IPU ISC KW LUA LZ4 MAXVALUE MINVALUE NONE ODH
  OFF OP OPTIONS PARTITION PASSWORD PERIOD PROCEDURE PUT READ REBUILD REC
  REFERENCES RESERVED RESTART RETENTION REVOKE RLE ROWLOCKS SCALAR SCHEMACHANGE
  SEQUENCE SKIPSCAN START SUMMARIZE THREADS THRESHOLD TIME TRUNCATE TUNABLE
  USERSCHEMA VERSION WRITE ZLIB .
%wildcard ANY.


// And "ids" is an identifer-or-string.
//
%token_class ids  ID|STRING.

// The name of a column or table can be any of the following:
//
%type nm {Token}
nm(A) ::= id(A).
nm(A) ::= STRING(A).
nm(A) ::= JOIN_KW(A).

// A typetoken is really zero or more tokens that form a type name such
// as can be found after the column name in a CREATE TABLE statement.
// Multiple tokens are concatenated to form the value of the typetoken.
//
%type typetoken {Token}
typetoken(A) ::= .   {A.n = 0; A.z = 0;}
typetoken(A) ::= typename(A).
typetoken(A) ::= typename(A) LP signed RP(Y). {
  A.n = (int)(&Y.z[Y.n] - A.z);
}
/*
typetoken(A) ::= typename(A) LP signed COMMA signed RP(Y). {
  A.n = (int)(&Y.z[Y.n] - A.z);
}
*/
%type typename {Token}
typename(A) ::= ids(A).
typename(A) ::= typename(A) ids(Y). {A.n=Y.n+(int)(Y.z-A.z);}
signed ::= plus_num.
signed ::= minus_num.

// "carglist" is a list of additional constraints that come after the
// column name and column type in a CREATE|ALTER TABLE statement.
//
carglist ::= carglist ccons.
carglist ::= .
ccons ::= CONSTRAINT nm(X).           {pParse->constraintName = X;}
ccons ::= DEFAULT term(X).            {comdb2AddDefaultValue(pParse,&X);}
ccons ::= DEFAULT LP expr(X) RP.      {comdb2AddDefaultValue(pParse,&X);}
ccons ::= DEFAULT PLUS term(X).       {comdb2AddDefaultValue(pParse,&X);}
ccons ::= DEFAULT MINUS(A) term(X).      {
  ExprSpan v;
  v.pExpr = sqlite3PExpr(pParse, TK_UMINUS, X.pExpr, 0, 0);
  v.zStart = A.z;
  v.zEnd = X.zEnd;
  comdb2AddDefaultValue(pParse,&v);
}
ccons ::= DEFAULT id(X).              {
  ExprSpan v;
  spanExpr(&v, pParse, TK_STRING, X);
  comdb2AddDefaultValue(pParse,&v);
}

// In addition to the type name, we also care about the primary key and
// UNIQUE constraints.
//
ccons ::= NULL onconf.           {comdb2AddNull(pParse);}
ccons ::= NOT NULL onconf(R).    {comdb2AddNotNull(pParse, R);}
ccons ::= PRIMARY KEY sortorder(Z) onconf(R) autoinc(I).
                                 {comdb2AddPrimaryKey(pParse,0,R,I,Z);}
ccons ::= UNIQUE onconf(R).      {comdb2AddIndex(pParse,0,R,
                                   SQLITE_IDXTYPE_UNIQUE);}
ccons ::= CHECK LP expr(X) RP.   {sqlite3AddCheckConstraint(pParse,X.pExpr);}
ccons ::= REFERENCES nm(T) eidlist_opt(TA) refargs(R).
                                 {comdb2CreateForeignKey(pParse,0,&T,TA,R);}
ccons ::= defer_subclause(D).    {sqlite3DeferForeignKey(pParse,D);}
ccons ::= COLLATE ids(C).        {sqlite3AddCollateType(pParse, &C);}
ccons ::= WITH DBPAD EQ INTEGER(X). {
    int tmp;
    if (!readIntFromToken(&X, &tmp))
        tmp = 0;
    comdb2AddDbpad(pParse, tmp);
}

// The optional AUTOINCREMENT keyword
// COMDB2: Use SEQUENCE
// TODO: Modify for sequences
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
refargs(A) ::= refargs(A) refarg(Y). { A = (A & ~Y.mask) | Y.value; }
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
conslist_opt(A) ::= COMMA(A) conslist.
conslist ::= conslist tconscomma tcons.
conslist ::= tcons.
tconscomma ::= COMMA.            {pParse->constraintName.n = 0;}
tconscomma ::= .
tcons ::= CONSTRAINT nm(X).      {pParse->constraintName = X;}
tcons ::= PRIMARY KEY LP sortlist(X) autoinc(I) RP onconf(R).
                                 {comdb2AddPrimaryKey(pParse,X,R,I,0);}
tcons ::= UNIQUE LP sortlist(X) RP onconf(R).
                                 {comdb2AddIndex(pParse,X,R,
                                   SQLITE_IDXTYPE_UNIQUE);}
tcons ::= CHECK LP expr(E) RP onconf.
                                 {sqlite3AddCheckConstraint(pParse,E.pExpr);}
tcons ::= FOREIGN KEY LP eidlist(FA) RP
          REFERENCES nm(T) eidlist_opt(TA) refargs(R) defer_subclause_opt(D). {
    comdb2CreateForeignKey(pParse, FA, &T, TA, R);
    comdb2DeferForeignKey(pParse, D);
}
%type defer_subclause_opt {int}
defer_subclause_opt(A) ::= .                    {A = 0;}
defer_subclause_opt(A) ::= defer_subclause(A).

// The following is a non-standard extension that allows us to declare the
// default behavior when there is a constraint conflict.
//
%type onconf {int}
%type orconf {int}
%type resolvetype {int}
onconf(A) ::= .                              {A = OE_Default;}
onconf(A) ::= ON CONFLICT resolvetype(X).    {A = X;}
orconf(A) ::= .                              {A = OE_Default;}
orconf(A) ::= OR resolvetype(X).             {A = X;}
resolvetype(A) ::= raisetype(A).
resolvetype(A) ::= IGNORE.                   {A = OE_Ignore;}
/* COMDB2 MODIFICATION
 * insert or replace logic not supported
 * resolvetype(A) ::= REPLACE.                  {A = OE_Replace;}
 */

////////////////////////// The DROP TABLE /////////////////////////////////////
//
cmd ::= DROP TABLE ifexists(E) fullname(X). {
  sqlite3DropTable(pParse, X, 0, E);
}
%type ifexists {int}
ifexists(A) ::= IF EXISTS.   {A = 1;}
ifexists(A) ::= .            {A = 0;}

///////////////////// The CREATE VIEW statement /////////////////////////////
//
%ifndef SQLITE_OMIT_VIEW
cmd ::= createkw(X) temp(T) VIEW ifnotexists(E) nm(Y) dbnm(Z) eidlist_opt(C)
          AS select(S). {
  sqlite3CreateView(pParse, &X, &Y, &Z, C, S, T, E);
}
cmd ::= DROP VIEW ifexists(E) fullname(X). {
  sqlite3DropTable(pParse, X, 1, E);
}
%endif  SQLITE_OMIT_VIEW

//////////////////////// The SELECT statement /////////////////////////////////
//
cmd ::= select(X).  {
  SelectDest dest = {SRT_Output, 0, 0, 0, 0, 0};
  sqlite3FingerprintSelect(pParse->db, X);
  sqlite3Select(pParse, X, &dest);
  sqlite3SelectDelete(pParse->db, X);
}

%type select {Select*}
%destructor select {sqlite3SelectDelete(pParse->db, $$);}
%type selectnowith {Select*}
%destructor selectnowith {sqlite3SelectDelete(pParse->db, $$);}
%type oneselect {Select*}
%destructor oneselect {sqlite3SelectDelete(pParse->db, $$);}

%include {
  /*
  ** For a compound SELECT statement, make sure p->pPrior->pNext==p for
  ** all elements in the list.  And make sure list length does not exceed
  ** SQLITE_LIMIT_COMPOUND_SELECT.
  */
  static void parserDoubleLinkSelect(Parse *pParse, Select *p){
    if( p->pPrior ){
      Select *pNext = 0, *pLoop;
      int mxSelect, cnt = 0;
      for(pLoop=p; pLoop; pNext=pLoop, pLoop=pLoop->pPrior, cnt++){
        pLoop->pNext = pNext;
        pLoop->selFlags |= SF_Compound;
      }
      if( (p->selFlags & SF_MultiValue)==0 &&
        (mxSelect = pParse->db->aLimit[SQLITE_LIMIT_COMPOUND_SELECT])>0 &&
        cnt>mxSelect
      ){
        sqlite3ErrorMsg(pParse, "too many terms in compound SELECT");
      }
    }
  }
}

select(A) ::= with(W) selectnowith(X). {
  Select *p = X;
  if( p ){
    p->pWith = W;
    parserDoubleLinkSelect(pParse, p);
  }else{
    sqlite3WithDelete(pParse->db, W);
  }
  A = p; /*A-overwrites-W*/
}

selectnowith(A) ::= oneselect(A).
%ifndef SQLITE_OMIT_COMPOUND_SELECT
selectnowith(A) ::= selectnowith(A) multiselect_op(Y) oneselect(Z).  {
  Select *pRhs = Z;
  Select *pLhs = A;
  if( pRhs && pRhs->pPrior ){
    SrcList *pFrom;
    Token x;
    x.n = 0;
    parserDoubleLinkSelect(pParse, pRhs);
    pFrom = sqlite3SrcListAppendFromTerm(pParse,0,0,0,&x,pRhs,0,0);
    pRhs = sqlite3SelectNew(pParse,0,pFrom,0,0,0,0,0,0,0);
  }
  if( pRhs ){
    pRhs->op = (u8)Y;
    pRhs->pPrior = pLhs;
    if( ALWAYS(pLhs) ) pLhs->selFlags &= ~SF_MultiValue;
    pRhs->selFlags &= ~SF_MultiValue;
    if( Y!=TK_ALL ) pParse->hasCompound = 1;
  }else{
    sqlite3SelectDelete(pParse->db, pLhs);
  }
  A = pRhs;
}
%type multiselect_op {int}
multiselect_op(A) ::= UNION(OP).             {A = @OP; /*A-overwrites-OP*/}
multiselect_op(A) ::= UNION ALL.             {A = TK_ALL;}
multiselect_op(A) ::= EXCEPT|INTERSECT(OP).  {A = @OP; /*A-overwrites-OP*/}
%endif SQLITE_OMIT_COMPOUND_SELECT
oneselect(A) ::= SELECT(S) distinct(D) selcollist(W) from(X) where_opt(Y)
                 groupby_opt(P) having_opt(Q) orderby_opt(Z) limit_opt(L). {
#if SELECTTRACE_ENABLED
  Token s = S; /*A-overwrites-S*/
#endif
  A = sqlite3SelectNew(pParse,W,X,Y.pExpr,P,Q,Z,D,L.pLimit,L.pOffset);
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
    const char *z = s.z+6;
    int i;
    sqlite3_snprintf(sizeof(A->zSelName), A->zSelName, "#%d",
                     ++pParse->nSelect);
    while( z[0]==' ' ) z++;
    if( z[0]=='/' && z[1]=='*' ){
      z += 2;
      while( z[0]==' ' ) z++;
      for(i=0; sqlite3Isalnum(z[i]); i++){}
      sqlite3_snprintf(sizeof(A->zSelName), A->zSelName, "%.*s", i, z);
    }
  }
#endif /* SELECTRACE_ENABLED */
}
oneselect(A) ::= values(A).

%type values {Select*}
%destructor values {sqlite3SelectDelete(pParse->db, $$);}
values(A) ::= VALUES LP nexprlist(X) RP. {
  A = sqlite3SelectNew(pParse,X,0,0,0,0,0,SF_Values,0,0);
}
values(A) ::= values(A) COMMA LP exprlist(Y) RP. {
  Select *pRight, *pLeft = A;
  pRight = sqlite3SelectNew(pParse,Y,0,0,0,0,0,SF_Values|SF_MultiValue,0,0);
  if( ALWAYS(pLeft) ) pLeft->selFlags &= ~SF_MultiValue;
  if( pRight ){
    pRight->op = TK_ALL;
    pRight->pPrior = pLeft;
    A = pRight;
  }else{
    A = pLeft;
  }
}

/* COMDB2 MODIFICATION
 * add the SELECTV instruction */
oneselect(A) ::= SELECTV distinct(D) selcollist(W) from(X) where_opt(Y)
                 groupby_opt(P) having_opt(Q) orderby_opt(Z) limit_opt(L). {
  A = sqlite3SelectNew(pParse,W,X,Y.pExpr,P,Q,Z,D,L.pLimit,L.pOffset);
  A->op = TK_SELECTV;
  A->recording = 1;
}


// The "distinct" nonterminal is true (1) if the DISTINCT keyword is
// present and false (0) if it is not.
//
%type distinct {int}
distinct(A) ::= DISTINCT.   {A = SF_Distinct;}
distinct(A) ::= ALL.        {A = SF_All;}
distinct(A) ::= .           {A = 0;}

// selcollist is a list of expressions that are to become the return
// values of the SELECT statement.  The "*" in statements like
// "SELECT * FROM ..." is encoded as a special expression with an
// opcode of TK_ASTERISK.
//
%type selcollist {ExprList*}
%destructor selcollist {sqlite3ExprListDelete(pParse->db, $$);}
%type sclp {ExprList*}
%destructor sclp {sqlite3ExprListDelete(pParse->db, $$);}
sclp(A) ::= selcollist(A) COMMA.
sclp(A) ::= .                                {A = 0;}
selcollist(A) ::= sclp(A) expr(X) as(Y).     {
   A = sqlite3ExprListAppend(pParse, A, X.pExpr);
   if( Y.n>0 ) sqlite3ExprListSetName(pParse, A, &Y, 1);
   sqlite3ExprListSetSpan(pParse,A,&X);
}
selcollist(A) ::= sclp(A) STAR. {
  Expr *p = sqlite3Expr(pParse->db, TK_ASTERISK, 0);
  A = sqlite3ExprListAppend(pParse, A, p);
}
selcollist(A) ::= sclp(A) nm(X) DOT STAR. {
  Expr *pRight = sqlite3PExpr(pParse, TK_ASTERISK, 0, 0, 0);
  Expr *pLeft = sqlite3PExpr(pParse, TK_ID, 0, 0, &X);
  Expr *pDot = sqlite3PExpr(pParse, TK_DOT, pLeft, pRight, 0);
  A = sqlite3ExprListAppend(pParse,A, pDot);
}

// An option "AS <id>" phrase that can follow one of the expressions that
// define the result set, or one of the tables in the FROM clause.
//
%type as {Token}
as(X) ::= AS nm(Y).    {X = Y;}
as(X) ::= ids(X).
as(X) ::= .            {X.n = 0; X.z = 0;}


%type seltablist {SrcList*}
%destructor seltablist {sqlite3SrcListDelete(pParse->db, $$);}
%type stl_prefix {SrcList*}
%destructor stl_prefix {sqlite3SrcListDelete(pParse->db, $$);}
%type from {SrcList*}
%destructor from {sqlite3SrcListDelete(pParse->db, $$);}

// A complete FROM clause.
//
from(A) ::= .                {A = sqlite3DbMallocZero(pParse->db, sizeof(*A));}
from(A) ::= FROM seltablist(X). {
  A = X;
  sqlite3SrcListShiftJoinType(A);
}

// "seltablist" is a "Select Table List" - the content of the FROM clause
// in a SELECT statement.  "stl_prefix" is a prefix of this list.
//
stl_prefix(A) ::= seltablist(A) joinop(Y).    {
   if( ALWAYS(A && A->nSrc>0) ) A->a[A->nSrc-1].fg.jointype = (u8)Y;
}
stl_prefix(A) ::= .                           {A = 0;}
seltablist(A) ::= stl_prefix(A) nm(Y) dbnm(D) as(Z) indexed_opt(I)
                  on_opt(N) using_opt(U). {
  A = sqlite3SrcListAppendFromTerm(pParse,A,&Y,&D,&Z,0,N,U);
  sqlite3SrcListIndexedBy(pParse, A, &I);
}
seltablist(A) ::= stl_prefix(A) nm(Y) dbnm(D) LP exprlist(E) RP as(Z)
                  on_opt(N) using_opt(U). {
  A = sqlite3SrcListAppendFromTerm(pParse,A,&Y,&D,&Z,0,N,U);
  sqlite3SrcListFuncArgs(pParse, A, E);
}
%ifndef SQLITE_OMIT_SUBQUERY
  seltablist(A) ::= stl_prefix(A) LP select(S) RP
                    as(Z) on_opt(N) using_opt(U). {
    A = sqlite3SrcListAppendFromTerm(pParse,A,0,0,&Z,S,N,U);
  }
  seltablist(A) ::= stl_prefix(A) LP seltablist(F) RP
                    as(Z) on_opt(N) using_opt(U). {
    if( A==0 && Z.n==0 && N==0 && U==0 ){
      A = F;
    }else if( F->nSrc==1 ){
      A = sqlite3SrcListAppendFromTerm(pParse,A,0,0,&Z,0,N,U);
      if( A ){
        struct SrcList_item *pNew = &A->a[A->nSrc-1];
        struct SrcList_item *pOld = F->a;
        pNew->zName = pOld->zName;
        pNew->zDatabase = pOld->zDatabase;
        pNew->pSelect = pOld->pSelect;
        pOld->zName = pOld->zDatabase = 0;
        pOld->pSelect = 0;
      }
      sqlite3SrcListDelete(pParse->db, F);
    }else{
      Select *pSubquery;
      sqlite3SrcListShiftJoinType(F);
      pSubquery = sqlite3SelectNew(pParse,0,F,0,0,0,0,SF_NestedFrom,0,0);
      A = sqlite3SrcListAppendFromTerm(pParse,A,0,0,&Z,pSubquery,N,U);
    }
  }
%endif  SQLITE_OMIT_SUBQUERY

%type dbnm {Token}
dbnm(A) ::= .          {A.z=0; A.n=0;}
dbnm(A) ::= DOT nm(X). {A = X;}

%type fullname {SrcList*}
%destructor fullname {sqlite3SrcListDelete(pParse->db, $$);}
fullname(A) ::= nm(X) dbnm(Y).
   {A = sqlite3SrcListAppend(pParse->db,0,&X,&Y); /*A-overwrites-X*/}

%type joinop {int}
joinop(X) ::= COMMA|JOIN.              { X = JT_INNER; }
joinop(X) ::= JOIN_KW(A) JOIN.
                  {X = sqlite3JoinType(pParse,&A,0,0);  /*X-overwrites-A*/}
joinop(X) ::= JOIN_KW(A) nm(B) JOIN.
                  {X = sqlite3JoinType(pParse,&A,&B,0); /*X-overwrites-A*/}
joinop(X) ::= JOIN_KW(A) nm(B) nm(C) JOIN.
                  {X = sqlite3JoinType(pParse,&A,&B,&C);/*X-overwrites-A*/}

%type on_opt {Expr*}
%destructor on_opt {sqlite3ExprDelete(pParse->db, $$);}
on_opt(N) ::= ON expr(E).   {N = E.pExpr;}
on_opt(N) ::= .             {N = 0;}

// Note that this block abuses the Token type just a little. If there is
// no "INDEXED BY" clause, the returned token is empty (z==0 && n==0). If
// there is an INDEXED BY clause, then the token is populated as per normal,
// with z pointing to the token data and n containing the number of bytes
// in the token.
//
// If there is a "NOT INDEXED" clause, then (z==0 && n==1), which is
// normally illegal. The sqlite3SrcListIndexedBy() function
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
%destructor using_opt {sqlite3IdListDelete(pParse->db, $$);}
using_opt(U) ::= USING LP idlist(L) RP.  {U = L;}
using_opt(U) ::= .                        {U = 0;}


%type orderby_opt {ExprList*}
%destructor orderby_opt {sqlite3ExprListDelete(pParse->db, $$);}

// the sortlist non-terminal stores a list of expression where each
// expression is optionally followed by ASC or DESC to indicate the
// sort order.
//
%type sortlist {ExprList*}
%destructor sortlist {sqlite3ExprListDelete(pParse->db, $$);}

orderby_opt(A) ::= .                          {A = 0;}
orderby_opt(A) ::= ORDER BY sortlist(X).      {A = X;}
sortlist(A) ::= sortlist(A) COMMA expr(Y) sortorder(Z). {
  A = sqlite3ExprListAppend(pParse,A,Y.pExpr);
  sqlite3ExprListSetSortOrder(A,Z);
}
sortlist(A) ::= expr(Y) sortorder(Z). {
  A = sqlite3ExprListAppend(pParse,0,Y.pExpr); /*A-overwrites-Y*/
  sqlite3ExprListSetSortOrder(A,Z);
}

%type sortorder {int}

sortorder(A) ::= ASC.           {A = SQLITE_SO_ASC;}
sortorder(A) ::= DESC.          {A = SQLITE_SO_DESC;}
sortorder(A) ::= .              {A = SQLITE_SO_UNDEFINED;}

%type groupby_opt {ExprList*}
%destructor groupby_opt {sqlite3ExprListDelete(pParse->db, $$);}
groupby_opt(A) ::= .                      {A = 0;}
groupby_opt(A) ::= GROUP BY nexprlist(X). {A = X;}

%type having_opt {Expr*}
%destructor having_opt {sqlite3ExprDelete(pParse->db, $$);}
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
//  sqlite3ExprDelete(pParse->db, $$.pLimit);
//  sqlite3ExprDelete(pParse->db, $$.pOffset);
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
  sqlite3WithPush(pParse, C, 1);
  sqlite3SrcListIndexedBy(pParse, X, &I);
  W.pExpr = sqlite3LimitWhere(pParse, X, W.pExpr, O, L.pLimit, L.pOffset, "DELETE");
  sqlite3FingerprintDelete(pParse->db, X, W.pExpr);
  sqlite3DeleteFrom(pParse,X,W.pExpr);
}
%endif
%ifndef SQLITE_ENABLE_UPDATE_DELETE_LIMIT
cmd ::= with(C) DELETE FROM fullname(X) indexed_opt(I) where_opt(W). {
  sqlite3WithPush(pParse, C, 1);
  sqlite3SrcListIndexedBy(pParse, X, &I);
  sqlite3FingerprintDelete(pParse->db, X, W.pExpr);
  sqlite3DeleteFrom(pParse,X,W.pExpr);
}
%endif

%type where_opt {ExprSpan}
%destructor where_opt {sqlite3ExprDelete(pParse->db, $$.pExpr);}

where_opt(A) ::= .                    {A.pExpr = 0;}
where_opt(A) ::= WHERE expr(X).       {A = X;}

////////////////////////// The UPDATE command ////////////////////////////////
//
%ifdef SQLITE_ENABLE_UPDATE_DELETE_LIMIT
cmd ::= with(C) UPDATE orconf(R) fullname(X) indexed_opt(I) SET setlist(Y)
        where_opt(W) orderby_opt(O) limit_opt(L).  {
  sqlite3WithPush(pParse, C, 1);
  sqlite3SrcListIndexedBy(pParse, X, &I);
  sqlite3ExprListCheckLength(pParse,Y,"set list");
  W.pExpr = sqlite3LimitWhere(pParse, X, W.pExpr, O, L.pLimit, L.pOffset, "UPDATE");
  sqlite3FingerprintUpdate(pParse->db, X, Y, W.pExpr, R);
  sqlite3Update(pParse,X,Y,W.pExpr,R);
}
%endif
%ifndef SQLITE_ENABLE_UPDATE_DELETE_LIMIT
cmd ::= with(C) UPDATE orconf(R) fullname(X) indexed_opt(I) SET setlist(Y)
        where_opt(W).  {
  sqlite3WithPush(pParse, C, 1);
  sqlite3SrcListIndexedBy(pParse, X, &I);
  sqlite3ExprListCheckLength(pParse,Y,"set list");
  sqlite3FingerprintUpdate(pParse->db, X, Y, W.pExpr, R);
  sqlite3Update(pParse,X,Y,W.pExpr,R);
}
%endif

%type setlist {ExprList*}
%destructor setlist {sqlite3ExprListDelete(pParse->db, $$);}

setlist(A) ::= setlist(A) COMMA nm(X) EQ expr(Y). {
  A = sqlite3ExprListAppend(pParse, A, Y.pExpr);
  sqlite3ExprListSetName(pParse, A, &X, 1);
}
setlist(A) ::= setlist(A) COMMA LP idlist(X) RP EQ expr(Y). {
  A = sqlite3ExprListAppendVector(pParse, A, X, Y.pExpr);
}
setlist(A) ::= nm(X) EQ expr(Y). {
  A = sqlite3ExprListAppend(pParse, 0, Y.pExpr);
  sqlite3ExprListSetName(pParse, A, &X, 1);
}
setlist(A) ::= LP idlist(X) RP EQ expr(Y). {
  A = sqlite3ExprListAppendVector(pParse, 0, X, Y.pExpr);
}

////////////////////////// The INSERT command /////////////////////////////////
//
cmd ::= with(W) insert_cmd(R) INTO fullname(X) idlist_opt(F) select(S). {
  sqlite3WithPush(pParse, W, 1);
  sqlite3FingerprintInsert(pParse->db, X, S, F, W);
  sqlite3Insert(pParse, X, S, F, R);
}
cmd ::= with(W) insert_cmd(R) INTO fullname(X) idlist_opt(F) DEFAULT VALUES.
{
  sqlite3WithPush(pParse, W, 1);
  sqlite3FingerprintInsert(pParse->db, X, NULL, F, W);
  sqlite3Insert(pParse, X, NULL, F, R);
}

%type insert_cmd {int}
insert_cmd(A) ::= INSERT orconf(R).   {A = R;}
/* COMDB2 MODIFICATION
 * insert or replace logic not supported
 * insert_cmd(A) ::= REPLACE.            {A = OE_Replace;}
 */

%type idlist_opt {IdList*}
%destructor idlist_opt {sqlite3IdListDelete(pParse->db, $$);}
%type idlist {IdList*}
%destructor idlist {sqlite3IdListDelete(pParse->db, $$);}

idlist_opt(A) ::= .                       {A = 0;}
idlist_opt(A) ::= LP idlist(X) RP.    {A = X;}
idlist(A) ::= idlist(A) COMMA nm(Y).
    {A = sqlite3IdListAppend(pParse->db,A,&Y);}
idlist(A) ::= nm(Y).
    {A = sqlite3IdListAppend(pParse->db,0,&Y); /*A-overwrites-Y*/}

/////////////////////////// Expression Processing /////////////////////////////
//

%type expr {ExprSpan}
%destructor expr {sqlite3ExprDelete(pParse->db, $$.pExpr);}
%type term {ExprSpan}
%destructor term {sqlite3ExprDelete(pParse->db, $$.pExpr);}

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
  static void spanExpr(ExprSpan *pOut, Parse *pParse, int op, Token t){
    Expr *p = sqlite3DbMallocRawNN(pParse->db, sizeof(Expr)+t.n+1);
    if( p ){
      memset(p, 0, sizeof(Expr));
      p->op = (u8)op;
      p->flags = EP_Leaf;
      p->iAgg = -1;
      p->u.zToken = (char*)&p[1];
      memcpy(p->u.zToken, t.z, t.n);
      p->u.zToken[t.n] = 0;
      if( sqlite3Isquote(p->u.zToken[0]) ){
        if( p->u.zToken[0]=='"' ) p->flags |= EP_DblQuoted;
        sqlite3Dequote(p->u.zToken);
      }
#if SQLITE_MAX_EXPR_DEPTH>0
      p->nHeight = 1;
#endif
    }
    pOut->pExpr = p;
    pOut->zStart = t.z;
    pOut->zEnd = &t.z[t.n];
  }
}

expr(A) ::= term(A).
expr(A) ::= LP(B) expr(X) RP(E).
            {spanSet(&A,&B,&E); /*A-overwrites-B*/  A.pExpr = X.pExpr;}
term(A) ::= NULL(X).        {spanExpr(&A,pParse,@X,X);/*A-overwrites-X*/}
expr(A) ::= id(X).          {spanExpr(&A,pParse,TK_ID,X); /*A-overwrites-X*/}
expr(A) ::= JOIN_KW(X).     {spanExpr(&A,pParse,TK_ID,X); /*A-overwrites-X*/}
expr(A) ::= nm(X) DOT nm(Y). {
  Expr *temp1 = sqlite3ExprAlloc(pParse->db, TK_ID, &X, 1);
  Expr *temp2 = sqlite3ExprAlloc(pParse->db, TK_ID, &Y, 1);
  spanSet(&A,&X,&Y); /*A-overwrites-X*/
  A.pExpr = sqlite3PExpr(pParse, TK_DOT, temp1, temp2, 0);
}
expr(A) ::= nm(X) DOT nm(Y) DOT nm(Z). {
  Expr *temp1 = sqlite3ExprAlloc(pParse->db, TK_ID, &X, 1);
  Expr *temp2 = sqlite3ExprAlloc(pParse->db, TK_ID, &Y, 1);
  Expr *temp3 = sqlite3ExprAlloc(pParse->db, TK_ID, &Z, 1);
  Expr *temp4 = sqlite3PExpr(pParse, TK_DOT, temp2, temp3, 0);
  spanSet(&A,&X,&Z); /*A-overwrites-X*/
  A.pExpr = sqlite3PExpr(pParse, TK_DOT, temp1, temp4, 0);
}
term(A) ::= FLOAT|BLOB(X). {spanExpr(&A,pParse,@X,X);/*A-overwrites-X*/}
term(A) ::= STRING(X).     {spanExpr(&A,pParse,@X,X);/*A-overwrites-X*/}
term(A) ::= INTEGER(X). {
  A.pExpr = sqlite3ExprAlloc(pParse->db, TK_INTEGER, &X, 1);
  A.zStart = X.z;
  A.zEnd = X.z + X.n;
  if( A.pExpr ) A.pExpr->flags |= EP_Leaf;
}
expr(A) ::= VARIABLE(X).     {
  if( !(X.z[0]=='#' && sqlite3Isdigit(X.z[1])) ){
    u32 n = X.n;
    spanExpr(&A, pParse, TK_VARIABLE, X);
    sqlite3ExprAssignVarNumber(pParse, A.pExpr, n);
  }else{
    /* When doing a nested parse, one can include terms in an expression
    ** that look like this:   #1 #2 ...  These terms refer to registers
    ** in the virtual machine.  #N is the N-th register. */
    Token t = X; /*A-overwrites-X*/
    assert( t.n>=2 );
    spanSet(&A, &t, &t);
    if( pParse->nested==0 ){
      sqlite3ErrorMsg(pParse, "near \"%T\": syntax error", &t);
      A.pExpr = 0;
    }else{
      A.pExpr = sqlite3PExpr(pParse, TK_REGISTER, 0, 0, 0);
      if( A.pExpr ) sqlite3GetInt32(&t.z[1], &A.pExpr->iTable);
    }
  }
}
expr(A) ::= expr(A) COLLATE ids(C). {
  A.pExpr = sqlite3ExprAddCollateToken(pParse, A.pExpr, &C, 1);
  A.zEnd = &C.z[C.n];
}

expr(A) ::= expr(A) COLLATE DATACOPY. {
  if (pParse->db->init.busy == 0) {
      sqlite3ErrorMsg(pParse, "Support for 'COLLATE DATACOPY' syntax "
                      "has been removed; use WITH DATACOPY.");
      pParse->rc = SQLITE_ERROR;
  } else {
      Token t = {"DATACOPY", 8};
      A.pExpr = sqlite3ExprAddCollateToken(pParse, A.pExpr, &t, 1);
      A.zEnd = &t.z[t.n];
  }
}

%ifndef SQLITE_OMIT_CAST
expr(A) ::= CAST(X) LP expr(E) AS typetoken(T) RP(Y). {
  spanSet(&A,&X,&Y); /*A-overwrites-X*/
  A.pExpr = sqlite3PExpr(pParse, TK_CAST, E.pExpr, 0, &T);
}
%endif  SQLITE_OMIT_CAST
expr(A) ::= id(X) LP distinct(D) exprlist(Y) RP(E). {
  if( Y && Y->nExpr>pParse->db->aLimit[SQLITE_LIMIT_FUNCTION_ARG] ){
    sqlite3ErrorMsg(pParse, "too many arguments on function %T", &X);
  }
  A.pExpr = sqlite3ExprFunction(pParse, Y, &X);
  spanSet(&A,&X,&E);
  if( D==SF_Distinct && A.pExpr ){
    A.pExpr->flags |= EP_Distinct;
  }
}
expr(A) ::= id(X) LP STAR RP(E). {
  A.pExpr = sqlite3ExprFunction(pParse, 0, &X);
  spanSet(&A,&X,&E);
}
term(A) ::= CTIME_KW(OP). {
  A.pExpr = sqlite3ExprFunction(pParse, 0, &OP);
  spanSet(&A, &OP, &OP);
}

%include {
  /* This routine constructs a binary expression node out of two ExprSpan
  ** objects and uses the result to populate a new ExprSpan object.
  */
  static void spanBinaryExpr(
    Parse *pParse,      /* The parsing context.  Errors accumulate here */
    int op,             /* The binary operation */
    ExprSpan *pLeft,    /* The left operand, and output */
    ExprSpan *pRight    /* The right operand */
  ){
    pLeft->pExpr = sqlite3PExpr(pParse, op, pLeft->pExpr, pRight->pExpr, 0);
    pLeft->zEnd = pRight->zEnd;
  }

  /* If doNot is true, then add a TK_NOT Expr-node wrapper around the
  ** outside of *ppExpr.
  */
  static void exprNot(Parse *pParse, int doNot, ExprSpan *pSpan){
    if( doNot ){
      pSpan->pExpr = sqlite3PExpr(pParse, TK_NOT, pSpan->pExpr, 0, 0);
    }
  }
}

expr(A) ::= LP(L) nexprlist(X) COMMA expr(Y) RP(R). {
  ExprList *pList = sqlite3ExprListAppend(pParse, X, Y.pExpr);
  A.pExpr = sqlite3PExpr(pParse, TK_VECTOR, 0, 0, 0);
  if( A.pExpr ){
    A.pExpr->x.pList = pList;
    spanSet(&A, &L, &R);
  }else{
    sqlite3ExprListDelete(pParse->db, pList);
  }
}

expr(A) ::= expr(A) AND(OP) expr(Y).    {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) OR(OP) expr(Y).     {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) LT|GT|GE|LE(OP) expr(Y).
                                        {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) EQ|NE(OP) expr(Y).  {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) BITAND|BITOR|LSHIFT|RSHIFT(OP) expr(Y).
                                        {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) PLUS|MINUS(OP) expr(Y).
                                        {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) STAR|SLASH|REM(OP) expr(Y).
                                        {spanBinaryExpr(pParse,@OP,&A,&Y);}
expr(A) ::= expr(A) CONCAT(OP) expr(Y). {spanBinaryExpr(pParse,@OP,&A,&Y);}
%type likeop {Token}
likeop(A) ::= LIKE_KW|MATCH(X).     {A=X;/*A-overwrites-X*/}
likeop(A) ::= NOT LIKE_KW|MATCH(X). {A=X; A.n|=0x80000000; /*A-overwrite-X*/}
expr(A) ::= expr(A) likeop(OP) expr(Y).  [LIKE_KW]  {
  ExprList *pList;
  int bNot = OP.n & 0x80000000;
  OP.n &= 0x7fffffff;
  pList = sqlite3ExprListAppend(pParse,0, Y.pExpr);
  pList = sqlite3ExprListAppend(pParse,pList, A.pExpr);
  A.pExpr = sqlite3ExprFunction(pParse, pList, &OP);
  exprNot(pParse, bNot, &A);
  A.zEnd = Y.zEnd;
  if( A.pExpr ) A.pExpr->flags |= EP_InfixFunc;
}
expr(A) ::= expr(A) likeop(OP) expr(Y) ESCAPE expr(E).  [LIKE_KW]  {
  ExprList *pList;
  int bNot = OP.n & 0x80000000;
  OP.n &= 0x7fffffff;
  pList = sqlite3ExprListAppend(pParse,0, Y.pExpr);
  pList = sqlite3ExprListAppend(pParse,pList, A.pExpr);
  pList = sqlite3ExprListAppend(pParse,pList, E.pExpr);
  A.pExpr = sqlite3ExprFunction(pParse, pList, &OP);
  exprNot(pParse, bNot, &A);
  A.zEnd = E.zEnd;
  if( A.pExpr ) A.pExpr->flags |= EP_InfixFunc;
}

%include {
  /* Construct an expression node for a unary postfix operator
  */
  static void spanUnaryPostfix(
    Parse *pParse,         /* Parsing context to record errors */
    int op,                /* The operator */
    ExprSpan *pOperand,    /* The operand, and output */
    Token *pPostOp         /* The operand token for setting the span */
  ){
    pOperand->pExpr = sqlite3PExpr(pParse, op, pOperand->pExpr, 0, 0);
    pOperand->zEnd = &pPostOp->z[pPostOp->n];
  }
}

expr(A) ::= expr(A) ISNULL|NOTNULL(E).   {spanUnaryPostfix(pParse,@E,&A,&E);}
expr(A) ::= expr(A) NOT NULL(E). {spanUnaryPostfix(pParse,TK_NOTNULL,&A,&E);}

%include {
  /* A routine to convert a binary TK_IS or TK_ISNOT expression into a
  ** unary TK_ISNULL or TK_NOTNULL expression. */
  static void binaryToUnaryIfNull(Parse *pParse, Expr *pY, Expr *pA, int op){
    sqlite3 *db = pParse->db;
    if( pA && pY && pY->op==TK_NULL ){
      pA->op = (u8)op;
      sqlite3ExprDelete(db, pA->pRight);
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
expr(A) ::= expr(A) IS expr(Y).     {
  spanBinaryExpr(pParse,TK_IS,&A,&Y);
  binaryToUnaryIfNull(pParse, Y.pExpr, A.pExpr, TK_ISNULL);
}
expr(A) ::= expr(A) IS NOT expr(Y). {
  spanBinaryExpr(pParse,TK_ISNOT,&A,&Y);
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
    pOut->zStart = pPreOp->z;
    pOut->pExpr = sqlite3PExpr(pParse, op, pOperand->pExpr, 0, 0);
    pOut->zEnd = pOperand->zEnd;
  }
}



expr(A) ::= NOT(B) expr(X).
              {spanUnaryPrefix(&A,pParse,@B,&X,&B);/*A-overwrites-B*/}
expr(A) ::= BITNOT(B) expr(X).
              {spanUnaryPrefix(&A,pParse,@B,&X,&B);/*A-overwrites-B*/}
expr(A) ::= MINUS(B) expr(X). [BITNOT]
              {spanUnaryPrefix(&A,pParse,TK_UMINUS,&X,&B);/*A-overwrites-B*/}
expr(A) ::= PLUS(B) expr(X). [BITNOT]
              {spanUnaryPrefix(&A,pParse,TK_UPLUS,&X,&B);/*A-overwrites-B*/}

%type between_op {int}
between_op(A) ::= BETWEEN.     {A = 0;}
between_op(A) ::= NOT BETWEEN. {A = 1;}
expr(A) ::= expr(A) between_op(N) expr(X) AND expr(Y). [BETWEEN] {
  ExprList *pList = sqlite3ExprListAppend(pParse,0, X.pExpr);
  pList = sqlite3ExprListAppend(pParse,pList, Y.pExpr);
  A.pExpr = sqlite3PExpr(pParse, TK_BETWEEN, A.pExpr, 0, 0);
  if( A.pExpr ){
    A.pExpr->x.pList = pList;
  }else{
    sqlite3ExprListDelete(pParse->db, pList);
  }
  exprNot(pParse, N, &A);
  A.zEnd = Y.zEnd;
}
%ifndef SQLITE_OMIT_SUBQUERY
  %type in_op {int}
  in_op(A) ::= IN.      {A = 0;}
  in_op(A) ::= NOT IN.  {A = 1;}
  expr(A) ::= expr(A) in_op(N) LP exprlist(Y) RP(E). [IN] {
    if( Y==0 ){
      /* Expressions of the form
      **
      **      expr1 IN ()
      **      expr1 NOT IN ()
      **
      ** simplify to constants 0 (false) and 1 (true), respectively,
      ** regardless of the value of expr1.
      */
      sqlite3ExprDelete(pParse->db, A.pExpr);
      A.pExpr = sqlite3PExpr(pParse, TK_INTEGER, 0, 0, &sqlite3IntTokens[N]);
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
      sqlite3ExprListDelete(pParse->db, Y);
      /* pRHS cannot be NULL because a malloc error would have been detected
      ** before now and control would have never reached this point */
      if( ALWAYS(pRHS) ){
        pRHS->flags &= ~EP_Collate;
        pRHS->flags |= EP_Generic;
      }
      A.pExpr = sqlite3PExpr(pParse, N ? TK_NE : TK_EQ, A.pExpr, pRHS, 0);
    }else{
      A.pExpr = sqlite3PExpr(pParse, TK_IN, A.pExpr, 0, 0);
      if( A.pExpr ){
        A.pExpr->x.pList = Y;
        sqlite3ExprSetHeightAndFlags(pParse, A.pExpr);
      }else{
        sqlite3ExprListDelete(pParse->db, Y);
      }
      exprNot(pParse, N, &A);
    }
    A.zEnd = &E.z[E.n];
  }
  expr(A) ::= LP(B) select(X) RP(E). {
    spanSet(&A,&B,&E); /*A-overwrites-B*/
    A.pExpr = sqlite3PExpr(pParse, TK_SELECT, 0, 0, 0);
    sqlite3PExprAddSelect(pParse, A.pExpr, X);
  }
  expr(A) ::= expr(A) in_op(N) LP select(Y) RP(E).  [IN] {
    A.pExpr = sqlite3PExpr(pParse, TK_IN, A.pExpr, 0, 0);
    sqlite3PExprAddSelect(pParse, A.pExpr, Y);
    exprNot(pParse, N, &A);
    A.zEnd = &E.z[E.n];
  }
  expr(A) ::= expr(A) in_op(N) nm(Y) dbnm(Z) paren_exprlist(E). [IN] {
    SrcList *pSrc = sqlite3SrcListAppend(pParse->db, 0,&Y,&Z);
    Select *pSelect = sqlite3SelectNew(pParse, 0,pSrc,0,0,0,0,0,0,0);
    if( E )  sqlite3SrcListFuncArgs(pParse, pSelect ? pSrc : 0, E);
    A.pExpr = sqlite3PExpr(pParse, TK_IN, A.pExpr, 0, 0);
    sqlite3PExprAddSelect(pParse, A.pExpr, pSelect);
    exprNot(pParse, N, &A);
    A.zEnd = Z.z ? &Z.z[Z.n] : &Y.z[Y.n];
  }
  expr(A) ::= EXISTS(B) LP select(Y) RP(E). {
    Expr *p;
    spanSet(&A,&B,&E); /*A-overwrites-B*/
    p = A.pExpr = sqlite3PExpr(pParse, TK_EXISTS, 0, 0, 0);
    sqlite3PExprAddSelect(pParse, p, Y);
  }
%endif SQLITE_OMIT_SUBQUERY

/* CASE expressions */
expr(A) ::= CASE(C) case_operand(X) case_exprlist(Y) case_else(Z) END(E). {
  spanSet(&A,&C,&E);  /*A-overwrites-C*/
  A.pExpr = sqlite3PExpr(pParse, TK_CASE, X, 0, 0);
  if( A.pExpr ){
    A.pExpr->x.pList = Z ? sqlite3ExprListAppend(pParse,Y,Z) : Y;
    sqlite3ExprSetHeightAndFlags(pParse, A.pExpr);
  }else{
    sqlite3ExprListDelete(pParse->db, Y);
    sqlite3ExprDelete(pParse->db, Z);
  }
}
%type case_exprlist {ExprList*}
%destructor case_exprlist {sqlite3ExprListDelete(pParse->db, $$);}
case_exprlist(A) ::= case_exprlist(A) WHEN expr(Y) THEN expr(Z). {
  A = sqlite3ExprListAppend(pParse,A, Y.pExpr);
  A = sqlite3ExprListAppend(pParse,A, Z.pExpr);
}
case_exprlist(A) ::= WHEN expr(Y) THEN expr(Z). {
  A = sqlite3ExprListAppend(pParse,0, Y.pExpr);
  A = sqlite3ExprListAppend(pParse,A, Z.pExpr);
}
%type case_else {Expr*}
%destructor case_else {sqlite3ExprDelete(pParse->db, $$);}
case_else(A) ::=  ELSE expr(X).         {A = X.pExpr;}
case_else(A) ::=  .                     {A = 0;}
%type case_operand {Expr*}
%destructor case_operand {sqlite3ExprDelete(pParse->db, $$);}
case_operand(A) ::= expr(X).            {A = X.pExpr; /*A-overwrites-X*/}
case_operand(A) ::= .                   {A = 0;}

%type exprlist {ExprList*}
%destructor exprlist {sqlite3ExprListDelete(pParse->db, $$);}
%type nexprlist {ExprList*}
%destructor nexprlist {sqlite3ExprListDelete(pParse->db, $$);}

exprlist(A) ::= nexprlist(A).
exprlist(A) ::= .                            {A = 0;}
nexprlist(A) ::= nexprlist(A) COMMA expr(Y).
    {A = sqlite3ExprListAppend(pParse,A,Y.pExpr);}
nexprlist(A) ::= expr(Y).
    {A = sqlite3ExprListAppend(pParse,0,Y.pExpr); /*A-overwrites-Y*/}

%ifndef SQLITE_OMIT_SUBQUERY
/* A paren_exprlist is an optional expression list contained inside
** of parenthesis */
%type paren_exprlist {ExprList*}
%destructor paren_exprlist {sqlite3ExprListDelete(pParse->db, $$);}
paren_exprlist(A) ::= .   {A = 0;}
paren_exprlist(A) ::= LP exprlist(X) RP.  {A = X;}
%endif SQLITE_OMIT_SUBQUERY


///////////////////////////// The CREATE INDEX command ///////////////////////
//
cmd ::= createkw(S) uniqueflag(U) INDEX ifnotexists(NE) nm(X) dbnm(D)
        ON nm(Y) LP sortlist(Z) RP with_opt(O) where_opt(W). {
  comdb2CreateIndex(pParse, &X, &D,
                    sqlite3SrcListAppend(pParse->db,0,&Y,0), Z, U,
                    &S, &W, SQLITE_SO_ASC, NE, SQLITE_IDXTYPE_APPDEF, O);
}

%type uniqueflag {int}
uniqueflag(A) ::= UNIQUE.  {A = OE_Abort;}
uniqueflag(A) ::= .        {A = OE_None;}

%type with_opt {int}
with_opt(A) ::= WITH DATACOPY. {A = 1;}
with_opt(A) ::= . {A = 0;}

// The eidlist non-terminal (Expression Id List) generates an ExprList
// from a list of identifiers.  The identifier names are in ExprList.a[].zName.
// This list is stored in an ExprList rather than an IdList so that it
// can be easily sent to sqlite3ColumnsExprList().
//
// eidlist is grouped with CREATE INDEX because it used to be the non-terminal
// used for the arguments to an index.  That is just an historical accident.
//
// IMPORTANT COMPATIBILITY NOTE:  Some prior versions of SQLite accepted
// COLLATE clauses and ASC or DESC keywords on ID lists in inappropriate
// places - places that might have been stored in the sqlite_master schema.
// Those extra features were ignored.  But because they might be in some
// (busted) old databases, we need to continue parsing them when loading
// historical schemas.
//
%type eidlist {ExprList*}
%destructor eidlist {sqlite3ExprListDelete(pParse->db, $$);}
%type eidlist_opt {ExprList*}
%destructor eidlist_opt {sqlite3ExprListDelete(pParse->db, $$);}

%include {
  /* Add a single new term to an ExprList that is used to store a
  ** list of identifiers.  Report an error if the ID list contains
  ** a COLLATE clause or an ASC or DESC keyword, except ignore the
  ** error while parsing a legacy schema.
  */
  static ExprList *parserAddExprIdListTerm(
    Parse *pParse,
    ExprList *pPrior,
    Token *pIdToken,
    int hasCollate,
    int sortOrder
  ){
    ExprList *p = sqlite3ExprListAppend(pParse, pPrior, 0);
    if( (hasCollate || sortOrder!=SQLITE_SO_UNDEFINED)
        && pParse->db->init.busy==0
    ){
      sqlite3ErrorMsg(pParse, "syntax error after column name \"%.*s\"",
                         pIdToken->n, pIdToken->z);
    }
    sqlite3ExprListSetName(pParse, p, pIdToken, 1);
    return p;
  }
} // end %include

eidlist_opt(A) ::= .                         {A = 0;}
eidlist_opt(A) ::= LP eidlist(X) RP.         {A = X;}
eidlist(A) ::= eidlist(A) COMMA nm(Y) collate(C) sortorder(Z).  {
  A = parserAddExprIdListTerm(pParse, A, &Y, C, Z);
}
eidlist(A) ::= nm(Y) collate(C) sortorder(Z). {
  A = parserAddExprIdListTerm(pParse, 0, &Y, C, Z); /*A-overwrites-Y*/
}

%type collate {int}
collate(C) ::= .              {C = 0;}
collate(C) ::= COLLATE ids.   {C = 1;}


///////////////////////////// The DROP INDEX command /////////////////////////
//
cmd ::= DROP INDEX ifexists(E) fullname(X).   {comdb2DropIndex(pParse, X, E);}
cmd ::= DROP INDEX ifexists(E) nm(X) ON nm(Y). {comdb2DropIndexExtn(pParse, &X, &Y, E);}

///////////////////////////// The VACUUM command /////////////////////////////
//
%ifndef SQLITE_OMIT_VACUUM
%ifndef SQLITE_OMIT_ATTACH
cmd ::= VACUUM.                {sqlite3Vacuum(pParse,0);}
cmd ::= VACUUM nm(X).          {sqlite3Vacuum(pParse,&X);}
%endif  SQLITE_OMIT_ATTACH
%endif  SQLITE_OMIT_VACUUM

///////////////////////////// The PRAGMA command /////////////////////////////
//
%ifndef SQLITE_OMIT_PRAGMA
cmd ::= PRAGMA nm(X) dbnm(Z).                {sqlite3Pragma(pParse,&X,&Z,0,0);}
cmd ::= PRAGMA nm(X) dbnm(Z) EQ nmnum(Y).    {sqlite3Pragma(pParse,&X,&Z,&Y,0);}
cmd ::= PRAGMA nm(X) dbnm(Z) LP nmnum(Y) RP. {sqlite3Pragma(pParse,&X,&Z,&Y,0);}
cmd ::= PRAGMA nm(X) dbnm(Z) EQ minus_num(Y).
                                             {sqlite3Pragma(pParse,&X,&Z,&Y,1);}
cmd ::= PRAGMA nm(X) dbnm(Z) LP minus_num(Y) RP.
                                             {sqlite3Pragma(pParse,&X,&Z,&Y,1);}

nmnum(A) ::= plus_num(A).
nmnum(A) ::= nm(A).
nmnum(A) ::= ON(A).
nmnum(A) ::= DELETE(A).
nmnum(A) ::= DEFAULT(A).
%endif SQLITE_OMIT_PRAGMA
%token_class number INTEGER|FLOAT.
plus_num(A) ::= PLUS number(X).       {A = X;}
plus_num(A) ::= number(A).
minus_num(A) ::= MINUS number(X).     {A = X;}
//////////////////////////// The CREATE TRIGGER command /////////////////////

%ifndef SQLITE_OMIT_TRIGGER

cmd ::= createkw trigger_decl(A) BEGIN trigger_cmd_list(S) END(Z). {
  Token all;
  all.z = A.z;
  all.n = (int)(Z.z - A.z) + Z.n;
  sqlite3FinishTrigger(pParse, S, &all);
}

trigger_decl(A) ::= temp(T) TRIGGER ifnotexists(NOERR) nm(B) dbnm(Z)
                    trigger_time(C) trigger_event(D)
                    ON fullname(E) foreach_clause when_clause(G). {
  sqlite3BeginTrigger(pParse, &B, &Z, C, D.a, D.b, E, G, T, NOERR);
  A = (Z.n==0?B:Z); /*A-overwrites-T*/
}

%type trigger_time {int}
trigger_time(A) ::= BEFORE.      { A = TK_BEFORE; }
trigger_time(A) ::= AFTER.       { A = TK_AFTER;  }
trigger_time(A) ::= INSTEAD OF.  { A = TK_INSTEAD;}
trigger_time(A) ::= .            { A = TK_BEFORE; }

%type trigger_event {struct TrigEvent}
%destructor trigger_event {sqlite3IdListDelete(pParse->db, $$.b);}
trigger_event(A) ::= DELETE|INSERT(X).   {A.a = @X; /*A-overwrites-X*/ A.b = 0;}
trigger_event(A) ::= UPDATE(X).          {A.a = @X; /*A-overwrites-X*/ A.b = 0;}
trigger_event(A) ::= UPDATE OF idlist(X).{A.a = TK_UPDATE; A.b = X;}

foreach_clause ::= .
foreach_clause ::= FOR EACH ROW.

%type when_clause {Expr*}
%destructor when_clause {sqlite3ExprDelete(pParse->db, $$);}
when_clause(A) ::= .             { A = 0; }
when_clause(A) ::= WHEN expr(X). { A = X.pExpr; }

%type trigger_cmd_list {TriggerStep*}
%destructor trigger_cmd_list {sqlite3DeleteTriggerStep(pParse->db, $$);}
trigger_cmd_list(A) ::= trigger_cmd_list(A) trigger_cmd(X) SEMI. {
  assert( A!=0 );
  A->pLast->pNext = X;
  A->pLast = X;
}
trigger_cmd_list(A) ::= trigger_cmd(A) SEMI. {
  assert( A!=0 );
  A->pLast = A;
}

// Disallow qualified table names on INSERT, UPDATE, and DELETE statements
// within a trigger.  The table to INSERT, UPDATE, or DELETE is always in
// the same database as the table that the trigger fires on.
//
%type trnm {Token}
trnm(A) ::= nm(A).
trnm(A) ::= nm DOT nm(X). {
  A = X;
  sqlite3ErrorMsg(pParse,
        "qualified table names are not allowed on INSERT, UPDATE, and DELETE "
        "statements within triggers");
}

// Disallow the INDEX BY and NOT INDEXED clauses on UPDATE and DELETE
// statements within triggers.  We make a specific error message for this
// since it is an exception to the default grammar rules.
//
tridxby ::= .
tridxby ::= INDEXED BY nm. {
  sqlite3ErrorMsg(pParse,
        "the INDEXED BY clause is not allowed on UPDATE or DELETE statements "
        "within triggers");
}
tridxby ::= NOT INDEXED. {
  sqlite3ErrorMsg(pParse,
        "the NOT INDEXED clause is not allowed on UPDATE or DELETE statements "
        "within triggers");
}



%type trigger_cmd {TriggerStep*}
%destructor trigger_cmd {sqlite3DeleteTriggerStep(pParse->db, $$);}
// UPDATE
trigger_cmd(A) ::=
   UPDATE orconf(R) trnm(X) tridxby SET setlist(Y) where_opt(Z).
   {A = sqlite3TriggerUpdateStep(pParse->db, &X, Y, Z.pExpr, R);}

// INSERT
trigger_cmd(A) ::= insert_cmd(R) INTO trnm(X) idlist_opt(F) select(S).
   {A = sqlite3TriggerInsertStep(pParse->db, &X, F, S, R);/*A-overwrites-R*/}

// DELETE
trigger_cmd(A) ::= DELETE FROM trnm(X) tridxby where_opt(Y).
   {A = sqlite3TriggerDeleteStep(pParse->db, &X, Y.pExpr);}

// SELECT
trigger_cmd(A) ::= select(X).
   {A = sqlite3TriggerSelectStep(pParse->db, X); /*A-overwrites-X*/}

// The special RAISE expression that may occur in trigger programs
expr(A) ::= RAISE(X) LP IGNORE RP(Y).  {
  spanSet(&A,&X,&Y);  /*A-overwrites-X*/
  A.pExpr = sqlite3PExpr(pParse, TK_RAISE, 0, 0, 0);
  if( A.pExpr ){
    A.pExpr->affinity = OE_Ignore;
  }
}
expr(A) ::= RAISE(X) LP raisetype(T) COMMA nm(Z) RP(Y).  {
  spanSet(&A,&X,&Y);  /*A-overwrites-X*/
  A.pExpr = sqlite3PExpr(pParse, TK_RAISE, 0, 0, &Z);
  if( A.pExpr ) {
    A.pExpr->affinity = (char)T;
  }
}
%endif  !SQLITE_OMIT_TRIGGER

%type raisetype {int}
raisetype(A) ::= ROLLBACK.  {A = OE_Rollback;}
raisetype(A) ::= ABORT.     {A = OE_Abort;}
raisetype(A) ::= FAIL.      {A = OE_Fail;}


////////////////////////  DROP TRIGGER statement //////////////////////////////
%ifndef SQLITE_OMIT_TRIGGER
cmd ::= DROP TRIGGER ifexists(NOERR) fullname(X). {
  sqlite3DropTrigger(pParse,X,NOERR);
}
%endif  !SQLITE_OMIT_TRIGGER

//////////////////////// ATTACH DATABASE file AS name /////////////////////////
%ifndef SQLITE_OMIT_ATTACH
cmd ::= ATTACH database_kw_opt expr(F) AS expr(D) key_opt(K). {
  sqlite3Attach(pParse, F.pExpr, D.pExpr, K);
}
cmd ::= DETACH database_kw_opt expr(D). {
  sqlite3Detach(pParse, D.pExpr);
}

%type key_opt {Expr*}
%destructor key_opt {sqlite3ExprDelete(pParse->db, $$);}
key_opt(A) ::= .                     { A = 0; }
key_opt(A) ::= KEY expr(X).          { A = X.pExpr; }

database_kw_opt ::= DATABASE.
database_kw_opt ::= .
%endif SQLITE_OMIT_ATTACH

////////////////////////// REINDEX collation //////////////////////////////////
%ifndef SQLITE_OMIT_REINDEX
cmd ::= REINDEX.                {sqlite3Reindex(pParse, 0, 0);}
cmd ::= REINDEX nm(X) dbnm(Y).  {sqlite3Reindex(pParse, &X, &Y);}
%endif  SQLITE_OMIT_REINDEX

/////////////////////////////////// ANALYZE ///////////////////////////////////
%ifndef SQLITE_OMIT_ANALYZE
cmd ::= ANALYZESQLITE.                {sqlite3Analyze(pParse, 0, 0);}
cmd ::= ANALYZESQLITE nm(X) dbnm(Y).  {sqlite3Analyze(pParse, &X, &Y);}
%endif

//////////////////////// ALTER TABLE table ... ////////////////////////////////
%ifndef SQLITE_OMIT_ALTERTABLE
cmd ::= ALTER TABLE fullname(X) RENAME TO nm(Z). {
  sqlite3AlterRenameTable(pParse,X,&Z);
}
cmd ::= ALTER TABLE add_column_fullname
        ADD kwcolumn_opt columnname(Y) carglist. {
  Y.n = (int)(pParse->sLastToken.z-Y.z) + pParse->sLastToken.n;
  sqlite3AlterFinishAddColumn(pParse, &Y);
}
add_column_fullname ::= fullname(X). {
  disableLookaside(pParse);
  sqlite3AlterBeginAddColumn(pParse, X);
}
kwcolumn_opt ::= .
kwcolumn_opt ::= COLUMNKW.
%endif  SQLITE_OMIT_ALTERTABLE

//////////////////////// CREATE VIRTUAL TABLE ... /////////////////////////////
%ifndef SQLITE_OMIT_VIRTUALTABLE
cmd ::= create_vtab.                       {sqlite3VtabFinishParse(pParse,0);}
cmd ::= create_vtab LP vtabarglist RP(X).  {sqlite3VtabFinishParse(pParse,&X);}
create_vtab ::= createkw VIRTUAL TABLE ifnotexists(E)
                nm(X) dbnm(Y) USING nm(Z). {
    sqlite3VtabBeginParse(pParse, &X, &Y, &Z, E);
}
vtabarglist ::= vtabarg.
vtabarglist ::= vtabarglist COMMA vtabarg.
vtabarg ::= .                       {sqlite3VtabArgInit(pParse);}
vtabarg ::= vtabarg vtabargtoken.
vtabargtoken ::= ANY(X).            {sqlite3VtabArgExtend(pParse,&X);}
vtabargtoken ::= lp anylist RP(X).  {sqlite3VtabArgExtend(pParse,&X);}
lp ::= LP(X).                       {sqlite3VtabArgExtend(pParse,&X);}
anylist ::= .
anylist ::= anylist LP anylist RP.
anylist ::= anylist ANY.
%endif  SQLITE_OMIT_VIRTUALTABLE


//////////////////////// COMMON TABLE EXPRESSIONS ////////////////////////////
%type with {With*}
%type wqlist {With*}
%destructor with {sqlite3WithDelete(pParse->db, $$);}
%destructor wqlist {sqlite3WithDelete(pParse->db, $$);}

with(A) ::= . {A = 0;}
%ifndef SQLITE_OMIT_CTE
with(A) ::= WITH wqlist(W).              { A = W; }
with(A) ::= WITH RECURSIVE wqlist(W).    { A = W; }

wqlist(A) ::= nm(X) eidlist_opt(Y) AS LP select(Z) RP. {
  A = sqlite3WithAdd(pParse, 0, &X, Y, Z); /*A-overwrites-X*/
}
wqlist(A) ::= wqlist(A) COMMA nm(X) eidlist_opt(Y) AS LP select(Z) RP. {
  A = sqlite3WithAdd(pParse, A, &X, Y, Z);
}
%endif  SQLITE_OMIT_CTE

///////////////////////////////////////////////////////////////////////
//////////////////////// COMDB2 EXTENSIONS ////////////////////////////
///////////////////////////////////////////////////////////////////////

///////////////////// COMDB2 SET/GET statements ////////////////////////////////
//

cmd ::= GET getcmd.

getcmd ::= ALIAS nm(U). {
    comdb2getAlias(pParse, &U);
}

getcmd ::= KW. {
    comdb2getkw(pParse, KW_ALL);
}

getcmd ::= RESERVED KW. {
    comdb2getkw(pParse, KW_RES);
}

getcmd ::= NOT RESERVED KW. {
    comdb2getkw(pParse, KW_FB);
}

getcmd ::= ANALYZE COVERAGE nm(Y) dbnm(Z). {
    comdb2getAnalyzeCoverage(pParse,&Y,&Z);
}

getcmd ::= ANALYZE THRESHOLD nm(Y) dbnm(Z). {
    comdb2getAnalyzeThreshold(pParse,&Y,&Z);
}

///////////////////// COMDB2 PUT statements //////////////////////////////////

cmd ::= PUT putcmd. {
    comdb2WriteTransaction(pParse);
}

putcmd ::= ANALYZE COVERAGE nm(Y) dbnm(Z) INTEGER(F). {
    int tmp;
    if (!readIntFromToken(&F, &tmp))
        tmp = 0;

    comdb2analyzeCoverage(pParse,&Y,&Z,tmp);
}

putcmd ::= GENID48 ENABLE. {
    comdb2enableGenid48(pParse, 1);
}

putcmd ::= GENID48 DISABLE. {
    comdb2enableGenid48(pParse, 0);
}

putcmd ::= ROWLOCKS ENABLE. {
    comdb2enableRowlocks(pParse, 1);
}

putcmd ::= ROWLOCKS DISABLE. {
    comdb2enableRowlocks(pParse, 0);
}

putcmd ::= ANALYZE THRESHOLD nm(Y) dbnm(Z) INTEGER(F). {
    int tmp;
    if (!readIntFromToken(&F, &tmp))
        tmp = 0;
    comdb2analyzeThreshold(pParse,&Y,&Z, tmp);
}

putcmd ::= SKIPSCAN ENABLE nm(Y) dbnm(Z). {
    comdb2setSkipscan(pParse,&Y, &Z, 1);
}

putcmd ::= SKIPSCAN DISABLE nm(Y) dbnm(Z). {
    comdb2setSkipscan(pParse,&Y, &Z, 0);
}

putcmd ::= DEFAULT PROCEDURE nm(N) INTEGER(V). {
    comdb2DefaultProcedure(pParse,&N,&V,0);
}

putcmd ::= DEFAULT PROCEDURE nm(N) STRING(V). {
    comdb2DefaultProcedure(pParse,&N,&V,1);
}

putcmd ::= ALIAS nm(Y) nm(U). {
    comdb2setAlias(pParse,&Y,&U);
}

putcmd ::= PASSWORD OFF FOR nm(N). {
    comdb2deletePassword(pParse, &N);
}

putcmd ::= PASSWORD STRING(P) FOR nm(N). {
    comdb2setPassword(pParse, &P, &N);
}

putcmd ::= AUTHENTICATION ON. {
    comdb2enableAuth(pParse, 1);
}

putcmd ::= AUTHENTICATION OFF. {
    comdb2enableAuth(pParse, 0);
}

putcmd ::= TIME PARTITION nm(Y) dbnm(Z) RETENTION  INTEGER(R). {
    int tmp;
    if (!readIntFromToken(&R, &tmp))
        tmp = 0;
    comdb2timepartRetention(pParse, &Y, &Z, tmp);
}

putcmd ::= SCHEMACHANGE COMMITSLEEP INTEGER(F). {
    int tmp;
    if (!readIntFromToken(&F, &tmp))
        tmp = 0;
    comdb2schemachangeCommitsleep(pParse, tmp);
}

putcmd ::= SCHEMACHANGE CONVERTSLEEP INTEGER(F). {
    int tmp;
    if (!readIntFromToken(&F, &tmp))
        tmp = 0;
    comdb2schemachangeConvertsleep(pParse, tmp);
}

putcmd ::= TUNABLE nm(N) INTEGER|FLOAT|STRING(M). {
    comdb2putTunable(pParse, &N, &M);
}

///////////////////// COMDB2 REBUILD STATEMENTS //////////////////////////////

cmd ::= rebuild.

rebuild ::= REBUILD nm(T) dbnm(X). { /* REBUILD FULL CANNOT BE USED
                                        BECAUSE OF SQLITE SYNTAX */
    comdb2RebuildFull(pParse,&T,&X);
}

rebuild ::= REBUILD INDEX nm(T) dbnm(Y) nm(X). {
    comdb2RebuildIndex(pParse, &T,&Y, &X);
}

rebuild ::= REBUILD DATA nm(T) dbnm(X). {
    comdb2RebuildData(pParse, &T, &X);
}

rebuild ::= REBUILD DATABLOB nm(N) dbnm(X). {
    comdb2RebuildDataBlob(pParse,&N, &X);
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
    comdb2WriteTransaction(pParse);
    comdb2grant(pParse, 0, P, &T,&Y,&U);
}

cmd ::= GRANT op_permission(P) TO nm(U). {
    comdb2WriteTransaction(pParse);
    comdb2grant(pParse, 0, P, NULL,NULL,&U);
}

cmd ::= GRANT userschema(P) nm(U1) TO nm(U2). {
    comdb2WriteTransaction(pParse);
    comdb2grant(pParse, 0, P, &U1,NULL,&U2);
}

cmd ::= REVOKE sql_permission(P) ON nm(T) dbnm(Y) TO nm(U). {
    comdb2WriteTransaction(pParse);
    comdb2grant(pParse, 1, P, &T,&Y,&U);
}

cmd ::= REVOKE op_permission(P) TO nm(U). {
    comdb2WriteTransaction(pParse);
    comdb2grant(pParse, 1, P, NULL,NULL,&U);
}

cmd ::= REVOKE userschema(P) nm(U1) TO nm(U2). {
    comdb2WriteTransaction(pParse);
    comdb2grant(pParse, 1, P, &U1,NULL,&U2);
}

//////////////////// COMDB2 TRUNCATE TABLE statement /////////////////////////

cmd ::= truncate.
truncate ::= TRUNCATE table_opt nm(T) dbnm(Y).
{
    comdb2Truncate(pParse, &T, &Y);
}

table_opt ::= .
table_opt ::= TABLE.

cmd ::= BULKIMPORT nm(A) DOT nm(B) nm(C) DOT nm(D). {
    comdb2bulkimport(pParse, &A, &B, &C, &D);
}

//////////////////// COMDB2 PARTITION //////////////////////////////////

cmd ::= createkw TIME PARTITION ON nm(A) AS nm(P) PERIOD STRING(D) RETENTION INTEGER(R) START STRING(S). {
    comdb2WriteTransaction(pParse);
    comdb2CreateTimePartition(pParse, &A, &P, &D, &R, &S);
}

cmd ::= DROP TIME PARTITION nm(N). {
    comdb2WriteTransaction(pParse);
    comdb2DropTimePartition(pParse, &N);
}

//////////////////// COMDB2 ANALYZE //////////////////////////////////////////
cmd ::= ANALYZE nm(N) dbnm(Y) analyzepercentage(P) analyzeopt(X). {
    comdb2analyze(pParse, X, &N, &Y, P);
}

cmd ::= ANALYZE analyzepercentage(P) analyzeopt(X). {
    comdb2analyze(pParse, X, NULL, NULL, P);
}

%type analyzepercentage {int}
analyzepercentage(A) ::= . {A = 0;}
analyzepercentage(A) ::= INTEGER(X). {
    if (!readIntFromToken(&X, &(A)))
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
    if (!readIntFromToken(&X, &(tmp)))
        SET_ANALYZE_THREAD(A,0);

    SET_ANALYZE_THREAD(A,tmp);
}

%type analyze_sumthds {int}
analyze_sumthds(A) ::= SUMMARIZE INTEGER(X). {
    int tmp;
    A = 0;
    if (!readIntFromToken(&X, &(tmp)))
        SET_ANALYZE_SUMTHREAD(A, 0);
    SET_ANALYZE_SUMTHREAD(A,tmp);
}

///////////////////// COMDB2 REBUILD TABLE statement //////////////////////////
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
blob_compress_type(A) ::= NONE. {A = BLOB_NONE;}
blob_compress_type(A) ::= RLE. {A = BLOB_RLE;}
//blob_compress_type(A) ::= CRLE. {A = BLOB_CRLE;}
blob_compress_type(A) ::= ZLIB. {A = BLOB_ZLIB;}
blob_compress_type(A) ::= LZ4. {A = BLOB_LZ4;}

%type compress_rec {int}
compress_rec(A) ::= REC rle_compress_type(T). {A = T;}

%type rle_compress_type {int}
rle_compress_type(A) ::= NONE. {A = REC_NONE;}
rle_compress_type(A) ::= RLE. {A = REC_RLE;}
rle_compress_type(A) ::= CRLE. {A = REC_CRLE;}
rle_compress_type(A) ::= ZLIB. {A = REC_ZLIB;}
rle_compress_type(A) ::= LZ4. {A = REC_LZ4;}

//////////////////// COMDB2 STORED PROCEDURES /////////////////////////////////

cmd ::= createkw PROCEDURE nm(N) NOSQL(X). {
    comdb2CreateProcedure(pParse, &N, NULL, &X);
}
cmd ::= createkw PROCEDURE nm(N) VERSION STRING(V) NOSQL(X). {
    comdb2CreateProcedure(pParse, &N, &V, &X);
}
cmd ::= DROP PROCEDURE nm(N) INTEGER(V). {
    comdb2DropProcedure(pParse, &N, &V, 0);
}
cmd ::= DROP PROCEDURE nm(N) STRING(V). {
    comdb2DropProcedure(pParse, &N, &V, 1);
}

////////////////////// CREATE LUA commands ////////////////////
cmd ::= createkw LUA SCALAR FUNCTION nm(Q). {
	comdb2CreateScalarFunc(pParse, &Q);
}

cmd ::= createkw LUA AGGREGATE FUNCTION nm(Q). {
	comdb2CreateAggFunc(pParse, &Q);
}

cmd ::= createkw LUA TRIGGER nm(Q) ON table_trigger_event(T). {
  comdb2CreateTrigger(pParse,0,&Q,T);
}

cmd ::= createkw LUA CONSUMER nm(Q) ON table_trigger_event(T). {
  comdb2CreateTrigger(pParse,1,&Q,T);
}

table_trigger_event(A) ::= table_trigger_event(B) COMMA LP TABLE fullname(T) FOR trigger_events(C) RP. {
  A = comdb2AddTriggerTable(pParse,B,T,C);
}

table_trigger_event(A) ::= LP TABLE fullname(T) FOR trigger_events(B) RP. {
  A = comdb2AddTriggerTable(pParse,0,T,B);
}

%type table_trigger_event {Cdb2TrigTables*}
%destructor table_trigger_event {sqlite3DbFree(pParse->db, $$);}

%type cdb2_trigger_event {Cdb2TrigEvent}
%destructor cdb2_trigger_event {sqlite3IdListDelete(pParse->db, $$.cols);}

%type trigger_events {Cdb2TrigEvents*}
%destructor trigger_events {sqlite3DbFree(pParse->db, $$);}

trigger_events(A) ::= trigger_events(B) AND cdb2_trigger_event(C). {
  A = comdb2AddTriggerEvent(pParse,B,&C);
}
trigger_events(A) ::= cdb2_trigger_event(B). {
  A = comdb2AddTriggerEvent(pParse,0,&B);
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
  comdb2DropScalarFunc(pParse,&A);
}
cmd ::= DROP LUA AGGREGATE FUNCTION nm(A). {
  comdb2DropAggFunc(pParse,&A);
}
cmd ::= DROP LUA TRIGGER nm(A). {
  comdb2DropTrigger(pParse,&A);
}
cmd ::= DROP LUA CONSUMER nm(A). {
  comdb2DropTrigger(pParse,&A);
}

/////////////////// COMDB2 ALTER TABLE STATEMENT  //////////////////////////////

/* ALTER TABLE (CSC2) */
cmd ::= comdb2_alter_table_csc2.
comdb2_alter_table_csc2 ::= dryrun(D) ALTER TABLE nm(Y) dbnm(Z) comdb2opt(O) NOSQL(C). {
  comdb2AlterTableCSC2(pParse,&Y,&Z,O,&C,D);
}

/* ALTER TABLE (Comdb2) */
cmd ::= alter_table alter_table_action_list. {
  comdb2AlterTableEnd(pParse);
}

alter_table ::= dryrun(D) ALTER TABLE nm(Y) dbnm(Z) . {
  comdb2AlterTableStart(pParse,&Y,&Z,D);
}

alter_table_action_list ::= alter_table_action_list COMMA alter_table_action.
alter_table_action_list ::= alter_table_action.

alter_table_action ::= alter_table_add_column.
alter_table_action ::= alter_table_drop_column.

alter_table_add_column ::= ADD kwcolumn_opt columnname carglist.
alter_table_drop_column ::= DROP kwcolumn_opt nm(Y) . {
  comdb2DropColumn(pParse, &Y);
}

%ifdef SQLITE_OMIT_ALTERTABLE
kwcolumn_opt ::= .
kwcolumn_opt ::= COLUMNKW.
%endif

%type dryrun {int}
dryrun(D) ::= DRYRUN.  {D=1;}
dryrun(D) ::= . {D=0;}
/* vim: set ft=lemon: */
