/*
   Copyright 2015 Bloomberg Finance L.P.
  
   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at
   
       http://www.apache.org/licenses/LICENSE-2.0
   
   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and 
   limitations under the License.
 */

/* GRAMMAR FOR MACC.   05/96 */
/* 01/97 - pl - added record types */

%union {
  int    number;
  double fltpoint;
  char *varname;
  char *opttext;
  char *where;
  char *comment;
  char *bytestr; /* first int is size */
  struct unum {
    int   number;
    char *numstr;
  } numstr;
}


%token T_STRING T_NUM T_FLOAT T_SQLHEXSTR
%token T_WHERE T_VARNAME T_COMMENT

%token T_LOGICAL T_INTEGER2 T_INTEGER4 
%token T_CSTR T_PSTR T_REAL4 T_REAL8 
%token T_UCHAR T_UINTEGER2 T_UINTEGER4 T_ULONG T_LONGLONG T_ULONGLONG
%token T_BLOB T_BLOB2 T_VUTF8
%token T_DATETIME T_DATETIMEUS
%token T_INTERVALYM
%token T_INTERVALDS T_INTERVALDSUS
%token T_DECIMAL32
%token T_DECIMAL64
%token T_DECIMAL128

%token T_KEYS 
%token T_CONSTANTS
%token T_PUBLIC T_PRIVATE
%token T_FLD_NULL T_FLD_STRDEFAULT T_FLD_LDDEFAULT T_FLD_NEXTSEQUENCE T_FLD_PADDING
%token T_TABLE_TAG T_DEFAULT T_ONDISK T_SCHEMA
%token T_CONSTRAINTS T_CASCADE T_SET T_NULL
%token T_CON_ON  T_CON_UPDATE T_CON_DELETE T_RESTRICT
%token T_CHECK
%token T_CON_NOOVERLAP
%token T_PERIODS

%token T_RECNUMS T_PRIMARY T_DATAKEY T_UNIQNULLS
%token T_YES T_NO

%token T_ASCEND T_DESCEND T_DUP                    /*MODIFIERS*/

%token T_LT T_GT 
%token T_EQ

%type <number> validctype validstrtype valididxstrtype
%type <numstr> number
%type <number> yesno
%type <where> where 
%type <varname> varname typename
%type <opttext> string verbatim_string
%type <comment> comment
%type <fltpoint> fltnumber
%type <bytestr> sqlhexstr
%type <opttext> defaultfunction

%{
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "macc.h"
#include "dynschemaload.h"
#include "csctypes.h"
#include "logmsg.h"
extern int current_line;
extern char *blankchar;
extern int yyleng;
extern int lastidx;
extern int declaration;
extern int range_or_array;
int parser_reset=0;
int gbl_allow_neg_column_size;
#ifndef YYDEBUG
#define YYDEBUG 1
#endif
char *remem_varname, *remem_com, *remem_string, *remem_where;
int yylex (void);
void yyerror(const char *msg);

extern void *csc2_malloc(size_t size);
void csc2_error(const char *fmt, ...);
void csc2_syntax_error(const char *fmt, ...);

%}


%%
comdbg_csc:    structdef
                { 
                  resolve_case_names();        /* when i'm all done, do this */
                }



structdef:    validstruct structdef
        |    validstruct
        ;

validstruct:    recstruct
            |    keystruct
            |   constantstruct
            |   constraintstruct
            |   periodstruct
            ;


/* constraintstruct: defines cross-table constraints */
constraintstruct: T_CONSTRAINTS comment '{' cnstrtdef '}' {}
                ;

ctmodifiers:    T_CON_ON T_CON_UPDATE T_CASCADE ctmodifiers           { set_constraint_mod(0,0,1); }
                | T_CON_ON T_CON_DELETE T_SET T_NULL ctmodifiers      { set_constraint_mod(0,2,1); }
                | T_CON_ON T_CON_UPDATE T_RESTRICT ctmodifiers        { set_constraint_mod(0,0,0); }
                | T_CON_ON T_CON_DELETE T_CASCADE ctmodifiers         { set_constraint_mod(0,1,1); }
                | T_CON_ON T_CON_DELETE T_RESTRICT ctmodifiers        { set_constraint_mod(0,1,0); }
                | /* %empty */
                ;

cnstrtstart:    string '-' T_GT { start_constraint_list($1, 0); }
                | varname '-' T_GT { start_constraint_list($1, 0); }
                | T_CON_NOOVERLAP string '-' T_GT { start_constraint_list($2, 1); }
                | T_CON_NOOVERLAP varname '-' T_GT { start_constraint_list($2, 1); }
                ;

/* Named constraint (introduced in r7) */
cnstrtnamedstart: string T_EQ string '-' T_GT {
                      start_constraint_list($3, 0);
                      set_constraint_name($1, CT_FKEY);
                  }

/* Named check constraint */
cnstrtnamedcheck: T_CHECK string T_EQ where {
                      add_check_constraint($4);
                      set_constraint_name($2, CT_CHECK);
                  }

/* Note: a named constraint does not allow a list of parent key references. */
cnstrtdef:      cnstrtdef cnstrtstart cnstrtparentlist ctmodifiers { }
                | cnstrtdef cnstrtnamedstart cnstrtparent ctmodifiers { }
                | cnstrtdef cnstrtnamedcheck
                | /* %empty */
                ;

cnstrtparentlist: cnstrtparentlist T_LT string ':' string T_GT  {  add_constraint($3,$5); }
                | T_LT string ':' string T_GT  {  add_constraint($2,$4); }
                | string ':' string {  add_constraint($1,$3); }
                | varname ':' varname {  add_constraint($1,$3); }
                ;

cnstrtparent:   T_LT string ':' string T_GT  {  add_constraint($2,$4); }
                ;



/* constantstruct: defines constants
**              ie.
**              constants {
**                          SIZE1=1,
**                          SIZE2=6,
**                          SIZE3=879
**                        }
*/
constantstruct:    T_CONSTANTS comment '{' cnstdef '}' 
        ;

cnstdef: varname '=' number ',' comment cnstdef { add_constant($1, $3.number, 0);}
         | varname '=' number           comment { add_constant($1, $3.number, 0);}
         | T_PUBLIC  varname '=' number ',' comment cnstdef { add_constant($2, $4.number, 0);}
         | T_PRIVATE  varname '=' number ',' comment cnstdef { add_constant($2, $4.number, 1);}
         | T_PUBLIC  varname '=' number           comment { add_constant($2, $4.number, 0);}
         | T_PRIVATE varname '=' number           comment { add_constant($2, $4.number, 1);}
         | /* %empty */
         ;

fieldopts: fieldterm fieldopts
    | /* empty */
    ;

fieldterm: T_FLD_STRDEFAULT '=' number { add_fldopt(FLDOPT_DBSTORE,CLIENT_INT, $3.numstr); }
           | T_FLD_LDDEFAULT '=' number { add_fldopt(FLDOPT_DBLOAD,CLIENT_INT, $3.numstr); }
           | T_FLD_STRDEFAULT '=' fltnumber { double f=$3; add_fldopt(FLDOPT_DBSTORE,CLIENT_REAL,&f); }
           | T_FLD_LDDEFAULT '=' fltnumber { double f=$3; add_fldopt(FLDOPT_DBLOAD,CLIENT_REAL,&f); }
           | T_FLD_STRDEFAULT '=' string  { add_fldopt(FLDOPT_DBSTORE,CLIENT_CSTR,$3); }
           | T_FLD_LDDEFAULT '=' string { add_fldopt(FLDOPT_DBLOAD,CLIENT_CSTR,$3); }
           | T_FLD_STRDEFAULT '=' sqlhexstr  { add_fldopt(FLDOPT_DBSTORE,CLIENT_BYTEARRAY,$3); }
           | T_FLD_LDDEFAULT '=' sqlhexstr { add_fldopt(FLDOPT_DBLOAD,CLIENT_BYTEARRAY,$3); }
           | T_FLD_NULL '=' yesno { int f=$3; add_fldopt(FLDOPT_NULL,CLIENT_INT,&f); }
           | T_FLD_PADDING '=' number { int f=$3.number; add_fldopt(FLDOPT_PADDING,CLIENT_INT,&f); }
           | T_FLD_STRDEFAULT '=' defaultfunction { add_fldopt(FLDOPT_DBSTORE,CLIENT_FUNCTION,$3); }
           | T_FLD_STRDEFAULT '=' T_FLD_NEXTSEQUENCE { add_fldopt(FLDOPT_DBSTORE,CLIENT_SEQUENCE,NULL); }
           ;

defaultfunction: '{' verbatim_string '}' {
        int origlen=strlen($2);
        char *str=(char*)csc2_malloc(origlen+2+1); //2 paren + \n
        if (str==0) {
            csc2_error("ERROR: OUT OF MEMORY: %s\n",yylval.opttext);
            exit(-1);
        }
        sprintf(str,"(%.*s)", origlen, $2);
        $$=str;
    }
    ;

verbatim_string: T_STRING { $$=csc2_strdup(yylval.varname); }
    ;

/* recstruct: defines a record
**        ie.
**        record {
**            integer*4 firm
**            integer*4 cust
**
**            rectype {
**            case ( IN_BLP ) :
**                integer*4 floor
**            case ( NOT_BLP) :
**                character*48 address
**            }
**
**            integer*4 term
**        }
*/

recstart:       T_TABLE_TAG  string   { reset_array(); start_table($2,0);  }
                | T_TABLE_TAG T_DEFAULT  { reset_array(); start_table(".DEFAULT",1);  }
                | T_TABLE_TAG T_ONDISK  { reset_array(); start_table(".ONDISK",1);  }
                | T_SCHEMA { reset_array(); start_table(".ONDISK",1);  }
                ;

recstruct:    recstart '{' recdef '}' { end_table();}
        ;

recdef:
    typedec recdef
        | /* %empty */
        ;

validctype:     T_INTEGER2    { $$=T_INTEGER2;}
                | T_INTEGER4    { $$=T_INTEGER4;}
                | T_UINTEGER2    { $$=T_UINTEGER2;}
                | T_UINTEGER4    { $$=T_UINTEGER4;}
                | T_REAL4       { $$=T_REAL4;}
                | T_REAL8       { $$=T_REAL8;}
                | T_LOGICAL     { $$=T_LOGICAL;}
                | T_ULONGLONG    { $$=T_ULONGLONG;} 
                | T_LONGLONG    { $$=T_LONGLONG;} 
                | T_DATETIME    { $$=T_DATETIME;}
                | T_DATETIMEUS  { $$=T_DATETIMEUS;}
                | T_INTERVALYM  { $$=T_INTERVALYM;}
                | T_INTERVALDS  { $$=T_INTERVALDS;}
                | T_INTERVALDSUS{ $$=T_INTERVALDSUS;}
                | T_DECIMAL32   { $$=T_DECIMAL32;}
                | T_DECIMAL64   { $$=T_DECIMAL64;}
                | T_DECIMAL128  { $$=T_DECIMAL128;}
                ;

validstrtype:      T_CSTR        { $$=T_CSTR;}
                | T_PSTR        { $$=T_PSTR;}
                | T_UCHAR        { $$=T_UCHAR;}
                | T_VUTF8        { $$=T_VUTF8;}
                | T_BLOB        { $$=T_BLOB;}
                ;

valididxstrtype:    T_CSTR      { $$=T_CSTR;}
                    | T_PSTR    { $$=T_PSTR;}
                    | T_UCHAR   { $$=T_UCHAR;}
                    ;

typedec:    validctype varname fieldopts comment
                                                { 
                          declaration=1;
                          rec_c_add($1, -1, $2, $4 /*$5*/); 
                          reset_array();
                          reset_fldopt();
                        }
                | validstrtype varname carray fieldopts comment 
                                                {
                          declaration=1; 
                                                  rec_c_add($1, -1, $2, $5);
                                                  reset_array();
                          reset_fldopt();
                                                } 
                ;


carray:         cstart                       { range_or_array=CLANG;}
        ;

cstart:         '[' number ']' cstart
                        {
                            lastidx++;
                            if ((gbl_allow_neg_column_size == 0) &&
                                ($2.number < 0)) {
                                csc2_error("ERROR AT LINE %3d: NEGATIVE ARRAY LENGTH\n",
                                    current_line);
                                csc2_syntax_error("ERROR AT LINE %3d: NEGATIVE ARRAY LENGTH",
                                    current_line);
                                any_errors++;
                            } else {
                                if ($2.number < 0) {
                                    logmsg(LOGMSG_WARN, "CSC2: ERROR AT LINE %3d: NEGATIVE ARRAY LENGTH\n",
                                           current_line);
                                }
                                add_array($2.number, NULL);
                            }
                        }
                | '[' varname ']' cstart
                        {
                            int i=constant($2);
                            if (i != -1) {
                                if ((gbl_allow_neg_column_size == 0) &&
                                    (constants[i].value < 0)) {
                                    csc2_error("ERROR AT LINE %3d: NEGATIVE ARRAY LENGTH\n",
                                        current_line);
                                    csc2_syntax_error("ERROR AT LINE %3d: NEGATIVE ARRAY LENGTH",
                                        current_line);
                                    any_errors++;
                                } else {
                                    if (constants[i].value < 0) {
                                        logmsg(LOGMSG_WARN, "CSC2: ERROR AT LINE %3d: NEGATIVE ARRAY LENGTH\n",
                                               current_line);
                                    }
                                    lastidx++;
                                    add_array(constants[i].value,
                                        constants[i].nm);
                                }
                            } else {
                                csc2_error("ARRAY ERROR AT LINE %3d: UNDEFINED CONSTANT\n", current_line);
                                csc2_syntax_error("ARRAY ERROR AT LINE %3d: UNDEFINED CONSTANT", current_line);
                                any_errors++;
                            }
                        }
                | /* %empty */
                ;


fltnumber:      T_FLOAT         { $$=yylval.fltpoint; }
                ;

number:        T_NUM        { $$=yylval.numstr; }
        ;

sqlhexstr:      T_SQLHEXSTR     { $$=yylval.bytestr; }
                ;
varname:    T_VARNAME
       {
            yylval.varname=csc2_strdup(yylval.varname);
            if (yylval.varname==0) {
              csc2_error("ERROR: OUT OF MEMORY: %s\n",yylval.comment);
              exit(-1);
            }
            $$=yylval.varname;
            } 
        ;

string:        T_STRING
            {
            char *str;
            str=(char*)csc2_malloc(strlen(yylval.opttext)+1);
            if (str==0) {
              csc2_error("ERROR: OUT OF MEMORY: %s\n",yylval.opttext);
              exit(-1);
            }
            strcpy(str, yylval.opttext+1);
            str[strlen(str)-1]=0;
            $$=str;
            }
                ;

comment:    T_COMMENT    
            {
            remem_com=(char*)csc2_malloc(yyleng+1);
            if (remem_com==0) {
              csc2_error("ERROR: OUT OF MEMORY: %s\n",yylval.comment);
              exit(-1);
            }
            memcpy(remem_com,yylval.comment,yyleng);
            remem_com[yyleng]=0;
            $$=remem_com;
            }

    | /* %empty */ {$$=blankchar;}
        ;

/* periodstruct: defines temporals
**	ie.
**	periods {
**		SYSTEM(sys_start, sys_end)
**		BUSINESS(bus_start, bus_end)
**	}
*/

periodstruct:	periodstart '{' multipddef '}'
        |
		;

periodstart:    T_PERIODS { start_periods_list(); }
        ;


multipddef:	pddef multipddef
		|   pddef
		;

pddef:  varname '(' varname ',' varname ')' comment
        {
            reset_array();
            reset_range();
            key_piece_clear();

            key_setdup();
            key_piece_add($3, 0);
            key_piece_add($5, 0);
            key_add_tag($1,0,0);
            key_piece_clear();

            add_period($1,$3,$5);
        }
    ;

/* keystruct: defines a key
**    ie.
**    keys {
**        0 = firm
**        1 = cust + term
**    }
*/

keystruct:    T_KEYS '{' multikeydef '}' 
        ;

multikeydef:    keydef multikeydef
        |        keydef
        ;

keydef:     multikeyflags string '=' compoundkey where comment
                            { 
                            key_add_tag($2,0,$5);
                            key_piece_clear(); 
                            }
		|	multikeyflags varname '=' compoundkey where comment
							{	/* conditional key */
							key_add_tag($2,0,$5);
							key_piece_clear();
							}
        |   multikeyflags string '(' typename ')' '=' compoundkey where comment
                            {    /* conditional key */
                            key_add_tag($2,$4,$8); 
                            key_piece_clear(); 
                            }
		|	multikeyflags varname '(' typename ')' '=' compoundkey where comment
							{	/* conditional key */
							key_add_tag($2,$4,$8);
							key_piece_clear();
							}
        ;


where:    T_WHERE
            {
            yylval.where=(char*)csc2_strdup(yylval.where);
            if (yylval.where==0) {
              csc2_error("ERROR: OUT OF MEMORY: %s\n",yylval.where);
              exit(-1);
            }
            $$=yylval.where;
            }
    | /* %empty */ {$$=blankchar;}
        ;

exprtype: '(' validctype ')'
          {
              key_exprtype_add($2, 0);
          }
        | '(' valididxstrtype '[' number ']' ')'
          {
              key_exprtype_add($2, $4.number);
          }
        ;

multikeyflags:    keyflags multikeyflags
        | /* %empty */
        ;


keyflags:    T_DUP        { key_setdup(); }
                | T_RECNUMS     { key_setrecnums(); }
                | T_PRIMARY     { key_setprimary(); }
                | T_DATAKEY '(' compounddatakey ')'     { key_setpartialdatakey(); }
                | T_DATAKEY     { key_setdatakey(); }
                | T_UNIQNULLS   { key_setuniqnulls(); }
        ;

compounddatakey: datakeypiece
    |   datakeypiece ',' compounddatakey
    ;

datakeypiece: varname {
                                                          datakey_piece_add($1);
                                                        };

compoundkey:    keypiece
        |        keypiece '+' compoundkey
        ;

keypiece:    varname    { 
                                                          key_piece_add($1, 0); 
                                                          reset_array(); 
                                                          reset_range();
                                                        }
        |    T_ASCEND varname { 
                                                          key_piece_add($2, 0); 
                                                          reset_array(); 
                                                          reset_range();
                                                }
        |    T_DESCEND varname { 
                                                  key_piece_setdescend(); 
                              key_piece_add($2, 0); 
                              reset_array();
                              reset_range();
                             }
        |    exprtype string {
                key_piece_add($2, 1);
                reset_key_exprtype();
                reset_array(); 
                reset_range();
            }
        |    T_ASCEND exprtype string {
                key_piece_add($3, 1);
                reset_key_exprtype();
                reset_array(); 
                reset_range();
            }
        |    T_DESCEND exprtype string {
                key_piece_setdescend();
                key_piece_add($3, 1);
                reset_key_exprtype();
                reset_array(); 
                reset_range();
            }
        ;


yesno:    T_YES        { $$=1; }
    |    T_NO        { $$=0; }
    ;
 
typename:        T_VARNAME    { 
                            char *name = (char*)csc2_malloc(yyleng+1);
                            if (!name) {
                                logmsgperror("typename");
                                exit(-1);
                            }
                            memcpy(name,yylval.varname,yyleng);
                            name[yyleng]=0;
                            $$=name;
                            }

%%

int yywrap()
{
parser_reset=1;
return 1;
}
