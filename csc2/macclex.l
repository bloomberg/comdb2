%{

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


#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <ctype.h>
#include <string.h>
#include "maccparse.h"
#include "logmsg.h"
extern int current_line;
extern YYSTYPE yylval;
static int    rectplvl=0;
extern int    parser_reset;
int    charidx=0;

#define YY_ALWAYS_INTERACTIVE 1

/* This doesn't work on LINUX, and isatty will always return a 0 if comdb2 is 
   invoked from comdb2ar.tsk.  So we're continuing down this slippery slope.  */
int macc_isatty(int fd);
#define isatty(fd) macc_isatty(fd)

/* prevents calling istty on yyin */
/* This is the mother of all hacks - to get lex reading from a character
   array, I override getc so that when it calls getc(yyin), it really calls
   my own function. -- SJ */
int macc_getc(FILE *);

#ifdef getc
#undef getc
#endif
#define getc(fh) macc_getc(fh)

#ifdef fread
#undef fread
#endif
#define fread macc_fread
int macc_fread(void *ptr, size_t size, size_t nmemb, FILE *stream);


void yyerror(const char *msg);

extern void *csc2_malloc(size_t size);
extern char *csc2_strdup(char*);
extern void csc2_error(const char *fmt, ...);
void csc2_syntax_error(const char *fmt, ...);

/* Now we're piling on more hacks to support this "yyin is really a file 
 * descriptor nonsense, god help us all */
extern int macc_ferror(FILE *fh);
#define ferror(fh) macc_ferror(fh)
%}
%e 10000
%p 10000
%n 1000
/* Adding \r here would also deal with Windows-style line endings,
 * essentially converting \r\n into \n.
 */
space   [\t \r]+
number  -?[0-9]+
dqstring \"(\\.|[^\n\\\"])*\"
sqstring \'(\\.|[^\n\\\'])*\'
where [{][\t ]*[Ww][Hh][Ee][Rr][Ee][\t ]({sqstring}|{dqstring}|[^}\"\'])*[\t ]*[}]
varname [a-zA-Z_][a-zA-Z_0-9]*
comment \/\/.*
fortran_comment \!.*
fltnumber -?([0-9])+"."([0-9])+
/*hexnumber 0[xX][0-9a-fA-F]{1,8}*/
hexbyte 0[xX][0-9a-fA-F]{1,2}
hexstring 0[Xx]([0-9a-fA-F]{1,2})+
sqlhexstr x{space}*"'"{space}*([a-fA-F0-9])*{space}*"'"
nonclosingbrkt [^}]*

%s RECTYPE
%s DBSTORE
%s DBSTORE_FUNC
%s CONSTRAINTS

%%
              if (parser_reset)
               {
                 BEGIN(INITIAL);
                 parser_reset=0;
                 rectplvl = 0;
               }

<INITIAL,RECTYPE>^[\t ]*{comment}$      /* ignore full line comment */ ;
<INITIAL,CONSTRAINTS,RECTYPE,DBSTORE>{space}            /* ignore whitespace */ ;
<INITIAL,CONSTRAINTS,RECTYPE>{comment}          ; /*{ yylval.comment = yytext; return T_COMMENT; } ignore more comments */
<INITIAL,RECTYPE>"/*"                   {
                                   register int c;

                                   for ( ; ; )
                                   {
                                     while ( (c = input()) != '*' &&  c != EOF );    /* eat up text of comment */
                                     if ( c == '*' )
                                     {
                                       while ( (c = input()) == '*' ) ; /* eat up '*' character */
                                       
                                       if ( c == '/' )
                                         break;    /* found the end */
                                     }

                                     if ( c == EOF )
                                     {
                                       logmsgperror( "EOF in comment" );
                                       break;
                                     }
                                   }
                                }

<RECTYPE>{sqlhexstr}            { 
                                  char * data=NULL;
                                  int i=0, slen=0, j=sizeof(int);
                                  char   bdata[3];
                                  data=strchr(yytext, (int)'\'');
                                  if (data==NULL)
                                   {
                                      yyerror("bad hex string");
                                      return 0;
                                   }
                                  
                                  while (*data=='\''||isspace(*data)) data++;

                                  slen=strlen(data);
              
                                  /* clear out the last (') */

                                  for (i=slen-1;data[i]=='\''||isspace(data[i]);i--) 
                                       {  data[i]=0; }

                                  /* get new length again */
                                  slen=strlen(data);
                                  if (slen < 0)
                                  {
                                     /* hex data has bad length..must be divisible by 2 (each byte) */
                                     yyerror("hex string has bad length");
                                     return 0;
                                  } 
                                  yylval.bytestr=(char*)csc2_malloc((slen+1)/2+sizeof(int));
                                  if (yylval.bytestr==NULL)
                                  {
                                     yyerror("out of memory");
                                     return 0;
                                  }
                                  bdata[2]=0;
                                  for (i=0,j=sizeof(int);i<slen;i+=2,j++)
                                  {
                                    if ((i+2)>slen)
                                      {
                                        /*if (slen==1)
                                          {
                                            bdata[0]='0';
                                            bdata[1]=data[i];
                                          }
                                        else*/
                                          {
                                            bdata[0]=data[i];
                                            bdata[1]='0';
                                          }
                                          yylval.bytestr[j]=(unsigned char)strtol(bdata,NULL,16);
                                          j++;
                                          /*fsnapf(stderr, yylval.bytestr,j); */
                                          j-=sizeof(int);
                                          memcpy(&yylval.bytestr[0], &j, sizeof(int));
                                          return T_SQLHEXSTR;
                                      }
                                    else
                                      {
                                         bdata[0]=data[i];
                                         bdata[1]=data[i+1];
                                      }
                                    yylval.bytestr[j]=(unsigned char)strtol(bdata,NULL,16);
                                  }
                                  j-=sizeof(int);
                                  /*fprintf(stderr, "malloc bytestr %x %d\n", yylval.bytestr,j);
                                  fsnapf(stderr, yylval.bytestr+sizeof(int), j); */
                                  memcpy(&yylval.bytestr[0], &j, sizeof(int));
                                  return T_SQLHEXSTR; 
                                }        

<INITIAL,CONSTRAINTS,RECTYPE>\n                 { current_line++; }

<INITIAL,RECTYPE>{number}       { yylval.numstr.number=atoi(yytext); yylval.numstr.numstr = csc2_strdup(yytext); return T_NUM; }
<INITIAL,RECTYPE>{fltnumber}            { yylval.fltpoint=(double)strtod(yytext,NULL); return T_FLOAT;}
<INITIAL,RECTYPE>short                  { return T_INTEGER2; }
<INITIAL,RECTYPE>u_short                { return T_UINTEGER2; }
<INITIAL,RECTYPE>int                    { return T_INTEGER4; }
<INITIAL,RECTYPE>u_int                  { return T_UINTEGER4;}
<INITIAL,RECTYPE>byte                   { return T_UCHAR; }
<INITIAL,RECTYPE>longlong               { return T_LONGLONG; }
<INITIAL,RECTYPE>u_longlong             { return T_ULONGLONG; }
<INITIAL,RECTYPE>u_long                 { return T_ULONG; }
<INITIAL,RECTYPE>float                  { return T_REAL4; }
<INITIAL,RECTYPE>double                 { return T_REAL8; }
<INITIAL,RECTYPE>bool                   { return T_LOGICAL; }
<INITIAL,RECTYPE>cstring                { return T_CSTR; }
<INITIAL,RECTYPE>pstring                { return T_PSTR; }
<INITIAL,RECTYPE>blob                   { return T_BLOB; }
<INITIAL,RECTYPE>vutf8                  { return T_VUTF8; }
<INITIAL,RECTYPE>datetime       { return T_DATETIME; }
<INITIAL,RECTYPE>datetimeus     { return T_DATETIMEUS; }
<INITIAL,RECTYPE>intervalym     { return T_INTERVALYM; }
<INITIAL,RECTYPE>intervalds     { return T_INTERVALDS; }
<INITIAL,RECTYPE>intervaldsus   { return T_INTERVALDSUS; }
<INITIAL,RECTYPE>decimal32      { return T_DECIMAL32; }
<INITIAL,RECTYPE>decimal64      { return T_DECIMAL64; }
<INITIAL,RECTYPE>decimal128     { return T_DECIMAL128; }

<INITIAL>dup                    { return T_DUP; }
<INITIAL>recnums                { return T_RECNUMS; }
<INITIAL>datacopy               { return T_DATAKEY; }
<INITIAL>primary                { return T_PRIMARY; }
<INITIAL>uniqnulls              { return T_UNIQNULLS; }


<RECTYPE>dbpad                  { return T_FLD_PADDING; }
<RECTYPE>dbstore                { BEGIN DBSTORE; return T_FLD_STRDEFAULT;}
<DBSTORE>[=]                    { return yytext[0]; }
<DBSTORE>"{"                    { BEGIN DBSTORE_FUNC; return yytext[0]; }
<DBSTORE>.                      { BEGIN RECTYPE; unput( yytext[0] );}
<DBSTORE_FUNC>"}"               { BEGIN RECTYPE; return yytext[0]; }
<DBSTORE_FUNC>{nonclosingbrkt}  { yylval.opttext=yytext; return T_STRING; }
<RECTYPE>nextsequence           { return T_FLD_NEXTSEQUENCE;}
<RECTYPE>dbload                 { return T_FLD_LDDEFAULT;}
<RECTYPE>null                   { return T_FLD_NULL;}

<INITIAL,RECTYPE>[Yy][Ee][Ss]           { return T_YES; }
<INITIAL,RECTYPE>[Nn][Oo]               { return T_NO; }

<INITIAL>constants              { return T_CONSTANTS; }
<INITIAL>keys                   { return T_KEYS; }
<INITIAL>tag                    { BEGIN RECTYPE;  rectplvl=0;  return T_TABLE_TAG; }
<INITIAL,RECTYPE>schema         {
                                  if (rectplvl > 0) {
                                    yylval.varname = yytext;
                                    return T_VARNAME;
                                  } else {
                                      BEGIN RECTYPE;
                                      return T_SCHEMA;
                                  } }
<RECTYPE>default                { return T_DEFAULT; }
<RECTYPE>ondisk                 { return T_ONDISK; }
<INITIAL>periods                { return T_PERIODS; }
<INITIAL>constraints            { BEGIN CONSTRAINTS; return T_CONSTRAINTS; }
<INITIAL,CONSTRAINTS,RECTYPE>{where} { char *p = yytext;
                                char *end= NULL;
                                for(++p; *p == '\t' || *p == ' '; ++p);
                                yylval.where = csc2_strdup(p);
                                p = yylval.where;
                                for(p = p + strlen(yylval.where); *p != '}' && p > yylval.where; --p);
                                if (p == yylval.where) {
                                    yyerror("where expression: missing '}'");
                                    return 0;
                                }
                                end = p;
                                for(++p; *p == '\t' || *p == ' '; ++p);
                                if (*p != '\0') {
                                    /* check comment */
                                    if (*p != '/') {
                                        yyerror("where expression: syntax error after '}'");
                                        return 0;
                                    }
                                    else {
                                        ++p;
                                        if (*p != '/') {
                                            yyerror("where expression: syntax error after '}'");
                                            return 0;
                                        }
                                    }
                                }
                                end[0] = '\0';
                                return T_WHERE;
                              }
<INITIAL,RECTYPE>{varname}              { yylval.varname=yytext; return T_VARNAME; }

<INITIAL,CONSTRAINTS,RECTYPE>{dqstring}         { yylval.opttext=yytext; return T_STRING; }
<INITIAL,CONSTRAINTS>"<"                        { return T_LT; }
<INITIAL,CONSTRAINTS>">"                        { return T_GT; }

<INITIAL>"<ASCEND>"             { return T_ASCEND; }
<INITIAL>"<DESCEND>"            { return T_DESCEND; }

<CONSTRAINTS>cascade            { return T_CASCADE; }
<CONSTRAINTS>restrict           { return T_RESTRICT; }
<CONSTRAINTS>check              { return T_CHECK; }
<CONSTRAINTS>on                 { return T_CON_ON; }
<CONSTRAINTS>update             { return T_CON_UPDATE; }
<CONSTRAINTS>delete             { return T_CON_DELETE; }
<CONSTRAINTS>"no_overlap"       { return T_CON_NOOVERLAP; }
<CONSTRAINTS>set                { return T_SET; }
<CONSTRAINTS>null               { return T_NULL; }
<CONSTRAINTS>[\-:]              { return yytext[0];  }
<CONSTRAINTS>{varname}          { yylval.varname=yytext; return T_VARNAME; }
<CONSTRAINTS>=                  { return T_EQ; }
<INITIAL,RECTYPE,CONSTRAINTS>[\{]                   { rectplvl++; return yytext[0]; }
<INITIAL,RECTYPE,CONSTRAINTS>[\}]                   { 
                                  rectplvl--; 
                                  if (rectplvl<=0)
                                   {
                                      BEGIN 0;
                                      return yytext[0];
                                   }
                                  else
                                   return yytext[0]; 
                                }
<RECTYPE>[\[\]_=]              { return yytext[0]; }
<RECTYPE>.                      { BEGIN 0; unput( yytext[0] ); }
<INITIAL>.                      { return yytext[0]; }
<CONSTRAINTS>.                  { BEGIN 0; unput( yytext[0] ); }

%%
void yyerror(const char *msg)
{
    parser_reset=1;
    csc2_error("ERROR at line %3d: %s: %s\n",current_line, msg, yytext);
    csc2_syntax_error("ERROR at line %3d: %s: %s",current_line, msg, yytext);
    rectplvl = 0;

#ifdef FLEX_SCANNER
    YY_FLUSH_BUFFER;
#endif
}
