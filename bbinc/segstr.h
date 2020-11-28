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

#ifndef INCLUDED_SEGSTRING
#define INCLUDED_SEGSTRING

/* segline  - return ptr to line and length.  updates offset so you
**            call in loop.  EOL is at \n or \r\n.
** seglinel - same as segline, but strips leading spaces/tabs.
** segtok   - return ptr to token. updates offset so you can call in loop.
**            token is bounded by ' ' \t \n \r
** segtokx  - same as segtok but you can pass additional seperators.
** segtok_exp-segtok, but you specify left skip & token separators.
**            segtok would be same as segtok_exp with left_skip=" \t" &
*all_separators=" \t\n\r"
** segscan  - scans all tokens for string match, uses segtok
** segscanx - scans all tokens for string match, uses segtokx
**
** INPUT: data  = pointer to base of buffer
**        ldata = overall length of data in buffer
**        *offp = starting offset in data.(usually 0 to start)
**        moresep = string with other seperators.
** OUTPUT: RETURNS pointer to data
**        *len  = length of token.  0 length means no more.
**
**
** tokcmp - case insensitive compare of token to target string.
**          token must match whole string.  returns <0 less, >0 greater,
**          and 0 for match.
**
** tokcmp_p - case insensitive compare of token to target string.
**          token can match partial string.  returns <0 less, >0 greater,
**          and 0 for match.
**
** toknum     - return token converted to int.
** toknumll   - return token converted to long long
** toknumbase - return token converted to int, specify base.
** toknumllbase - return token converted to long long, specify base
** tokunum    - return token converted to unsigned int.
** toknumd    - return token converted to double.
**
** tokdup  - strdup a token.
**
** tokcpy  - copy from token to c string.  returns dest*
** tokcpy0 - copy from token to c string, with destination length check
**
** (this lot used to be in pl/segstr.h)
*/

#include <stdlib.h>

#if defined __cplusplus
extern "C" {
#endif

char *segline(char *data, int ldata, int *offp, int *lenp);
char *seglinel(char *data, int ldata, int *offp, int *lenp);
char *segtok(char *data, int ldata, int *offp, int *lenp);
char *segtok2(char *data, int ldata, int *offp, int *lenp);
char *segtokx(char *data, int ldata, int *offp, int *lenp, char *moresep);
char *segtok_exp(char *data, int ldata, int *offp, int *len, char *left_skip,
                 char *all_separators);
char *segscan(char *data, int ldata, int *offp, int *lenp, char *findme);
char *segscanx(char *data, int ldata, int *offp, int *lenp, char *moresep,
               char *findme);
int tokcmp(const char *tok, int ltok, const char *targ);
int tokcmp_p(const char *tok, int ltok, const char *targ);
int toknum(const char *tok, int ltok);
int toknumbase(const char *tok, int ltok, int base);
unsigned int tokunum(const char *tok, int ltok);
double toknumd(const char *tok, int ltok);
long long toknumll(const char *tok, int ltok);
long long toknumllbase(const char *tok, int ltok, int base);
char *tokdup(const char *tok, int ltok);
char *tokcpy(const char *tok, int ltok, char *dest);
char *tokcpy0(const char *tok, size_t ltok, char *dest, size_t ldest);

#if defined __cplusplus
}
#endif

#endif
