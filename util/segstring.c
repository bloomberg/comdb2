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

#include <segstring.h>
#include <f2cstr.h>

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#ifndef BUILDING_TOOLS
#include "mem_util.h"
#include "mem_override.h"
#endif

/*EXTRACT A LINE FROM DATA. HANDLES CRLF OR LF*/
/* RETURNS PTR TO LINE AND LENGTH. KEEPS NEXT POS IN OFFP.
   SIMILAR TO STRTOK EXCEPT DOES NOT WRITE NULL INTO BUFFER!
   ALSO TERMINATES AT \0 OR LDATA!
   ALSO returns null for end of buffer 09/18/2002
 */

char *segline(char *data, int ldata, int *offp, int *len)
{
    int start, off = *offp;
    if (off >= ldata || data[off] == 0) {
        *len = 0;
        return 0;
    }
    start = off;
    while (off < ldata) {
        if (data[off] == 0) {
            *offp = off;
            *len = off - start;
            if (*len == 0)
                return 0;
            return &data[start];
        }
        if (data[off] == '\n') {
            *offp = off + 1;
            if (off > start && data[off - 1] == '\r')
                off--;
            *len = off - start;
            return &data[start];
        }
        off++;
    }
    *offp = ldata;
    *len = ldata - start;
    if (*len == 0)
        return 0;
    return &data[start];
}

/*SAME AS SEGLINE, BUT STRIP LEADING SPACES*/
char *seglinel(char *data, int ldata, int *offp, int *lenp)
{
    char *line;
    int ii;
    line = segline(data, ldata, offp, lenp);
    if (line == 0)
        return 0;
    for (ii = 0; ii < *lenp; ii++) {
        if (line[ii] != ' ' && line[ii] != '\t')
            break;
    }
    (*lenp) -= ii; /*less leading spaces*/
    return &line[ii];
}

/*EXTRACT TOKENS FROM LINE. STRIPS WHITE SPACE*/
/*TERMINATES AT NEWLINE*/
char *segtok(char *data, int ldata, int *offp, int *len)
{
    int start, off = *offp;
    if (off >= ldata || data[off] == 0) {
        *len = 0;
        return data;
    }
    start = off;
    while (off < ldata) {
        if (data[start] == ' ' || data[start] == '\t') {
            /*STRIP LEFT SPACES*/
            start++;
            off++;
            continue;
        }
        switch (data[off]) {
        case 0:
        case ' ':
        case '\t':
        case '\n':
        case '\r': {
            *offp = off;
            *len = off - start;
            if (*len == 0)
                return data;
            return &data[start];
        }
        }
        off++;
    }
    *offp = ldata;
    *len = ldata - start;
    if (*len == 0)
        return data;
    return &data[start];
}

/*SAME AS ABOVE EXCEPT TERMINATES ON ADDITIONAL TOKENS.*/
char *segtokx(char *data, int ldata, int *offp, int *len, char *moresep)
{
    int start, off = *offp;
    if (off >= ldata || data[off] == 0) {
        *len = 0;
        return data;
    }
    start = off;
    while (off < ldata) {
        if (data[start] == ' ' || data[start] == '\t') {
            /*STRIP LEFT SPACES*/
            start++;
            off++;
            continue;
        }
        if (data[off] == 0) {
            *offp = off; /*point to null terminator*/
            *len = off - start;
            if (*len == 0)
                return data;
            return &data[start];
        } else if (strchr(moresep, data[off]) || strchr(" \t\n\r", data[off])) {
            *offp = off + 1; /*past seperator token*/
            *len = off - start;
            if (*len == 0)
                return data;
            return &data[start];
        }
        off++;
    }
    *offp = ldata;
    *len = ldata - start;
    if (*len == 0)
        return data;
    return &data[start];
}

/*SCAN FOR TOKENS, explicitly set left skip & token separators
  regular segtok would be same as   segtok_exp(data,ldata,offp,len," \t","
  \t\n\r");
*/
char *segtok_exp(char *data, int ldata, int *offp, int *len, char *left_skip,
                 char *all_separators)
{
    int start, off = *offp;
    if (off >= ldata || data[off] == 0) {
        *len = 0;
        return data;
    }
    start = off;
    while (off < ldata) {
        if (start == off && data[start] != 0 &&
            strchr(left_skip, data[start])) {
            /*STRIP LEFT SPACES*/
            start++;
            off++;
            continue;
        }
        if (data[off] == 0) {
            *offp = off; /*point to null terminator*/
            *len = off - start;
            if (*len == 0)
                return data;
            return &data[start];
        } else if (strchr(all_separators, data[off])) {
            *offp = off + 1; /*past separator token*/
            *len = off - start;
            if (*len == 0)
                return data;
            return &data[start];
        }
        off++;
    }
    *offp = ldata;
    *len = ldata - start;
    if (*len == 0)
        return data;
    return &data[start];
}

/*SCAN AHEAD FOR TOKEN.  RETURNS TOKEN IF FOUND, or 0 IF NOT.
  OFFSET IS RESTORED ON NOT FOUND CASE*/
char *segscan(char *data, int ldata, int *offp, int *lenp, char *findme)
{
    int offsav;
    char *tok;
    offsav = *offp;
    while (1) {
        tok = segtok(data, ldata, offp, lenp);
        if (*lenp == 0) {
            *offp = offsav; /*restore position on NOT FND case*/
            return 0;
        }
        if (tokcmp(tok, *lenp, findme) == 0) {
            return tok; /*FOUND*/
        }
    }
}

/*SCAN AHEAD FOR TOKEN.  uses segtokx RETURNS TOKEN IF FOUND, or 0 IF NOT.
  OFFSET IS RESTORED ON NOT FOUND CASE*/
char *segscanx(char *data, int ldata, int *offp, int *lenp, char *moresep,
               char *findme)
{
    int offsav;
    char *tok;
    offsav = *offp;
    while (1) {
        tok = segtokx(data, ldata, offp, lenp, moresep);
        if (*lenp == 0) {
            *offp = offsav; /*restore position on NOT FND case*/
            return 0;
        }
        if (tokcmp(tok, *lenp, findme) == 0) {
            return tok; /*FOUND*/
        }
    }
}

/* TOKEN COMPARE - DOES CASE INSENSITIVE COMPARE OF TOKEN TO
   WHOLE TARGET C STRING*/
int tokcmp(const char *tok, int ltok, const char *targ)
{
    int rc;
    if (ltok == 0) {
        if (targ[0] == 0)
            return 0; /*match, both no data*/
        return -1;    /*no match, if target has data*/
    }
    rc = strncasecmp(tok, targ, ltok);
    if (rc != 0)
        return rc;
    if (targ[ltok] != 0)
        return -1; /*target is greater*/
    return 0;
}

/* TOKEN COMPARE - DOES CASE INSENSITIVE COMPARE OF TOKEN TO
   TARGET C STRING.  PARTIAL MATCH OF C STRING OK.*/
int tokcmp_p(const char *tok, int ltok, const char *targ)
{
    int rc;
    if (ltok == 0) {
        if (targ[0] == 0)
            return 0; /*match, both no data*/
        return -1;    /*no match, if target has data*/
    }
    rc = strncasecmp(tok, targ, ltok);
    if (rc != 0)
        return rc;
    return 0;
}

/* RETURN STRTOL OF TOKEN */
int toknum(const char *tok, int ltok)
{
    char lnum[32];
    F2CSTRD(tok, ltok, lnum);
    return strtol(lnum, 0, 0);
}
/* RETURN STRTOL OF TOKEN */
int toknumbase(const char *tok, int ltok, int base)
{
    char lnum[32];
    F2CSTRD(tok, ltok, lnum);
    return strtol(lnum, 0, base);
}

/* RETURN STRTOLL OF TOKEN */
long long toknumll(const char *tok, int ltok)
{
    char lnum[64];
    F2CSTRD(tok, ltok, lnum);
    return strtoll(lnum, 0, 0);
}

/* RETURN STRTOLL OF TOKEN, specify base */
long long toknumllbase(const char *tok, int ltok, int base)
{
    char lnum[64];
    F2CSTRD(tok, ltok, lnum);
    return strtoll(lnum, 0, base);
}

unsigned int tokunum(const char *tok, int ltok)
{
    char lnum[32];
    F2CSTRD(tok, ltok, lnum);
    return strtoul(lnum, 0, 0);
}

double toknumd(const char *tok, int ltok)
{
    char lnum[32];
    F2CSTRD(tok, ltok, lnum);
    return strtod(lnum, 0);
}

/* STRDUP A TOKEN */
char *tokdup(const char *tok, int ltok)
{
    char *dup = malloc(ltok + 1);
    if (dup == 0)
        return 0;
    memcpy(dup, tok, ltok);
    dup[ltok] = 0;
    return dup;
}

/* COPY FROM TOKEN TO C STRING */
char *tokcpy(const char *tok, int ltok, char *dest)
{
    memcpy(dest, tok, ltok);
    dest[ltok] = 0;
    return dest;
}

/* SAFE tokcpy() WHICH GUARANTEES A C STRING.  WILL TRUNCATE TOKEN IF
 * NECESSARY. */
char *tokcpy0(const char *tok, size_t ltok, char *dest, size_t ldest)
{
    if (ldest == 0 || !dest || !tok)
        return NULL;
    if (ltok >= ldest)
        ltok = ldest - 1;
    memcpy(dest, tok, ltok);
    dest[ltok] = 0;
    return dest;
}

#ifdef SEGSTRING_TESTDRIVER

#include <assert.h>

static int test_segtok_exp(void)
{
    static const char *line = "ABC| DE|1234|\n\000???";
    enum { MAX_FIELDS = 10, MAX_STR_LEN = 128 };
    char *tok[MAX_FIELDS];
    int ltok[MAX_FIELDS];
    int expected_ltok[MAX_FIELDS] = {3, 2, 4, 1, 0};
    int off = 0;

    // Tokenize line passing real length
    for (int ii = 0; ii < MAX_FIELDS; ii++) {
        tok[ii] = segtok_exp(line, strlen(line), &off, &ltok[ii], " \t", "|");
        printf("FIELD #%d LEN %d: '%.*s'\n", ii, ltok[ii], ltok[ii], tok[ii]);
        assert(tok[ii] != NULL && ltok[ii] == expected_ltok[ii]);
    }

    // Test again, but this time pass in an arbitrary upper-bound length
    // (Null-terminator should still be used)
    off = 0;
    printf("\n");
    for (int ii = 0; ii < MAX_FIELDS; ii++) {
        tok[ii] = segtok_exp(line, MAX_STR_LEN, &off, &ltok[ii], " \t", "|");
        printf("FIELD #%d LEN %d: '%.*s'\n", ii, ltok[ii], ltok[ii], tok[ii]);
        assert(tok[ii] != NULL && ltok[ii] == expected_ltok[ii]);
    }

    return 0;
}

static int test_segtokx(void)
{
    static const char *line = "ABC| DE|1234|A\000???";
    enum { MAX_FIELDS = 10, MAX_STR_LEN = 128 };
    char *tok[MAX_FIELDS];
    int ltok[MAX_FIELDS];
    int expected_ltok[MAX_FIELDS] = {3, 2, 4, 1, 0};
    int off = 0;

    // Tokenize line passing real length
    for (int ii = 0; ii < MAX_FIELDS; ii++) {
        tok[ii] = segtokx(line, strlen(line), &off, &ltok[ii], "|");
        printf("FIELD #%d LEN %d: '%.*s'\n", ii, ltok[ii], ltok[ii], tok[ii]);
        assert(tok[ii] != NULL && ltok[ii] == expected_ltok[ii]);
    }

    // Test again, but this time pass in an arbitrary upper-bound length
    // (Null-terminator should still be used)
    off = 0;
    printf("\n");
    for (int ii = 0; ii < MAX_FIELDS; ii++) {
        tok[ii] = segtokx(line, MAX_STR_LEN, &off, &ltok[ii], "|");
        printf("FIELD #%d LEN %d: '%.*s'\n", ii, ltok[ii], ltok[ii], tok[ii]);
        assert(tok[ii] != NULL && ltok[ii] == expected_ltok[ii]);
    }

    return 0;
}

int main(int argc, char **argv)
{
    test_segtok_exp();
    test_segtokx();
}

#endif
