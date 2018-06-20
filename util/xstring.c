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

#include <xstring.h>

#include <ctype.h>
#include <limits.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <logmsg.h>

/*	----------------------------------------------------------------------
 */
/*	Statics
 */
/*	----------------------------------------------------------------------
 */
static const char szWhiteChars[] = " \t\n\r"; /*	Default white chars.
                                                 */
#define _is_hex_digit(x)                                                       \
    (((x) >= '0' && (x) <= '9') || ((x) >= 'a' && (x) <= 'f') ||               \
     ((x) >= 'A' && (x) <= 'F'))

/*****************************************************************************
DESCRIPTION:
        Converts C-escape characters in string.

                decode:		Converts escape characters to ASCII.
                encode:		Converts unprintable characters to
C-escape sequences.
                                        The cstrn_escape_encode() version
encodes up to nSize
                                        characters.

        Ps1 may not necessarily be terminated with the null character if the
        converted string exceeds the given size.

REQUIRES:
        ps1			The destination string.
        ps2			The string to convert.
        nSize		The maximum length of ps1.

        cnp1		Length of ps1.
        cnp2		Length of ps2.

MODIFIES:
        ps1			The destination string.

RETURNS:
        The number of characters not including the terminating null.

HISTORY:
        04/28/97	Created.
        08/08/97	\nnn should be interpreted as octal.
        09/11/97	Added cstrn_escape_encode().

NOTES:
*****************************************************************************/
static size_t cstr_escape_encode_ext(char *ps1, size_t cnp1, const char *ps2,
                                     size_t cnp2, int bExitAtNull)
{
    CPSTR psi = ps2;
    char *pso = ps1;

    if (ps2) {
        for (; cnp2 && cnp1; psi++, cnp2--) {
            if ('\\' == *psi && 2 <= cnp1) {
                strcpy(pso, "\\\\");
                pso += 2;
                cnp1 -= 2;
            } else if ('\'' == *psi || '"' == *psi) {
                if (2 <= cnp1) {
                    *pso++ = '\\';
                    *pso++ = *psi;
                    cnp1 -= 2;
                } else {
                    *pso++ = *psi;
                    cnp1--;
                }
            } else if (isprint(*psi)) {
                *pso++ = *psi;
                cnp1--;
            } else {
                size_t n;
                CPSTR psx;
                char sz[6];

                switch (*psi) {
                case '\0':
                    if (bExitAtNull)
                        goto EXIT;
                    psx = "\\0";
                    break;

                case '\007':
                    psx = "\\a";
                    break;
                case '\b':
                    psx = "\\b";
                    break;
                case '\f':
                    psx = "\\f";
                    break;
                case '\n':
                    psx = "\\n";
                    break;
                case '\r':
                    psx = "\\r";
                    break;
                case '\t':
                    psx = "\\t";
                    break;
                case '\v':
                    psx = "\\v";
                    break;
                default:
                    sprintf(sz, "\\%03o", (unsigned char)*psi);
                    psx = sz;
                }
                if (cnp1 < (n = strlen(psx))) {
                    break; /*	Done!
                              */
                }
                strcpy(pso, psx);
                pso += n;
                cnp1 -= n;
            }
        }
    }
EXIT:
    if (cnp1)
        *pso = '\0';  /*	Null terminate if still have room.	*/
    return pso - ps1; /*	Length of the converted string.     */
}

/*	----------------------------------------------------------------------
 */
size_t cstr_escape_encode(char *ps1, const char *ps2, size_t nSize)
{
    return cstr_escape_encode_ext(ps1, nSize, ps2, nSize, true);
}

/*	----------------------------------------------------------------------
 */
size_t cstrn_escape_encode(char *ps1, size_t cnp1, const char *ps2, size_t cnp2)
{
    return cstr_escape_encode_ext(ps1, cnp1, ps2, cnp2, false);
}
/*	----------------------------------------------------------------------
 */
size_t cstr_escape_decode(char *ps1, const char *ps2, size_t nSize)
{
    CPSTR psi = ps2;
    char *pso = ps1;

    if (ps2) {
        for (; *psi && nSize; pso++, nSize--) {
            if ('\\' == *psi) {
                psi++;

                if (isdigit(*psi)) /*	\nnn
                                      */
                {
                    int i;
                    char sz[4];

                    for (i = 0; isdigit(*psi) && (i < 3); i++, psi++) {
                        sz[i] = *psi;
                    }
                    sz[i] = '\0';
                    *pso = (char)strtoul(sz, NULL,
                                         8); /*	atoi(sz);		*/
                } else {
                    switch (*psi) {
                    case 'a':
                        *pso = '\007';
                        break;
                    case 'b':
                        *pso = '\b';
                        break;
                    case 'f':
                        *pso = '\f';
                        break;
                    case 'n':
                        *pso = '\n';
                        break;
                    case 'r':
                        *pso = '\r';
                        break;
                    case 't':
                        *pso = '\t';
                        break;
                    case 'v':
                        *pso = '\v';
                        break;
                    default:
                        *pso = *psi;
                        break;
                    }
                    psi++;
                }
            } else {
                *pso = *psi++;
            }
        }
    }
    if (nSize)
        *pso = '\0';
    return pso - ps1;
}
/*****************************************************************************
DESCRIPTION:
        Converts a C-string to a FORTRAN string in-place.

REQUIRES:
        ps			The string.
        n			The maximum length of the FORTRAN string.

MODIFIES:
        (none)

RETURNS:
        (none)

HISTORY:
        06/26/98	Created.

NOTES:
*****************************************************************************/
void ctofstr(char *ps, int n)
{
    int i = strlen(ps);
    if (i < n)
        memset(ps + i, ' ', n - i);
}
/*****************************************************************************
DESCRIPTION:
        FORTRAN to C string copy.

REQUIRES:
        ps1			Destination.
        n1			Length of 'C' string (position n1 guaranteed
to be nulled).
        ps2			FORTRAN string.
        n2			Length of FORTRAN string.

MODIFIES:
        (none)

RETURNS:
        (none)

HISTORY:
        03/23/98	Created.
        04/16/98	fstrlen off by one calculating strlen.
        04/17/98	fstrcpy now uses fstrlen.
        06/26/98	Moved from fixfapi.c

NOTES:
*****************************************************************************/
char *fstrcpy(char *ps1, const char *ps2, int n2)
{
    n2 = fstrlen(ps2, n2);
    strncpy(ps1, ps2, n2);
    ps1[n2] = '\0';
    return ps1;
}

/*	----------------------------------------------------------------------
 */
char *fstrncpy(char *ps1, size_t n1, const char *ps2, int n2)
{
    n2 = fstrlen(ps2, n2);
    n1 = n1 < n2 ? n1 : n2;
    strncpy(ps1, ps2, n1);
    ps1[n1] = '\0';
    return ps1;
}

/*	----------------------------------------------------------------------
 */
size_t fstrlen(const char *ps, int n)
{
    const char *psz;

    for (psz = ps + n; ps < psz;) {
        switch (*--psz) {
        case '\0': /*	FORTRAN white-space characters.
                      */
        case ' ':
            break;

        default:
            return psz - ps + 1;
        }
    }
    return 0;
}

/*	----------------------------------------------------------------------
 */
size_t fstrtoc(char *ps, int n)
{
    n = fstrlen(ps, n);
    ps[n] = '\0';
    return n;
}
/*****************************************************************************
DESCRIPTION:
        Memory duplication similar to strdup().

REQUIRES:
        p			Pointer to data to duplicate.
        n			Amount of data to duplicate.

MODIFIES:
        (none)

RETURNS:
        Pointer to duplicated malloced memory.

HISTORY:
        08/07/96	Created.

NOTES:
*****************************************************************************/
void *memdup(const void *p, size_t n)
{
    void *pnew = malloc(n);

    return (pnew) ? memcpy(pnew, p, n) : NULL;
}
/*****************************************************************************
DESCRIPTION:
        Finds the next word in the string

REQUIRES:
        p		A pointer to the string

MODIFIES:
        (none)

RETURNS:
        A pointer to the next word in the string.  If next word is not found,
        retunrs NULL

HISTORY:
        04/02/98	Created.

NOTES:
*****************************************************************************/
char *nextword(char *p)
{
    int len;

    if (!p)
        return NULL;
    len = strspn(p, " ");
    p += len;
    if (!*p)
        return NULL;
    len = strcspn(p, " ");
    p += len;
    if (!*p)
        return NULL;
    len = strspn(p, " ");
    p += len;
    if (!*p)
        return NULL;
    return p;
}
/*****************************************************************************
DESCRIPTION:
        Does nothing, i.e., does not free or copy the data block.

        Use these as copy constructors and destructor.

REQUIRES:
        mem			A pointer.

MODIFIES:
        (none)

RETURNS:
        (none)

HISTORY:
        08/08/96	Created.

NOTES:
*****************************************************************************/
void *no_copy(const void *mem) { return (void *)mem; }

/*	----------------------------------------------------------------------
 */
/*ARGSUSED*/
void no_free(void *mem) { return; }
/*****************************************************************************
DESCRIPTION:
        Concatenates a character.

REQUIRES:
        s			The string.
        c			The character.

MODIFIES:
        (none)

RETURNS:
        Pointer to the string.

HISTORY:
        12/12/96	Created.

NOTES:
*****************************************************************************/
char *strcatc(char *s, char c)
{
    if (s && c) {
        char *p = s + strlen(s);
        *p = c;
        *++p = '\0';
    }
    return s;
}
/*****************************************************************************
DESCRIPTION:
        Copies characters from s2 to s1 until c is found or n characters have
        been copied.

        The characters 'c' is not copied. s1 will be null terminated.

REQUIRES:
        s1			Copy to.
        s2			Copy from.
        c			Delimiting character.
        n			Max characters to copy.

MODIFIES:
        s1

RETURNS:
        s1

HISTORY:
        08/09/96	Created.

NOTES:
*****************************************************************************/
char *strccpyz(char *s1, const char *s2, int c, size_t n)
{
    char *p;

    if (!s2) {
        *s1 = '\0';
        return s1;
    }

    if (!s1) {
        return NULL;
    }

    p = memccpy(s1, s2, c, n);

    if (p)
        *(p - 1) = '\0';
    else
        *(s1 + n) = '\0';

    return s1;
}
/*****************************************************************************
DESCRIPTION:
        Empty the string.

REQUIRES:
        str			The string.

MODIFIES:
        (none)

RETURNS:
        (none)

HISTORY:
        12/23/98	Created.

NOTES:
*****************************************************************************/
char *strempty_(char *str)
{
    if (str)
        *str = 0;
    return str;
}
/*****************************************************************************
DESCRIPTION:
        Copies a C-string to a FORTRAN-string.

REQUIRES:
        ps1			The FORTRAN-string.
        ps2			The C-string.
        n			The maximum length of the FORTRAN-string.

MODIFIES:
        (none)

RETURNS:
        (none)

HISTORY:
        06/26/98	Created.

NOTES:
*****************************************************************************/
void strfcpy(char *ps1, const char *ps2, int n)
{
    char *psz = memccpy(ps1, ps2, 0, n);

    if (psz) {
        psz--;
        memset(psz, ' ', n - (psz - ps1));
    }
}
/*****************************************************************************
DESCRIPTION:
        String hash from K&R. Use strihash for case insensitive hash.

REQUIRES:
        s			The string.
        hashsize	The hashsize.

MODIFIES:
        (none)

RETURNS:
        Hashvalue 0..(hashsize-1)

HISTORY:
        08/05/96	Created.
        04/11/97	Added strihash.

NOTES:
*****************************************************************************/
unsigned strhash(const char *s, unsigned hashsize)
{
    unsigned hash;

    for (hash = 0; *s; s++) {
        hash = *s + 31 * hash;
    }
    return hash % hashsize;
}

/*	----------------------------------------------------------------------
 */
unsigned strihash(const char *s, unsigned hashsize)
{
    unsigned hash;

    for (hash = 0; *s; s++) {
        hash = toupper(*s) + 31 * hash;
    }
    return hash % hashsize;
}
/*****************************************************************************
DESCRIPTION:
        Case insensitive string comparison similar to strcmp(3C) and
strncmp(3C).

REQUIRES:
        s1		The first string.
        s2		The second string.

MODIFIES:
        (none)

RETURNS:
        -1		s1 <  s2
         0		s1 == s2
        +1		s1 >  s2

HISTORY:
        07/26/96	Created.
        01/07/97	Added strnicmp()
        11/24/97	Return the difference (d) between (a) and (b).

NOTES:
*****************************************************************************/
int xstricmp(const char *s1, const char *s2)
{
    int a;
    int b;
    int d;

    for (;;) {
        a = toupper((unsigned char)*s1);
        b = toupper((unsigned char)*s2);
        d = a - b;

        if (d)
            return d; /*	d != 0 => a != b
                         */
        if (!a)
            return 0; /*	If !a then a == b == 0 => matched!
                         */

        s1++;
        s2++;
    }
}

/*	----------------------------------------------------------------------
 */
int xstrnicmp(const char *s1, const char *s2, size_t n)
{
    int a;
    int b;
    int d;

    for (; n; n--) {
        a = toupper((unsigned char)*s1);
        b = toupper((unsigned char)*s2);
        d = a - b;

        if (d)
            return d; /*	a != b
                         */
        if (!a)
            return 0; /*	If !a then a == b == 0 => matched!
                         */

        s1++;
        s2++;
    }
    return 0; /*	Matched so far.
                 */
}
/*****************************************************************************
DESCRIPTION:
        Returns the 0-based index position of the character in the string.

REQUIRES:
        ps			The string.
        c			The char.

MODIFIES:
        (none)

RETURNS:
        -1			Not found.
        0..n		The index.

HISTORY:
        06/25/98	Created.

NOTES:
*****************************************************************************/
int strindex(const char *ps, int c)
{
    const char *pc = strchr(ps, c);
    return pc ? (int)(pc - ps) : -1;
}

/*	----------------------------------------------------------------------
 */
int strrindex(const char *ps, int c)
{
    const char *pc = strrchr(ps, c);
    return pc ? (int)(pc - ps) : -1;
}
/*****************************************************************************
DESCRIPTION:
        Returns true if the string is blank, i.e., only white spaces.

REQUIRES:
        psz			The string.

MODIFIES:
        (none)

RETURNS:
        Non-0 if blank.

HISTORY:
        05/15/97	Created.

NOTES:
*****************************************************************************/
int strisblank(const char *psz)
{
    if (psz) {
        while (*psz) {
            if (!isspace(*psz))
                return 0;
            psz++;
        }
    }
    return 1;
}
/*****************************************************************************
DESCRIPTION:
        Returns true if the string is empty or NULL, i.e., no characters.

REQUIRES:
        psz			The string.

MODIFIES:
        (none)

RETURNS:
        Non-zero if empty.

HISTORY:
        10/22/96	Created.

NOTES:
*****************************************************************************/
int strisempty(const char *psz)
{
    return (psz) ? '\0' == *psz : 1; /*	A null pointer is also empty.	*/
}
/*****************************************************************************
DESCRIPTION:
        Returns true if string consists only of digits. Leading and trailing
        "white-space" are allowed.

REQUIRES:
        this		This object.

MODIFIES:
        (none)

RETURNS:
        Non-zero if consists of digits.

HISTORY:
        05/24/99	Created.

NOTES:
*****************************************************************************/
int strisnumber(const char *psz)
{
    if (!psz)
        return 0; /*	Cannot be NULL			*/
    while (isspace(*psz))
        ++psz; /*	Skip spaces				*/
    if (!isdigit(*psz))
        return 0; /*	Must be digits.			*/
    do {
        ++psz;
    } while (isdigit(*psz));
    if (0 == *psz)
        return 1; /*	EOL						*/
    if (!isspace(*psz))
        return 0; /*	Must be spaces.			*/
    do {
        ++psz;
    } while (isspace(*psz)); /*	Skip spaces.			*/
    return (0 == *psz);      /*	Must be eol.			*/
}
/*****************************************************************************
DESCRIPTION:
        Concatenate a list of strings together.

REQUIRES:
        sz			The string buffer.
        nMax		Its length (must actually be nMax+1).
        ...			String pointers terminated with NULL.

MODIFIES:
        (none)

RETURNS:
        Pointer to the string buffer.

HISTORY:
        01/27/98	Created.
        05/12/98	Added strcatl()
        06/29/98	Optimized using memccpy().

NOTES:
*****************************************************************************/
char *strncatl(char *sz, size_t nMax, ...)
{
    char *psv;
    char *psz;
    va_list vargs;

    strempty(sz);
    va_start(vargs, nMax);

    for (psz = sz; NULL != (psv = va_arg(vargs, char *));) {
        psz = memccpy(psz, psv, 0, nMax - (psz - sz));
        if (!psz) {
            sz[nMax] = '\0';
            break;
        }
        psz--;
    }
    va_end(vargs);
    return sz;
}

/*	----------------------------------------------------------------------
 */
char *strcatl(char *sz, ...)
{
    char *psv;
    char *psz;
    va_list vargs;

    strempty(sz);
    va_start(vargs, sz);

    for (psz = sz; NULL != (psv = va_arg(vargs, char *));) {
        psz = ((char *)memccpy(psz, psv, 0, UINT_MAX)) - 1;
    }
    va_end(vargs);
    return sz;
}
/*****************************************************************************
DESCRIPTION:
        Copies n characters of a string and guarantees that the n+1 character
will
        be a null. This means that the location copied too must accomodate n+1
        characters.

        If the source has more than n characters, then this routine asserts.

        Use the macro strncpyx() to invoke.

REQUIRES:
        s1			To. (Destination).
        s2			From.
        n			Maximum number of characters to copy.
        psFile		The calling file.
        nLine		The line in the calling file.

MODIFIES:
        (none)

RETURNS:
        Pointer to s1.

HISTORY:
        11/13/96	Created.

NOTES:
*****************************************************************************/
char *strncpyx_assert(char *s1, const char *s2, size_t n, const char *psFile,
                      size_t nLine)
{
    strncpyz(s1, s2, n);
    if (n < strlen(s2)) {
        logmsg(LOGMSG_ERROR, "ASSERT: string [%s] truncated in %s at %zd", s2,
                psFile, nLine);
    }
    return s1;
}
/*****************************************************************************
DESCRIPTION:
        Copies n characters of a string and guarantees that the n+1 character
will
        be a null. This means that the location copied too must accomodate n+1
        characters.

REQUIRES:
        s1			To.
        s2			From.
        n			Maximum number of characters to copy.

MODIFIES:
        (none)

RETURNS:
        Pointer to s1.

HISTORY:
        07/26/96	Created.
        10/03/96	Check for NULL s2.

NOTES:
*****************************************************************************/
char *strncpyz(char *s1, const char *s2, size_t n)
{
    if (s2) {
        strncpy(s1, s2, n);
        *(s1 + n) = '\0';
    } else {
        *s1 = '\0';
    }

    return s1;
}

char *strncatz(char *s1, const char *s2, size_t n)
{
    if (s1 == NULL || s2 == NULL) {
        logmsg(LOGMSG_ERROR, "ASSERT: strncatz() received null ptr argument\n");
        return "null pointer";
    }

    (void)strncat(s1, s2, (n - strlen(s1)));
    s1[n - 1] = 0;

    return s1;
}
/*****************************************************************************
DESCRIPTION:
        Skips past s2 at the beginning of s1.

REQUIRES:
        s1			String to scan.
        s2			String to point past.

MODIFIES:
        (none)

RETURNS:
        NULL is returned if s2 is not found in s1,
        otherwise points in s1 past the found string.

HISTORY:
        08/09/96	Created.
        01/27/98	Added stripast.

NOTES:
*****************************************************************************/
char *strpast(const char *s1, const char *s2)
{
    size_t n = strlen(s2);

    return (char *)((0 == strncmp(s1, s2, n)) ? s1 + n : NULL);
}

/*	----------------------------------------------------------------------
 */
char *stripast(const char *s1, const char *s2)
{
    size_t n = strlen(s2);

    return (char *)((0 == strnicmp(s1, s2, n)) ? s1 + n : NULL);
}
/*****************************************************************************
DESCRIPTION:
        Reverses characters in a string in place, ie., "ABCD" >> "DCBA".

REQUIRES:
        s			The string.

MODIFIES:
        (none)

RETURNS:
        (none)

HISTORY:
        03/31/99	Created.

NOTES:
*****************************************************************************/
char *zoe_strrev(char *s)
{
    size_t n = strlen(s);

    if (2 <= n) {
        char c;
        char *e = s + n - 1; /*	end		*/
        char *h = s;         /*	head	*/

        for (n = n / 2; n; n--) {
            c = *e;
            *e = *h;
            *h = c;
            h++;
            e--;
        }
    }
    return s;
}
/*****************************************************************************
DESCRIPTION:
        Trims off leading characters by shifting the characters left.

REQUIRES:
        s			The string.
        trims		The characters to trim off. If NULL, then defaults
to white
                                spaces.

MODIFIES:
        s

RETURNS:
        Pointer to the string.

HISTORY:
        07/30/96	Created.

NOTES:
*****************************************************************************/
char *strltrims(char *s, const char *trims)
{
    char *p;

    if (NULL == trims)
        trims = szWhiteChars;

    p = s + strspn(s, trims);

    if (s < p)
        strcpy(s, p);

    return s;
}
/*****************************************************************************
DESCRIPTION:
        Returns a pointer to the first trailing character.

        strtrail	Returns a pointer to the first trailing character.

        strrtrim	Trims off trailing white spaces by nulling the first
                                trailing character.

REQUIRES:
        s			The string.
        trims		The string of trailing characters to detect. If
NULL, then
                                will detect trailing white spaces.

MODIFIES:
        (none)

RETURNS:
        Returns a pointer to the first trailing character,
        or the end of the string.

HISTORY:
        07/26/96	Created.

NOTES:
*****************************************************************************/
char *strtrail(char *s, const char *trims)
{
    char *pTail = s;

    /*	Detect trailing white spaces by default.
     */
    if (strIsEmpty(trims))
        trims = szWhiteChars;

    while (*s) {
        if (!strchr(trims, *s++))
            pTail = s;
    }

    return pTail;
}

/*	-----------------------------------------------------------------------
 */
char *strrtrims(char *s, const char *trims)
{
    (*strtrail(s, trims)) = '\0';

    return s;
}
/*****************************************************************************
DESCRIPTION:
        Changes string case.

        strupper	Upper cases the string.
        strlower	Lower cases the string.

REQUIRES:
        s			The string.

MODIFIES:
        (none)

RETURNS:
        Pointer to the string.

HISTORY:
        07/30/96	Created.
        07/01/98	Suppress warning about assignment/comparison.

NOTES:
*****************************************************************************/
char *strlower(char *s)
{
    char *p = s;

    while ('\0' != (*p = tolower(*p))) {
        p++;
    }
    return s;
}

/*	-----------------------------------------------------------------------
 */
char *strupper(char *s)
{
    char *p = s;

    while ('\0' != (*p = toupper(*p))) {
        p++;
    }
    return s;
}
/*****************************************************************************
DESCRIPTION:
        Tokenize a string without use of the non-reentrant strtok function.

REQUIRES:
        p			The string buffer.
        psArgv		The array of pointers to the buffer to get.
        nSize		The size of the array.
        psFS		The fields separator characters.

MODIFIES:
        psArgv		Nulled, then filled with pointers to tokens.

RETURNS:
        Number of tokens created.

HISTORY:
        01/17/97	Created.

NOTES:
*****************************************************************************/
int strtokenize(char *p, char *psArgv[], int nSize, const char *psFS)
{
    int index;

    /*	Default field separators are white spaces.
     */
    if (!psFS)
        psFS = " \t\n\r";

    /*	NULL out the entire array.
     */
    memset(psArgv, 0, sizeof(char *) * nSize);

    /*	Break the string into little pieces.
     */
    for (index = 0; *p; p += 1) {
        /*	Skip past leading blanks.
         */
        p = p + strspn(p, psFS);

        /*	This is the start of a word.
         */
        psArgv[index] = p;

        /*	Is this the end of the string?
         */
        if (!*p)
            break;

        /*	One more token.
         */
        index += 1;

        /*	Cannot tokenize anymore. Last token is the remainder.
         */
        if (nSize <= index)
            break;

        /*	Find the end of the word.
         */
        p = p + strcspn(p, psFS);

        /*	If the end is the end of the string, then done!
         */
        if (!*p)
            break;

        /*	Null it out.
         */
        *p = '\0';
    }

    /*	Actual number of tokens found.
     */
    return index;
}
/*****************************************************************************
DESCRIPTION:
        Tokenize a string without use of the non-reentrant strtok function.
  Do not skip past leading blanks or separator characters like strtokenize.
  Only difference from strtokenize is removal of first statement in for loop.

REQUIRES:
        p			The string buffer.
        psArgv		The array of pointers to the buffer to get.
        nSize		The size of the array.
        psFS		The fields separator characters.

MODIFIES:
        psArgv		Nulled, then filled with pointers to tokens.

RETURNS:
        Number of tokens created.

HISTORY:
        09/29/11	Created.

NOTES:
*****************************************************************************/
int strtokenize_noskip(char *p, char *psArgv[], int nSize, const char *psFS)
{
    int index;

    /*	Default field separators are white spaces.
     */
    if (!psFS)
        psFS = " \t\n\r";

    /*	NULL out the entire array.
     */
    memset(psArgv, 0, sizeof(char *) * nSize);

    /*	Break the string into little pieces.
     */
    for (index = 0; *p; p += 1) {
        /*	This is the start of a word.
         */
        psArgv[index] = p;

        /*	Is this the end of the string?
         */
        if (!*p)
            break;

        /*	One more token.
         */
        index += 1;

        /*	Cannot tokenize anymore. Last token is the remainder.
         */
        if (nSize <= index)
            break;

        /*	Find the end of the word.
         */
        p = p + strcspn(p, psFS);

        /*	If the end is the end of the string, then done!
         */
        if (!*p)
            break;

        /*	Null it out.
         */
        *p = '\0';
    }

    /*	Actual number of tokens found.
     */
    return index;
}
/*****************************************************************************
DESCRIPTION:
        Finds whole words in a string.

REQUIRES:
        psStr		The string to search.
        psWord		The word to find.
        psFS		The field separators (characters delimiting the word).
                                Implied FS are the beginning and end of the
search string.
                                If NULL then default set of white chars are
assume.

MODIFIES:
        (none)

RETURNS:
        Pointer to found word. NULL otherwise.

HISTORY:
        01/10/97	Created.

NOTES:
*****************************************************************************/
char *strwords(const char *psStr, const char *psWord, const char *psFS)
{
    size_t nWord = strlen(psWord);
    char *psz = (char *)psStr;

    if (!psFS)
        psFS = szWhiteChars;

    /*	While strings are found.
     */
    while (NULL != (psz = strstr(psz, psWord))) {
        if (((psz == psStr) ||
             strchr(psFS, *(psz - 1))) &&   /*	Beginning?	*/
            (strchr(psFS, *(psz + nWord)))) /*	End?		*/
        {
            return psz;
        }
        psz += 1; /*	Continue the search further up the string.
                     */
    }
    return NULL;
}
/*****************************************************************************
DESCRIPTION:
        Memory management functions.

REQUIRES:
        ptr			Pointer.
        size		Amount.

MODIFIES:
        (none)

RETURNS:
        (none)

HISTORY:
        01/07/99	Created.

NOTES:
*****************************************************************************/
void *zalloc(size_t size)
{
    void *ptr = malloc(size);
    memset(ptr, 0, size);
    return ptr;
}

/*	----------------------------------------------------------------------
 */
void zdelete(void *ptr) { free(ptr); }

/* returns true if string is a signed integer (i.e. "-145")*/
int strissignedinteger(const char *psz)
{
    if (*psz == '-')
        psz++;
    return strisnumber(psz);
}

/* returns true if string is a floating pt number (i.e. "-56.793") */
int strisfltpntnumber(const char *psz)
{
    unsigned char dot_found = 0;
    if (!psz)
        return 0;
    if (*psz == '-')
        psz++;
    while (*psz) {
        if (isdigit(*psz))
            psz++;
        else if ((dot_found == 0) && (*psz == '.')) {
            dot_found = 1;
            psz++;
        } else
            return 0;
    }
    return 1;
}

/* returns true if string is a hex number (i.e. "12B0F9 ") */
int strishexnumber(const char *psz)
{
    if (!psz)
        return 0;
    while (isspace(*psz))
        ++psz;
    if (!_is_hex_digit(*psz))
        return 0;
    do {
        ++psz;
    } while (_is_hex_digit(*psz));
    if (0 == *psz)
        return 1;
    if (!isspace(*psz))
        return 0;
    do {
        ++psz;
    } while (isspace(*psz));
    return (0 == *psz);
}
