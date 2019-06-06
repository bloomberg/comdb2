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

/*****************************************************************************
DESCRIPTION:
        Useful extended string functions.

FORTRAN-C String Functions:
        void ctofstr(char* ps,int n)
        void strfcpy(char* ps1,const char* ps2,int n)
        char* fstrcpy(char* ps1,const char* ps2,int n2);
        char* fstrncpy(char* ps1,size_t n1,const char* ps2,int n2);
        size_t fstrlen(const char* ps,int n);
        size_t fstrtoc(char* ps,int n);

SYNOPSIS:
        size_t cstr_escape_encode(char *ps1,const char* ps2,size_t nSize);

                Converts non-printable chars to C-escaped characters until
                end-of-string or n chars.

        size_t cstrn_escape_encode(char *ps1,size_t nOut,const char* ps2,size_t
nIn);

                Converts n2 non-printable chars to n1 chars C-escaped including
nulls.

        size_t cstr_escape_decode(char *ps1,const char* ps2,size_t nSize);

                Converts C-escaped characters to ASCII.

        void ctofstr(char* ps,int n);

                Converts a C-string to a FORTRAN-string of maximum length 'n'
in-place.

        char* fstrcpy(char* ps1,const char* ps2,int n2);

                Copies a FORTRAN string 'ps2' of maximum length 'n2' to
                a C-string buffer 'ps1'. The C-string buffer must be able to
                hold at least 'n2+1' characters.

        char* fstrncpy(char* ps1,size_t n1,const char* ps2,int n2);

                Copies a FORTRAN string 'ps2' of maximum length 'n2' to
                a C-string buffer 'ps1' of maximum length 'n1' (+1 null).

        size_t fstrlen(const char* ps,int n);

                Actual C-length of a FORTRAN string 'ps' of maximum length 'n'.

        size_t fstrtoc(char* ps,int n);

                Convert's a FORTRAN string 'ps' of maximum length 'n' to a
                null terminated C-string. Returns actual C-string length.

        void* memdup(const void* mem,size_t n);

                Like strdup(3C), makes a copy of mem.

        void* no_copy(const void* mem);
        void no_free(void*);

                Unlike memdup()/strdup(3C)/free(3C), do nothing. Returns mem.

        char* strcatc(char* s,char c);

                Appends character 'c' to string 's' like strcat(3C).

        char* strccpyz(char* s1,const char* s2,int c,size_t n);

                Copies characters from s2 to s1 until c is found or n characters
                have been copied.  The characters 'c' is not copied.
                s1 will be null terminated.
                See also memccpy(3C).

        #define strempty(s)		((s)[0] = '\0')

                Marks a string as empty.

        void strfcpy(char* ps1,const char* ps2,int n);

                Copies a C-string 'ps2' to a FORTRAN-string 'ps1' of maximum
                length 'n'.

        char* strgrep(const char* psPat,const char* psStr,int flags,int* rcode)

                Returns pointer to a string in psStr matching psPat, or NULL.
                'rcode' returns the length of the match, or an error code.
                See also strismatch().
                'flags' control:
                        GREP_FLAG_ANCHOR:	^ and $ anchor front and back
of search.
                        GREP_FLAG_MAXIMAL	* expands maximally.

        unsigned strhash(const char* s,unsigned n);
        unsigned strihash(const char* s,unsigned n);

                String hashing from K&R. Returns a number 0..n-1.
                'n' specifies number of hash slots 0..n-1.
                strihash() is case insensitive.

        int stricmp(const char* s1,const char* s2);
        int strnicmp(const char* s1,const char* s2,size_t n);

                Lexical insenstive string compare like strcmp(3C)/strncmp(3C).

        int strindex(const char* ps,int c);
        int strrindex(const char* ps,int c);

                Return the 0-based index position of character 'c' in the
string.
                Returns -1 if not found. strrindex starts from the end.

        int strisblank(const char* psz);
        #define strIsBlank(psz)	strisblank(psz)

                Is the string NULL or empty or does it consist only of white
spaces?

        int strisempty(const char* psz);
        #define strIsEmpty(psz)	strisempty(psz)

                Is the string NULL or empty, i.e., no characters at all?

        int strismatch(const char* psPattern,const char* psString);

                Does psPattern match psString (true/false)? Strings
                are automatically anchored front and back.
                Pattern supports these usual meta-codes:
                        *	Zero or more characters.
                        ?	Any single character.
                        #	A digit.
                        \a	Bell.
                        \d	Digit.
                        \e	Escape.
                        \f	Form feed.
                        \l	Lower case char.
                        \n	New line.
                        \r	Carriage return.
                        \s	White Space.
                        \S	Non-white space.
                        \t	Tab.
                        \u	Upper case.
                        \w	Alpha numeric.
                        \W	Non-alpha numeric.
                        \x	Hexadecimal digit.
                        \X	Non-Hexadecimal digit.

                Use backslash to un-escape these meta-codes.
                See also, strgrep().

        int strisnumber(const char* psz);
        #define strIsNumber(psz) strisnumber(psz)

                True if string consists only of digits (isdigit), with ignored
                leading/trailing spaces (isspace).  Suitable for atoi().

        int strissignedinteger(const char *psz)
                True if string is a signed integer (i.e. "-154"). Not
leading/trailing
                spaces friendly.
        int strisfltpntnumber(const char *psz)
                True if string is a floating pt number (i.e. "-56.793").
                Not leading/trailing spaces friendly.

        char* strncatl(char* s,size_t n,const char* s0,...,const char* sn,NULL);
        char* strcatl(char* s,const char* s0,...,const char* sn,NULL);

                Concatenate a list of strings onto 's' of maximum length 'n'.
                The last string is denoted with a NULL.

        #define strncpyx(s1,s2,n)
strncpyx_assert(s1,s2,n,__FILE__,__LINE__)
        char* strncpyx_assert(char* s1,const char* s2,size_t n,const
char*,long);

                Check for eXact string copy. An assert-style macro/function pair
that
                asserts when copied 's2' to 's1' is longer than 'n' characters.

        char* strlower(char* s);
        char* strupper(char* s);

                Lower/Upper cases a string.

        char* strltrims(char* s,const char* trims);
        char* strrtrims(char* s,const char* trims);

                Trim off left/right 'trims' characters from string 's'.
                Pointer to beginning of 's' is returned.
                Left trimming actually shifts characters left in string 's'.
                Using NULL 'trims' trims off standard white spaces.
                See also strtrail() below.

        #define strltrim(s)		strltrims(s,NULL)
        #define strrtrim(s)		strrtrims(s,NULL)
        #define strtrim(s)		strltrim(strrtrim(s))

                Trim off white spaces from the left/right/both ends of the
string.

        char* strncpyz(char* s1,const char* s2,size_t n);

                Similar to strncpy(3C) except that "s1" is guaranteed to be null
                terminated. "s1" must be at least n+1 characters long.

        char* strncatz(char* s1, const char* s2, size_t n);
                strcat with guaranteed \0

        char* strpast(const char* s1,const char* s2);
        char* stripast(const char* s1,const char* s2);

                If "s2" are the first characters in "s1", points at the
character
                past "s2" in "s1". Returns NULL otherwise.

        char* strrev(char* s);

                Reverses the characters of the string.

        char* strtrail(char* s,const char* trims);

                Returns pointer to first of specified trailing characters.
                Useful for trimming off trailing white spaces.

        int strtokenize(char* p,char* psArgv[],int nSize,const char* psFS);

                Breaks string s into tokens (without using strtok) and
                places up to 'nSize' pointers to the tokens in psArgv.

        int strtokenize_noskip(char* p,char* psArgv[],int nSize,const char*
psFS);

                Same as strtokenize except it doesn't skip past leading
blanks/separators

        char* strwords(const char* psStr,const char* psWord,const char* psFS);
        #define strword(s1,s2)		strwords(s1,s2,NULL)

                Finds whole words s2 in s1 where words are delimited by field
                separator characters sFS.
                NULL 'psFS' implies whitespace field separators.

        void* zalloc(size_t size);
        void zdelete(void* ptr);

                Allocate 0-ed memory space and delete memory.

HISTORY:
        DATE		NAME	REASON
        07/26/96	bzoe	Created
        11/13/96	bzoe	Added strncpyx().
        01/07/97	bzoe	Added strnicmp().
        01/10/97	bzoe	Added strwords().
        05/15/97	bzoe	Added strisblank().
        01/27/98	bzoe	Added stripast() and rewrote synopsis.
        02/25/98	bzoe	Added prototype for library function itoa().
        06/25/98	bzoe	Added strindex.
        06/26/98	bzoe	Added FORTRAN-C string functions.
        08/10/98	bzoe	Prototyped ctofstr.
        10/22/98	amullok	Prototyped strrmch.
        03/25/99	bzoe	Added missing SunOs calls.
        03/31/99	bzoe	Added strrev.
        05/24/99	bzoe	Added strisnumber.
        07/23/99	bzoe	Added strismatch.
        08/10/05        lnikolic   Added strissignedinteger
        08/10/05        lnikolic   Added strisfltpntnumber
        07/11/06        lnikolic   Added strishexnumber


NOTES:
*****************************************************************************/
#ifndef INCLUDED_XSTRING_H
#define INCLUDED_XSTRING_H

#ifdef __cplusplus
extern "C" {
#endif

#include <string.h>

#ifndef CPSTR
#define CPSTR const char *
#endif

/*	Various C-string character escape mechanisms.
 */
size_t cstr_escape_encode(char *ps1, const char *ps2, size_t nSize);
size_t cstrn_escape_encode(char *ps1, size_t nOut, const char *ps2, size_t nIn);
size_t cstr_escape_decode(char *ps1, const char *ps2, size_t nSize);

void ctofstr(char *ps, int n);
char *fstrcpy(char *ps1, const char *ps2, int n2);
char *fstrncpy(char *ps1, size_t n1, const char *ps2, int n2);
size_t fstrlen(const char *ps, int n);
size_t fstrtoc(char *ps, int n);

/*	Non-prototyped library function.
 */
char *itoa(int, char *);

/*	Memory management.
 */
void mem_hexdump(const void *ptr, size_t size);
void *memdup(const void *mem, size_t n);
char *nextword(char *p);
void *no_copy(const void *mem);
void no_free(void *);

/*	Useful string functions.
 */
char *strcatc(char *s, char c);
char *strccpyz(char *s1, const char *s2, int c, size_t n);

/*	Removes all occurences of a character from the string
 */
void strrmch(char *str, char ch);

char *strempty_(char *str);
#define strempty(s) ((s)[0] = '\0')

void strfcpy(char *ps1, const char *ps2, int n);

unsigned strhash(const char *s, unsigned hashsize);
unsigned strihash(const char *s, unsigned hashsize);

int xstricmp(const char *s1, const char *s2);
int xstrnicmp(const char *s1, const char *s2, size_t n);
#ifndef stricmp
#define stricmp xstricmp
#endif
#ifndef strnicmp
#define strnicmp xstrnicmp
#endif

int strindex(const char *ps, int c);
int strrindex(const char *ps, int c);

int strisblank(const char *psz);
#define strIsBlank(psz) strisblank(psz)

int strisempty(const char *psz);
#define strIsEmpty(psz) strisempty(psz)

int strismatch(const char *psPat, const char *psStr);
char *strgrep(const char *psPat, const char *psStr, int flags, int *rcode);
#define GREP_FLAG_ANCHOR (0x1)
#define GREP_FLAG_MAXIMAL (0x2)

int strisnumber(const char *psz);
int strisfltpntnumber(const char *psz);
int strissignedinteger(const char *psz);
int strishexnumber(const char *psz);
#define strIsNumber(psz) strisnumber(psz)

#if 0
char *strltrims(char *s, const char *trims);
#define strltrim(s) strltrims(s, NULL)
#endif

char *strrtrims(char *s, const char *trims);
#define strrtrim(s) strrtrims(s, NULL)

#define strtrim(s) strltrim(strrtrim(s))

char *strncatl(char *s, size_t n, ...);
char *strcatl(char *s, ...);

char *strncpyx_assert(char *s1, const char *s2, size_t n, const char *,
                      size_t nLine);
#define strncpyx(s1, s2, n) strncpyx_assert(s1, s2, n, __FILE__, __LINE__)

char *strncpyz(char *s1, const char *s2, size_t n);
char *strncatz(char *s1, const char *s2, size_t n);

char *strpast(const char *s1, const char *s2);
char *stripast(const char *s1, const char *s2);

char *zoe_strrev(char *s);
#define strrev zoe_strrev

char *strtrail(char *s, const char *trims);

char *strlower(char *s);
char *strupper(char *s);

int strtokenize(char *p, char *psArgv[], int nSize, const char *psFS);
int strtokenize_noskip(char *p, char *psArgv[], int nSize, const char *psFS);

char *strwords(const char *psStr, const char *psWord, const char *psFS);
#define strword(s1, s2) strwords(s1, s2, NULL)

void *zalloc(size_t size);
void zdelete(void *ptr);

void zeroterminate(char *str, int str_size);

#ifdef __cplusplus
}
#endif

#endif /* _XSTRING_H 		*/
