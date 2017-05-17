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

/* portable inlining, see http://www.greenend.org.uk/rjk/2003/03/inline.html */
#define INLINE
#include "flibc.h"
#include "stdio.h"

/**
 * So it turns out that when printing large floats, AIX truncates them after 30
 * significant figures to the left of the decimal.  Since AIX is our de facto
 * type system standard, we need to emulate this behavior.
 * @param p_str pointer to string to print double to
 * @param str_buf_len   length of the p_str buffer
 * @param d double to print
 * @return 0 on success; !0 otherwise
 */
enum { AIX_LRG_REAL_DIGITS = 30 };
int flibc_snprintf_dbl(char *p_str, const size_t str_buf_len, double d)
{
    char *pStart;
    const char *pEnd;
    const char *pLast;
    int addDigit;
    int rc;

    rc = snprintf(p_str, str_buf_len, "%f", d);
    if (rc < 0 || rc >= str_buf_len)
        return -1;

    pEnd = p_str + str_buf_len;

    if (*p_str == '-')
        ++p_str;

    pStart = p_str;

    pLast = p_str + AIX_LRG_REAL_DIGITS;

    /* Set p_str to whichever comes first:
     * just past the last sig-fig AIX prints,
     * the decimal point,
     * or the end of the string (indicates a malformed decimal number) */
    for (; p_str < pEnd && p_str < pLast && *p_str != '\0' && *p_str != '.';
         ++p_str)
        ;

    /* if we need to round up */
    addDigit = 0;
    if (*p_str >= '6' && *p_str <= '9') {
        char *pBack;

        /* move backwards, rolling over any consecutive digits that are 9's */
        for (pBack = p_str - 1; *pBack == '9' && pBack >= pStart; --pBack)
            *pBack = '0';

        /* if we rolled all the way back to the start of the string */
        if (pBack < pStart) {
            /* change the first digit to a 1 */
            *pStart = '1';

            /* make a note that we will add a digit to the end of the number */
            addDigit = 1;
        } else
            ++(*pBack);
    }

    /* 0 out from where we are to the decimal point.  If we are already at the
     * decimal point (ie AIX would have printed all the digits) or if we are at
     * the end of the string (the decimal is malformed) this will do nothing */
    for (; p_str < pEnd && *p_str != '\0' && *p_str != '.'; ++p_str)
        *p_str = '0';

    /* if we're supposed to add a new digit to the end of the integral part */
    if (addDigit) {
        char saved;

        saved = '0'; /* digit to add */
        /* shift the decimal and all the trailing digits to the right one */
        for (; p_str < pEnd && saved != '\0'; ++p_str) {
            char tmp;

            tmp = *p_str;
            *p_str = saved;
            saved = tmp;
        }

        if (p_str >= pEnd)
            return -1;

        /* add new NUL byte */
        *p_str = saved;
    }

    return 0;
}

/* TODO this test func was used when flibc_snprintf_dbl() was called
 * fixup_sigfigs_real_cstr() and it took a string that already had a double
 * printed to it */

/* input, expected output */
/*const char *test_fixup_cases[][2] =*/
/*{*/
/*{ "11111111111111111.111111",*/
/*"11111111111111111.111111" },*/
/*{ "11111111111111111111111111.000000",*/
/*"11111111111111111111111111.000000" },*/
/*{ "11111111111111111111111111.123000",*/
/*"11111111111111111111111111.123000" },*/
/*{ "111111111111111111111111111111.000000",*/
/*"111111111111111111111111111111.000000" },*/
/*{ "1111111111111111111111111111111.000000",*/
/*"1111111111111111111111111111110.000000" },*/
/*{ "1111111111111111111111111111116.000000",*/
/*"1111111111111111111111111111120.000000" },*/
/*{ "1111111111111111111111111111119.000000",*/
/*"1111111111111111111111111111120.000000" },*/
/*{ "1111111111111111111111111111199.000000",*/
/*"1111111111111111111111111111200.000000" },*/
/*{ "9999999999999999999999999999999.000000",*/
/*"10000000000000000000000000000000.000000" },*/
/*{ "1111111111111111111111111111110000.000000",*/
/*"1111111111111111111111111111110000.000000" },*/
/*{ "1111111111111111111111111111111111.000000",*/
/*"1111111111111111111111111111110000.000000" },*/
/*{ "1111111111111111111111111111117111.000000",*/
/*"1111111111111111111111111111120000.000000" }*/
/*};*/
/*int test_fixup(void)*/
/*{*/
/*unsigned i;*/

/*for(i = 0; i < sizeof(test_fixup_cases) / sizeof(test_fixup_cases[0]); ++i)*/
/*{*/
/*char str_buf[128];*/
/*strncpy(str_buf, test_fixup_cases[i][0], sizeof(str_buf));*/

/*|+ do fixup +|*/
/*if(fixup_sigfigs_real_cstr(str_buf, sizeof(str_buf)))*/
/*{*/
/*fprintf(stderr, "%s: fixup %u failed, in: %s\n", __func__, i, */
/*test_fixup_cases[i][0]);*/
/*return 1;*/
/*}*/

/*|+ check output +|*/
/*if(strcmp(str_buf, test_fixup_cases[i][1]))*/
/*{*/
/*fprintf(stderr, "%s: fixup %u failed, in: %s out: %s expected: "*/
/*"%s\n", __func__, i, test_fixup_cases[i][0], str_buf, */
/*test_fixup_cases[i][1]);*/
/*return 1;*/
/*}*/
/*}*/

/*return 0;*/
/*}*/
