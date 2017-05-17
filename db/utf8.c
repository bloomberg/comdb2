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

#include <stdint.h>

/*
** RFC 3629: UTF-8, a transformation format of ISO 10646
**
** The table below summarizes the format of these different octet types.
** The letter x indicates bits available for encoding bits of the
** character number.
**
** Char. number range  |        UTF-8 octet sequence
**    (hexadecimal)    |              (binary)
** --------------------+---------------------------------------------
** 0000 0000-0000 007F | 0xxxxxxx
** 0000 0080-0000 07FF | 110xxxxx 10xxxxxx
** 0000 0800-0000 FFFF | 1110xxxx 10xxxxxx 10xxxxxx
** 0001 0000-0010 FFFF | 11110xxx 10xxxxxx 10xxxxxx 10xxxxxx
**
**
** The definition of UTF-8 prohibits encoding character numbers between
** U+D800 and U+DFFF, which are reserved for use with the UTF-16
** encoding form (as surrogate pairs) and do not directly represent
** characters.
*/

#define PROCESS_10xxxxxx                                                       \
    do {                                                                       \
        ++u;                                                                   \
        if (((*u) & 0xc0) != 0x80) return 1;                                   \
        v = (v << 6) | ((*u) & 0x3f);                                          \
    } while (0)

static int utf8_validate_int(const char *u, int max, int *valid_len)
{
    *valid_len = 0;
    const char *end = max >= 0 ? u + max : 0;
    while (u != end && *u) {
        uint32_t need, v = 0;
        if      (((*u) & 0x80) == 0x00) { need = 1; v = (*u) & 0x7f; } /* 0xxxxxxx */
        else if (((*u) & 0xe0) == 0xc0) { need = 2; v = (*u) & 0x1f; } /* 110xxxxx */
        else if (((*u) & 0xf0) == 0xe0) { need = 3; v = (*u) & 0x0f; } /* 1110xxxx */
        else if (((*u) & 0xf8) == 0xf0) { need = 4; v = (*u) & 0x07; } /* 11110xxx */
        else return 1;
        if (end && (u + need) > end) return 1;
        switch (need) {
        case 4: PROCESS_10xxxxxx;
        case 3: PROCESS_10xxxxxx;
        case 2: PROCESS_10xxxxxx;
            /* check overlong encoding */
            switch (need) {
            case 4: if (v < 0x10000) return 1; break;
            case 3: if (v < 0x800)   return 1; break;
            case 2: if (v < 0x80)    return 1; break;
            }
            /* check for out-of-range and reserved numbers */
            if (v > 0x10ffff || (v >= 0xd800 && v <= 0xdfff)) {
                return 1;
            }
            /* fall through */
        case 1:
            ++u;
            break;
        }
        *valid_len += need;
    }
    // consumed max bytes or found null byte
    return 0;
}

int utf8_validate(const char *str, int max, int *valid_len)
{
    int len;
    int rc = utf8_validate_int(str, max, &len);
    if (valid_len) *valid_len = len;
    return rc;
}

int utf8_bytelen(const char *str, int max)
{
    int len;
    int rc = utf8_validate_int(str, max, &len);
    return rc == 0 ? len : -1;
}
