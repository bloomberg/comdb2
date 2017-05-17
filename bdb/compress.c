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

#include "compress.h"
#include <alloca.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include <bb_inttypes.h>
#include "logmsg.h"

/* Basic RLE compression.  I think this is pretty similar to how .pcx files
 * work.
 *
 * A byte with MSB clear denotes that many bytes of uncompressed data.
 * A byte with MSB set denotes that the following byte is repeated
 * that many times (clearing the MSB to calculate many).
 */

/* Apply basic RLE compression.  Returns length of compressed data or -1
 * if we don't have enough space to compress this. */
int rle8_compress(const void *from, size_t fromlen, void *to, size_t tolen)
{
    const uint8_t *from_pos = from;
    const uint8_t *from_end = from_pos + fromlen;
    const uint8_t *from_end_minus4 = from_end - 4;
    uint8_t *to_pos = to;

    /* Never compress anything four bytes or less */
    if (fromlen < 4) {
        return -1;
    }

    while (from_pos < from_end) {

        const uint8_t *start;
        uint32_t len;

        /* Do an uncompressed run */
        start = from_pos;
        while (from_pos < from_end) {
            if (from_pos <= from_end_minus4) {
                if (from_pos[0] == from_pos[1]) {
                    if (from_pos[1] == from_pos[2]) {
                        if (from_pos[2] == from_pos[3]) {
                            /* This is the start of a run of identical bytes */
                            break;
                        } else {
                            from_pos += 3;
                            continue;
                        }
                    } else {
                        from_pos += 2;
                        continue;
                    }
                }
            } else {
                from_pos = from_end;
                break;
            }
            from_pos++;
        }

        /* Emit the non-identical bytes */
        while (start < from_pos) {
            len = from_pos - start;
            if (len > 0x7f) {
                len = 0x7f;
            }
            /*
            printf("** uncompressed run of %u bytes\n", len);
            fsnapf(stdout, start, len);
            */
            if (tolen < len + 1) {
                return -1;
            }
            *(to_pos++) = (uint8_t)len;
            tolen--;
            memcpy(to_pos, start, len);
            tolen -= len;
            to_pos += len;
            start += len;
        }

        if (from_pos == from_end) {
            break;
        }

        /* Now see how long the run lasts.  If we get here then we know that
         * the first 4 bytes of the run are identical. */
        start = from_pos;
        from_pos += 4;
        while (from_pos < from_end) {
            if (*from_pos != *start) {
                break;
            }
            from_pos++;
        }

        /* Emit the run */
        while (start < from_pos) {
            if (tolen < 2) {
                return -1;
            }
            len = from_pos - start;
            if (len > 0x7f) {
                len = 0x7f;
            }
            /*printf("** compressed run of %u x %02x\n", len, *start);*/
            *(to_pos++) = (uint8_t)(len | 0x80);
            *(to_pos++) = *start;
            tolen -= 2;
            start += len;
        }
    }

    return (to_pos - (uint8_t *)to);
}

/* Reverse the compression of rle_compress().
 * Returns length of decompressed data on success, -1 on error.
 */
int rle8_decompress(const void *from, size_t fromlen, void *to, size_t tolen)
{
    const uint8_t *from_pos = from;
    const uint8_t *from_end = from_pos + fromlen;
    uint8_t *to_pos = to;
    uint8_t *to_end = to_pos + tolen;

    while (from_pos != from_end) {
        uint8_t byte, count;
        byte = *(from_pos++);
        count = byte & 0x7f;
        if (to_pos + count > to_end) {
            logmsg(LOGMSG_ERROR, "%s: not enough space in output buffer\n",
                    __func__);
            return -1;
        }
        if (byte == count) {
            /* uncompressed stream - copy next count bytes unchanged */
            if (from_pos + count > from_end) {
                logmsg(LOGMSG_ERROR, "%s: RLE stream corrupt (1)\n", __func__);
                return -1;
            }
            memcpy(to_pos, from_pos, count);
            to_pos += count;
            from_pos += count;
        } else {
            /* compressed stream - repeat next byte count times */
            if (from_pos == from_end) {
                logmsg(LOGMSG_ERROR, "%s: RLE stream corrupt (2)\n", __func__);
                return -1;
            }
            byte = *(from_pos++);
            memset(to_pos, byte, count);
            to_pos += count;
        }
    }

    return (to_pos - (uint8_t *)to);
}

#ifdef RLE_TESTS

static void test_rle8(const char *in)
{
    int len;
    char *out;
    char *compr;
    int compr_len, out_len;

    len = strlen(in);
    out = alloca(len * 2);
    compr = out + len;

    compr_len = rle8_compress(in, len, compr, len);
    if (compr_len < 0) {
        printf("%s:'%s': no gain from rle8 compression\n", __func__, in);
    } else {
        out_len = rle8_decompress(compr, compr_len, out, len);
        if (out_len < 0) {
            fprintf(stderr, "%s:'%s': error decompressing\n", __func__, in);
            fprintf(stderr, "Input:\n");
            fsnapf(stderr, in, len);
            fprintf(stderr, "Compressed:\n");
            fsnapf(stderr, compr, compr_len);
        } else if (out_len != len || memcmp(in, out, len) != 0) {
            fprintf(stderr, "%s:'%s': decompressed does not match input\n",
                    __func__, in);
            fprintf(stderr, "Input:\n");
            fsnapf(stderr, in, len);
            fprintf(stderr, "Compressed:\n");
            fsnapf(stderr, compr, compr_len);
            fprintf(stderr, "Output:\n");
            fsnapf(stderr, out, out_len);
        } else {
            printf("%s:'%s': ok %d->%d\n", __func__, in, len, compr_len);
        }
    }
}

int main(int argc, char *argv[])
{
    int count = 0;

    test_rle8("Hello, world");
    test_rle8("");
    test_rle8("                    ");
    test_rle8("1      2      3      4      5      6");
    test_rle8("aaaabbbbccccddddeeee");
    test_rle8("Excellent   wibble   wobble");
    test_rle8("       .................!%^$%$^&&&&!!");
    test_rle8("(((xxxcccdddeee)))");
    test_rle8("____");
    return 0;
}
#endif
