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

#include <arpa/inet.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <alloca.h>

#undef NDEBUG
#include <assert.h>
#include <comdb2rle.c> //need access to static funcs

#define N 1024

static int mydecode(uint8_t *in, uint32_t insz, uint8_t *out, uint32_t outmax)
{
    Comdb2RLE d = {.in = in, .insz = insz, .out = out, .outsz = outmax};
    return decompressComdb2RLE(&d);
}

#define TEST_REPEAT(d, rcnt, rsize)                                            \
    do {                                                                       \
        dt.sz = sizeof(d);                                                     \
        dt.dt = d;                                                             \
        i = 0;                                                                 \
        while (i < CNT(sizes)) {                                               \
            if (repeats(dt, sizes[i], &r))                                     \
                break;                                                         \
            ++i;                                                               \
        }                                                                      \
        assert(r == rcnt);                                                     \
        if (rsize < 0)                                                         \
            assert(i == CNT(sizes));                                           \
        else                                                                   \
            assert(sizes[i] == (uint8_t)rsize);                                \
        break;                                                                 \
    } while (0)

static void test_repeat()
{
    uint32_t i, r;
    Data dt;

    uint8_t d0[] = {0x00};
    uint8_t d1[] = {0x00, 0x00};
    uint8_t d2[] = {0x00, 0x01, 0x00, 0x01, 0x00, 0x01, 0x00, 0x01};
    uint8_t d3[] = {0x00, 0x01, 0x02, 0x00, 0x01, 0x02, 0x00, 0x01, 0x02};
    uint8_t d4[] = {0x00, 0x01, 0x02, 0x03, 0x04, 0x00, 0x01, 0x02, 0x03, 0x04};
    uint8_t d5[] = {0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
                    0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08};

    uint8_t d6[] = {0x00, 0xff};

    TEST_REPEAT(d0, 0, -1);
    TEST_REPEAT(d1, 1, 1);
    TEST_REPEAT(d2, 3, 2);
    TEST_REPEAT(d3, 2, 3);
    TEST_REPEAT(d4, 1, 5);
    TEST_REPEAT(d5, 4, 9);
    TEST_REPEAT(d6, 0, -1);

    fprintf(stderr, "passed %s\n", __func__);
}

static void test_well_known()
{
    uint32_t i, p;
    /* Well Known Patterns */
    for (i = 0; i < MAXPAT; ++i) {
        assert(well_known(patterns[i], psizes[i], &p));
        assert(p == i);
    }

    /* Random garbage */
    for (i = 0; i < MAXPAT; ++i) {
        uint8_t buf[psizes[i]];
        memset(buf, 0xdb, psizes[i]);
        assert(well_known(buf, psizes[i], &p) == 0);
    }

    /* Random garbage */
    for (i = 0; i < MAXPAT; ++i) {
        uint8_t buf[psizes[i]];
        memset(buf, 0xdb, psizes[i]);
        assert(well_known(buf, psizes[i] + 100, &p) == 0);
    }
    fprintf(stderr, "passed %s\n", __func__);
}

static void test_encode_repeat()
{
    int i;
    int p;
    for (p = 0; p < MAXPAT; ++p) {
        uint8_t inp[psizes[p] * 10];
        for (i = 0; i < 10; ++i)
            memcpy(&inp[psizes[p] * i], patterns[p], psizes[p]);

        uint8_t out[256];
        memset(out, 0xdb, sizeof(out));

        Data input = {.dt = inp, .sz = sizeof(inp)};
        Data output = {.dt = out, .sz = sizeof(out)};

        uint32_t r;
        uint32_t s = psizes[p];
        repeats(input, s, &r);
        assert(encode_repeat(&output, &input, r, s) == 0);
        uint8_t decode[sizeof(inp)];
        assert(mydecode(out, sizeof(out) - output.sz, decode, sizeof(decode)) == 0);
        assert(memcmp(inp, decode, sizeof(inp)) == 0);
    }

    uint8_t buf[10];
    uint8_t out[10];
    memset(buf, 0xdb, sizeof(buf));
    memset(out, 0xff, sizeof(out));
    Data input = {.dt = buf, .sz = sizeof(buf)};
    Data output = {.dt = out, .sz = sizeof(out)};
    uint32_t r;
    for (i = CNT(sizes) - 1; i; --i) {
        if (repeats(input, sizes[i], &r))
            break;
    }
    assert(encode_repeat(&output, &input, r, sizes[i]) == 0);
    uint8_t decode[sizeof(buf)];
    assert(mydecode(out, sizeof(out) - output.sz, decode, sizeof(decode)) == 0);
    assert(memcmp(buf, decode, sizeof(buf)) == 0);
    assert(*output.dt == 0xff);
    fprintf(stderr, "passed %s\n", __func__);
}

static void test_varint()
{
    uint32_t i;
    uint8_t buf[8];
    uint8_t *out;
    for (i = 0; i < 409600; ++i) {
        uint32_t j, k;
        j = i;
        out = buf;
        ENCODE_NUMBER(j, out);
        out = buf;
        DECODE_NUMBER(out, k);
        assert(i == k);
    }
    fprintf(stderr, "passed %s\n", __func__);
}

static void test_encode_prev()
{
    int i;
    FILE *f = fopen("/dev/urandom", "r");
    for (i = 1; i < N; ++i) {
        uint8_t in[i];
        uint8_t out[i + 10];
        uint8_t out_rev[i + 10];
        int lrc = fread(in, 1, i, f);
        if (lrc == 0) {
            printf("ERROR: %s no data to read\n", __func__);
        }

        Data input, output, output_rev;
        input.dt = in + sizeof(in); // need to be at end of buf
        input.sz = sizeof(in);
        output.dt = out;
        output.sz = sizeof(out);
        output_rev.dt = out_rev;
        output_rev.sz = sizeof(out_rev);
        uint32_t prev = sizeof(in);
        uint32_t prev_rev = sizeof(in);

        assert(encode_prev(&output, &input, &prev) == 0);
        assert(prev == 0);
        assert(encode_prev_rev(&output_rev, &input, &prev_rev) == 0);
        assert(prev_rev == 0);

        uint8_t decode[i];
        bzero(decode, sizeof(decode));
        assert(mydecode(out, sizeof(out) - output.sz, decode, sizeof(decode)) == 0);
        assert(memcmp(in, decode, sizeof(in)) == 0);

        bzero(decode, sizeof(decode));
        assert(mydecode(out_rev, sizeof(out_rev) - output_rev.sz, decode, sizeof(decode)) == 0);
        assert(memcmp(in, decode, sizeof(in)) == 0);
    }
    fclose(f);
    fprintf(stderr, "passed %s\n", __func__);
}

static void test_encode_well_known()
{
    int i;
    int p;
#define R 10
    for (p = 0; p < MAXPAT; ++p) {
        uint8_t inp[psizes[p] * R];
        for (i = 0; i < R; ++i)
            memcpy(&inp[psizes[p] * i], patterns[p], psizes[p]);

        uint8_t out[256];
        memset(out, 0xdb, sizeof(out));
        Data input = {.dt = inp, .sz = sizeof(inp)};
        Data output = {.dt = out, .sz = sizeof(out)};
        uint32_t w;
        uint32_t r;
        uint32_t s = psizes[p];
        repeats(input, s, &r);
        well_known(inp, psizes[p], &w);
        assert(r == R - 1);
        assert(p == w);
        assert(encode_wellknown(&output, &input, w, r) == 0);
        assert(*output.dt == 0xdb);
        uint8_t decode[sizeof(inp)];
        assert(mydecode(out, sizeof(out) - output.sz, decode, sizeof(decode)) ==
               0);
        assert(memcmp(inp, decode, sizeof(inp)) == 0);
    }
    fprintf(stderr, "passed %s\n", __func__);
}

static void test_decode_int(size_t sz, uint8_t *seq)
{
    typedef uint8_t t[sz];
    t *a, *b, *c;
    t *b_hints, *c_hints;
    uint16_t hints[N * sz + 1];
    size_t asz = sizeof(t) * N;
    size_t bsz = asz * 2;
    size_t csz = asz;
    a = malloc(asz);
    b = malloc(bsz + 1);
    b_hints = malloc(bsz + 1);
    c = malloc(csz + 1);
    c_hints = malloc(csz + 1);
    assert(a);
    assert(b);
    assert(c);
    assert(b_hints);
    assert(c_hints);
    for (int i = 0; i < N; ++i) {
        memcpy(&a[i][0], seq, sz);
        hints[i] = sz;
        hints[i + 1] = 0;
        size_t isz = (i + 1) * sz;
        memset(b, 0xff, bsz + 1);
        memset(b_hints, 0xff, bsz + 1);
        memset(c, 0xff, csz + 1);
        memset(c_hints, 0xff, csz + 1);
        Comdb2RLE comp = { .in = &a[0][0], .insz = isz, .out = &b[0][0], .outsz = bsz};
        Comdb2RLE comp_hints = { .in = &a[0][0], .insz = isz, .out = &b_hints[0][0], .outsz = bsz};
        assert(compressComdb2RLE(&comp) == 0);       // was able to compress (even if took more space)
        assert(compressComdb2RLE_hints(&comp_hints, hints) == 0); // was able to compress (even if took more space)
        assert(*((uint8_t *)b + bsz) == 0xff);       // didn't write beyond provided buffer
        assert(*((uint8_t *)b + comp.outsz) == 0xff);// didn't write beyond compressed output
        assert(*((uint8_t *)b_hints + bsz) == 0xff); // didn't write beyond provided buffer
        assert(*((uint8_t *)b_hints + comp_hints.outsz) == 0xff);    // didn't write beyond compressed output
        if (sz != 6) {   // 6 is not a wellknown size - so won't compress
            if (i > 2) { // small inputs don't compress
                assert(comp.insz > comp.outsz);      // other inputs should compress
                assert(comp_hints.insz > comp_hints.outsz);
            }
        }
        Comdb2RLE domp = { .in = &b[0][0], .insz = comp.outsz, .out = &c[0][0], .outsz = csz};
        Comdb2RLE domp_hints = {.in = &b_hints[0][0], .insz = comp_hints.outsz, .out = &c_hints[0][0], .outsz = csz};
        assert(decompressComdb2RLE(&domp) == 0);       // was able to decode
        assert(decompressComdb2RLE(&domp_hints) == 0); // was able to decode
        assert(domp.outsz == isz);                     // matches input size
        assert(domp_hints.outsz == isz);               // matches input size
        assert(memcmp(a, c, isz) == 0);                // matches input bytes
        assert(memcmp(a, c_hints, isz) == 0);          // matches input bytes
        assert(*((uint8_t *)c + csz) == 0xff);         // didn't write beyond provided output
        assert(*((uint8_t *)c_hints + csz) == 0xff);   // didn't write beyond provided output
        assert(*((uint8_t *)c + domp.outsz) == 0xff);  // didn't write beyond decompressed output
        assert(*((uint8_t *)c_hints + domp_hints.outsz) == 0xff); // didn't write beyond decompressed output
    }
    free(a);
    free(b);
    free(b_hints);
    free(c);
    free(c_hints);
}

static void test_decode()
{
    uint8_t a0[] = {0xa0};
    uint8_t a1[] = {0xa0, 0xa1};
    uint8_t a2[] = {0xa0, 0xa1, 0xa2};
    uint8_t a3[] = {0xa0, 0xa1, 0xa2, 0xa3, 0xa4};
    uint8_t a4[] = {0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5};
    uint8_t a5[] = {0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8};
    test_decode_int(sizeof(a0), &a0[0]);
    test_decode_int(sizeof(a1), &a1[0]);
    test_decode_int(sizeof(a2), &a2[0]);
    test_decode_int(sizeof(a3), &a3[0]);
    test_decode_int(sizeof(a4), &a4[0]);
    test_decode_int(sizeof(a5), &a5[0]);
    fprintf(stderr, "passed %s\n", __func__);
}

static void test_repeat_rev()
{
    uint8_t buf[N];
    memset(buf, 0xdb, N);
    for (unsigned i = 1; i <= N; ++i) {
        buf[N - i] = 0xff;
        for (unsigned j = 0; j < N - i; ++j) {
            Data d = {.dt = buf + j, .sz = N - j};
            uint32_t r;
            repeats_rev(&d, N - j, &r);
            assert(r == i - 1);
        }
    }
    fprintf(stderr, "passed %s\n", __func__);
}

/*
Problematic when starting with short or int that = 0 followed by 2 compressible strings
Given short, vutf8[1000], vutf8[500], insert (0, 'hi', 'hj') = (0, 036869, 03686a)
Previously second field would not be compressed since would jump to middle of second field and not compress in this case
Previously:
compressComdb2RLE_hints
   encode_repeat: 2 x 0x080000
     encode_prev: 0x0003686900000(many many 0s)000800000003686a
encode_wellknown: 498 x 0x00
Now:
compressComdb2RLE_hints
   encode_repeat: 2 x 0x080000
     encode_prev: 0x00036869
encode_wellknown: 998 x 0x00
     encode_prev: 0x0800000003686a
encode_wellknown: 498 x 0x00
*/
static void test_encode_middle_field()
{
    const uint8_t data = 0x08;
    #pragma pack(1)
    struct row {
        uint8_t h1; /* header 1 */
        u_short c1; //uint8_t c1[2]; /* column 1 */

        uint8_t h2;
        struct {
            int32_t len;
            uint8_t dta[1000]; // previously would fail to encode this
        } c2;

        uint8_t h3;
        struct {
            int32_t len;
            uint8_t dta[500];
        } c3;
    };
    #pragma pack()

    struct row in =  {
        .h1 = data,
        .c1 = 0,

        .h2 = data,
        .c2 = { .len = htonl(strlen("hi") + 1), .dta = "hi"},

        .h3 = data,
        .c3 = { .len = htonl(strlen("hj") + 1), .dta = "hj"},
    };

    uint8_t out[sizeof(in) * 2];
    uint8_t out_in[sizeof(in)];

    uint16_t hints[] = {
        sizeof(in.c1) + 1,
        sizeof(in.c2) + 1,
        sizeof(in.c3) + 1,
        0
    };

    Comdb2RLE c = { .in = (uint8_t *)&in, .insz = sizeof(in), .out = out, .outsz = sizeof(out) };
    assert(compressComdb2RLE_hints(&c, hints) == 0);

    assert(c.outsz < 100); // assert this compresses well

    Comdb2RLE d = { .in = (uint8_t *)out, .insz = c.outsz, .out = out_in, .outsz = sizeof(out_in) };
    assert(decompressComdb2RLE(&d) == 0);
    assert(d.outsz == c.insz);
    assert(memcmp(&in, out_in, sizeof(in)) == 0);
    fprintf(stderr, "passed %s\n", __func__);
}

/*
Given short, vutf8[1000], short, vutf8[500], insert (0, 'hi', 1, 'hj') = (0, 036869, 1, 03686a)
Similar to above, previously second field would not be compressed since would jump to middle of second field and not compress in this case
The difference is that in this case when you reach c3 (short=1), which=0 in compressComdb2RLE_hints()
*/
static void test_encode_middle_field_2()
{
    const uint8_t data = 0x08;
    #pragma pack(1)
    struct row {
        uint8_t h1; /* header 1 */
        u_short c1; //uint8_t c1[2]; /* column 1 */

        uint8_t h2;
        struct {
            int32_t len;
            uint8_t dta[1000]; // previously would fail to encode this
        } c2;

        uint8_t h3;
        u_short c3;

        uint8_t h4;
        struct {
            int32_t len;
            uint8_t dta[500];
        } c4;
    };
    #pragma pack()

    struct row in =  {
        .h1 = data,
        .c1 = 0,

        .h2 = data,
        .c2 = { .len = htonl(strlen("hi") + 1), .dta = "hi"},

        .h3 = data,
        .c3 = 1,

        .h4 = data,
        .c4 = { .len = htonl(strlen("hj") + 1), .dta = "hj"},
    };

    uint8_t out[sizeof(in) * 2];
    uint8_t out_in[sizeof(in)];

    uint16_t hints[] = {
        sizeof(in.c1) + 1,
        sizeof(in.c2) + 1,
        sizeof(in.c3) + 1,
        sizeof(in.c4) + 1,
        0
    };

    Comdb2RLE c = { .in = (uint8_t *)&in, .insz = sizeof(in), .out = out, .outsz = sizeof(out) };
    assert(compressComdb2RLE_hints(&c, hints) == 0);

    assert(c.outsz < 100); // assert this compresses well

    Comdb2RLE d = { .in = (uint8_t *)out, .insz = c.outsz, .out = out_in, .outsz = sizeof(out_in) };
    assert(decompressComdb2RLE(&d) == 0);
    assert(d.outsz == c.insz);
    assert(memcmp(&in, out_in, sizeof(in)) == 0);
    fprintf(stderr, "passed %s\n", __func__);
}

int main(int argc, char *argv[])
{
    test_varint();
    test_repeat();
    test_repeat_rev();
    test_well_known();
    test_encode_prev();
    test_encode_repeat();
    test_encode_well_known();
    test_decode();
    test_encode_middle_field();
    test_encode_middle_field_2();

    fprintf(stderr, "PASSED ALL TESTS\n");
    return EXIT_SUCCESS;
}
