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
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <arpa/nameser_compat.h>
#include "comdb2rle.h"

#ifndef BYTE_ORDER
#   error "BYTE_ORDER not defined"
#endif

#ifdef CRLE_VERBOSE
#include <tohex.h>

static int doprint = 0;
#endif

#define CNT(x) (sizeof(x) / sizeof(x[0]))

#define STATIC_ASSERT(condition, name)                                         \
    static void assert_failed_##name(void)                                     \
    {                                                                          \
        switch (0) {                                                           \
        case 0:                                                                \
        case condition:                                                        \
            ;                                                                  \
        }                                                                      \
    }

/* Various NULLs */
static uint8_t p0[] = {0x02, 0x00, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00}; /*
static uint8_t p1[] = { 0x02, 0x00, 0x00, 0x00, 0x00 };
static uint8_t p2[] = { 0x02, 0x00, 0x00 }; */
/* Various 0s */
static uint8_t p3[] = {0x08, 0x80, 0x00, 0x00, 0x00,
                       0x00, 0x00, 0x00, 0x00}; /*
static uint8_t p4[] = { 0x08, 0x80, 0x00, 0x00, 0x00 };
static uint8_t p5[] = { 0x08, 0x80, 0x00 }; */
/* Various -1s */
static uint8_t p6[] = {0x08, 0x7f, 0xff, 0xff, 0xff,
                       0xff, 0xff, 0xff, 0xff}; /*
static uint8_t p7[] = { 0x08, 0x7f, 0xff, 0xff, 0xff };
static uint8_t p8[] = { 0x08, 0x7f, 0xff }; */
/* Floating point -1 */
static uint8_t p9[] = {0x08, 0x40, 0x0f, 0xff, 0xff,
                       0xff, 0xff, 0xff, 0xff}; // double
static uint8_t pa[] = {0x08, 0x40, 0x7f, 0xff, 0xff}; // float
/* Misc */
static uint8_t pb[] = {0x00}; // null
static uint8_t pc[] = {0x30}; // ascii 0
//	           pd MAXPAT
//	           pe ONEBYTE
//	           pf RESERVED

#define PATTERNS                                                               \
    XMACRO_PATTERNS(p0, sizeof(p0), "p0")                                      \
    XMACRO_PATTERNS(p0, 5, "p1")                                               \
    XMACRO_PATTERNS(p0, 3, "p2")                                               \
    XMACRO_PATTERNS(p3, sizeof(p3), "p3")                                      \
    XMACRO_PATTERNS(p3, 5, "p4")                                               \
    XMACRO_PATTERNS(p3, 3, "p5")                                               \
    XMACRO_PATTERNS(p6, sizeof(p6), "p6")                                      \
    XMACRO_PATTERNS(p6, 5, "p7")                                               \
    XMACRO_PATTERNS(p6, 3, "p8")                                               \
    XMACRO_PATTERNS(p9, sizeof(p9), "p9")                                      \
    XMACRO_PATTERNS(pa, sizeof(pa), "pa")                                      \
    XMACRO_PATTERNS(pb, sizeof(pb), "pb")                                      \
    XMACRO_PATTERNS(pc, sizeof(pc), "pc")

#define XMACRO_PATTERNS(pattern, size, name) pattern,
static uint8_t *patterns[] = {PATTERNS};
#undef XMACRO_PATTERNS
#define MAXPAT CNT(patterns)
#define ONEBYTE (MAXPAT + 1)

#define XMACRO_PATTERNS(pattern, size, name) size,
static size_t psizes[] = {PATTERNS};
#undef XMACRO_PATTERNS

/*
#define XMACRO_PATTERNS(pattern, size, name) name,
static const char *pnames[] = { PATTERNS };
#undef XMACRO_PATTERNS
*/

static uint8_t sizes[] = {1, 9, 5, 3, 2};

typedef struct {
#if BYTE_ORDER == BIG_ENDIAN
    uint32_t repeat : 3;  // num of times pattern repeats
    uint32_t more : 1;    // look at next byte for additional repeats
    uint32_t pattern : 4; // index into patterns[]
#elif BYTE_ORDER == LITTLE_ENDIAN
    uint32_t pattern : 4; // index into patterns[]
    uint32_t more : 1;    // look at next byte for additional repeats
    uint32_t repeat : 3;  // num of times pattern repeats
#else
#error "Unknown BYTE_ORDER defined"
#endif
    uint32_t unused : 24;
} Header;

typedef uint8_t HdrSz;

typedef union {
    Header header;
    HdrSz h;
} UHeader;

STATIC_ASSERT(sizeof(HdrSz) == 1, size_of_actual_hdr_should_be_1)
STATIC_ASSERT(sizeof(Header) == 4, size_of_header_should_be_4)
STATIC_ASSERT(sizeof(UHeader) == 4, size_of_uheader_should_be_4)

static uint8_t hmax = 0x07;

typedef struct {
    uint8_t *dt;
    size_t sz;
} Data;

static uint8_t varint_need(uint32_t i)
{
    if (i < 0x80)
        return 1;
    if (i < 0xff)
        return 2;
    if (i < 0xffff)
        return 3;
    if (i < 0xffffff)
        return 4;
    return 5;
}

#define ENCODE_NUMBER(i, o)                                                    \
    do {                                                                       \
        uint8_t need = varint_need(i);                                         \
        switch (need) {                                                        \
        case 5: o[need - 5] = 0x80 | (i >> 28);                                \
        case 4: o[need - 4] = 0x80 | (i >> 21);                                \
        case 3: o[need - 3] = 0x80 | (i >> 14);                                \
        case 2: o[need - 2] = 0x80 | (i >> 7);                                 \
        case 1: o[need - 1] = 0x7f & i;                                        \
        }                                                                      \
        o += need;                                                             \
    } while (0)

#define DECODE_NUMBER(i, o)                                                    \
    do {                                                                       \
        o = 0;                                                                 \
        while (*i & 0x80) {                                                    \
            uint8_t t = *i & 0x7f;                                             \
            o |= t;                                                            \
            o <<= 7;                                                           \
            ++i;                                                               \
        }                                                                      \
        o |= *i++;                                                             \
    } while (0)

/* p:ointer to pattern
 * s:ize of pattern
 * r:epeat pattern these many times
 * Adjusts input by the number of bytes consumed
 * Returns number of bytes reqd to decode */
static uint32_t decode(Data *input, uint8_t **p_, uint32_t *s_, uint32_t *r_)
{
    uint32_t r = 0;
    uint8_t *in = input->dt;
    Header *h = (Header *)in++;
#ifdef _SUN_SOURCE
    Header temp;
    memcpy(&temp, h, sizeof(temp));
    h = &temp;
#endif
    if (h->more)
        DECODE_NUMBER(in, r);
    r += h->repeat;

    uint8_t *p;
    uint32_t s;
    if (h->pattern < MAXPAT) {
        s = psizes[h->pattern];
        p = patterns[h->pattern];
    } else {
        if (h->pattern == ONEBYTE)
            s = 1;
        else
            DECODE_NUMBER(in, s);
        p = in;
        in += s;
    }
    input->sz -= (in - input->dt);
    input->dt = in;
    *p_ = p;
    *s_ = s;
    *r_ = r;
#ifdef CRLE_VERBOSE
    if (doprint) {
        fprintf(stderr, "%d x 0x", r + 1);
        print_hex(p, s);
    }
#endif
    return s * (r + 1);
}

/* Space required for encoding */
static uint32_t space_reqd(uint32_t r, uint32_t s)
{
    return sizeof(HdrSz) + (r > hmax ? varint_need(r - hmax) : 0) +
           (s > 1 ? (varint_need(s) + s) : s);
}

/* Check if 'sz' bytes repeat */
static uint32_t repeats(Data in, uint32_t sz, uint32_t *r_)
{
    uint32_t r;
    r = *r_ = 0;
    if (in.sz < (sz * 2))
        return 0;
    uint8_t *bp, *bx, bt;
    uint16_t *wp, word;
    switch (sz) {
    case 1:
        bt = *in.dt;        // 1st byte
        bp = in.dt + sz;    // byte ptr
        bx = in.dt + in.sz; // byte ptr max
#ifndef _SUN_SOURCE
        if (in.sz > 16) {
            size_t qz = in.sz - in.sz % 8; // # of quads
            uint64_t qw;
            memset(&qw, bt, sizeof(qw));             // 1st quad
            uint64_t *qp = (uint64_t *)in.dt;        // quad ptr
            uint64_t *qx = (uint64_t *)(in.dt + qz); // quad ptr max
            while (qp < qx && *qp == qw)
                ++qp;
            if ((uint8_t *)qp != in.dt) {
                if (qp < qx) // last quad != 1st
                    --qp;
                bp = (uint8_t *)qp;
            }
        }
#endif
        // check remainder byte at a time
        while (bp < bx && *bp == bt)
            ++bp;
        r = bp - in.dt - 1;
        break;
#ifndef _SUN_SOURCE
    case 2:
        word = *(uint16_t *)in.dt;
        wp = (uint16_t *)(in.dt + sz);
        in.sz -= (in.sz % sz);
        while ((in.sz -= sz) != 0 && word == *wp) {
            ++wp;
            ++r;
        }
        break;
#endif
    default:
        bp = in.dt + sz;
        in.sz -= (in.sz % sz);
        while ((in.sz -= sz) != 0) {
            if (memcmp(in.dt, bp, sz))
                break;
            bp += sz;
            ++r;
        }
        break;
    }
    *r_ = r;
    return r;
}

/* Look for known pattern of size s at d */
static int well_known(uint8_t *d, uint32_t s, uint32_t *w)
{
    *w = MAXPAT;
    for (uint32_t i = 0; i < MAXPAT; ++i) {
        if (s == psizes[i])
            if (memcmp(d, patterns[i], psizes[i]) == 0) {
                *w = i;
                return 1;
            }
    }
    return 0;
}

static uint8_t *encode_header(Data *output, uint32_t w, uint32_t r)
{
    UHeader h;
    h.header.pattern = w;
    uint32_t v; // part of r encoded as varint
    if (r > hmax) {
        h.header.more = 1;
        h.header.repeat = hmax;
        v = r - hmax;
    } else {
        h.header.more = 0;
        h.header.repeat = r;
        v = 0;
    }
    uint8_t *out = output->dt;
    *out++ = h.h;
    if (v)
        ENCODE_NUMBER(v, out);
    return out;
}

static int encode_prev(Data *output, const Data *input, uint32_t *prev)
{
    UHeader h;
    uint32_t p = *prev;
    uint8_t *out = output->dt;
    uint8_t *from = input->dt - p;
    uint32_t need = sizeof(h.h) + varint_need(p) + p;
    if (output->sz < need)
        return 1;
#ifdef CRLE_VERBOSE
    if (doprint) {
        fprintf(stderr, "     %s: 0x", __func__);
        print_hex(from, p);
    }
#endif
    h.h = 0;
    h.header.pattern = MAXPAT;
    *out++ = h.h;
    ENCODE_NUMBER(p, out);
    memcpy(out, from, p);
    out += p;
    uint32_t used = out - output->dt;
    assert(need == used);
    output->sz -= used;
    // xxx print_hex(output->dt, used);
    output->dt += used;
    *prev = 0;
    return 0;
}

/* r: num of times pattern repeats
 * w: index into wellknown patterns[] */
static int encode_wellknown(Data *output, Data *input, uint32_t w, uint32_t r)
{
    uint32_t reqd;
    if ((reqd = space_reqd(r, 0)) > output->sz)
        return 1;
#ifdef CRLE_VERBOSE
    if (doprint) {
        fprintf(stderr, "%s: %u x 0x", __func__, r + 1);
        print_hex(patterns[w], psizes[w]);
    }
#endif
    uint8_t *out = encode_header(output, w, r);
    uint32_t used = out - output->dt;
    uint32_t consumed = psizes[w] * (r + 1);
    assert(reqd == used);
    output->sz -= used;
    // xxx print_hex(output->dt, used);
    output->dt += used;
    input->sz -= consumed;
    input->dt += consumed;
    return 0;
}

/* r: num of times pattern repeats
 * s: size of pattern */
static int encode_repeat(Data *output, Data *input, uint32_t r, uint32_t s)
{
    uint32_t reqd;
    if ((reqd = space_reqd(r, s)) > output->sz)
        return 1;
#ifdef CRLE_VERBOSE
    if (doprint) {
        fprintf(stderr, "   %s: %u x 0x", __func__, r + 1);
        print_hex(input->dt, s);
    }
#endif
    uint8_t *out = encode_header(output, s == 1 ? ONEBYTE : MAXPAT, r);
    if (s > 1)
        ENCODE_NUMBER(s, out);
    memcpy(out, input->dt, s);
    out += s;
    uint32_t used = out - output->dt;
    uint32_t consumed = s * (r + 1);
    assert(reqd == used);
    output->sz -= used;
    // xxx print_hex(output->dt, used);
    output->dt += used;
    input->sz -= consumed;
    input->dt += consumed;
    return 0;
}

static int verify(Comdb2RLE *c)
{
#ifdef VERIFY_CRLE
    uint8_t bad = 0;
    uint8_t buf[c->insz];
    Comdb2RLE d = { .in = c->out, .insz = c->outsz, .out = buf, .outsz = c->insz};
    if (decompressComdb2RLE(&d) != 0) {
        fprintf(stderr, "Comdb2RLE decompress error size:%zu data:0x", c->insz);
        print_hex(c->in, c->insz);
        bad = 1;
    } else if (memcmp(c->in, buf, c->insz) != 0) {
        fprintf(stderr, "Comdb2RLE memcmp error - Input size:%zu data:0x", c->insz);
        print_hex(c->in, c->insz);
        bad = 1;
    }
    return bad;
#else
    return 0;
#endif
}

/*
** compressComdb2RLE returns =>
**   0: Success
**   1: Ran out of output buffer
*/
int compressComdb2RLE(Comdb2RLE *c)
{
    Data input = {.dt = c->in, .sz = c->insz};
    Data output = {.dt = c->out, .sz = c->outsz};
    uint32_t prev = 0;
    int greedy = input.sz > 1024;
next:
    while (input.sz) {
        uint32_t w; // wellknown pattern of bytes?
        uint32_t r; // pattern repeats
        uint32_t s; // pattern size
        uint32_t bw, br, bs; // best w, r, s
        uint32_t best, saved;
        best = saved = 0;
        bw = br = bs = UINT32_MAX;
        for (s = 0; s < CNT(sizes); ++s) {
            if (input.sz < sizes[s])
                continue;
            if (repeats(input, sizes[s], &r)) {
                uint32_t orig, save, need, size, which;
            check_wellknown:
                if (well_known(input.dt, sizes[s], &w)) {
                    size = 0;
                    which = 'w';
                } else if (r) {
                    size = sizes[s];
                    which = 'r';
                } else {
                    continue;
                }
                save = 0;
                orig = (r + 1) * sizes[s];
                need = space_reqd(r, size);
                save = orig - need;
                if (need < orig && save > saved) {
                    saved = save;
                    best = which;
                    bw = w;
                    br = r;
                    bs = s;
                }
                if (greedy)
                    break;
            } else {
                // no repeats, maybe this is a well known pattern
                r = 0;
                goto check_wellknown;
            }
        }

        if (best) {
            if (prev && encode_prev(&output, &input, &prev) != 0)
                return 1;
            if (best == 'w') {
                if (encode_wellknown(&output, &input, bw, br) != 0)
                    return 1;
            } else if (best == 'r') {
                if (encode_repeat(&output, &input, br, sizes[bs]) != 0)
                    return 1;
            } else {
                abort();
            }
            goto next;
        }
        ++prev;
        ++input.dt;
        --input.sz;
    }
    if (input.sz) {
        prev += input.sz;
        input.dt += input.sz;
        input.sz = 0;
    }
    if (prev && encode_prev(&output, &input, &prev))
        return 1;
    c->outsz = output.dt - c->out;
    return verify(c);
}

int decompressComdb2RLE(Comdb2RLE *d)
{
    Data input, output;
    input.dt = d->in;
    input.sz = d->insz;
    output.dt = d->out;
    output.sz = d->outsz;
    while (input.sz) {
        uint8_t *p;
        uint32_t reqd, s, r;
        if ((reqd = decode(&input, &p, &s, &r)) > output.sz)
            return 1;
        if (s == 1) {
            ++r;
            memset(output.dt, *p, r);
            output.dt += r;
            output.sz -= r;
        } else
            for (uint32_t i = 0; i <= r; ++i) {
                switch (s) {
                case 9:
                    output.dt[8] = p[8];
                    output.dt[7] = p[7];
                    output.dt[6] = p[6];
                    output.dt[5] = p[5];
                case 5:
                    output.dt[4] = p[4];
                    output.dt[3] = p[3];
                case 3:
                    output.dt[2] = p[2];
                case 2:
                    output.dt[1] = p[1];
                case 1:
                    output.dt[0] = p[0];
                    break;
                default:
                    memcpy(output.dt, p, s);
                    break;
                }
                output.dt += s;
                output.sz -= s;
            }
    }
    d->outsz = output.dt - d->out;
    return 0;
}

/* input: start of field
 * sz: of current field
 * r: output param */
static int repeats_rev(const Data *input, uint32_t sz, uint32_t *r)
{
    uint8_t *first = input->dt - 1; //sentinal -- one before first
    uint8_t *last = first + sz;
    uint8_t b = *last--;
    uint32_t dups = 0;
    while (last != first && b == *last--) {
        ++dups;
    }
    *r = dups;
    return dups;
}

static int encode_repeat_rev(Data *output, Data *input, uint32_t r, uint32_t sz, uint32_t *prev)
{
    uint32_t pfx = sz - r - 1;
    *prev += pfx;
    input->dt += pfx;
    input->sz -= pfx;
    if (*prev && encode_prev(output, input, prev)) {
        return 1;
    }
    uint32_t w;
    if (well_known(input->dt, 1, &w)) {
        if (encode_wellknown(output, input, w, r)) {
            return 1;
        }
    } else if (encode_repeat(output, input, r, 1)) {
        return 1;
    }
    *prev = 0;
    return 0;
}

static int encode_prev_rev(Data *output, const Data *input, uint32_t *prev)
{
    Data tmp = *input;
    uint32_t sz = *prev;
    tmp.dt -= sz;
    tmp.sz += sz;
    uint32_t r;
    if (repeats_rev(&tmp, sz, &r)) {
        *prev = 0;
        if (encode_repeat_rev(output, &tmp, r, sz, prev)) {
            return 1;
        }
    } else if (encode_prev(output, input, prev)) {
        return 1;
    }
    return 0;
}

int compressComdb2RLE_hints(Comdb2RLE *c, uint16_t *fld_hints)
{
    Data input = {.dt = c->in, .sz = c->insz};
    Data output = {.dt = c->out, .sz = c->outsz};
    uint32_t prev = 0; // should only be > 0 if references fields that cannot be compressed
    uint16_t sz;
    while ((sz = *fld_hints) != 0) {
        uint32_t w = 0; // wellknown pattern of bytes?
        uint32_t r = 0; // pattern repeats
        uint32_t which = 0;
        if (repeats(input, sz, &r)) {
            which = 'r';
        }
        uint32_t tmp_r;
        switch (sz) {
        case 1: case 2: case 3: case 5: case 9:
            if (well_known(input.dt, sz, &w)) {
                which = 'w';
                break;
            }
            // fall through
        default:
            if (repeats_rev(&input, sz, &tmp_r)) {
                if (r == 0) {
                    r = tmp_r;
                    which = 'v';
                    break;
                }
                uint32_t need, tmp_prev, tmp_need;
                need = space_reqd(r, sz); //total space to encode 'r'
                tmp_prev = sz - (tmp_r + 1); //bytes which don't repeat
                tmp_need = space_reqd(0, tmp_prev) + space_reqd(tmp_r, 1);
                tmp_need *= (r + 1); // total space to encode 'v'
                if (tmp_need < need) {
                    r = tmp_r;
                    which = 'v';
                }
            }
        }
        if (which) {
            if (which == 'v' && encode_repeat_rev(&output, &input, r, sz, &prev)) {
                return 1;
            } else if (prev && encode_prev_rev(&output, &input, &prev)) {
                return 1;
            }
            if (which == 'w' && encode_wellknown(&output, &input, w, r)) {
                return 1;
            } else if (which == 'r' && encode_repeat(&output, &input, r, sz)) {
                return 1;
            }
            if (which == 'v') {
                ++fld_hints;
            } else {
                // landed in the middle of a field?
                uint32_t consumed = (r + 1) * sz;
                uint32_t next = 0;
                while (next < consumed) {
                    next += *fld_hints;
                    ++fld_hints;
                }
                // adjust to point at next field
                uint32_t adj = next - consumed;

                // encode last adj bytes now if repeats so prev doesn't contain compressible data
                if (adj > 0 && repeats_rev(&input, adj, &tmp_r)) {
                    if (encode_repeat_rev(&output, &input, tmp_r, adj, &prev)) {
                        return 1;
                    }
                } else {
                    prev += adj;
                    input.dt += adj;
                    input.sz -= adj;
                }
            }
        } else {
            prev += sz;
            input.dt += sz;
            input.sz -= sz;
            ++fld_hints;
        }
    }
    if (prev && encode_prev_rev(&output, &input, &prev)) {
        return 1;
    }
    c->outsz = output.dt - c->out;
    return verify(c);
}
