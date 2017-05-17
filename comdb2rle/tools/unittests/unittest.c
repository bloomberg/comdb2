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

#include <comdb2rle.c>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <alloca.h>
#include <assert.h>

static int mydecode(uint8_t *in, uint32_t insz, uint8_t *out, uint32_t outmax)
{
	Comdb2RLE d = { .in = in, .insz = insz, .out = out, .outsz = outmax };
	return decompressComdb2RLE(&d);
}

#define TEST_REPEAT(d, rcnt, rsize)				\
do {								\
	dt.sz = sizeof(d);					\
	dt.dt = d;						\
	i = 0;							\
	while (i < CNT(sizes)) {				\
		if (repeats(dt, sizes[i], &r))			\
			break;					\
		++i;						\
	}							\
	assert(r == rcnt);					\
	if (rsize < 0) assert(i == CNT(sizes));			\
	else           assert(sizes[i] == rsize);		\
	break;							\
} while (0)

static void test_repeat()
{
	uint32_t i, r;
	Data dt;

	uint8_t d0[] = { 0x00 };

	uint8_t d1[] = { 0x00,
			 0x00 };

	uint8_t d2[] = { 0x00, 0x01,
			 0x00, 0x01,
			 0x00, 0x01,
			 0x00, 0x01 };

	uint8_t d3[] = { 0x00, 0x01, 0x02,
			 0x00, 0x01, 0x02,
			 0x00, 0x01, 0x02 };

	uint8_t d4[] = { 0x00, 0x01, 0x02, 0x03, 0x04,
			 0x00, 0x01, 0x02, 0x03, 0x04 };


	uint8_t d5[] = { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08,
			 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 };

	uint8_t d6[] = { 0x00,
			 0xff };

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
		mymemset(buf, 0xdb, psizes[i]);
		assert(well_known(buf, psizes[i], &p) == 0);
	}

	/* Random garbage */
	for (i = 0; i < MAXPAT; ++i) {
		uint8_t buf[psizes[i]];
		mymemset(buf, 0xdb, psizes[i]);
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
		mymemset(out, 0xdb, sizeof(out));

		Data input = { .dt = inp, .sz = sizeof(inp) };
		Data output = { .dt = out, .sz = sizeof(out) };

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
	mymemset(buf, 0xdb, sizeof(buf));
	mymemset(out, 0xff, sizeof(out));
	Data input = { .dt = buf, .sz = sizeof(buf) };
	Data output = { .dt = out, .sz = sizeof(out) };
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
	for (i = 1; i < 1024; ++i) {
		uint8_t in[i];
		uint8_t out[i + 10];
		fread(in, 1, i, f);

		Data input, output;
		input.dt = in + sizeof(in); //need to be at end of buf
		input.sz = sizeof(in);
		output.dt = out;
		output.sz = sizeof(out);
		uint32_t prev = sizeof(in);
		assert(encode_prev(&output, &input, &prev) == 0);
		assert(prev == 0);
		uint8_t decode[i];
		bzero(decode, sizeof(decode));
		assert(mydecode(out, sizeof(out) - output.sz, decode, sizeof(decode)) == 0);
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
		mymemset(out, 0xdb, sizeof(out));
		Data input = { .dt = inp, .sz = sizeof(inp) };
		Data output = { .dt = out, .sz = sizeof(out) };
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
		assert(mydecode(out, sizeof(out) - output.sz, decode, sizeof(decode)) == 0);
		assert(memcmp(inp, decode, sizeof(inp)) == 0);
	}
	fprintf(stderr, "passed %s\n", __func__);
}

#define N 1024
static void test_decode(size_t sz, uint8_t *seq)
{
	typedef uint8_t t[sz];
	t *a, *b, *c;
	size_t asz = sizeof(t) * N;
	size_t bsz = asz * 2;
	size_t csz = asz;
	a = malloc(asz);
	b = malloc(bsz + 1);
	c = malloc(csz + 1);
	assert(a);
	assert(b);
	assert(c);
	int i;
	for (i = 0; i < N; ++i) {
		memcpy(&a[i][0], seq, sz);
	}
	for (i = 0; i < N; ++i) {
		size_t isz = i * sz;
		mymemset(b, 0xff, bsz + 1);
		mymemset(c, 0xff, csz + 1);
		Comdb2RLE comp = { .in = &a[0][0], .insz = isz, .out = &b[0][0], .outsz = bsz };
		if (isz < 2) {
			assert(compressComdb2RLE(&comp) != 0);
			continue;
		}
		assert(compressComdb2RLE(&comp) == 0); // was able to compress (even if took more space)
		assert(*((uint8_t *)b + bsz) == 0xff); // didn't write beyond provided buffer
		assert(*((uint8_t *)b + comp.outsz) == 0xff); // didn't write beyond compressed output
		if (sz != 6) { // 6 is not a wellknown size - so won't compress
			if (i > 2) { // small inputs don't compress
				assert(comp.insz > comp.outsz); // other inputs should compress
			}
		}
		Comdb2RLE domp = { .in = &b[0][0], .insz = comp.outsz, .out = &c[0][0], .outsz = csz };
		assert(decompressComdb2RLE(&domp) == 0); // was able to decode
		assert(domp.outsz == isz); // matches input size
		assert(memcmp(a, c, isz) == 0); // matches input bytes
		assert(*((uint8_t*)c + csz) == 0xff); // didn't write beyond provided output
		assert(*((uint8_t*)c + domp.outsz) == 0xff); // didn't write beyond decompressed output
	}
	free(a);
	free(b);
	free(c);
}

static void test_refactored_decode()
{
	uint8_t a0[] = { 0xa0 };
	uint8_t a1[] = { 0xa0, 0xa1 };
	uint8_t a2[] = { 0xa0, 0xa1, 0xa2 };
	uint8_t a3[] = { 0xa0, 0xa1, 0xa2, 0xa3, 0xa4 };
	uint8_t a4[] = { 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5 };
	uint8_t a5[] = { 0xa0, 0xa1, 0xa2, 0xa3, 0xa4, 0xa5, 0xa6, 0xa7, 0xa8 };
	test_decode(sizeof(a0), &a0[0]);
	test_decode(sizeof(a1), &a1[0]);
	test_decode(sizeof(a2), &a2[0]);
	test_decode(sizeof(a3), &a3[0]);
	test_decode(sizeof(a4), &a4[0]);
	test_decode(sizeof(a5), &a5[0]);
	fprintf(stderr, "passed %s\n", __func__);
}

void test_mymemset()
{
	unsigned i;
	for (i = 1; i < 40960; ++i) {
		uint8_t a[i + 1], b[i + 1];
		b[i] = a[i] = 0x00;
		memset(a, 0xff, i);
		mymemset(b, 0xff, i);
		assert(memcmp(a, b, i) == 0);
		assert(a[i] == b[i]);

		b[i] = a[i] = 0xff;
		memset(a, 0x00, i);
		mymemset(b, 0x00, i);
		assert(memcmp(a, b, i) == 0);
		assert(a[i] == b[i]);
	}
	fprintf(stderr, "passed %s\n", __func__);
}

int main(int argc, char *argv[])
{
	test_mymemset();
	test_varint();
	test_repeat();
	test_well_known();
	test_encode_prev();
	test_encode_repeat();
	test_encode_well_known();
	test_refactored_decode();

	fprintf(stderr, "PASSED ALL TESTS\n");
	return EXIT_SUCCESS;
}
