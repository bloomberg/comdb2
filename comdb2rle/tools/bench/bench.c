#include <comdb2rle.c>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <sys/time.h>
#include <lz4.h>

#if LZ4_VERSION_NUMBER < 10701
#define LZ4_compress_default LZ4_compress_limitedOutput
#endif


#define ff

static int timeval_subtract(struct timeval *result, struct timeval *x, struct timeval *y);

int main()
{
	double sec;
	int rc;
	struct timeval a, b, c;
	uint8_t *in, *out, *chk; 

	const int isz = INT32_MAX; // This is max lz4 will take without using the streaming api
	const double mb = ((double)isz + 1) / (1024 * 1024);
	const int osz = 10 * 1024 * 1024; // MB
	printf("Input:  %d bytes (%g MB)\n", isz, mb);

	if ((in = malloc(isz)) == NULL) {
		perror("malloc 1");
		return EXIT_FAILURE;
	}

	if ((out = malloc(osz)) == NULL) {
		perror("malloc 2");
		return EXIT_FAILURE;
	}

	if ((chk = malloc(isz)) == NULL) {
		perror("malloc 3");
		return EXIT_FAILURE;
	}

	puts("0xdb'ing input buf");
	mymemset(in, 0xdb, isz);

#if 1
	printf("lz4_compress: ");
	gettimeofday(&a, NULL);
	if ((rc = LZ4_compress_default(in, out, isz, osz)) <= 0) {
		puts("lz4 compression failed");
		return EXIT_FAILURE;
	}
	gettimeofday(&b, NULL);
	timeval_subtract(&c, &b, &a);
	sec = (double)c.tv_sec + c.tv_usec / 1000000.0;
	printf("%f%% sz:%u sec:%d msec:%d mb/sec:%g\n",
	  rc * 100.0 / isz, rc, c.tv_sec, c.tv_usec / 1000, mb / sec);

#ifdef ff
	puts("0xff'ing output buf");
	mymemset(chk, 0xff, isz);
#endif
	printf("LZ4_decompress_fast: ");
	gettimeofday(&a, NULL);
	if ((rc = LZ4_decompress_fast(out, chk, isz)) <= 0) {
		puts("lz4 decompression failed");
		return EXIT_FAILURE;
	}
	gettimeofday(&b, NULL);
	timeval_subtract(&c, &b, &a);
	sec = (double)c.tv_sec + c.tv_usec / 1000000.0;
	printf("memcmp:%d sec:%d msec:%d mb/sec:%g\n",
	  memcmp(in, chk, isz), c.tv_sec, c.tv_usec / 1000, mb / sec);

	puts("***********************");
#endif
	Comdb2RLE rle = { .in = in, .insz = isz, .out = out, .outsz = osz };
	printf("compressComdb2RLE: ");
	gettimeofday(&a, NULL);
	if (compressComdb2RLE(&rle) != 0) {
		puts("crle compression failed");
		return EXIT_FAILURE;
	}
	gettimeofday(&b, NULL);
	timeval_subtract(&c, &b, &a);
	sec = (double)c.tv_sec + c.tv_usec / 1000000.0;
	printf("%f%% sz:%u sec:%d msec:%d mb/sec:%g\n",
	  rle.outsz * 100.0 / isz, rle.outsz, c.tv_sec, c.tv_usec / 1000, mb / sec);

	rle.in = out;
	rle.insz = rle.outsz;
	rle.out = chk;
	rle.outsz = isz;
#ifdef ff
	puts("0xff'ing output buf");
	mymemset(chk, 0xff, isz);
#endif
	printf("decompressComdb2RLE:");
	gettimeofday(&a, NULL);
	if (decompressComdb2RLE(&rle) != 0) {
		puts("crle compression failed");
		return EXIT_FAILURE;
	}
	gettimeofday(&b, NULL);
	timeval_subtract(&c, &b, &a);
	sec = (double)c.tv_sec + c.tv_usec / 1000000.0;
	printf("memcmp:%d sec:%d msec:%d mb/sec:%g\n",
	  memcmp(in, chk, isz), c.tv_sec, c.tv_usec / 1000, mb / sec);
	free(in);
	free(out);
	free(chk);
	return EXIT_SUCCESS;
}

static int timeval_subtract(struct timeval *result, struct timeval *x, struct timeval *y)
{
  /* Perform the carry for the later subtraction by updating y. */
  if (x->tv_usec < y->tv_usec) {
    int nsec = (y->tv_usec - x->tv_usec) / 1000000 + 1;
    y->tv_usec -= 1000000 * nsec;
    y->tv_sec += nsec;
  }
  if (x->tv_usec - y->tv_usec > 1000000) {
    int nsec = (x->tv_usec - y->tv_usec) / 1000000;
    y->tv_usec += 1000000 * nsec;
    y->tv_sec -= nsec;
  }

  /* Compute the time remaining to wait.
     tv_usec is certainly positive. */
  result->tv_sec = x->tv_sec - y->tv_sec;
  result->tv_usec = x->tv_usec - y->tv_usec;

  /* Return 1 if result is negative. */
  return x->tv_sec < y->tv_sec;
}
