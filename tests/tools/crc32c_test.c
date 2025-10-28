#include <crc32c.h> /* subject under test */

#include <logmsg.h>

#include <assert.h>
#include <sys/time.h>
#include <stdlib.h>

int logmsg(loglvl lvl, const char *fmt, ...)
{
    va_list args;
    va_start(args, fmt);
    int ret = printf(fmt, args);
    va_end(args);
    return ret;
}

void timediff(const char *s)
{
    static struct timeval tv;
    struct timeval tmp;

    gettimeofday(&tmp, NULL);
    int sec = (tmp.tv_sec - tv.tv_sec) * 1000000;
    int usec = (tmp.tv_usec - tv.tv_usec);
    if (tv.tv_sec)
        printf("%20.20s diff = %12.dusec\n", s, sec + usec);
    tv = tmp;
}

__attribute__((noinline)) int f(int a)
{
    return a;
}

void test_null_string()
{
    printf("Test that NULL does not crash hw %d", crc32c_comdb2(NULL, 0));
    printf("...check\n");
}

void test_empty_string()
{
    printf("''   sw 0x%x hw 0x%x", crc32c_software((const uint8_t *)"", 0, 0), crc32c_comdb2((const uint8_t *)"", 0));

    if (crc32c_comdb2((const uint8_t *)"", 0) != 0x0) {
        printf("...crc2c for '' not correct\n");
        exit(1);
    } else {
        printf("...check\n");
    }
}

void test_non_empty_string()
{
    printf("'a'  sw 0x%x hw 0x%x", crc32c_software((const uint8_t *)"a", 1, 0), crc32c_comdb2((const uint8_t *)"a", 1));

    if (crc32c_comdb2((const uint8_t *)"a", 1) != 0x93ad1061) {
        printf("...crc2c for 'a' not correct\n");
        exit(1);
    } else {
        printf("...check\n");
    }
}

void test_software_vs_hardware()
{
#define MAXLEN 1 << 15
    uint8_t lbuf[MAXLEN];
    int i;
    lbuf[0] = 'a';

    for (i = 1; i < MAXLEN; i++) {
        lbuf[i] = i;
    }

    timediff("start");

    for (i = 1; i < MAXLEN; i++) {
        int a = crc32c_software(lbuf, i, 0);
        f(a);
    }
    timediff("software: ");

    for (i = 1; i < MAXLEN; i++) {
        int a = crc32c_comdb2(lbuf, i);
        f(a);
    }
    timediff("hardware: ");

    for (i = 1; i < MAXLEN; i++) {
        assert(crc32c_software(lbuf, i, 0) == crc32c_comdb2(lbuf, i));
    }

    printf("successfully tested %d strings\n", i);
}

void test_misaligned_access_bug()
{
    printf("Test that memory accesses are aligned on buffers larger than 1024 bytes");

    uint32_t size = 1025;
    char *buf = (char *)malloc(sizeof(char) * size);

    for (uint32_t i = 0; i < size; i++) {
        buf[i] = i;
    }

    crc32c((uint8_t *)buf, size);

    free(buf);
    printf("...check\n");
}

int main()
{
    crc32c_init(1);

    test_null_string();
    test_empty_string();
    test_non_empty_string();
    test_software_vs_hardware();
    test_misaligned_access_bug();

    return 0;
}
