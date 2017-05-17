#ifndef GETTIMEOFDAY_MS_H
#define GETTIMEOFDAY_MS_H
#include <inttypes.h>
#include <sys/time.h>
#include <stdlib.h>

static inline uint64_t gettimeofday_ms()
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_sec * 1000ULL + t.tv_usec / 1000ULL;
}
#endif
