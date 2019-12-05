#include<gettimeofday_ms.h>

uint64_t gettimeofday_ms()
{
    struct timeval t;
    gettimeofday(&t, NULL);
    return t.tv_sec * 1000ULL + t.tv_usec / 1000ULL;
}
