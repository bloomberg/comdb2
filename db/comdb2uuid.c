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

#include "comdb2uuid.h"

#include <stdint.h>
#include <time.h>
#include <openssl/rand.h>

int gbl_uuid_v7 = 1;

static __thread int uuid_rng_seeded;
static __thread uint64_t uuid_rng_s[4];

static inline uint64_t uuid_rotl(uint64_t x, int k)
{
    return (x << k) | (x >> (64 - k));
}

static uint64_t uuid_rng_next(void)
{
    uint64_t *s = uuid_rng_s;
    const uint64_t r = uuid_rotl(s[1] * 5, 7) * 9;
    const uint64_t t = s[1] << 17;
    s[2] ^= s[0];
    s[3] ^= s[1];
    s[1] ^= s[2];
    s[0] ^= s[3];
    s[2] ^= t;
    s[3] = uuid_rotl(s[3], 45);
    return r;
}

void comdb2uuid(uuid_t u)
{
    if (!gbl_uuid_v7) {
        uuid_generate(u);
        return;
    }

    if (!uuid_rng_seeded) {
        RAND_bytes((unsigned char *)uuid_rng_s, sizeof(uuid_rng_s));
        uuid_rng_seeded = 1;
    }

    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    uint64_t ms = (uint64_t)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
    uint64_t ra = uuid_rng_next(); /* rand_a: 12 bits */
    uint64_t rb = uuid_rng_next(); /* rand_b: 62 bits */

    u[0] = ms >> 40;
    u[1] = ms >> 32;
    u[2] = ms >> 24;
    u[3] = ms >> 16;
    u[4] = ms >> 8;
    u[5] = ms;
    u[6] = 0x70 | ((ra >> 8) & 0x0f);  /* version 7 | rand_a hi */
    u[7] = ra;                         /* rand_a lo */
    u[8] = 0x80 | ((rb >> 56) & 0x3f); /* variant | rand_b hi */
    u[9] = rb >> 48;
    u[10] = rb >> 40;
    u[11] = rb >> 32;
    u[12] = rb >> 24;
    u[13] = rb >> 16;
    u[14] = rb >> 8;
    u[15] = rb;
}

char *comdb2uuidstr(uuid_t u, char out[37]);
inline char *comdb2uuidstr(uuid_t u, char out[37])
{
    uuid_unparse(u, out);
    return out;
}

void comdb2uuid_clear(uuid_t u) { uuid_clear(u); }

int comdb2uuidcmp(uuid_t u1, uuid_t u2) { return uuid_compare(u1, u2); }

void comdb2uuidcpy(uuid_t dst, uuid_t src) { uuid_copy(dst, src); }

int comdb2uuid_is_zero(uuid_t u)
{
    uuid_t zero;
    comdb2uuid_clear(zero);
    return !comdb2uuidcmp(u, zero);
}
