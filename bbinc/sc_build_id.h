#ifndef INCLUDED_SC_BUILD_ID_H
#define INCLUDED_SC_BUILD_ID_H

#include <stdint.h>
#include <string.h>

enum { SC_BUILD_ID_LEN = 16 };

typedef struct sc_build_id {
    uint8_t bytes[SC_BUILD_ID_LEN];
} sc_build_id_t;

static inline int sc_build_id_is_zero(const sc_build_id_t *id)
{
    static const sc_build_id_t zero;
    return id == NULL || memcmp(id, &zero, sizeof(*id)) == 0;
}

static inline int sc_build_id_equal(const sc_build_id_t *a, const sc_build_id_t *b)
{
    return a != NULL && b != NULL && memcmp(a, b, sizeof(*a)) == 0;
}

#endif