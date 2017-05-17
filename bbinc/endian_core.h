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

#ifndef _INCLUDED_ENDIAN_H
#define _INCLUDED_ENDIAN_H

#include <inttypes.h>
#include <string.h>
#include <strings.h>
#include <stdint.h>

/*
** buf*get(dst, dst_len, src, src_end)
** buf*put(src, src_len, dst, dst_end)
*/

#define buf_get(d, l, s, e)                                                    \
    (const uint8_t *)                                                          \
        buf_get_int((d), (l), (void *)(s), (void *)(e), (void *)(s))
#define buf_put(s, l, d, e)                                                    \
    (uint8_t *) buf_get_int((d), (l), (void *)(s), (void *)(e), (d))
#define buf_little_get(d, l, s, e)                                             \
    (const uint8_t *)                                                          \
        buf_little_get_int((d), (l), (void *)(s), (void *)(e), (void *)(s))
#define buf_little_put(s, l, d, e)                                             \
    (uint8_t *) buf_little_get_int((d), (l), (void *)(s), (void *)(e), (d))
#define buf_no_net_get(d, l, s, e)                                             \
    (const uint8_t *)                                                          \
        buf_no_net_get_int((d), (l), (void *)(s), (void *)(e), (void *)(s))
#define buf_no_net_put(s, l, d, e)                                             \
    (uint8_t *) buf_no_net_get_int((d), (l), (void *)(s), (void *)(e), (d))

#if defined(_LINUX_SOURCE)
#include <endian_core.amd64.h>
#elif defined(_IBM_SOURCE)
#include <endian_core.powerpc.h>
#elif defined(_SUN_SOURCE)
#include <endian_core.sparc.h>
#else
#error "PROVIDE MACHINE SPECIFIC BYTE FLIPPERS"
#endif

static inline void *buf_no_net_get_int(void *v_dst, size_t len, void *v_src,
                                       void *v_end, void *v_chk)
{
    uint8_t *src = v_src;
    uint8_t *dst = v_dst;
    uint8_t *chk = v_chk;
    uint8_t *end = v_end;
    if (chk == NULL || end == NULL)
        return NULL;
    if (end < chk || len > (end - chk))
        return NULL;
    end = chk + len;
    memcpy(dst, src, len);
    return end;
}

static inline uint8_t *buf_zero_put(size_t src_len, uint8_t *p_dst,
                                    const uint8_t *p_dst_end)
{
    uint8_t *p_dst_cpy_end;
    if (p_dst_end < p_dst || src_len > (p_dst_end - p_dst))
        return NULL;
    p_dst_cpy_end = p_dst + src_len;
    bzero(p_dst, src_len);
    return p_dst_cpy_end;
}

static inline uint8_t *buf_skip(size_t len, uint8_t *p_dst,
                                const uint8_t *p_dst_end)
{
    if (p_dst_end < p_dst || len > (p_dst_end - p_dst))
        return NULL;
    return p_dst + len;
}

#endif
