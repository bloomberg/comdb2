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

static inline const void *buf_get_int(void *v_dst, size_t len,
                                      const void *v_src, const void *v_end,
                                      void *v_chk)
{
    const uint8_t *src = v_src;
    uint8_t *chk = v_chk;
    uint8_t *dst = v_dst;
    const uint8_t *end = v_end;
    if (chk == NULL || end == NULL)
        return NULL;
    if (end < chk || len > (end - chk))
        return NULL;
    end = chk + len;
    switch (len) {
    case sizeof(uint16_t):
        *(uint16_t *)dst = __builtin_bswap16(*(uint16_t *)src);
        break;
    case sizeof(uint32_t):
        *(uint32_t *)dst = __builtin_bswap32(*(uint32_t *)src);
        break;
    case sizeof(uint64_t):
        *(uint64_t *)dst = __builtin_bswap64(*(uint64_t *)src);
        break;
    default:
        memcpy(dst, src, len);
        break;
    }
    return end;
}

static inline void *buf_little_get_int(void *v_dst, size_t len, void *v_src,
                                       void *v_end, void *v_chk)
{
    uint8_t *src = v_src;
    uint8_t *chk = v_chk;
    uint8_t *dst = v_dst;
    uint8_t *end = v_end;
    if (chk == NULL || end == NULL)
        return NULL;
    if (end < chk || len > (end - chk))
        return NULL;
    end = chk + len;
    switch (len) {
    case sizeof(uint16_t):
        *(uint16_t *)dst = *(uint16_t *)src;
        break;
    case sizeof(uint32_t):
        *(uint32_t *)dst = *(uint32_t *)src;
        break;
    case sizeof(uint64_t):
        *(uint64_t *)dst = *(uint64_t *)src;
        break;
    default:
        memcpy(dst, src, len);
        break;
    }
    return end;
}
