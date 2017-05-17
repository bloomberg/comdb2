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

static inline void *buf_get_int(void *v_dst, size_t len, void *v_src,
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
    uint16_t *s16;
    uint32_t *s32, *d32;
    switch (len) {
    case sizeof(uint16_t):
        s16 = v_src;
        __asm__ __volatile__("sthbrx %1,0,%2"
                             : "=m"(*dst)
                             : "r"(*s16), "r"(dst));
        break;
    case sizeof(uint32_t):
        s32 = v_src;
        __asm__ __volatile__("stwbrx %1,0,%2"
                             : "=m"(*dst)
                             : "r"(*s32), "r"(dst));
        break;
    case sizeof(uint64_t):
        s32 = v_src;
        d32 = v_dst;
        __asm__ __volatile__("stwbrx %1,0,%2"
                             : "=m"(*d32)
                             : "r"(*(s32 + 1)), "r"(d32));
        __asm__ __volatile__("stwbrx %1,0,%2"
                             : "=m"(*(d32 + 1))
                             : "r"(*s32), "r"(d32 + 1));
        break;
    default:
        memcpy(dst, src, len);
        break;
    }
    return end;
}
