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
    memcpy(dst, src, len);
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
        dst[0] = src[1];
        dst[1] = src[0];
        break;
    case sizeof(uint32_t):
        dst[0] = src[3];
        dst[1] = src[2];
        dst[2] = src[1];
        dst[3] = src[0];
        break;
    case sizeof(uint64_t):
        dst[0] = src[7];
        dst[1] = src[6];
        dst[2] = src[5];
        dst[3] = src[4];
        dst[4] = src[3];
        dst[5] = src[2];
        dst[6] = src[1];
        dst[7] = src[0];
        break;
    default:
        memcpy(dst, src, len);
        break;
    }
    return end;
}
