#include <stdlib.h>
#include "endian_core.h"

/**
 * Copies src_len bytes from pv_src to p_dst, converting to network byte order 
 * if neccessary.
 * Note: this only converts to network byte order if src_len == 2 || 4 || 8
 * @param pv_src    pointer to the source
 * @param src_len   length of the source
 * @param p_dst pointer to the destination
 * @param p_dst_end pointer to just past the end of p_dst
 * @return pointer to just after the written data if success; NULL otherwise
 * @see buf_get()
 */
uint8_t *buf_put(const void *pv_src, size_t src_len, uint8_t * p_dst,
        const uint8_t *p_dst_end)
{
    uint8_t *p_dst_cpy_end;

    if(p_dst == NULL || p_dst_end == NULL)
        return NULL;

    if(p_dst_end < p_dst || src_len > (p_dst_end - p_dst))
        return NULL;

    p_dst_cpy_end = p_dst + src_len;

    switch(src_len)
    {
        case sizeof(uint16_t):
            {
                uint16_t w;

                /* w = htons(*((uint16_t*)pv_src));  since all bb environment is
                 * of Big-endian this doesn't have any effect. But in case of 
                 * little endian architecture also this is not needed because 
                 * the following shift operators directly give right values of 
                 * MSB and LSB, no matter whatever may be the architecture */
                w = *((const uint16_t *)pv_src);
                p_dst[0] = w >> BITS_PER_BYTE;
                p_dst[1] = w & LSB_MASK;
            }
            break;

        case sizeof(uint32_t):
            {
                uint32_t d;

                /* d = htonl(*((uint32_t *)pv_src));
                 * The following shift operators converting directly into 
                 * network byte order */
                d = *((const uint32_t *)pv_src);
                p_dst[0] = d >> (3 * BITS_PER_BYTE);
                p_dst[1] = (d >> (2 * BITS_PER_BYTE)) & LSB_MASK;
                p_dst[2] = (d >> BITS_PER_BYTE) & LSB_MASK;
                p_dst[3] = d & LSB_MASK;
            }
            break;

        case sizeof(uint64_t):
            {
                uint64_t q;

                /* q = htonll(*((uint64_t *)pv_src));
                 * The following shift operators converting directly into 
                 * network byte order */
                q = *((const uint64_t *)pv_src);
                p_dst[0] = q >> (7 * BITS_PER_BYTE);
                p_dst[1] = (q >> (6 * BITS_PER_BYTE)) & LSB_MASK;
                p_dst[2] = (q >> (5 * BITS_PER_BYTE)) & LSB_MASK;
                p_dst[3] = (q >> (4 * BITS_PER_BYTE)) & LSB_MASK;
                p_dst[4] = (q >> (3 * BITS_PER_BYTE)) & LSB_MASK;
                p_dst[5] = (q >> (2 * BITS_PER_BYTE)) & LSB_MASK;
                p_dst[6] = (q >> BITS_PER_BYTE) & LSB_MASK;
                p_dst[7] = q & LSB_MASK;
            }
            break;

        default:
            {
                const uint8_t *p_src = (const uint8_t *)pv_src;
                for(; p_dst < p_dst_cpy_end; ++p_src, ++p_dst)
                    *p_dst = *p_src;
            }
            break;
    }

    return p_dst_cpy_end;
}

/**
 * Copies src_len bytes from p_src to pv_dst, converting to network byte order 
 * if neccessary.
 * Note: this only converts to network byte order if src_len == 2 || 4 || 8
 * @param pv_dst pointer to the destination
 * @param dst_len   length of the destination
 * @param p_src pointer to the source
 * @param p_src_end pointer to just after the end of p_src
 * @return pointer to just after the read data if success; NULL otherwise
 * @see buf_put()
 */
const uint8_t *buf_get(void *pv_dst, size_t dst_len, const uint8_t * p_src,
        const uint8_t *p_src_end)
{
    if(p_src == NULL || p_src_end == NULL)
        return NULL;

    if(p_src_end < p_src || dst_len > (p_src_end - p_src))
        return NULL;

    p_src_end = p_src + dst_len;

    switch(dst_len)
    {
        case sizeof(uint16_t): /* short */
            /* *((uint16_t *)pv_dst) = ntohs(w);  No need to do the conversion 
             * because the shift operator does the conversion and returns 
             * the value in the architecture dependent endian format. 
             * As you know, if the machine is big-endian then ntohs or 
             * htonl, etc doesn't make a difference */
            *(((uint16_t *)pv_dst)) = (p_src[0] << BITS_PER_BYTE) | p_src[1];
            break;

        case sizeof(uint32_t): /* double uint16_t, int */
            /* *((uint32_t *)pv_dst) = ntohl(d); */
            *((uint32_t *)pv_dst) = (p_src[0] << (3 * BITS_PER_BYTE))
               | (p_src[1] << (2 * BITS_PER_BYTE))
               | (p_src[2] << BITS_PER_BYTE)
               | p_src[3];
            break;

        case sizeof(uint64_t): /* quad uint16_t, long long */
            /* *((uint64_t *)pv_dst) = ntohll(q); */
            *((uint64_t *)pv_dst)
               = (((uint64_t)p_src[0]) << (7 * BITS_PER_BYTE))
               | (((uint64_t)p_src[1]) << (6 * BITS_PER_BYTE))
               | (((uint64_t)p_src[2]) << (5 * BITS_PER_BYTE))
               | (((uint64_t)p_src[3]) << (4 * BITS_PER_BYTE))
               | (((uint64_t)p_src[4]) << (3 * BITS_PER_BYTE))
               | (((uint64_t)p_src[5]) << (2 * BITS_PER_BYTE))
               | (((uint64_t)p_src[6]) << BITS_PER_BYTE)
               | p_src[7];
            break;

        default:
            {
                uint8_t *p_dst = (uint8_t *)pv_dst;
                for(; p_src < p_src_end; ++p_src, ++p_dst)
                    *p_dst = *p_src;
            }
            break;
    }

    return p_src_end;
}

/**
 * Copies src_len bytes from pv_src to p_dst, does not convert to network byte 
 * order.
 * TODO make this use memcpy() internally?
 * @param pv_src    pointer to the source
 * @param src_len   length of the source
 * @param p_dst pointer to the destination
 * @param p_dst_end pointer to just past the end of p_dst
 * @return pointer to just after the written data if success; NULL otherwise
 * @see buf_get()
 */
uint8_t *buf_no_net_put(const void *pv_src, size_t src_len, uint8_t * p_dst,
        const uint8_t *p_dst_end)
{
    uint8_t *p_dst_cpy_end;
    const uint8_t *p_src;

    if(p_dst == NULL || p_dst_end == NULL)
        return NULL;
    
    if(p_dst_end < p_dst || src_len > (p_dst_end - p_dst))
        return NULL;

    p_dst_cpy_end = p_dst + src_len;
    p_src = (const uint8_t *)pv_src;

    for(; p_dst < p_dst_cpy_end; ++p_src, ++p_dst)
        *p_dst = *p_src;

    return p_dst_cpy_end;
}

/**
 * Copies src_len bytes from p_src to pv_dst, does not convert to network byte 
 * order.
 * TODO make this use memcpy() internally?
 * @param pv_dst pointer to the destination
 * @param dst_len   length of the destination
 * @param p_src pointer to the source
 * @param p_src_end pointer to just after the end of p_src
 * @return pointer to just after the read data if success; NULL otherwise
 * @see buf_put()
 */
const uint8_t *buf_no_net_get(void *pv_dst, size_t dst_len,
        const uint8_t * p_src, const uint8_t *p_src_end)
{
    uint8_t *p_dst;

    if(p_src == NULL || p_src_end == NULL)
        return NULL;

    if(p_src_end < p_src || dst_len > (p_src_end - p_src))
        return NULL;

    p_src_end = p_src + dst_len;
    p_dst = (uint8_t *)pv_dst;

    for(; p_src < p_src_end; ++p_src, ++p_dst)
        *p_dst = *p_src;

    return p_src_end;
}

/**
 * Fills src_len bytes in p_dst with 0.
 * TODO make this use bzero() internally?
 * @param src_len   length of the source
 * @param p_dst pointer to the destination
 * @param p_dst_end pointer to just past the end of p_dst
 * @return pointer to just after the written data if success; NULL otherwise
 * @see buf_get()
 */
uint8_t *buf_zero_put(size_t src_len, uint8_t * p_dst, const uint8_t *p_dst_end)
{
    uint8_t *p_dst_cpy_end;

    if(p_dst_end < p_dst || src_len > (p_dst_end - p_dst))
        return NULL;

    p_dst_cpy_end = p_dst + src_len;

    for(; p_dst < p_dst_cpy_end; ++p_dst)
        *p_dst = 0;

    return p_dst_cpy_end;
}


/**
 * Skips over len bytes in p_dest
 * @param len length of the buffer to skip
 * @param p_dst pointer to the destination
 * @param p_dst_end pointer to just past the end of p_dst
 * @return pointer to just after the written data if success; NULL otherwise
 * @see buf_put
 */
uint8_t *buf_skip(size_t len, uint8_t * p_dst, const uint8_t *p_dst_end)
{
    if(p_dst_end < p_dst || len > (p_dst_end - p_dst))
        return NULL;

    return p_dst + len;
}


