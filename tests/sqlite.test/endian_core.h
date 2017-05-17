#ifndef _INCLUDED_ENDIAN_H
#define _INCLUDED_ENDIAN_H

#include <sys/types.h>
#include <stdint.h>

/* endian enums */
enum
{
    BITS_PER_BYTE   = 8,
    LSB_MASK        = 0xff,
    BYTES_PER_WORD  = 4
};


/* core endian functions */
uint8_t *buf_put(const void *pv_src, size_t src_len, uint8_t * p_dst,
        const uint8_t *p_dst_end);

const uint8_t *buf_get(void *pv_dst, size_t dst_len, const uint8_t * p_src,
        const uint8_t *p_src_end);

uint8_t *buf_no_net_put(const void *pv_src, size_t src_len, uint8_t * p_dst,
        const uint8_t *p_dst_end);

const uint8_t *buf_no_net_get(void *pv_dst, size_t dst_len,
        const uint8_t * p_src, const uint8_t *p_src_end);

uint8_t *buf_zero_put(size_t src_len, uint8_t * p_dst, const uint8_t 
        *p_dst_end);

uint8_t *buf_skip(size_t len, uint8_t * p_dst, const uint8_t *p_dst_end);

#endif
