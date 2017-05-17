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

#ifndef INCLUDED_SLTPCK_H
#define INCLUDED_SLTPCK_H

#include <stdio.h>

/**
 * @brief   Small package to pack slots and flags.
 * Scheme is has follow:
 * SLTPCK_FORWARD:
 * _______________________________________________________________________
 * | bufStart | flags | hdr0 | slot0 | hdr1 | slot1 | ... | hdrN | slotN |
 * -----------------------------------------------------------------------
 *
 * SLTPCK_BACKWARD:
 * _______________________________________________________________________
 * | bufStart | slot0 | hdr0 | slot1 | hdr1 | ... | slotN | hdrN | flags |
 * -----------------------------------------------------------------------
 *  flags will be used by receiver to determine what information is
 *  packed inside buffer. By going backwards, receiver can read a
 *  header information to get a pointer to corresponding slot.
 *
 *  Order:
 *  cursor = slt_init( ... );
 *  err = slt_pck( cursor, ... );
 *  payload = slt_stamp( cursor );  // data is packed, cursor is freed
 */

/**
 * @brief  Enum to indicate packing direction.
 */
typedef enum { SLTPCK_FORWARD, SLTPCK_BACKWARD } slt_dir_t;

/**
 * @brief  Cursor to pack data.
 */
struct slt_cur_t;

/**
 * @brief  Error codes return when packing data.
 */
typedef enum {
    SLT_ERR_OK,
    SLT_ERR_PARAM, /**< Invalid parameter */
    SLT_ERR_FLG,   /**< Invalid flag 0? already packed? */
    SLT_ERR_SZ     /**< Not enough room to pack data */
} slt_err_t;

/**
 * @brief  Returns a valid cursor on given buffer to pack slots and
 * flags.
 *
 * @param[in]       buf     Buffer to pack in.
 * @param[in]       bufsz   Size of buffer to pack in.
 * @param[in]       dir     Direction packing should be done.
 *
 * @return A valid cursor if parameters are correct and it is
 * possible to pack data in buffer.
 */
struct slt_cur_t *slt_init(void *buf, size_t bufsz, slt_dir_t dir);

/**
 * @brief  Packs given data using a slot cursor. Data will be marked
 * by FLAG. This function will ensure there is enough space inside
 * buffer room to pack in data. Given flag will be marked to
 * indicate data has been packed.
 * @note Data must be packed in order of flags.
 *
 * @param[in/out]   cur     Cursor to pack data in.
 * @param[in]       FLAG    Flag to mark data.
 * @param[in]       dta     Data to be packed.
 * @param[in]       dtasz   Size of data to be packed.
 *
 * @return @see slt_err_t
 */
int slt_pck(struct slt_cur_t *cur, unsigned int FLAG, void *dta, size_t dtasz);

/**
 * @brief  Last function to call to finalize packing. This will pack
 * flags for receiver. It will also free cursor so that caller does
 * not have to take care of it. It returns total size of packed
 * data.
 *
 * @param[in/out]   cur     Cursor to pack data in.
 *
 * @return Size of packed data and an invalidated cursor (aka DO NOT USE)
 */
size_t slt_stamp(struct slt_cur_t *cur);

/**
 * @brief  Function to unpack data corresponding to given flag from
 * buffer.
 *
 * @param[in]       FLAG    Flag to identify data.
 * @param[in]       buf     Buffer containing data and flags.
 * @param[in]       bufsz   Size of buffer.
 * @param[out]      sltsz   Returned size of packed data requested
 * @param[in]       dir     Packing direction.
 *
 * @return  Pointer INSIDE given buffer to requested data. NULL if
 * flag not present.
 */
void *slt_unpck(unsigned int FLAG, void *buf, int bufsz, size_t *sltsz,
                slt_dir_t dir);

/**
 * @brief  Filters out given flag in buffer. This allows
 * intercepting a flagged data. This is not meant to delete packed
 * data just avoid looking at it.
 *
 * @param[in]       FLAG    Flag to identify data.
 * @param[in]       buf     Buffer containing data and flags.
 * @param[in]       bufsz   Size of buffer.
 * @param[in]       dir     Packing direction.
 */
void slt_filt(unsigned int FLAG, void *buf, int bufsz, slt_dir_t dir);

#endif
