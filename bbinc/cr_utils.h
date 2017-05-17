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

#ifndef INCLUDED_CR_UTILS_H
#define INCLUDED_CR_UTILS_H

/**
 * Calc Route Client Utilities
 *
 * Author: Shabbir Dahodwala
 **/

#ifndef _CRC_UTILS_H_
#define _CRC_UTILS_H_

struct crc_Iterator;
typedef struct crc_Iterator CRCIterator;

CRCIterator *crc_iterator_get(void);
void crc_iterator_free(CRCIterator *iterator);
void crc_iterator_set(CRCIterator *iterator, const void *items, int nitems);

/**
 * crc_iterator_first: Get the first element from the iterator
 *                     Returns NULL if none available
 **/
const void *crc_iterator_first(CRCIterator *iterator);

/**
 * crc_iterator_next: Get the next element from the iterator
 *                     Returns NULL if none available
 **/
const void *crc_iterator_next(CRCIterator *iterator);

/**
 * crc_iterator_hasNext: does the iterator have another element?
 * Returns:
 *                     1 - if available
 *                     0 - if not.
 **/
int crc_iterator_hasNext(CRCIterator *iterator);

/**
 * crc_iterator_current: Get the current element from the iterator
 *                     Returns NULL if none available
 **/
const void *crc_iterator_current(CRCIterator *iterator);

#endif /* _CRC_UTILS_H_ */

#endif
