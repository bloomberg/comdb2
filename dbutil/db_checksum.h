/*
   Copyright 2021, Bloomberg Finance L.P.

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

#ifndef INCLUDED_CHKSUM_H
#define INCLUDED_CHKSUM_H
#ifdef __cplusplus
extern "C" {
#endif
#include <stdint.h>
uint32_t crc32c(const uint8_t* buf, uint32_t sz);
uint32_t comdb2_ham_func4(const uint8_t *key, uint32_t len);
#ifdef __cplusplus
}
#endif
#endif
