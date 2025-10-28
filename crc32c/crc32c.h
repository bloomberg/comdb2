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

#ifndef INCLUDED_CRC32C_H
#define INCLUDED_CRC32C_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

extern int gbl_crc32c;
#define CRC32C_SEED 0 //The sparse files with all 0s will get a 0 checksum

uint32_t crc32c_software(const uint8_t* data, uint32_t size, uint32_t crc);

#if defined (__x86_64) || defined(_HAS_CRC32_ARMV7) || defined(_HAS_CRC32_ARMV8) 
    void crc32c_init(int v);
    uint32_t crc32c_comdb2(const uint8_t* buf, uint32_t sz);
#else
    #define crc32c_init(...)
    #define crc32c_comdb2(buf, sz) crc32c_software((buf), (sz), CRC32C_SEED)
#endif

#define crc32c(buf, sz) crc32c_comdb2(buf, sz)

#ifdef __cplusplus
}
#endif

#endif
