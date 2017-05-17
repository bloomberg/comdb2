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

static inline uint16_t flibc_shortflip(uint16_t in)
{
    shortswp_t in_val, flip_val;
    in_val.u16 = in;
    flip_val.u8[0] = in_val.u8[1];
    flip_val.u8[1] = in_val.u8[0];
    return flip_val.u16;
}
static inline uint32_t flibc_intflip(uint32_t in)
{
    intswp_t in_val, flip_val;
    in_val.u32 = in;
    flip_val.u8[0] = in_val.u8[3];
    flip_val.u8[1] = in_val.u8[2];
    flip_val.u8[2] = in_val.u8[1];
    flip_val.u8[3] = in_val.u8[0];
    return flip_val.u32;
}
static inline uint64_t flibc_llflip(uint64_t in)
{
    twin_t in_val, flip_val;
    in_val.u64 = in;
    flip_val.u8[0] = in_val.u8[7];
    flip_val.u8[1] = in_val.u8[6];
    flip_val.u8[2] = in_val.u8[5];
    flip_val.u8[3] = in_val.u8[4];
    flip_val.u8[4] = in_val.u8[3];
    flip_val.u8[5] = in_val.u8[2];
    flip_val.u8[6] = in_val.u8[1];
    flip_val.u8[7] = in_val.u8[0];
    return flip_val.u64;
}
#define flibc_ntohll(x) (x)
#define flibc_ntohd(x) (x)
#define flibc_ntohf(x) (x)
