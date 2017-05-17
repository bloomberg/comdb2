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
    uint16_t out;
    __asm__ __volatile__("sthbrx %1,0,%2" : "=m"(out) : "r"(in), "r"(&out));
    return out;
}
static uint32_t flibc_intflip(uint32_t in)
{
    uint32_t out;
    __asm__ __volatile__("stwbrx %1,0,%2" : "=m"(out) : "r"(in), "r"(&out));
    return out;
}
static inline uint64_t flibc_llflip(uint64_t value)
{
    twin_t in, out;
    in.u64 = value;
    __asm__ __volatile__("stwbrx %1,0,%2"
                         : "=m"(out.u32[0])
                         : "r"(in.u32[1]), "r"(&out.u32[0]));
    __asm__ __volatile__("stwbrx %1,0,%2"
                         : "=m"(out.u32[1])
                         : "r"(in.u32[0]), "r"(&out.u32[1]));
    return out.u64;
}
#define flibc_ntohll(x) (x)
#define flibc_ntohf(x) (x)
#define flibc_ntohd(x) (x)
