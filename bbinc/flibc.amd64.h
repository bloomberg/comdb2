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
#include <byteswap.h>

#define flibc_shortflip(x) bswap_16(x)
#define flibc_intflip(x) bswap_32(x)
#define flibc_llflip(x) bswap_64(x)
#define flibc_ntohll(x) flibc_llflip(x)
#define flibc_ntohf(x) flibc_floatflip(x)
#define flibc_ntohd(x) flibc_dblflip(x)
