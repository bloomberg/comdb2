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

#ifndef INCLUDED_COMDB2_RLE
#define INCLUDED_COMDB2_RLE

#include <stdio.h>
#include <inttypes.h>

typedef struct {
    uint8_t *in;
    size_t insz;
    uint8_t *out;
    size_t outsz; // both in, out
} Comdb2RLE;

/* Return 0: Success
**        1: Failure */
int compressComdb2RLE(Comdb2RLE *);
int compressComdb2RLE_hints(Comdb2RLE *, uint16_t *);
int decompressComdb2RLE(Comdb2RLE *);

#endif
