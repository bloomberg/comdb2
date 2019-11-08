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

#include "util.h"

#include <stdint.h>
#include <stdlib.h>
#include <netinet/in.h>
#include <string.h>
#include <stdio.h>

char *inet_ntoa_r(in_addr_t addr, char out[16])
{
    uint8_t b[4];
    memcpy(b, &addr, sizeof(in_addr_t));
    snprintf(out, 16, "%d.%d.%d.%d", b[0], b[1], b[2], b[3]);
    return out;
}
