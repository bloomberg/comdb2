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

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <sys/types.h>
#include <string.h>

#include <pool.h>

int main()
{
    int ii, zz;
    pool_t *p;
    int *pp[500000];
    p = pool_init(16, 5000);
    for (zz = 1; zz <= 5; zz++) {
        printf("%-10d GETTING...\n", zz);
        for (ii = 0; ii < 100000 * zz; ii++) {
            pp[ii] = (int *)pool_getablk(p);
            if (pp[ii] == 0) {
                printf("FAILED TO GET A BLOCK!");
                exit(-1);
            }
            pp[ii][0] = ii;
            pp[ii][1] = -ii;
            pp[ii][2] = ii << 1;
            if ((ii & 0xffff) == 0)
                printf("GOT %d\n", ii);
        }
        printf("%-10d FREEING...\n", zz);
        for (ii = 0; ii < 100000 * zz; ii++) {
            if (pp[ii][0] != ii || pp[ii][1] != -ii || pp[ii][2] != (ii << 1)) {
                printf("ERR: MISMATCH %d: %d %d %d\n", ii, pp[ii][0], pp[ii][1],
                       pp[ii][2]);
                break;
            }
            pool_relablk(p, pp[ii]);
            if ((ii & 0xffff) == 0)
                printf("FREED %d\n", ii);
        }
    }
    printf("DONE.\n");
    pool_dump(p, "TEST");
    printf("CLEAR.\n");
    pool_clear(p);
    pool_dump(p, "CLEARED");
}
