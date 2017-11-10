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

#include <stdlib.h>
#include <stddef.h>
#include <stdio.h>

#include <pool.h>
#include <list.h>
#include <queue.h>

int main(void)
{
    int ii, rc;
    queue_type *q;

    q = queue_new();
    if (!q)
        abort();

    for (ii = 0; ii < 10; ii++) {
        int *z = malloc(sizeof(int));
        *z = ii;
        if ((rc = queue_add(q, z)) != 0) {
            printf("error queue_add, ii %d rc %d\n", ii, rc);
            abort();
        }
        printf("queued up %d\n", ii);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = queue_next(q);
        if (z == 0) {
            printf("error dequeue\n");
            abort();
        }
        printf("dequeued %d\n", *z);
        free(z);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = (int *)malloc(sizeof(int));
        *z = ii;
        if ((rc = queue_insert(q, z)) != 0) {
            printf("error queue_ins, ii %d rc %d\n", ii, rc);
            abort();
        }
        printf("inserted %d\n", ii);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = queue_next(q);
        if (z == 0) {
            printf("error dequeue\n");
            abort();
        }
        printf("dequeued %d\n", *z);
        free(z);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = (int *)malloc(sizeof(int));
        *z = ii;
        if (ii & 1) {
            if ((rc = queue_insert(q, z)) != 0) {
                printf("error queue_ins, ii %d rc %d\n", ii, rc);
                abort();
            }
            printf("inserted %d\n", ii);
        } else {
            if ((rc = queue_add(q, z)) != 0) {
                printf("error queue_add, ii %d rc %d\n", ii, rc);
                abort();
            }
            printf("appended %d\n", ii);
        }
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = queue_next(q);
        if (z == 0) {
            printf("error dequeue\n");
            abort();
        }
        printf("dequeued %d\n", *z);
        free(z);
    }

    for (ii = 0; ii < 10; ii++) {
        int *z = queue_next(q);
        if (z == 0) {
            printf("null dequeue (this is good.)\n");
        } else {
            printf("error, dequeued %d\n", *z);
            abort();
        }
        free(z);
    }

    queue_free(q);
}
