#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include "dlmalloc.h"

void* alloc(size_t sz) {
    void *p;
    p = malloc(sz);
    printf("alloc %d -> %p\n", sz, p);
    return p;
}

void dealloc(void *p) {
    printf("free %p\n", p);
    free(p);
}

int main(int argc, char *argv[]) {
    mspace m;
    int i; 
    static void *p[2048];
    int nb;

    m = create_mspace_with_base(alloc(1024*1024), 1024*1024, 0);
    mspace_setmorecore(m, alloc);
    mspace_setdestcore(m, dealloc);

    for (i = 0; i < 2048; i++)
        p[i] = mspace_malloc(m, 1024);
    for (i = 0; i < 2048; i++)
        mspace_free(m, p[i]);

    nb = destroy_mspace(m);
    printf("returned nb %d\n", nb);
    return 0;
}
