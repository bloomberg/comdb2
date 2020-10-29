#ifndef INCLUDED_PB_ALLOC_H
#define INCLUDED_PB_ALLOC_H

#include <google/protobuf-c/protobuf-c.h> /* for ProtobufCAllocator */
#include "mem_protobuf.h" /* for comdb2_malloc_protobuf */

extern comdb2bma blobmem;
extern unsigned gbl_blob_sz_thresh_bytes;
extern int gbl_protobuf_prealloc_buffer_size;

static inline void* malloc_wrap(void *allocator_data, size_t n)
{
    return comdb2_malloc_protobuf (n);
}

static inline void free_wrap(void *allocator_data, void *p)
{
    comdb2_free_protobuf(p);
}

static ProtobufCAllocator pb_alloc = {
    .alloc = malloc_wrap,
    .free  = free_wrap
};


struct NewsqlProtobufCAllocator {
    ProtobufCAllocator protobuf_allocator;
    void *protobuf_data;
    void *(*malloc_func)(size_t size);
    void (*free_func)(void *ptr);
    int protobuf_offset;
    int protobuf_size;
    int alloced_outside_buffer;
};

static inline void newsql_protobuf_reset_offset(struct NewsqlProtobufCAllocator *npa)
{
    npa->protobuf_offset = 0;
}

static inline void *newsql_protobuf_alloc(void *allocator_data, size_t size)
{
    struct NewsqlProtobufCAllocator *npa = allocator_data;
    void *p = NULL;
    if (size <= npa->protobuf_size - npa->protobuf_offset) {
        p = npa->protobuf_data + npa->protobuf_offset;
        npa->protobuf_offset += size;
    } else {
        p = npa->malloc_func(size);
        npa->alloced_outside_buffer++;
    }
    return p;
}

static inline void newsql_protobuf_free(void *allocator_data, void *p)
{
    struct NewsqlProtobufCAllocator *npa = allocator_data;
    if (p < npa->protobuf_data || p > (npa->protobuf_data + npa->protobuf_size)) {
        npa->free_func(p);
        npa->alloced_outside_buffer--;
    }
}

static inline void newsql_protobuf_init(struct NewsqlProtobufCAllocator *npa, void *(*malloc_func)(size_t size),
                                        void (*free_func)(void *ptr))
{
    npa->protobuf_size = gbl_protobuf_prealloc_buffer_size;
    npa->protobuf_offset = 0;
    npa->protobuf_data = malloc_func(npa->protobuf_size);
    npa->malloc_func = malloc_func;
    npa->free_func = free_func;
    npa->protobuf_allocator.alloc = &newsql_protobuf_alloc;
    npa->protobuf_allocator.free = &newsql_protobuf_free;
    npa->protobuf_allocator.allocator_data = npa;
    npa->alloced_outside_buffer = 0;
}

static inline void newsql_protobuf_set_allocator_data(struct NewsqlProtobufCAllocator *npa)
{
    npa->protobuf_allocator.allocator_data = npa;
}

static inline void newsql_protobuf_destroy(struct NewsqlProtobufCAllocator *npa)
{
    assert(npa->alloced_outside_buffer == 0);
    npa->free_func(npa->protobuf_data);
}

#endif
