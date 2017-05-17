#ifndef INCLUDED_PB_ALLOC_H
#define INCLUDED_PB_ALLOC_H

#include <google/protobuf-c/protobuf-c.h> /* for ProtobufCAllocator */
#include "mem_protobuf.h" /* for comdb2_malloc_protobuf */

extern comdb2bma blobmem;
extern unsigned gbl_blob_sz_thresh_bytes;

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

#endif
