/*
   Copyright 2020 Bloomberg Finance L.P.

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

#include <pb_alloc.h>
#include <mem_protobuf.h>

static void* malloc_wrap(void *allocator_data, size_t n)
{
    return comdb2_malloc_protobuf (n);
}

static void free_wrap(void *allocator_data, void *p)
{
    comdb2_free_protobuf(p);
}

ProtobufCAllocator pb_alloc = {
    .alloc = malloc_wrap,
    .free  = free_wrap
};

ProtobufCAllocator setup_pb_allocator(pb_alloc_func *af, pb_free_func *ff, void *arg)
{
    ProtobufCAllocator pb = { .alloc = af, .free  = ff, .allocator_data = arg };
    return pb;
}
