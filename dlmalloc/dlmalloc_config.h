#ifndef DLMALLOC_CONFIG_H
#define DLMALLOC_CONFIG_H

#undef MSPACES
#undef USE_DL_PREFIX
#undef HAVE_MORECORE
#undef HAVE_MMAP

#define MSPACES          1
#define USE_DL_PREFIX    1
#define HAVE_MORECORE    0
#define HAVE_MMAP        0

#endif
