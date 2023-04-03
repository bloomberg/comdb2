#ifndef INCLUDED_STACKUTIL_H
#define INCLUDED_STACKUTIL_H

#include <dbinc/maxstackframes.h>
#include <inttypes.h>

int stackutil_get_stack_id(const char *type);
int stackutil_get_stack(int id, char **type, int *nframes, void *frames[MAX_STACK_FRAMES], int64_t *hits);
char *stackutil_get_stack_str(int id, char **type, int *nframes, int64_t *hits);
int stackutil_get_num_stacks(void);

#endif
