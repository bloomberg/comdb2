#ifndef INCLUDED_STACKUTIL_H
#define INCLUDED_STACKUTIL_H

#define MAXFRAMES 50
int stackutil_get_stack_id(void);
int stackutil_get_stack_description(int id, char *frames[MAXFRAMES], int64_t *hits);
int stackutil_get_num_stacks(void);

#endif
