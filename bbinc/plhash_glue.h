#ifndef INCLUDED_PLHASH_GLUE_H
#define INCLUDED_PLHASH_GLUE_H
#include <plhash.h>

/* Following are not available in sysutil */
hash_t *hash_init_ptr(void); /* hash of pointers (addresses) */
hash_t *hash_init_strcase(int keyoff); /* string starts at keyoff (case-insensitive) */
hash_t *hash_init_strcaseptr(int keyoff); /* like above, case insensitive */
#endif /*INCLUDED_PLHASH_GLUE_H*/
