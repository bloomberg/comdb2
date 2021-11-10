#ifndef INCLUDED_PLHASH_GLUE_H
#define INCLUDED_PLHASH_GLUE_H
#include <plhash.h>
#ifdef COMDB2_BBCMAKE
/* Following are not available in sysutil */
hash_t *hash_init_ptr(void);
hash_t *hash_init_strcase(int);
hash_t *hash_init_strcaseptr(int);
#endif /*COMDB2_BBCMAKE*/
#endif /*INCLUDED_PLHASH_GLUE_H*/
