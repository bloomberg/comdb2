#include <string.h>
#include <sys/types.h> // ssize_t

#include "str_util.h"

int str_has_ending(const char * const str, const char * const ending) {
    const ssize_t s_len = strlen(str);
    const ssize_t e_len = strlen(ending);
    return ((s_len >= e_len) && !strcmp(str+(s_len-e_len), ending));
}
