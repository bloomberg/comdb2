#include <string.h>
#include <sys/types.h> // ssize_t
#include <ctype.h> // isalnum

#include "str_util.h"

int str_has_ending(const char * const str, const char * const ending) {
    const ssize_t s_len = strlen(str);
    const ssize_t e_len = strlen(ending);
    return ((s_len >= e_len) && !strcmp(str+(s_len-e_len), ending));
}

// Checks that a string is alphanumeric. Allowed non-alphanumeric characters can be passed in `exceptions`.
// Pass emptystring to `exceptions` to pass no exceptions.
int str_is_alphanumeric(const char * const name, const char * const exceptions)
{
    char c;
    for (int i=0; (c = name[i]), c != '\0'; ++i) {
        if (!isalnum(c) && !strchr(exceptions, c)) {
            return 0;
        }
    }
    return 1;
}
