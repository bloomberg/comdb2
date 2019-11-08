#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

#include <unistd.h>

int roll_file(const char *file, int nkeep)
{
    char *from = NULL, *to = NULL;

    from = malloc(strlen(file) + 1 + 20);
    to = malloc(strlen(file) + 1 + 20);

    sprintf(from, "%s.%d", file, nkeep);
    unlink(from);
    for (int i = nkeep - 1; i >= 1; i--) {
        sprintf(from, "%s.%d", file, i);
        sprintf(to, "%s.%d", file, i + 1);
        rename(from, to);
    }
    sprintf(to, "%s.1", file);
    rename(file, to);
    free(from);
    free(to);

    return 0;
}
