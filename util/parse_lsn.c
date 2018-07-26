#include <stdlib.h>
#include "parse_lsn.h"

#define skipws(p) { while (*p != '\0' && *p == ' ') p++; }
#define isnum(p) ( *p >= '0' && *p <= '9' )

int char_to_lsn(const char *lsnstr, unsigned int* file, unsigned int* offset)
{
    const char *p = lsnstr;
    while (*p != '\0' && *p == ' ') p++;
    skipws(p);

    /* Parse opening '{' */
    if (*p != '{')
        return -1;
    p++;
    skipws(p);
    if ( !isnum(p) )
        return -1;

    /* Parse file */
    *file = atoi(p);
    while( isnum(p) )
        p++;
    skipws(p);
    if ( *p != ':' )
        return -1;
    p++;
    skipws(p);
    if ( !isnum(p) )
        return -1;

    /* Parse offset */
    *offset = atoi(p);
    while( isnum(p) )
        p++;

    skipws(p);

    /* Parse closing '}' */
    if (*p != '}')
        return -1;
    p++;

    skipws(p);
    if (*p != '\0')
        return -1;

    return 0;
}

