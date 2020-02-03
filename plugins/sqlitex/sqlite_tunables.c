#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <string.h>
#include <stddef.h>

//#include <dbutil/db.h>
//#include <dbutil.h>

#include "sqliteInt.h"

struct sqlitex_tunables_type sqlitex_gbl_tunables;

void sqlitex_tunables_init(void) {
#define DEF_ATTR(NAME, name, type, dflt) sqlitex_gbl_tunables.name = dflt;
#include "sqlite_tunables.h"
}
#undef DEF_ATTR

void sqlitex_dump_tunables(void) {
#define DEF_ATTR(NAME, name, type, dflt) \
    printf("%-35s", #NAME);              \
    switch (SQLITE_ATTR_##type) {        \
        default:                         \
        case SQLITE_ATTR_QUANTITY:       \
            printf("%d\n", sqlitex_gbl_tunables.name); \
            break;                       \
        case SQLITE_ATTR_BOOLEAN:        \
            printf("%s\n", sqlitex_gbl_tunables.name ? "ON" : "OFF");  \
    };
#include "sqlite_tunables.h"
}
#undef DEF_ATTR

void sqlitex_set_tunable_by_name(char *tname, char *val) {
    int nval;
    char *endp;
    nval = strtol(val, &endp, 10);
    if (0);
#define DEF_ATTR(NAME, name, type, dflt) \
    else if (strcasecmp(tname, #NAME) == 0) {     \
        switch (SQLITE_ATTR_##type) {    \
        default:                         \
        case SQLITE_ATTR_QUANTITY:       \
            sqlitex_gbl_tunables.name = nval;                  \
            break;                       \
        case SQLITE_ATTR_BOOLEAN:        \
            if (strcasecmp(val, "on")==0)\
                sqlitex_gbl_tunables.name = 1;                \
            else if (strcasecmp(val, "off")==0)  \
                sqlitex_gbl_tunables.name = 0;                \
            else {                       \
                printf("Expected ON or OFF\n");  \
            }                            \
            break; \
        }  \
    }
#include "sqlite_tunables.h"
    else {
        printf("Unknown tunable %s\n", tname);
    }
}
#undef DEF_ATTR
